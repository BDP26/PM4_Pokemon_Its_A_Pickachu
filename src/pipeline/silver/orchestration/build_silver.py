from __future__ import annotations

from collections import Counter, defaultdict
import gc
import json
import logging
import re
import shutil
import time
from pathlib import Path
from typing import Any, Callable, cast

from src.pipeline.common.type_chart import build_type_chart, save_as_json
from src.pipeline.common.cast import to_bool, to_list
from src.pipeline.common.http import build_session
from src.pipeline.common.io import read_json, read_jsonl, read_parquet, write_json, write_parquet, _remove_path_if_exists
from src.pipeline.common.simulation_config import load_runtime_battle_policy_config
from src.pipeline.settings import BRONZE_DIR, SILVER_DIR, ensure_medallion_dirs, get_silver_subdirs
from src.pipeline.silver.config.boss_config import BOSS_ALIASES, STRIATON_CONDITIONAL_BOSSES, boss_id
from src.pipeline.silver.config.game_config import get_games_config, get_starter_choices, get_starter_family_members
from src.pipeline.silver.config.team_config import (
    ALLOW_ITEM_EVOLUTIONS,
    ITEM_EVOLUTION_DEFAULT_LEVEL,
    resolve_runtime_team_config,
)
from src.pipeline.silver.enrichment.location_pokemon_enrichment import (
    enrich_records_with_location_pokemon,
    get_location_area_and_pokemon_maps,
)
from src.pipeline.silver.enrichment.schema_normalizer import (
    create_encounter_methods_reference,
    create_pokemon_reference_index,
    write_normalized_silver,
)
from src.pipeline.silver.inputs.location_mapper import LocationMapper
from src.pipeline.silver.inputs.connectors.pokeapi_evolution import get_species_evolution_rules
from src.pipeline.silver.inputs.reference_context import load_reference_context, normalize_move_name, normalize_species_slug
from src.pipeline.silver.move_power import resolve_effective_power
from src.pipeline.silver.transforms.keys import make_pokemon_instance_id, normalize_key_part, stable_digest
from src.pipeline.silver.writers.outputs import (
    build_input_signature,
    fingerprint_path,
    fingerprint_python_files,
    load_state,
    save_state,
)
from src.pipeline.silver.inputs.builders.pokemon_profiles import (
    INVALID_NORMALIZED_POKEMON_TOKENS,
    POKEMON_COMBAT_REQUIRED_COLUMNS,
    POKEMON_RESOLUTION_ALIAS_FALLBACKS,
    STAT_NAME_TO_COLUMN,
    _build_enriched_pokemon_profiles,
    _extract_pokeapi_id_from_source_url,
    _has_complete_pokemon_profile_payload,
    _normalize_pokebase_payload,
    _normalize_requested_pokemon_name,
    _profile_from_pokemon_payload,
    _resolve_requested_pokemon_profile,
    _species_classification_from_payload,
    _species_resolution_candidates,
    _validate_and_persist_pokemon_data_contract,
    pokebase_get_data,
)
from src.pipeline.silver.transforms.encounter_transforms import (
    _canonicalize_boss_teams_to_references,
    _canonicalize_encounter_boss_ids,
    _coerce_alias_values,
    _collect_kaggle_boss_species_and_moves,
    _expand_striaton_encounters,
    _filter_bosses_with_encounter_pools,
    _normalize_boss_teams_with_conditional_striaton,
    _validation_profile,
)
from src.pipeline.silver.inputs.builders.bootstrap_moves import (
    _all_starter_family_bootstrap_entries,
    _build_boss_species_bootstrap_entries,
    _build_evolution_rules_by_game_from_encounters,
    _build_kaggle_bootstrap_entries,
    _build_progression_bootstrap_entries,
    _collect_missing_bootstrap_move_hints,
    _collect_missing_learnable_species_pairs,
    _collect_starter_chain_species_by_game,
    _dedupe_bootstrap_entries,
    _diagnose_player_missing_damaging_moves,
    _ensure_moves_in_combat_profiles,
    _evolution_rules_map_from_rows,
    _evolution_rules_rows_from_map,
    _expand_bootstrap_entries_with_evolution_lines,
    _move_profiles_from_reference,
    _species_by_game_from_bootstrap_entries,
    _starter_chain_bootstrap_entries,
    _validate_kaggle_moves_in_move_reference,
    _validate_starter_chain_move_coverage,
    _validate_universal_starter_family_move_coverage,
    _build_bootstrap_move_entries,
)

import pandas as pd

logger = logging.getLogger(__name__)


def summarize_unmapped_locations(misses: list[dict]) -> dict:
    by_reason = Counter()
    by_tried_slug = Counter()
    by_raw_title = Counter()
    examples_by_reason = defaultdict(list)

    for miss in misses:
        reason = miss.get("reason", "unknown")
        raw_title = miss.get("raw_title", "")
        tried_slug = miss.get("tried_slug") or ""
        by_reason[reason] += 1
        if tried_slug:
            by_tried_slug[tried_slug] += 1
        if raw_title:
            by_raw_title[raw_title] += 1
        if len(examples_by_reason[reason]) < 10:
            examples_by_reason[reason].append(miss)

    return {
        "total_unmapped_events": len(misses),
        "by_reason": dict(by_reason.most_common()),
        "top_tried_slugs": dict(by_tried_slug.most_common(50)),
        "top_raw_titles": dict(by_raw_title.most_common(50)),
        "examples_by_reason": dict(examples_by_reason),
    }



def _validate_bronze_inputs_or_raise(*, bronze_dir: Path, location_index_path: Path, bulbapedia_dir: Path, kaggle_csv_path: Path) -> None:
    if not location_index_path.exists():
        raise FileNotFoundError("Bronze input missing: location_index.json. Run bronze layer first.")
    if not bulbapedia_dir.exists():
        raise FileNotFoundError("Bronze input missing: bulbapedia directory. Run bronze layer first.")

    location_index_payload = read_json(location_index_path)
    location_results = location_index_payload.get("results") if isinstance(location_index_payload, dict) else None
    if not isinstance(location_results, list) or not location_results:
        raise ValueError("Bronze location_index.json is empty or invalid; expected non-empty 'results'.")

    location_snapshot_path = bronze_dir / "pokeapi" / "location_pokemon_snapshot.json"
    if not location_snapshot_path.exists():
        raise FileNotFoundError("Bronze input missing: location_pokemon_snapshot.json. Run bronze layer first.")
    location_snapshot_payload = read_json(location_snapshot_path)
    location_map = location_snapshot_payload.get("location_pokemon_map") if isinstance(location_snapshot_payload, dict) else None
    if not isinstance(location_map, dict) or not location_map:
        raise ValueError(
            "Bronze location snapshot is empty: location_pokemon_map has no entries. "
            "Re-run bronze layer and check PokeAPI diagnostics."
        )

    game_files = sorted(bulbapedia_dir.glob("*.json"))
    if not game_files:
        raise ValueError("Bronze bulbapedia payloads are missing; expected *.json files under data/bronze/bulbapedia.")

    if not kaggle_csv_path.exists():
        raise FileNotFoundError("Bronze Kaggle export missing: data/bronze/kagglehub/gym_leaders_elite_four.csv")


def _game_output_paths(simulation_dir: Path, game_key: str) -> dict[str, Path]:
    return {
        "source_teams": simulation_dir / f"source_teams_{game_key}.parquet",
        "source_team_members": simulation_dir / f"source_team_members_{game_key}.parquet",
        "member_moveset_combos": simulation_dir / f"member_moveset_combos_{game_key}.parquet",
        "member_move_options": simulation_dir / f"member_move_options_{game_key}.parquet",
        "pokemon_moveset_options": simulation_dir / f"pokemon_moveset_options_{game_key}.parquet",
        "simulation_sampling_plan": simulation_dir / f"simulation_sampling_plan_{game_key}.parquet",
        "combat_pool": simulation_dir / f"pokemon_combat_pool_{game_key}.parquet",
    }




def _validate_progression_source_team_boss_targets(
    progression_source_teams: list[dict[str, Any]],
    bosses_reference_df: pd.DataFrame,
) -> None:
    if not progression_source_teams or bosses_reference_df.empty:
        return

    valid_boss_ids = {
        str(row.get("boss_id") or "").strip().lower()
        for row in bosses_reference_df.to_dict(orient="records")
        if str(row.get("boss_id") or "").strip()
    }
    invalid_rows = [
        row
        for row in progression_source_teams
        if str(row.get("boss_id") or "").strip()
        and str(row.get("boss_id") or "").strip().lower() not in valid_boss_ids
    ]
    if not invalid_rows:
        return

    sample = ", ".join(
        f"{str(row.get('game_version') or '').strip().lower()}:{str(row.get('boss_id') or '').strip().lower()}"
        for row in invalid_rows[:10]
    )
    raise ValueError(
        "Player progression source teams reference bosses missing from bosses.parquet: "
        f"invalid_rows={len(invalid_rows)} first_10=[{sample}]"
    )


def _validate_boss_team_targets(
    boss_teams: list[dict[str, Any]],
    bosses_reference_df: pd.DataFrame,
) -> None:
    if not boss_teams or bosses_reference_df.empty:
        return

    valid_boss_ids = {
        str(row.get("boss_id") or "").strip().lower()
        for row in bosses_reference_df.to_dict(orient="records")
        if str(row.get("boss_id") or "").strip()
    }
    invalid_rows = [
        row
        for row in boss_teams
        if str(row.get("boss_id") or "").strip()
        and str(row.get("boss_id") or "").strip().lower() not in valid_boss_ids
    ]
    if not invalid_rows:
        return

    sample = ", ".join(
        f"{str(row.get('game_version') or '').strip().lower()}:{str(row.get('boss_id') or '').strip().lower()}"
        for row in invalid_rows[:10]
    )
    raise ValueError(
        "Canonicalized boss teams reference bosses missing from bosses.parquet: "
        f"invalid_rows={len(invalid_rows)} first_10=[{sample}]"
    )


def _build_boss_compact_tables(
    boss_teams: list[dict[str, Any]],
    move_data: dict[str, Any],
    *,
    progression_depth_context: Any | None = None,
) -> dict[str, list[dict[str, Any]]]:
    del move_data

    source_teams: list[dict[str, Any]] = []
    source_team_members: list[dict[str, Any]] = []
    pokemon_moveset_options: list[dict[str, Any]] = []
    seen_contexts: set[tuple[str, str, int]] = set()
    skipped_without_progression = 0

    for team in boss_teams:
        if to_bool(team.get("is_optional"), default=False) or to_bool(team.get("is_postgame"), default=False):
            continue
        source_team_id = str(team.get("team_id") or "").strip()
        if not source_team_id:
            continue
        game_version = str(team.get("game_version") or "").strip().lower()
        boss_name = str(team.get("boss_name") or team.get("gym") or "").strip().lower()
        progression = None
        if progression_depth_context is not None:
            try:
                progression = progression_depth_context.require(game_version=game_version, boss_id="", boss_name=boss_name)
            except ValueError:
                progression = None
            if progression is None:
                skipped_without_progression += 1
                logger.info(
                    "[silver/teams] skipping boss source team without progression mapping game=%s boss_name=%s team_id=%s",
                    game_version,
                    boss_name,
                    source_team_id,
                )
                continue

        source_teams.append(
            {
                "source_team_id": source_team_id,
                "game_version": game_version,
                "team_role": "boss",
                "origin": "kaggle",
                "boss_id": str(team.get("boss_id") or source_team_id).strip().lower(),
                "boss_name": boss_name,
                "battle_type": str(team.get("battle_type") or "single").strip().lower() or "single",
                "gym_index": team.get("gym_index"),
                "starter_condition": team.get("starter_condition"),
                "starter_type": team.get("starter_type"),
                "team_variant": team.get("team_variant"),
                "variant_dimension": team.get("variant_dimension"),
                "starter_base": None,
                "starter_evolved_species": None,
                "progression_source_team_id": None,
                "progression_pool_id": None,
                "avg_level": int(team.get("avg_level") or 0),
                "member_count": len(team.get("pokemon", [])) if isinstance(team.get("pokemon"), list) else 0,
                "is_player_candidate": False,
                "boss_index": progression.boss_index if progression is not None else None,
                "max_boss_index": progression.max_boss_index if progression is not None else None,
                "available_species_count": progression.available_species_count if progression is not None else None,
                "max_species_count": progression.max_species_count if progression is not None else None,
                "progression_depth": progression.progression_depth if progression is not None else None,
            }
        )

        members = list(team.get("pokemon", [])) if isinstance(team.get("pokemon"), list) else []
        levels = list(team.get("levels", [])) if isinstance(team.get("levels"), list) else []
        member_ids = list(team.get("pokemon_instance_ids", [])) if isinstance(team.get("pokemon_instance_ids"), list) else []

        for slot, species_raw in enumerate(members, start=1):
            species = str(species_raw or "").strip().lower()
            if not species:
                continue
            level = int(levels[slot - 1] if slot - 1 < len(levels) else team.get("avg_level") or 1)
            member_id = str(member_ids[slot - 1]).strip() if slot - 1 < len(member_ids) else f"{source_team_id}:m{slot}"
            fixed_moves = [
                normalize_move_name(move_name)
                for move_name in (team.get("moves", [])[slot - 1] if slot - 1 < len(team.get("moves", [])) else [])
                if normalize_move_name(move_name)
            ]
            source_team_members.append(
                {
                    "team_member_id": member_id,
                    "source_team_id": source_team_id,
                    "game_version": game_version,
                    "team_role": "boss",
                    "origin": "kaggle",
                    "boss_id": str(team.get("boss_id") or source_team_id).strip().lower(),
                    "boss_name": boss_name,
                    "gym_index": team.get("gym_index"),
                    "starter_condition": team.get("starter_condition"),
                    "slot": slot,
                    "pokemon_species": species,
                    "level": level,
                    "fixed_moves": fixed_moves,
                    "progression_pool_id": None,
                    "is_starter": False,
                }
            )

            context_key = (game_version, species, level)
            context_id = f"ctx:{stable_digest(*context_key, length=20)}"
            if context_key not in seen_contexts:
                seen_contexts.add(context_key)
                pokemon_moveset_options.append(
                    {
                        "moveset_context_id": context_id,
                        "game_version": game_version,
                        "pokemon_species": species,
                        "level": level,
                        "move_policy": "boss-fixed-kaggle-v1",
                        "candidate_move_count": len(fixed_moves),
                    }
                )

    if skipped_without_progression > 0:
        logger.info(
            "[silver/teams] skipped boss source teams without progression mapping count=%s",
            skipped_without_progression,
        )

    return {
        "source_teams": source_teams,
        "source_team_members": source_team_members,
        "member_move_options": [],
        "pokemon_moveset_options": pokemon_moveset_options,
    }


def _validate_boss_reference_coverage(
    *,
    boss_team_members_df: pd.DataFrame,
    boss_teams: list[dict[str, Any]],
    pokemon_data_df: pd.DataFrame,
    move_reference_df: pd.DataFrame,
    learnable_moves_df: pd.DataFrame,
    diagnostics_dir: Path,
) -> None:
    pokemon_species = {
        normalize_species_slug(row.get("pokemon_species") or row.get("name") or "")
        for row in pokemon_data_df.to_dict(orient="records")
        if normalize_species_slug(row.get("pokemon_species") or row.get("name") or "")
    }
    moves_in_reference = {
        normalize_move_name(row.get("move_name") or "")
        for row in move_reference_df.to_dict(orient="records")
        if normalize_move_name(row.get("move_name") or "")
    }
    move_profiles = {
        normalize_move_name(row.get("move_name") or ""): row
        for row in move_reference_df.to_dict(orient="records")
        if normalize_move_name(row.get("move_name") or "")
    }
    learnable_pairs = {
        (
            str(row.get("game_version") or "").strip().lower(),
            normalize_species_slug(row.get("pokemon_species") or ""),
            normalize_move_name(row.get("move_name") or ""),
        )
        for row in learnable_moves_df.to_dict(orient="records")
        if str(row.get("game_version") or "").strip()
        and normalize_species_slug(row.get("pokemon_species") or "")
        and normalize_move_name(row.get("move_name") or "")
    }

    coverage_rows: list[dict[str, Any]] = []
    errors: list[str] = []
    if boss_team_members_df.empty:
        errors.append("boss_team_members reference is empty")

    team_member_counts: dict[str, int] = {}
    for team in boss_teams:
        boss_id = str(team.get("team_id") or "").strip().lower()
        members = list(team.get("pokemon", [])) if isinstance(team.get("pokemon"), list) else []
        if boss_id:
            team_member_counts[boss_id] = len([species for species in members if normalize_species_slug(species)])

    grouped_move_counts: dict[tuple[str, int, str], int] = defaultdict(int)
    boss_ids_with_member_rows: set[str] = set()
    for row in boss_team_members_df.to_dict(orient="records"):
        boss_id = str(row.get("boss_id") or "").strip().lower()
        slot = int(row.get("slot") or 0)
        species = normalize_species_slug(row.get("pokemon_species") or "")
        move_name = normalize_move_name(row.get("move_name") or "")
        game_version = str(row.get("game_version") or "").strip().lower()
        level = int(row.get("level") or 0)
        if boss_id:
            boss_ids_with_member_rows.add(boss_id)
        if slot <= 0 or not species:
            continue

        pokemon_present = species in pokemon_species
        move_present = (move_name in moves_in_reference) if move_name else False
        learnable_present = (game_version, species, move_name) in learnable_pairs if move_name else False

        severity = "OK"
        reason = "complete"
        if not pokemon_present:
            severity = "ERROR"
            reason = "missing_pokemon_data"
        elif not move_present:
            severity = "ERROR"
            reason = "missing_move_reference"
        elif move_name:
            profile = move_profiles.get(move_name, {})
            damage_class = str(profile.get("damage_class") or "").strip().lower()
            move_type = str(profile.get("type") or "").strip().lower()
            if not damage_class or not move_type:
                severity = "ERROR"
                reason = "missing_move_metadata"

        coverage_rows.append(
            {
                "game_version": game_version,
                "boss_name": str(row.get("boss_name") or "").strip().lower(),
                "pokemon_species": species,
                "level": level if level > 0 else None,
                "move_name": move_name or None,
                "pokemon_in_pokemon_data": pokemon_present,
                "move_in_move_reference": move_present,
                "learnable_pair_present": learnable_present,
                "severity": severity,
                "reason": reason,
            }
        )

        if move_name:
            grouped_move_counts[(boss_id, slot, species)] += 1

    report_path = diagnostics_dir / "boss_silver_reference_coverage.csv"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(coverage_rows).to_csv(report_path, index=False)

    missing_boss_member_rows = sorted(boss_id for boss_id in team_member_counts if boss_id not in boss_ids_with_member_rows)
    if missing_boss_member_rows:
        errors.append(f"bosses_missing_reference_rows={len(missing_boss_member_rows)} sample={missing_boss_member_rows[:10]}")

    invalid_move_count_members = sorted(key for key, count in grouped_move_counts.items() if count < 1 or count > 4)
    if invalid_move_count_members:
        errors.append(f"boss_members_with_invalid_move_count={len(invalid_move_count_members)} sample={invalid_move_count_members[:10]}")

    error_rows = [row for row in coverage_rows if row["severity"] == "ERROR"]
    if error_rows:
        sample = ", ".join(
            f"{row['game_version']}:{row['boss_name']}:{row['pokemon_species']}:{row['move_name']}:{row['reason']}"
            for row in error_rows[:20]
        )
        errors.append(f"coverage_errors={len(error_rows)} first_20=[{sample}]")

    if errors:
        raise ValueError(
            "Silver boss reference coverage validation failed: "
            + " | ".join(errors)
            + f" | diagnostics={report_path}"
        )


def build_silver_from_bronze(
    bronze_dir: Path = BRONZE_DIR,
    silver_dir: Path = SILVER_DIR,
    hard_cleanup: bool = False,
) -> None:
    started_at = time.perf_counter()
    stage_durations: dict[str, float] = {}

    def _stage_checkpoint(stage: str, **details: Any) -> None:
        elapsed = time.perf_counter() - started_at
        payload = " ".join(f"{key}={value}" for key, value in details.items())
        logger.info("[silver/progress] stage=%s elapsed_s=%.2f %s", stage, elapsed, payload)

    _stage_checkpoint("init", hard_cleanup=int(bool(hard_cleanup)))
    from src.pipeline.silver.inputs.builders.team_compaction import (
        build_player_team_compact_tables,
    )
    from src.pipeline.silver.inputs.builders.team_selection import (
        build_progression_source_teams_from_encounters,
    )
    from src.pipeline.silver.inputs.connectors.pokeapi_moves import (
        bootstrap_move_reference_cache,
        persist_move_reference_cache,
    )
    from src.pipeline.silver.inputs.kaggle_boss_mapping import load_kaggle_rows_by_game
    from src.pipeline.silver.inputs.sources.boss_teams import load_kaggle_boss_rows_by_game
    from src.pipeline.silver.orchestration.stages import run_parse_stage
    from src.pipeline.silver.orchestration.references_stage import cleanup_simulation_shards
    from src.pipeline.silver.orchestration.teams_stage import group_teams_by_game
    from src.pipeline.silver.transforms.normalized_tables import build_games_table, build_locations_table
    from src.pipeline.silver.transforms.progression_depth import (
        build_boss_level_table_with_mapping,
        build_progression_depth_context,
        build_progression_depth_table,
    )

    ensure_medallion_dirs()

    type_chart_path = bronze_dir / "type_chart.json"
    if not type_chart_path.exists():
        save_as_json(build_type_chart(), type_chart_path)

    silver_dir.mkdir(parents=True, exist_ok=True)
    silver_subdirs = get_silver_subdirs(silver_dir)
    for directory in silver_subdirs.values():
        directory.mkdir(parents=True, exist_ok=True)

    snapshots_dir = silver_subdirs["snapshots"]
    mappings_dir = silver_subdirs["mappings"]
    references_dir = silver_subdirs["references"]
    diagnostics_dir = silver_subdirs["diagnostics"]
    simulation_dir = silver_subdirs["simulation"]

    if hard_cleanup:
        for cleanup_path in (snapshots_dir, mappings_dir, references_dir, diagnostics_dir, simulation_dir):
            _remove_path_if_exists(cleanup_path)
            cleanup_path.mkdir(parents=True, exist_ok=True)
        _stage_checkpoint("hard_cleanup_done")

    location_index_path = bronze_dir / "pokeapi" / "location_index.json"
    bulbapedia_dir = bronze_dir / "bulbapedia"
    kaggle_csv_path = bronze_dir / "kagglehub" / "gym_leaders_elite_four.csv"
    _validate_bronze_inputs_or_raise(
        bronze_dir=bronze_dir,
        location_index_path=location_index_path,
        bulbapedia_dir=bulbapedia_dir,
        kaggle_csv_path=kaggle_csv_path,
    )
    _stage_checkpoint("inputs_validated")

    games_config = get_games_config()
    allowed_versions = {game["game_key"] for game in games_config}
    runtime_team_config = resolve_runtime_team_config()
    runtime_simulation_config = load_runtime_battle_policy_config().__dict__

    state_dir = silver_dir / "_state"
    state_dir.mkdir(parents=True, exist_ok=True)
    state_path = state_dir / "silver_state.json"
    repo_root = Path(__file__).resolve().parents[4]
    code_fingerprint = fingerprint_python_files([
        repo_root / "src" / "pipeline" / "silver",
        repo_root / "src" / "pipeline" / "common" / "simulation_config.py",
        repo_root / "src" / "pipeline" / "settings.py",
    ])

    current_signature = build_input_signature(
        {
            "location_index": fingerprint_path(location_index_path),
            "bulbapedia": fingerprint_path(bulbapedia_dir),
            "kaggle": fingerprint_path(kaggle_csv_path) if kaggle_csv_path.exists() else None,
            "type_chart": fingerprint_path(type_chart_path),
            "allowed_versions": sorted(allowed_versions),
            "runtime_team_config": runtime_team_config,
            "runtime_simulation_config": runtime_simulation_config,
            "pipeline_code_fingerprint": code_fingerprint,
        }
    )

    previous_state = load_state(state_path)
    expected_outputs = [
        mappings_dir / "location_to_area_map.json",
        mappings_dir / "location_to_pokemon_map.json",
        mappings_dir / "boss_mapping_by_version.json",
        references_dir / "pokemon_reference.parquet",
        references_dir / "pokemon_data.parquet",
        references_dir / "encounter_methods_reference.json",
        references_dir / "games.parquet",
        references_dir / "bosses.parquet",
        references_dir / "boss_teams.parquet",
        references_dir / "locations.parquet",
        references_dir / "encounters.parquet",
        references_dir / "progression_edges.parquet",
        references_dir / "move_reference.parquet",
        references_dir / "learnable_moves.parquet",
        references_dir / "boss_team_members.parquet",
        silver_dir / "manifest.json",
        diagnostics_dir / "boss_harmonization_report.json",
    ]
    expected_snapshot_files = [snapshots_dir / f"{game['game_key']}_boss_snapshots.jsonl" for game in games_config]
    expected_team_shards = [simulation_dir / f"source_teams_{game['game_key']}.parquet" for game in games_config]
    expected_member_shards = [simulation_dir / f"source_team_members_{game['game_key']}.parquet" for game in games_config]
    expected_move_option_shards = [simulation_dir / f"member_moveset_combos_{game['game_key']}.parquet" for game in games_config]

    if previous_state.get("input_signature") == current_signature and all(path.exists() for path in (expected_outputs + expected_snapshot_files + expected_team_shards + expected_member_shards + expected_move_option_shards)):
        logger.info("[silver] incremental skip; input signature unchanged")
        return

    _stage_checkpoint("parse_start")
    location_index = cast(dict[str, Any], read_json(location_index_path))
    mapper = LocationMapper(location_index)
    kaggle_rows_by_game = load_kaggle_rows_by_game(bronze_dir)
    kaggle_boss_rows_by_game = load_kaggle_boss_rows_by_game(bronze_dir, allowed_versions=allowed_versions)

    parse_started_at = time.perf_counter()
    parse_output = run_parse_stage(game_files=sorted(bulbapedia_dir.glob("*.json")), mapper=mapper, kaggle_rows_by_game=kaggle_rows_by_game)
    stage_durations["parse_stage_s"] = time.perf_counter() - parse_started_at
    _stage_checkpoint("parse_done", records=len(parse_output.all_records), games=len(parse_output.records_with_game_keys))

    all_records = parse_output.all_records
    all_slugs = parse_output.all_slugs
    boss_mapping_by_version = parse_output.boss_mapping_by_version
    records_with_game_keys = parse_output.records_with_game_keys

    mapping_started_at = time.perf_counter()
    area_map, location_pokemon_map = get_location_area_and_pokemon_maps(all_slugs, allowed_versions=allowed_versions, silver_dir=silver_dir, bronze_dir=bronze_dir)
    stage_durations["mapping_stage_s"] = time.perf_counter() - mapping_started_at
    _stage_checkpoint("mapping_done", location_slugs=len(all_slugs))
    write_json(mappings_dir / "location_to_area_map.json", area_map)
    write_json(mappings_dir / "location_to_pokemon_map.json", location_pokemon_map)

    encounters_file = references_dir / "encounters.jsonl"
    if encounters_file.exists():
        encounters_file.unlink()
    for snapshot_file in snapshots_dir.glob("*_boss_snapshots.jsonl"):
        snapshot_file.unlink()

    all_pokemon_references: dict[str, Any] = {}
    for game_key, records in records_with_game_keys:
        enrich_records_with_location_pokemon(records, location_pokemon_map)
        pokemon_refs = write_normalized_silver(records=records, snapshots_dir=snapshots_dir, encounters_output_path=encounters_file, game_key=game_key)
        if pokemon_refs:
            all_pokemon_references.update(pokemon_refs)

    create_pokemon_reference_index(all_pokemon_references, references_dir)
    create_encounter_methods_reference(all_records, references_dir)
    write_json(mappings_dir / "boss_mapping_by_version.json", boss_mapping_by_version)

    games_table = build_games_table(games_config)
    locations_table = build_locations_table(all_records, area_map, mapper.misses)
    write_parquet(references_dir / "games.parquet", games_table)
    write_parquet(references_dir / "locations.parquet", locations_table)
    from src.pipeline.silver.references.boss_harmonization import (
        build_and_write_boss_references,
        build_boss_team_members_reference_rows,
        build_boss_team_payloads,
    )
    boss_reference_result = build_and_write_boss_references(
        bronze_dir=bronze_dir,
        references_dir=references_dir,
        diagnostics_dir=diagnostics_dir,
    )
    boss_teams_df = boss_reference_result.boss_teams.copy()
    _stage_checkpoint("boss_references_done")

    encounters_frame = pd.DataFrame()
    bosses_reference_df = read_parquet(references_dir / "bosses.parquet") if (references_dir / "bosses.parquet").exists() else pd.DataFrame()
    if encounters_file.exists():
        encounters_frame = read_jsonl(encounters_file)
        encounters_frame = _expand_striaton_encounters(encounters_frame)
        encounters_frame = _canonicalize_encounter_boss_ids(encounters_frame, bosses_reference_df)
        # Deduplicate: Striaton expansion can produce duplicate rows when the
        # same encounter exists both in the original JSONL and the expanded clone.
        _encounter_key_cols = [
            "game", "boss_id", "location", "pokemon",
            "level_min", "level_max", "encounter_chance_min", "encounter_chance_max",
        ]
        existing_key_cols = [c for c in _encounter_key_cols if c in encounters_frame.columns]
        if existing_key_cols:
            encounters_frame = encounters_frame.drop_duplicates(subset=existing_key_cols).reset_index(drop=True)
            logger.info("[silver/encounters] dedup done rows=%s", len(encounters_frame))
        write_parquet(references_dir / "encounters.parquet", encounters_frame)
    encounters_reference_path = references_dir / "encounters.parquet"
    encounters_reference_df = read_parquet(encounters_reference_path) if encounters_reference_path.exists() else pd.DataFrame()
    progression_bosses_reference_df = _filter_bosses_with_encounter_pools(bosses_reference_df, encounters_reference_df)
    progression_depth_df = build_progression_depth_table(
        bosses_df=progression_bosses_reference_df,
        encounters_df=encounters_reference_df,
    )
    write_parquet(
        references_dir / "progression_depth.parquet",
        progression_depth_df,
    )

    write_json(diagnostics_dir / "unmapped_locations_detailed.json", mapper.misses)
    write_json(diagnostics_dir / "unmapped_locations_summary.json", summarize_unmapped_locations(mapper.misses))
    write_json(
        diagnostics_dir / "unmapped_locations.json",
        [{"raw_title": miss["raw_title"], "tried_slug": miss["tried_slug"], "reason": miss["reason"]} for miss in mapper.misses],
    )

    move_reference_path = references_dir / "move_reference.parquet"
    learnable_moves_path = references_dir / "learnable_moves.parquet"
    existing_move_reference_df = read_parquet(move_reference_path) if move_reference_path.exists() else pd.DataFrame()
    existing_learnable_moves_df = read_parquet(learnable_moves_path) if learnable_moves_path.exists() else pd.DataFrame()
    starter_chain_species_by_game = _collect_starter_chain_species_by_game(games_config)
    starter_chain_entries = _starter_chain_bootstrap_entries(starter_chain_species_by_game)
    universal_starter_entries = _all_starter_family_bootstrap_entries(games_config)
    starter_bootstrap_entries = _dedupe_bootstrap_entries(starter_chain_entries + universal_starter_entries)
    universal_starter_species_by_game = {game: set(species) for game, species in starter_chain_species_by_game.items()}
    base_bootstrap_entries = _build_bootstrap_move_entries(records_with_game_keys)
    boss_species_bootstrap_entries = _build_boss_species_bootstrap_entries(boss_teams_df)
    kaggle_bootstrap_entries = _build_kaggle_bootstrap_entries(
        kaggle_boss_rows_by_game,
        learnable_moves_df=existing_learnable_moves_df,
        move_reference_df=existing_move_reference_df,
    )
    bootstrap_entries = _dedupe_bootstrap_entries(
        base_bootstrap_entries + starter_bootstrap_entries + boss_species_bootstrap_entries + kaggle_bootstrap_entries
    )
    bootstrap_entries = _expand_bootstrap_entries_with_evolution_lines(bootstrap_entries)
    missing_bootstrap_moves = _collect_missing_bootstrap_move_hints(bootstrap_entries, existing_move_reference_df)
    move_reference_missing = not move_reference_path.exists()
    learnable_moves_missing = not learnable_moves_path.exists()
    should_bootstrap = move_reference_missing or learnable_moves_missing or bool(missing_bootstrap_moves)
    logger.info(
        "[silver/moves] bootstrap decision entries=%s move_reference_missing=%s learnable_moves_missing=%s missing_move_hints=%s",
        len(bootstrap_entries),
        move_reference_missing,
        learnable_moves_missing,
        len(missing_bootstrap_moves),
    )
    if should_bootstrap:
        bootstrap_stats = bootstrap_move_reference_cache(bootstrap_entries, silver_dir=silver_dir)
        logger.info(
            "[silver] bootstrap move refs entries=%s missing_move_hints=%s",
            bootstrap_stats.get("entry_count", 0),
            len(missing_bootstrap_moves),
        )
    if bootstrap_entries:
        logger.info("[silver/moves] persisting move reference cache entries=%s", len(bootstrap_entries))
        persist_move_reference_cache(bootstrap_entries, silver_dir=silver_dir)
        logger.info("[silver/moves] persisted move reference cache entries=%s", len(bootstrap_entries))
    learnable_reference_df = read_parquet(learnable_moves_path) if learnable_moves_path.exists() else pd.DataFrame()
    missing_starter_pairs = _collect_missing_learnable_species_pairs(
        learnable_reference_df,
        starter_chain_species_by_game,
        reason="starter_chain_missing_moves",
    )
    missing_universal_starter_pairs = _collect_missing_learnable_species_pairs(
        learnable_reference_df,
        universal_starter_species_by_game,
        reason="universal_starter_family_missing_moves",
    )
    pd.DataFrame(
        missing_starter_pairs,
        columns=["game_version", "species_name", "reason"],
    ).to_csv(diagnostics_dir / "starter_chain_move_gaps.csv", index=False)
    pd.DataFrame(
        missing_universal_starter_pairs,
        columns=["game_version", "species_name", "reason"],
    ).to_csv(diagnostics_dir / "starter_family_move_gaps.csv", index=False)
    if missing_starter_pairs:
        preview = ",".join(f"{row['game_version']}:{row['species_name']}" for row in missing_starter_pairs[:20])
        diagnostics_paths = [
            diagnostics_dir / "starter_chain_move_gaps.csv",
            diagnostics_dir / "starter_family_move_gaps.csv",
        ]
        raise ValueError(
            "Starter move reference validation failed: "
            f"starter_chain_missing_pairs={len(missing_starter_pairs)} first_20=[{preview}] diagnostics={diagnostics_paths}"
        )

    if missing_universal_starter_pairs:
        preview = ",".join(
            f"{row['game_version']}:{row['species_name']}" for row in missing_universal_starter_pairs[:20]
        )
        logger.warning(
            "[silver/moves] universal starter-family move gaps remain after refresh; continuing "
            "missing_pairs=%s first_20=[%s] diagnostics=%s",
            len(missing_universal_starter_pairs),
            preview,
            diagnostics_dir / "starter_family_move_gaps.csv",
        )

    move_reference_df = read_parquet(move_reference_path) if move_reference_path.exists() else pd.DataFrame()
    _validate_kaggle_moves_in_move_reference(kaggle_boss_rows_by_game, move_reference_df, diagnostics_dir)

    boss_teams = build_boss_team_payloads(boss_teams_df)
    boss_move_data: dict[str, Any] = {}
    simulatable_bosses_reference_df = bosses_reference_df
    if "is_simulatable" in bosses_reference_df.columns:
        simulatable_bosses_reference_df = bosses_reference_df[
            bosses_reference_df["is_simulatable"].fillna(True).astype(bool)
        ].copy()
    progression_simulatable_bosses_reference_df = _filter_bosses_with_encounter_pools(
        simulatable_bosses_reference_df,
        encounters_reference_df,
    )
    _validate_boss_team_targets(
        boss_teams,
        simulatable_bosses_reference_df,
    )
    _validate_kaggle_boss_move_profiles(boss_move_data, diagnostics_dir)
    kaggle_boss_species, kaggle_boss_moves = _collect_kaggle_boss_species_and_moves(boss_teams)
    boss_team_members_rows = build_boss_team_members_reference_rows(boss_teams_df)
    write_parquet(references_dir / "boss_team_members.parquet", boss_team_members_rows)
    boss_team_members_reference_df = read_parquet(references_dir / "boss_team_members.parquet")
    boss_level_df = build_boss_level_table_with_mapping(
        boss_team_members_reference_df,
        bosses_reference_df,
        bronze_dir=bronze_dir,
    )
    progression_depth_context = build_progression_depth_context(
        progression_depth_df=progression_depth_df,
        boss_level_df=boss_level_df,
    )
    _stage_checkpoint("progression_context_done")

    cleanup_simulation_shards(simulation_dir)

    team_values: dict[str, set[str]] = {"source_team_id": set(), "game_version": set()}
    member_values: dict[str, set[str]] = {"team_member_id": set(), "source_team_id": set(), "game_version": set()}
    move_values: dict[str, set[str]] = {
        "team_member_id": set(),
        "source_team_id": set(),
        "move_1": set(),
        "move_2": set(),
        "move_3": set(),
        "move_4": set(),
    }

    total_source_teams = 0
    total_members = 0
    total_moveset_combos = 0
    total_boss_teams = 0
    total_boss_rows_skipped_from_move_options = 0
    total_player_rows_used_for_move_options = 0
    total_fixed_boss_teams_carried_forward = 0

    boss_teams_by_game = group_teams_by_game(boss_teams)

    evolution_rules_path = references_dir / "evolution_rules.parquet"
    evolution_rules_by_game: dict[str, dict[str, dict[str, Any]]] = (
        _evolution_rules_map_from_rows(read_parquet(evolution_rules_path))
        if evolution_rules_path.exists()
        else {}
    )

    if not evolution_rules_by_game:
        evolution_rules_by_game = _build_evolution_rules_by_game_from_encounters(encounters_reference_df)
    if evolution_rules_by_game:
        evolution_rows = _evolution_rules_rows_from_map(evolution_rules_by_game)
        if evolution_rows:
            write_parquet(evolution_rules_path, evolution_rows)
            logger.info(
                "[silver/evolution] persisted canonical evolution rules parquet rows=%s games=%s path=%s",
                len(evolution_rows),
                len({str(row.get('game_version') or '').strip().lower() for row in evolution_rows}),
                evolution_rules_path,
            )
        else:
            logger.warning("[silver/evolution] no evolution rules persisted; map is empty")
    else:
        logger.info(
            "[silver/evolution] loaded evolution rules parquet games=%s path=%s",
            len(evolution_rules_by_game),
            evolution_rules_path,
        )
    restricted_encounter_species = {
        normalize_species_slug(species)
        for species in all_pokemon_references.keys()
        if normalize_species_slug(species)
    }
    starter_species_required = {
        normalize_species_slug(species)
        for species_set in universal_starter_species_by_game.values()
        for species in species_set
        if normalize_species_slug(species)
    }
    evolution_rule_species_required = {
        normalize_species_slug(species_name)
        for species_map in evolution_rules_by_game.values()
        for species_name in species_map.keys()
        if normalize_species_slug(species_name)
    }
    total_required_species = (
        restricted_encounter_species
        | kaggle_boss_species
        | starter_species_required
        | evolution_rule_species_required
    )
    # Also require every species that already has learnable moves, so that
    # pokemon_data and learnable_moves stay in sync.
    if learnable_moves_path.exists():
        _lm_df = read_parquet(learnable_moves_path)
        if not _lm_df.empty and "pokemon_species" in _lm_df.columns:
            learnable_species_required = {
                normalize_species_slug(s)
                for s in _lm_df["pokemon_species"].dropna().unique()
                if normalize_species_slug(s)
            }
            total_required_species = total_required_species | learnable_species_required
    total_required_moves = kaggle_boss_moves
    move_reference_df = read_parquet(move_reference_path) if move_reference_path.exists() else pd.DataFrame()
    move_reference_profiles = _move_profiles_from_reference(move_reference_df)

    logger.info(
        "[silver/reference_enrichment] pre-team restricted_encounter_species=%s kaggle_boss_species=%s total_required_species=%s "
        "learnable_reference_moves=%s kaggle_boss_moves=%s total_required_moves=%s",
        len(restricted_encounter_species),
        len(kaggle_boss_species),
        len(total_required_species),
        len(move_reference_profiles),
        len(kaggle_boss_moves),
        len(total_required_moves),
    )

    pokemon_data_path = references_dir / "pokemon_data.parquet"
    pokemon_data_df, pokemon_diagnostics = _build_enriched_pokemon_profiles(all_pokemon_references, total_required_species)
    if pokemon_diagnostics:
        write_parquet(diagnostics_dir / "pokemon_profile_fetch_errors.parquet", pokemon_diagnostics)
    _validate_and_persist_pokemon_data_contract(pokemon_data_df, diagnostics_dir, total_required_species)
    write_parquet(pokemon_data_path, pokemon_data_df)
    create_pokemon_reference_index(
        {
            str(row.get("pokemon_species") or "").strip().lower(): {
                "name": str(row.get("name") or row.get("pokemon_species") or "").strip().lower(),
                "url": str(row.get("source_url") or "").strip() or None,
            }
            for row in pokemon_data_df.to_dict(orient="records")
            if str(row.get("pokemon_species") or "").strip()
        },
        references_dir,
    )

    pokemon_types_by_species: dict[str, tuple[str | None, str | None]] = {}
    for row in pokemon_data_df.to_dict(orient="records"):
        species = str(row.get("pokemon_species") or "").strip().lower()
        if not species:
            continue
        type_1 = str(row.get("type_1") or "").strip().lower() or None
        type_2 = str(row.get("type_2") or "").strip().lower() or None
        if type_1 is None and type_2 is None:
            continue
        pokemon_types_by_species[species] = (type_1, type_2)

    encounter_species_col = "pokemon_species" if "pokemon_species" in encounters_reference_df.columns else ("pokemon" if "pokemon" in encounters_reference_df.columns else None)
    if encounter_species_col is not None:
        encounter_species = {
            normalize_species_slug(value)
            for value in encounters_reference_df[encounter_species_col].tolist()
            if normalize_species_slug(value)
        }
        missing_type_species = sorted(species for species in encounter_species if species not in pokemon_types_by_species)
        if missing_type_species:
            raise ValueError(
                "Missing pokemon typing coverage for encounter species before team generation: "
                f"count={len(missing_type_species)} sample=[{','.join(missing_type_species[:20])}]"
            )

    boss_type_profile_by_key: dict[tuple[str, str], dict[str, float]] = {}
    for team in boss_teams:
        game_version = str(team.get("game_version") or "").strip().lower()
        boss_id = str(team.get("boss_id") or "").strip().lower()
        if not game_version or not boss_id:
            continue
        counts: dict[str, float] = {}
        total = 0.0
        for species in list(team.get("pokemon") or []):
            species_key = str(species or "").strip().lower()
            if not species_key:
                continue
            type_1, type_2 = pokemon_types_by_species.get(species_key, (None, None))
            if type_1 and type_2:
                counts[type_1] = counts.get(type_1, 0.0) + 0.5
                counts[type_2] = counts.get(type_2, 0.0) + 0.5
                total += 1.0
            elif type_1:
                counts[type_1] = counts.get(type_1, 0.0) + 1.0
                total += 1.0
            elif type_2:
                counts[type_2] = counts.get(type_2, 0.0) + 1.0
                total += 1.0
        if total > 0:
            boss_type_profile_by_key[(game_version, boss_id)] = {
                type_name: value / total
                for type_name, value in counts.items()
            }

    reference_context = load_reference_context(silver_dir=silver_dir)
    progression_source_teams = build_progression_source_teams_from_encounters(
        encounters_df=encounters_reference_df,
        bosses_df=progression_simulatable_bosses_reference_df,
        progression_depth_context=progression_depth_context,
        evolution_rules_by_game=evolution_rules_by_game,
        reference_context=reference_context,
        allow_item_evolutions=ALLOW_ITEM_EVOLUTIONS,
        item_evolution_default_level=ITEM_EVOLUTION_DEFAULT_LEVEL,
        pokemon_types_by_species=pokemon_types_by_species,
        boss_type_profile_by_key=boss_type_profile_by_key,
    )
    _stage_checkpoint("progression_teams_done", count=len(progression_source_teams))
    reference_context = load_reference_context(silver_dir=silver_dir)
    player_move_gap_df = _diagnose_player_missing_damaging_moves(
        progression_source_teams,
        reference_context,
        diagnostics_dir,
    )
    if not player_move_gap_df.empty:
        logger.warning(
            "[silver/moves] found player species-level contexts without damaging moves count=%s diagnostics=%s",
            len(player_move_gap_df),
            diagnostics_dir / "player_no_damaging_move_gaps.csv",
        )

    _validate_progression_source_team_boss_targets(
        progression_source_teams,
        simulatable_bosses_reference_df,
    )
    progression_source_teams_by_game: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in progression_source_teams:
        game_version = str(row.get("game_version") or "").strip().lower()
        if game_version:
            progression_source_teams_by_game[game_version].append(row)

    sharded_game_keys = sorted(
        {
            str(game.get("game_key") or "").strip().lower()
            for game in games_config
            if str(game.get("game_key") or "").strip()
        }
        | set(boss_teams_by_game.keys())
        | set(progression_source_teams_by_game.keys())
    )

    all_move_data = dict(boss_move_data)
    all_move_data = _ensure_moves_in_combat_profiles(all_move_data, total_required_moves, move_reference_profiles)

    for game_key in sharded_game_keys:
        if not game_key:
            continue
        paths = _game_output_paths(simulation_dir, game_key)
        boss_teams_game = boss_teams_by_game.get(game_key, [])
        progression_source_teams = progression_source_teams_by_game.get(game_key, [])
        player_compact = build_player_team_compact_tables(
            progression_source_teams,
            reference_context,
            evolution_rules_by_game=evolution_rules_by_game,
            allow_item_evolutions=ALLOW_ITEM_EVOLUTIONS,
            item_evolution_default_level=ITEM_EVOLUTION_DEFAULT_LEVEL,
        )
        boss_compact = _build_boss_compact_tables(
            boss_teams_game,
            boss_move_data,
            progression_depth_context=progression_depth_context,
        )

        source_teams_rows = list(boss_compact["source_teams"]) + list(player_compact["source_teams"])
        source_member_rows = list(boss_compact["source_team_members"]) + list(player_compact["source_team_members"])
        source_team_meta = {
            str(row.get("source_team_id") or "").strip(): row
            for row in source_teams_rows
            if str(row.get("source_team_id") or "").strip()
        }
        member_id_to_team_id = {
            str(row.get("team_member_id") or "").strip(): str(row.get("source_team_id") or "").strip()
            for row in source_member_rows
            if str(row.get("team_member_id") or "").strip() and str(row.get("source_team_id") or "").strip()
        }

        boss_rows_skipped = 0
        player_rows_used = 0
        member_move_rows: list[dict[str, Any]] = []
        for row in player_compact["member_move_options"]:
            team_id = str(row.get("source_team_id") or "").strip()
            meta = source_team_meta.get(team_id, {})
            team_role = str(meta.get("team_role") or "").strip().lower()
            origin = str(meta.get("origin") or "").strip().lower()
            if team_role == "boss" or origin == "kaggle":
                boss_rows_skipped += 1
                continue
            if bool(meta.get("is_player_candidate")) and team_role == "player":
                member_move_rows.append(row)
                player_rows_used += 1
                continue
            boss_rows_skipped += 1

        member_moveset_combo_rows: list[dict[str, Any]] = []
        for row in player_compact["member_moveset_combos"]:
            team_id = str(row.get("team_id") or "").strip()
            if not team_id:
                team_id = member_id_to_team_id.get(str(row.get("pokemon_instance_id") or "").strip(), "")
            meta = source_team_meta.get(team_id, {})
            team_role = str(meta.get("team_role") or "").strip().lower()
            origin = str(meta.get("origin") or "").strip().lower()
            if team_role == "boss" or origin == "kaggle" or (meta and not bool(meta.get("is_player_candidate", False))):
                continue
            if meta and team_role != "player":
                continue
            member_moveset_combo_rows.append(row)

        move_options_by_member: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in member_move_rows:
            member_id = str(row.get("team_member_id") or "").strip()
            if member_id:
                move_options_by_member[member_id].append(row)
        for options in move_options_by_member.values():
            options.sort(
                key=lambda item: (
                    int(item.get("option_rank") or 0),
                    str(item.get("move_name") or ""),
                )
            )

        for row in source_member_rows:
            member_id = str(row.get("team_member_id") or "").strip()
            team_id = str(row.get("source_team_id") or "").strip()
            species = normalize_species_slug(row.get("pokemon_species") or "")
            game_version = str(row.get("game_version") or "").strip().lower()
            slot_index = int(row.get("slot") or 0)
            try:
                level = int(row.get("level") or 0)
            except (TypeError, ValueError):
                level = 0
            if not member_id or not team_id or not species or not game_version or slot_index <= 0 or level <= 0:
                continue

            team_meta = source_team_meta.get(team_id, {})
            team_role = str(row.get("team_role") or team_meta.get("team_role") or "").strip().lower()
            origin = str(row.get("origin") or team_meta.get("origin") or "").strip().lower()
            is_boss_like = team_role == "boss" or origin == "kaggle"

            provided_moves: list[str] = []
            if is_boss_like:
                fixed_moves = row.get("fixed_moves")
                if isinstance(fixed_moves, list):
                    provided_moves = [
                        normalize_move_name(move)
                        for move in fixed_moves
                        if normalize_move_name(move)
                    ][:4]
            else:
                for option in move_options_by_member.get(member_id, []):
                    move_name = normalize_move_name(option.get("move_name") or "")
                    if move_name and move_name not in provided_moves:
                        provided_moves.append(move_name)
                    if len(provided_moves) >= 4:
                        break

            move_payload = reference_context.build_member_moves(
                name=species,
                level=level,
                moves=provided_moves,
                game_version=game_version,
            )
            move_payload["pokemon_instance_id"] = member_id
            move_payload["team_id"] = team_id
            move_payload["slot_index"] = slot_index
            all_move_data[member_id] = move_payload

        pokemon_moveset_rows = list(boss_compact["pokemon_moveset_options"]) + list(player_compact["pokemon_moveset_options"])
        sampling_rows = list(player_compact["simulation_sampling_plan"])

        write_parquet(paths["source_teams"], source_teams_rows)
        write_parquet(paths["source_team_members"], source_member_rows)
        write_parquet(paths["member_moveset_combos"], member_moveset_combo_rows)
        write_parquet(paths["member_move_options"], member_move_rows)
        write_parquet(paths["pokemon_moveset_options"], pokemon_moveset_rows)
        write_parquet(paths["simulation_sampling_plan"], sampling_rows)

        combat_rows: list[dict[str, Any]] = []
        combat_seen: set[tuple[str, str, int]] = set()
        for row in source_member_rows:
            gv = str(row.get("game_version") or "").strip().lower()
            sp = str(row.get("pokemon_species") or "").strip().lower()
            lv = int(row.get("level") or 0)
            key = (gv, sp, lv)
            if gv and sp and lv > 0 and key not in combat_seen:
                combat_seen.add(key)
                combat_rows.append({"game_version": gv, "pokemon_species": sp, "level": lv})
        write_parquet(paths["combat_pool"], combat_rows)

        team_values["source_team_id"].update(str(row.get("source_team_id") or "").strip().lower() for row in source_teams_rows if str(row.get("source_team_id") or "").strip())
        team_values["game_version"].update(str(row.get("game_version") or "").strip().lower() for row in source_teams_rows if str(row.get("game_version") or "").strip())

        member_values["team_member_id"].update(str(row.get("team_member_id") or "").strip().lower() for row in source_member_rows if str(row.get("team_member_id") or "").strip())
        member_values["source_team_id"].update(str(row.get("source_team_id") or "").strip().lower() for row in source_member_rows if str(row.get("source_team_id") or "").strip())
        member_values["game_version"].update(str(row.get("game_version") or "").strip().lower() for row in source_member_rows if str(row.get("game_version") or "").strip())

        move_values["team_member_id"].update(
            str(row.get("pokemon_instance_id") or "").strip().lower()
            for row in member_moveset_combo_rows
            if str(row.get("pokemon_instance_id") or "").strip()
        )
        move_values["source_team_id"].update(
            str(row.get("team_id") or "").strip().lower()
            for row in member_moveset_combo_rows
            if str(row.get("team_id") or "").strip()
        )
        for move_col in ("move_1", "move_2", "move_3", "move_4"):
            normalized_moves = {
                normalize_move_name(row.get(move_col) or "")
                for row in member_moveset_combo_rows
                if normalize_move_name(row.get(move_col) or "")
            }
            move_values[move_col].update(normalized_moves)

        total_source_teams += len(source_teams_rows)
        total_members += len(source_member_rows)
        total_moveset_combos += len(member_moveset_combo_rows)
        total_boss_teams += len(boss_teams_game)
        total_boss_rows_skipped_from_move_options += boss_rows_skipped
        total_player_rows_used_for_move_options += player_rows_used
        total_fixed_boss_teams_carried_forward += len(boss_compact["source_teams"])

        boss_move_option_rows = sum(
            1
            for row in member_move_rows
            if str(source_team_meta.get(str(row.get("source_team_id") or "").strip(), {}).get("team_role") or "").strip().lower() == "boss"
        )
        boss_moveset_combo_rows = sum(
            1
            for row in member_moveset_combo_rows
            if str(source_team_meta.get(str(row.get("team_id") or "").strip(), {}).get("team_role") or "").strip().lower() == "boss"
        )
        if boss_move_option_rows or boss_moveset_combo_rows:
            raise ValueError(
                f"Boss rows leaked into compact move tables for game={game_key}: "
                f"member_move_options_boss_rows={boss_move_option_rows} "
                f"member_moveset_combos_boss_rows={boss_moveset_combo_rows}"
            )

        logger.info(
            "[silver] wrote compact team shards game=%s source_teams=%s members=%s moveset_combos=%s move_options=%s "
            "boss_rows_skipped_from_move_option_generation=%s player_rows_used_for_move_option_generation=%s fixed_boss_teams_carried_forward=%s",
            game_key,
            len(source_teams_rows),
            len(source_member_rows),
            len(member_moveset_combo_rows),
            len(member_move_rows),
            boss_rows_skipped,
            player_rows_used,
            len(boss_compact["source_teams"]),
        )
        _stage_checkpoint(
            "game_shard_written",
            game=game_key,
            source_teams=len(source_teams_rows),
            members=len(source_member_rows),
            moveset_combos=len(member_moveset_combo_rows),
        )

        gc.collect()

    learnable_reference_df = read_parquet(learnable_moves_path) if learnable_moves_path.exists() else pd.DataFrame()
    _validate_boss_reference_coverage(
        boss_team_members_df=pd.DataFrame(boss_team_members_rows),
        boss_teams=boss_teams,
        pokemon_data_df=pokemon_data_df,
        move_reference_df=move_reference_df,
        learnable_moves_df=learnable_reference_df,
        diagnostics_dir=diagnostics_dir,
    )

    from src.pipeline.silver.schemas.relational_checks import validate_normalized_silver_tables
    relational_report = validate_normalized_silver_tables(
        {
            "games": pd.DataFrame(games_table),
            "bosses": bosses_reference_df,
            "locations": pd.DataFrame(locations_table),
            "encounters": encounters_frame,
            "progression_depth": progression_depth_df,
            "teams": _validation_profile(team_values, total_source_teams),
            "team_members": _validation_profile(member_values, total_members),
            "team_member_moves": _validation_profile(move_values, total_moveset_combos),
            "move_reference": move_reference_df,
            "learnable_moves": learnable_reference_df,
            "pokemon_data": pokemon_data_df,
        }
    )
    write_json(diagnostics_dir / "relational_validation.json", relational_report.as_dict())
    if not relational_report.is_valid:
        raise ValueError("Silver relational validation failed; see diagnostics/relational_validation.json")
    _stage_checkpoint("relational_validation_done")

    from src.pipeline.silver.reporting.silver_manifest import create_silver_manifest
    create_silver_manifest(silver_dir)
    _stage_checkpoint("manifest_written")

    save_state(
        state_path,
        {
            "input_signature": current_signature,
            "updated_at": time.time(),
            "games_processed": len(records_with_game_keys),
            "boss_teams": total_boss_teams,
            "source_teams": total_source_teams,
            "source_team_members": total_members,
            "member_moveset_combos": total_moveset_combos,
            "runtime_team_config": runtime_team_config,
            "runtime_simulation_config": runtime_simulation_config,
            "pipeline_code_fingerprint": code_fingerprint,
        },
    )

    write_json(
        diagnostics_dir / "performance_summary.json",
        {
            "generated_at_epoch_s": time.time(),
            "stage_durations_s": stage_durations,
            "totals": {
                "games_processed": len(records_with_game_keys),
                "boss_teams": total_boss_teams,
                "source_teams": total_source_teams,
                "source_team_members": total_members,
                "member_moveset_combos": total_moveset_combos,
            },
        },
    )
    _stage_checkpoint("performance_summary_written", stages=len(stage_durations))

    logger.info(
        "[silver] build finished records=%s source_teams=%s source_team_members=%s member_moveset_combos=%s "
        "move_option_generation_boss_rows_skipped=%s move_option_generation_player_rows_used=%s fixed_boss_teams_carried_forward=%s "
        "unmapped=%s elapsed_s=%.2f",
        len(all_records),
        total_source_teams,
        total_members,
        total_moveset_combos,
        total_boss_rows_skipped_from_move_options,
        total_player_rows_used_for_move_options,
        total_fixed_boss_teams_carried_forward,
        len(mapper.misses),
        time.perf_counter() - started_at,
    )

def _validate_kaggle_boss_move_profiles(move_data: dict[str, Any], diagnostics_dir: Path) -> None:
    missing_rows: list[dict[str, Any]] = []

    for pokemon_instance_id, payload in move_data.items():
        provided_moves = payload.get("provided_moves") or []
        move_details = payload.get("move_details") or {}

        for move_name in provided_moves:
            normalized_move = normalize_move_name(move_name)
            if normalized_move and normalized_move not in move_details:
                missing_rows.append(
                    {
                        "pokemon_instance_id": pokemon_instance_id,
                        "species": payload.get("species"),
                        "game_version": payload.get("game_version"),
                        "move_name": normalized_move,
                        "reason": "provided_kaggle_move_missing_profile",
                    }
                )

    diagnostics_path = diagnostics_dir / "kaggle_boss_move_profile_gaps.csv"
    diagnostics_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        missing_rows,
        columns=["pokemon_instance_id", "species", "game_version", "move_name", "reason"],
    ).to_csv(diagnostics_path, index=False)

    if missing_rows:
        preview = ", ".join(
            f"{row['game_version']}:{row['species']}:{row['move_name']}"
            for row in missing_rows[:20]
        )
        raise ValueError(
            "Kaggle boss move reference validation failed: "
            f"missing_move_profiles={len(missing_rows)} "
            f"first_20=[{preview}] "
            f"diagnostics={diagnostics_path}"
        )
