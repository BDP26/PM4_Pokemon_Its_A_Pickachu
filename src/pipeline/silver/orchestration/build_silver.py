from __future__ import annotations

from collections import Counter, defaultdict
import gc
import logging
import shutil
import time
from pathlib import Path
from typing import Any, cast

import pandas as pd

from src.pipeline.bronze.inputs.create_type_chart import build_type_chart, save_as_json
from src.pipeline.common.io import read_json, read_jsonl, read_parquet, write_json, write_parquet
from src.pipeline.common.simulation_config import load_runtime_battle_policy_config
from src.pipeline.settings import BRONZE_DIR, SILVER_DIR, ensure_medallion_dirs, get_silver_subdirs
from src.pipeline.silver.config.game_config import get_games_config
from src.pipeline.silver.config.team_config import resolve_runtime_team_config
from src.pipeline.silver.enrichment.location_pokemon_enrichment import (
    enrich_records_with_location_pokemon,
    get_location_area_and_pokemon_maps,
)
from src.pipeline.silver.enrichment.schema_normalizer import (
    create_encounter_methods_reference,
    create_pokemon_reference_index,
    write_normalized_silver,
)
from src.pipeline.silver.inputs.builders.player_teams import (
    build_player_team_compact_tables,
    build_progression_source_teams,
)
from src.pipeline.silver.inputs.connectors.pokeapi_moves import (
    bootstrap_move_reference_cache,
    persist_move_reference_cache,
)
from src.pipeline.silver.inputs.kaggle_boss_mapping import load_kaggle_rows_by_game
from src.pipeline.silver.inputs.location_mapper import LocationMapper
from src.pipeline.silver.inputs.reference_context import load_reference_context, normalize_move_name, normalize_species_slug
from src.pipeline.silver.inputs.sources.boss_teams import extract_boss_teams_from_kaggle_source
from src.pipeline.silver.orchestration.stages import run_parse_stage
from src.pipeline.silver.reporting.silver_manifest import create_silver_manifest
from src.pipeline.silver.schemas.relational_checks import validate_normalized_silver_tables
from src.pipeline.silver.transforms.keys import stable_digest
from src.pipeline.silver.transforms.normalized_tables import build_bosses_table, build_games_table, build_locations_table
from src.pipeline.silver.writers.outputs import (
    build_input_signature,
    fingerprint_path,
    fingerprint_python_files,
    load_state,
    save_state,
    write_validated_move_data,
)

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


def _remove_if_exists(path: Path) -> None:
    if not path.exists():
        return
    if path.is_dir():
        shutil.rmtree(path)
    else:
        path.unlink()


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


def _validation_profile(values_by_column: dict[str, set[str]], row_count: int) -> dict[str, Any]:
    return {"row_count": row_count, "columns": {column: sorted(values) for column, values in values_by_column.items()}}


def _build_bootstrap_move_entries(
    records_with_game_keys: list[tuple[str, list[dict[str, Any]]]],
) -> list[tuple[str, int, str, list[str]]]:
    bootstrap_entries: list[tuple[str, int, str, list[str]]] = []
    for game_key, records in records_with_game_keys:
        for record in records:
            reachable_encounters = record.get("reachable_location_encounters", {})
            if isinstance(reachable_encounters, dict):
                for encounters in reachable_encounters.values():
                    if not isinstance(encounters, list):
                        continue
                    for encounter in encounters:
                        if not isinstance(encounter, dict):
                            continue
                        species = str(encounter.get("species") or "").strip().lower()
                        if not species:
                            continue
                        try:
                            level = int(encounter.get("level_max") or encounter.get("level") or record.get("boss_avg_level") or 20)
                        except (TypeError, ValueError):
                            level = 20
                        bootstrap_entries.append((species, max(level, 1), game_key, []))

    deduped_entries: list[tuple[str, int, str, list[str]]] = []
    seen: set[tuple[str, int, str]] = set()
    for species, level, game_version, moves in bootstrap_entries:
        key = (str(species).strip().lower(), int(level), str(game_version).strip().lower())
        if key in seen:
            continue
        seen.add(key)
        deduped_entries.append((species, level, game_version, moves))
    return deduped_entries


def _build_kaggle_bootstrap_entries(kaggle_rows_by_game: dict[str, list[dict[str, Any]]]) -> list[tuple[str, int, str, list[str]]]:
    entries: list[tuple[str, int, str, list[str]]] = []
    for game_key, rows in kaggle_rows_by_game.items():
        game_norm = str(game_key or "").strip().lower()
        if not game_norm:
            continue
        for row in rows:
            species = normalize_species_slug(row.get("Pokemon") or "")
            if not species:
                continue
            try:
                level = int(row.get("Level") or 20)
            except (TypeError, ValueError):
                level = 20
            moves = [
                normalize_move_name(row.get("Move 1", "")),
                normalize_move_name(row.get("Move 2", "")),
                normalize_move_name(row.get("Move 3", "")),
                normalize_move_name(row.get("Move 4", "")),
            ]
            entries.append((species, max(level, 1), game_norm, [move for move in moves if move]))
    return entries


def _build_boss_compact_tables(
    boss_teams: list[dict[str, Any]],
    move_data: dict[str, Any],
) -> dict[str, list[dict[str, Any]]]:
    source_teams: list[dict[str, Any]] = []
    source_team_members: list[dict[str, Any]] = []
    member_move_options: list[dict[str, Any]] = []
    pokemon_moveset_options: list[dict[str, Any]] = []

    seen_contexts: set[tuple[str, str, int]] = set()
    seen_context_moves: set[tuple[str, str, int, str]] = set()

    for team in boss_teams:
        source_team_id = str(team.get("team_id") or "").strip()
        if not source_team_id:
            continue
        game_version = str(team.get("game_version") or "").strip().lower()
        boss_name = str(team.get("boss_name") or team.get("gym") or "").strip().lower()
        source_teams.append(
            {
                "source_team_id": source_team_id,
                "game_version": game_version,
                "team_role": "boss_source",
                "boss_name": boss_name,
                "starter_base": None,
                "starter_evolved_species": None,
                "progression_source_team_id": None,
                "progression_pool_id": None,
                "avg_level": int(team.get("avg_level") or 0),
                "member_count": len(team.get("pokemon", [])) if isinstance(team.get("pokemon"), list) else 0,
                "is_player_candidate": False,
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
            source_team_members.append(
                {
                    "team_member_id": member_id,
                    "source_team_id": source_team_id,
                    "game_version": game_version,
                    "boss_name": boss_name,
                    "slot": slot,
                    "pokemon_species": species,
                    "level": level,
                    "progression_pool_id": None,
                    "is_starter": False,
                }
            )

            payload = move_data.get(member_id, {}) if isinstance(move_data, dict) else {}
            learnable = payload.get("learnable_moves", []) if isinstance(payload, dict) else []
            ranked_moves = sorted({str(move).strip().lower() for move in learnable if str(move).strip()})
            if not ranked_moves:
                ranked_moves = [str(move).strip().lower() for move in (team.get("moves", [])[slot - 1] if slot - 1 < len(team.get("moves", [])) else []) if str(move).strip()]

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
                        "move_policy": "boss-source-v1",
                        "candidate_move_count": len(ranked_moves),
                    }
                )

            for rank, move_name in enumerate(ranked_moves, start=1):
                member_move_options.append(
                    {
                        "team_member_id": member_id,
                        "source_team_id": source_team_id,
                        "game_version": game_version,
                        "slot": slot,
                        "pokemon_species": species,
                        "level": level,
                        "move_name": move_name,
                        "option_rank": rank,
                        "option_score": float(max(1, len(ranked_moves) - rank + 1)),
                        "moveset_context_id": context_id,
                    }
                )
                ctx_move_key = (*context_key, move_name)
                if ctx_move_key in seen_context_moves:
                    continue
                seen_context_moves.add(ctx_move_key)
                pokemon_moveset_options.append(
                    {
                        "moveset_context_id": context_id,
                        "game_version": game_version,
                        "pokemon_species": species,
                        "level": level,
                        "move_policy": "boss-source-v1",
                        "move_name": move_name,
                        "option_rank": rank,
                        "option_score": float(max(1, len(ranked_moves) - rank + 1)),
                    }
                )

    return {
        "source_teams": source_teams,
        "source_team_members": source_team_members,
        "member_move_options": member_move_options,
        "pokemon_moveset_options": pokemon_moveset_options,
    }


def build_silver_from_bronze(
    bronze_dir: Path = BRONZE_DIR,
    silver_dir: Path = SILVER_DIR,
    hard_cleanup: bool = False,
) -> None:
    started_at = time.perf_counter()
    stage_durations: dict[str, float] = {}
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
            _remove_if_exists(cleanup_path)
            cleanup_path.mkdir(parents=True, exist_ok=True)

    location_index_path = bronze_dir / "pokeapi" / "location_index.json"
    bulbapedia_dir = bronze_dir / "bulbapedia"
    if not location_index_path.exists() or not bulbapedia_dir.exists():
        raise FileNotFoundError("Bronze inputs are missing. Run: python -m src.pipeline.run_pipeline layers bronze")

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

    kaggle_csv_path = bronze_dir / "kagglehub" / "gym_leaders_elite_four.csv"
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
        references_dir / "locations.parquet",
        references_dir / "encounters.parquet",
        references_dir / "move_reference.parquet",
        references_dir / "learnable_moves.parquet",
        silver_dir / "manifest.json",
    ]
    expected_snapshot_files = [snapshots_dir / f"{game['game_key']}_boss_snapshots.jsonl" for game in games_config]
    expected_team_shards = [simulation_dir / f"source_teams_{game['game_key']}.parquet" for game in games_config]
    expected_member_shards = [simulation_dir / f"source_team_members_{game['game_key']}.parquet" for game in games_config]
    expected_move_option_shards = [simulation_dir / f"member_moveset_combos_{game['game_key']}.parquet" for game in games_config]

    if previous_state.get("input_signature") == current_signature and all(path.exists() for path in (expected_outputs + expected_snapshot_files + expected_team_shards + expected_member_shards + expected_move_option_shards)):
        logger.info("[silver] incremental skip; input signature unchanged")
        return

    location_index = cast(dict[str, Any], read_json(location_index_path))
    mapper = LocationMapper(location_index)
    kaggle_rows_by_game = load_kaggle_rows_by_game(bronze_dir)

    parse_started_at = time.perf_counter()
    parse_output = run_parse_stage(game_files=sorted(bulbapedia_dir.glob("*.json")), mapper=mapper, kaggle_rows_by_game=kaggle_rows_by_game)
    stage_durations["parse_stage_s"] = time.perf_counter() - parse_started_at

    all_records = parse_output.all_records
    all_slugs = parse_output.all_slugs
    boss_mapping_by_version = parse_output.boss_mapping_by_version
    records_with_game_keys = parse_output.records_with_game_keys

    mapping_started_at = time.perf_counter()
    area_map, location_pokemon_map = get_location_area_and_pokemon_maps(all_slugs, allowed_versions=allowed_versions, silver_dir=silver_dir, bronze_dir=bronze_dir)
    stage_durations["mapping_stage_s"] = time.perf_counter() - mapping_started_at
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
    bosses_table = build_bosses_table(boss_mapping_by_version)
    locations_table = build_locations_table(all_records, area_map, mapper.misses)
    write_parquet(references_dir / "games.parquet", games_table, partition_cols=["region"])
    write_parquet(references_dir / "bosses.parquet", bosses_table, partition_cols=["game_version", "boss_role"])
    write_parquet(references_dir / "locations.parquet", locations_table, partition_cols=["game_version", "mapping_status"])

    encounters_frame = pd.DataFrame()
    if encounters_file.exists():
        encounters_frame = read_jsonl(encounters_file)
        write_parquet(references_dir / "encounters.parquet", encounters_frame, partition_cols=["game"])

    write_json(diagnostics_dir / "unmapped_locations_detailed.json", mapper.misses)
    write_json(diagnostics_dir / "unmapped_locations_summary.json", summarize_unmapped_locations(mapper.misses))
    write_json(
        diagnostics_dir / "unmapped_locations.json",
        [{"raw_title": miss["raw_title"], "tried_slug": miss["tried_slug"], "reason": miss["reason"]} for miss in mapper.misses],
    )

    move_reference_path = references_dir / "move_reference.parquet"
    learnable_moves_path = references_dir / "learnable_moves.parquet"
    if not move_reference_path.exists() or not learnable_moves_path.exists():
        bootstrap_stats = bootstrap_move_reference_cache(_build_bootstrap_move_entries(records_with_game_keys), silver_dir=silver_dir)
        logger.info("[silver] bootstrap move refs entries=%s", bootstrap_stats.get("entry_count", 0))
    kaggle_bootstrap_entries = _build_kaggle_bootstrap_entries(kaggle_rows_by_game)
    if kaggle_bootstrap_entries:
        persist_move_reference_cache(kaggle_bootstrap_entries, silver_dir=silver_dir)

    reference_context = load_reference_context(silver_dir=silver_dir)
    boss_teams, boss_move_data = extract_boss_teams_from_kaggle_source(bronze_dir, allowed_versions=allowed_versions, reference_context=reference_context)
    all_move_data = dict(boss_move_data)

    for pattern in [
        "source_teams_*.parquet",
        "source_team_members_*.parquet",
        "member_moveset_combos_*.parquet",
        "member_move_options_*.parquet",
        "pokemon_moveset_options_*.parquet",
        "simulation_sampling_plan_*.parquet",
        "pokemon_combat_pool_*.parquet",
    ]:
        for old_file in simulation_dir.glob(pattern):
            old_file.unlink()

    team_values: dict[str, set[str]] = {"team_id": set(), "game_version": set()}
    member_values: dict[str, set[str]] = {"team_member_id": set(), "team_id": set(), "game_version": set()}
    move_values: dict[str, set[str]] = {
        "team_member_id": set(),
        "team_id": set(),
        "move_1": set(),
        "move_2": set(),
        "move_3": set(),
        "move_4": set(),
    }

    total_source_teams = 0
    total_members = 0
    total_moveset_combos = 0
    total_boss_teams = 0

    boss_teams_by_game: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for team in boss_teams:
        game_version = str(team.get("game_version") or "").strip().lower()
        if game_version:
            boss_teams_by_game[game_version].append(team)

    for game_key, records in records_with_game_keys:
        paths = _game_output_paths(simulation_dir, game_key)
        boss_teams_game = boss_teams_by_game.get(game_key, [])
        progression_source_teams = build_progression_source_teams(records, boss_teams_game)
        player_compact = build_player_team_compact_tables(progression_source_teams, reference_context)
        boss_compact = _build_boss_compact_tables(boss_teams_game, boss_move_data)

        source_teams_rows = list(boss_compact["source_teams"]) + [
            {**row, "is_player_candidate": True} for row in player_compact["source_teams"]
        ]
        source_member_rows = list(boss_compact["source_team_members"]) + list(player_compact["source_team_members"])
        member_move_rows = list(boss_compact["member_move_options"]) + list(player_compact["member_move_options"])
        member_moveset_combo_rows = list(player_compact["member_moveset_combos"])
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

        team_values["team_id"].update(str(row.get("source_team_id") or "").strip().lower() for row in source_teams_rows if str(row.get("source_team_id") or "").strip())
        team_values["game_version"].update(str(row.get("game_version") or "").strip().lower() for row in source_teams_rows if str(row.get("game_version") or "").strip())

        member_values["team_member_id"].update(str(row.get("team_member_id") or "").strip().lower() for row in source_member_rows if str(row.get("team_member_id") or "").strip())
        member_values["team_id"].update(str(row.get("source_team_id") or "").strip().lower() for row in source_member_rows if str(row.get("source_team_id") or "").strip())
        member_values["game_version"].update(str(row.get("game_version") or "").strip().lower() for row in source_member_rows if str(row.get("game_version") or "").strip())

        move_values["team_member_id"].update(
            str(row.get("pokemon_instance_id") or "").strip().lower()
            for row in member_moveset_combo_rows
            if str(row.get("pokemon_instance_id") or "").strip()
        )
        move_values["team_id"].update(
            str(row.get("team_id") or "").strip().lower()
            for row in member_moveset_combo_rows
            if str(row.get("team_id") or "").strip()
        )
        for move_col in ("move_1", "move_2", "move_3", "move_4"):
            move_values[move_col].update(
                str(row.get(move_col) or "").strip().lower()
                for row in member_moveset_combo_rows
                if str(row.get(move_col) or "").strip()
            )

        total_source_teams += len(source_teams_rows)
        total_members += len(source_member_rows)
        total_moveset_combos += len(member_moveset_combo_rows)
        total_boss_teams += len(boss_teams_game)

        logger.info(
            "[silver] wrote compact team shards game=%s source_teams=%s members=%s moveset_combos=%s move_options=%s",
            game_key,
            len(source_teams_rows),
            len(source_member_rows),
            len(member_moveset_combo_rows),
            len(member_move_rows),
        )

        gc.collect()

    write_validated_move_data(simulation_dir / "move_data.parquet", all_move_data, chunk_threshold=120_000, chunk_size=40_000)

    move_reference_df = read_parquet(move_reference_path) if move_reference_path.exists() else pd.DataFrame()
    learnable_reference_df = read_parquet(learnable_moves_path) if learnable_moves_path.exists() else pd.DataFrame()
    pokemon_data_df = read_parquet(references_dir / "pokemon_data.parquet") if (references_dir / "pokemon_data.parquet").exists() else pd.DataFrame()

    relational_report = validate_normalized_silver_tables(
        {
            "games": pd.DataFrame(games_table),
            "bosses": pd.DataFrame(bosses_table),
            "locations": pd.DataFrame(locations_table),
            "encounters": encounters_frame,
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

    create_silver_manifest(silver_dir)

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

    logger.info(
        "[silver] build finished records=%s source_teams=%s source_team_members=%s member_moveset_combos=%s unmapped=%s elapsed_s=%.2f",
        len(all_records),
        total_source_teams,
        total_members,
        total_moveset_combos,
        len(mapper.misses),
        time.perf_counter() - started_at,
    )


if __name__ == "__main__":
    build_silver_from_bronze()
