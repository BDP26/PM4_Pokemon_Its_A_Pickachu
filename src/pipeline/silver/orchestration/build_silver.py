from collections import Counter, defaultdict
import gc
import logging
import shutil
import time
from pathlib import Path
from typing import Any, cast

import pandas as pd

from src.pipeline.common.io import read_json, read_jsonl, read_parquet, write_json, write_parquet
from src.pipeline.settings import BRONZE_DIR, SILVER_DIR, ensure_medallion_dirs, get_silver_subdirs
from src.pipeline.bronze.inputs.create_type_chart import build_type_chart, save_as_json
from src.pipeline.silver.config.game_config import get_games_config
from src.pipeline.silver.config.team_config import resolve_runtime_team_config
from src.pipeline.common.simulation_config import load_runtime_battle_policy_config
from src.pipeline.silver.inputs.kaggle_boss_mapping import (
    load_kaggle_rows_by_game,
)
from src.pipeline.silver.inputs.location_mapper import LocationMapper
from src.pipeline.silver.enrichment.location_pokemon_enrichment import (
    enrich_records_with_location_pokemon,
    get_location_area_and_pokemon_maps,
)
from src.pipeline.silver.inputs.builders.player_teams import (
    build_player_teams_from_progression_context,
    build_progression_source_teams,
)
from src.pipeline.silver.orchestration.stages import run_parse_stage
from src.pipeline.silver.inputs.sources.boss_teams import extract_boss_teams_from_kaggle_source
from src.pipeline.silver.inputs.connectors.pokeapi_moves import (
    bootstrap_move_reference_cache,
    persist_move_reference_cache,
)
from src.pipeline.silver.inputs.reference_context import load_reference_context, normalize_move_name, normalize_species_slug
from src.pipeline.silver.reporting.silver_manifest import create_silver_manifest
from src.pipeline.silver.enrichment.schema_normalizer import (
    write_normalized_silver,
    create_pokemon_reference_index,
    create_encounter_methods_reference,
)
from src.pipeline.silver.writers.outputs import (
    build_input_signature,
    fingerprint_path,
    fingerprint_python_files,
    load_state,
    save_state,
    write_validated_move_data,
)
from src.pipeline.silver.transforms.normalized_tables import (
    build_bosses_table,
    build_games_table,
    build_locations_table,
    build_team_member_moves_table,
    build_team_members_table,
)
from src.pipeline.silver.schemas.relational_checks import validate_normalized_silver_tables
from src.pipeline.silver.schemas.contracts import validate_team_payloads


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


def _top_counts(values: list[str], limit: int = 5) -> dict[str, int]:
    return dict(Counter(values).most_common(limit))


def _remove_if_exists(path: Path) -> None:
    if not path.exists():
        return
    if path.is_dir():
        shutil.rmtree(path)
    else:
        path.unlink()


def _build_team_metadata_rows(teams: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for team in teams:
        rows.append(
            {
                "team_id": team.get("team_id"),
                "game_version": team.get("game_version"),
                "team_role": team.get("team_role"),
                "boss_name": team.get("boss_name"),
                "gym": team.get("gym"),
                "is_player_candidate": bool(team.get("is_player_candidate", False)),
                "starter_base": team.get("starter_base"),
                "starter_evolved_species": team.get("starter_evolved_species"),
                "source_team_id": team.get("source_team_id"),
                "avg_level": team.get("avg_level"),
            }
        )
    return rows


def _game_output_paths(simulation_dir: Path, game_key: str) -> dict[str, Path]:
    return {
        "teams": simulation_dir / f"teams_{game_key}.parquet",
        "team_members": simulation_dir / f"team_members_{game_key}.parquet",
        "team_member_moves": simulation_dir / f"team_member_moves_{game_key}.parquet",
        "combat_pool": simulation_dir / f"pokemon_combat_pool_{game_key}.parquet",
    }


def _validation_profile(values_by_column: dict[str, set[str]], row_count: int) -> dict[str, Any]:
    return {"row_count": row_count, "columns": {column: sorted(values) for column, values in values_by_column.items()}}


def _build_combat_pool_rows_for_game(
    source_teams: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[tuple[str, str, int]] = set()

    for team in source_teams:
        game_version = str(team.get("game_version") or "").strip().lower()
        avg_level = int(team.get("avg_level") or 0)
        pokemon = team.get("pokemon", [])
        if not game_version or avg_level <= 0 or not isinstance(pokemon, list):
            continue

        for species in pokemon:
            species_name = str(species or "").strip().lower()
            if not species_name:
                continue
            key = (game_version, species_name, avg_level)
            if key in seen:
                continue
            seen.add(key)
            rows.append(
                {
                    "game_version": game_version,
                    "pokemon_species": species_name,
                    "level": avg_level,
                }
            )

    return rows


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
                            level = int(
                                encounter.get("level_max")
                                or encounter.get("level")
                                or record.get("boss_avg_level")
                                or 20
                            )
                        except (TypeError, ValueError):
                            level = 20
                        bootstrap_entries.append((species, max(level, 1), game_key, []))

            reachable_species = record.get("reachable_location_pokemon", {})
            if isinstance(reachable_species, dict):
                for species_list in reachable_species.values():
                    if not isinstance(species_list, list):
                        continue
                    for species in species_list[:12]:
                        species_name = str(species or "").strip().lower()
                        if not species_name:
                            continue
                        bootstrap_entries.append((species_name, 20, game_key, []))

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


def build_silver_from_bronze(
    bronze_dir: Path = BRONZE_DIR,
    silver_dir: Path = SILVER_DIR,
    hard_cleanup: bool = False,
) -> None:
    started_at = time.perf_counter()
    stage_durations: dict[str, float] = {}
    peak_counts: dict[str, int] = {
        "all_move_data_size": 0,
        "max_team_member_moves_per_game": 0,
        "max_team_members_per_game": 0,
    }
    ensure_medallion_dirs()

    type_chart_path = bronze_dir / "type_chart.json"
    if not type_chart_path.exists():
        chart = build_type_chart()
        save_as_json(chart, type_chart_path)

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
        logger.info("[silver] hard cleanup enabled; removing prior silver artifacts")
        for cleanup_path in (snapshots_dir, mappings_dir, references_dir, diagnostics_dir, simulation_dir):
            _remove_if_exists(cleanup_path)
            cleanup_path.mkdir(parents=True, exist_ok=True)

    location_index_path = bronze_dir / "pokeapi" / "location_index.json"
    bulbapedia_dir = bronze_dir / "bulbapedia"

    if not location_index_path.exists() or not bulbapedia_dir.exists():
        raise FileNotFoundError(
            "Bronze inputs are missing. Run the bronze step first: python -m src.pipeline.run_pipeline layers bronze"
        )

    games_config = get_games_config()
    allowed_versions = {game["game_key"] for game in games_config}
    runtime_team_config = resolve_runtime_team_config()
    runtime_simulation_config = load_runtime_battle_policy_config().__dict__

    location_index = cast(dict[str, Any], read_json(location_index_path))
    mapper = LocationMapper(location_index)
    kaggle_rows_by_game = load_kaggle_rows_by_game(bronze_dir)

    game_files = sorted(bulbapedia_dir.glob("*.json"))
    state_dir = silver_dir / "_state"
    state_dir.mkdir(parents=True, exist_ok=True)
    state_path = state_dir / "silver_state.json"
    repo_root = Path(__file__).resolve().parents[4]
    code_fingerprint = fingerprint_python_files(
        [
            repo_root / "src" / "pipeline" / "silver",
            repo_root / "src" / "pipeline" / "common" / "simulation_config.py",
            repo_root / "src" / "pipeline" / "settings.py",
        ]
    )
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
    expected_team_shards = [simulation_dir / f"teams_{game['game_key']}.parquet" for game in games_config]
    expected_member_shards = [simulation_dir / f"team_members_{game['game_key']}.parquet" for game in games_config]
    expected_move_shards = [simulation_dir / f"team_member_moves_{game['game_key']}.parquet" for game in games_config]
    expected_combat_pool_shards = [simulation_dir / f"pokemon_combat_pool_{game['game_key']}.parquet" for game in games_config]
    skip_outputs = (
        expected_outputs
        + expected_snapshot_files
        + expected_team_shards
        + expected_member_shards
        + expected_move_shards
        + expected_combat_pool_shards
    )
    if previous_state.get("input_signature") == current_signature and all(path.exists() for path in skip_outputs):
        logger.info("[silver] incremental skip; input signature unchanged")
        return

    logger.info("[silver] processing %s bulbapedia game files", len(game_files))

    parse_started_at = time.perf_counter()
    parse_output = run_parse_stage(
        game_files=game_files,
        mapper=mapper,
        kaggle_rows_by_game=kaggle_rows_by_game,
    )
    stage_durations["parse_stage_s"] = time.perf_counter() - parse_started_at
    all_records = parse_output.all_records
    all_slugs = parse_output.all_slugs
    boss_mapping_by_version = parse_output.boss_mapping_by_version
    records_with_game_keys = parse_output.records_with_game_keys

    logger.info(
        "[silver] parsing complete games_with_records=%s total_records=%s unique_location_slugs=%s",
        len(records_with_game_keys),
        len(all_records),
        len(set(all_slugs)),
    )

    mapping_started_at = time.perf_counter()
    area_map, location_pokemon_map = get_location_area_and_pokemon_maps(
        all_slugs,
        allowed_versions=allowed_versions,
        silver_dir=silver_dir,
        bronze_dir=bronze_dir,
    )
    logger.info(
        "[silver] mapping locations+pokemon done elapsed_s=%.2f locations=%s pokemon_locations=%s",
        time.perf_counter() - mapping_started_at,
        len(area_map),
        len(location_pokemon_map),
    )
    stage_durations["mapping_stage_s"] = time.perf_counter() - mapping_started_at
    write_json(mappings_dir / "location_to_area_map.json", area_map)
    write_json(mappings_dir / "location_to_pokemon_map.json", location_pokemon_map)

    encounters_file = references_dir / "encounters.jsonl"
    if encounters_file.exists():
        encounters_file.unlink()

    for snapshot_file in snapshots_dir.glob("*_boss_snapshots.jsonl"):
        snapshot_file.unlink()

    all_pokemon_references = {}

    normalization_started_at = time.perf_counter()
    for game_key, records in records_with_game_keys:
        enrich_records_with_location_pokemon(records, location_pokemon_map)
        pokemon_refs = write_normalized_silver(
            records=records,
            snapshots_dir=snapshots_dir,
            encounters_output_path=encounters_file,
            game_key=game_key,
        )
        if pokemon_refs:
            all_pokemon_references.update(pokemon_refs)

    logger.info(
        "[silver] normalization complete snapshots=%s pokemon_reference_entries=%s",
        len(records_with_game_keys),
        len(all_pokemon_references),
    )
    stage_durations["normalization_stage_s"] = time.perf_counter() - normalization_started_at

    create_pokemon_reference_index(all_pokemon_references, references_dir)
    create_encounter_methods_reference(all_records, references_dir)

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

    diagnostics_write_started_at = time.perf_counter()
    write_json(diagnostics_dir / "unmapped_locations_detailed.json", mapper.misses)

    unmapped_summary = summarize_unmapped_locations(mapper.misses)
    write_json(diagnostics_dir / "unmapped_locations_summary.json", unmapped_summary)

    compact_unmapped = [
        {
            "raw_title": miss["raw_title"],
            "tried_slug": miss["tried_slug"],
            "reason": miss["reason"],
        }
        for miss in mapper.misses
    ]
    write_json(diagnostics_dir / "unmapped_locations.json", compact_unmapped)
    del compact_unmapped
    gc.collect()

    logger.info(
        "[silver] diagnostics written unmapped_events=%s top_reasons=%s",
        len(mapper.misses),
        _top_counts([str(miss.get("reason", "unknown")) for miss in mapper.misses], limit=3),
    )
    stage_durations["diagnostics_writes_s"] = time.perf_counter() - diagnostics_write_started_at

    write_json(mappings_dir / "boss_mapping_by_version.json", boss_mapping_by_version)

    move_reference_path = references_dir / "move_reference.parquet"
    learnable_moves_path = references_dir / "learnable_moves.parquet"
    legacy_pokemon_learnable_moves_path = references_dir / "pokemon_learnable_moves.parquet"
    has_learnable_reference = learnable_moves_path.exists()
    if not move_reference_path.exists() or not has_learnable_reference:
        logger.info("[silver] bootstrapping move reference parquet before team extraction")
        bootstrap_entries = _build_bootstrap_move_entries(records_with_game_keys)
        bootstrap_stats = bootstrap_move_reference_cache(bootstrap_entries, silver_dir=silver_dir)
        logger.info(
            "[silver] bootstrap move references done entries=%s target_pairs=%s learnable_rows=%s move_rows=%s",
            bootstrap_stats.get("entry_count", 0),
            bootstrap_stats.get("target_pairs", 0),
            bootstrap_stats.get("learnable_rows", 0),
            bootstrap_stats.get("move_rows", 0),
        )
    kaggle_bootstrap_entries = _build_kaggle_bootstrap_entries(kaggle_rows_by_game)
    if kaggle_bootstrap_entries:
        persisted_kaggle = persist_move_reference_cache(kaggle_bootstrap_entries, silver_dir=silver_dir)
        logger.info(
            "[silver] persisted kaggle move cache entries=%s target_pairs=%s learnable_rows=%s move_rows=%s",
            persisted_kaggle.get("entry_count", 0),
            persisted_kaggle.get("target_pairs", 0),
            persisted_kaggle.get("learnable_rows", 0),
            persisted_kaggle.get("move_rows", 0),
        )
    if legacy_pokemon_learnable_moves_path.exists():
        legacy_pokemon_learnable_moves_path.unlink()
        logger.info("[silver] removed legacy learnable move parquet output=%s", legacy_pokemon_learnable_moves_path.name)

    reference_context = load_reference_context(silver_dir=silver_dir)
    logger.info(
        "[silver] loaded offline reference context move_profiles=%s species_pairs=%s",
        len(reference_context.move_profiles),
        len(reference_context.learnable_by_game_species),
    )

    logger.info("[silver] extracting boss teams for simulation")
    teams_started_at = time.perf_counter()
    boss_teams, boss_move_data = extract_boss_teams_from_kaggle_source(
        bronze_dir,
        allowed_versions=allowed_versions,
        reference_context=reference_context,
    )

    all_move_data = dict(boss_move_data)
    peak_counts["all_move_data_size"] = max(peak_counts["all_move_data_size"], len(all_move_data))
    logger.info("[silver] all_move_data initialized size=%s", len(all_move_data))

    for pattern in [
        "teams_*.parquet",
        "team_members_*.parquet",
        "team_member_moves_*.parquet",
        "pokemon_combat_pool_*.parquet",
    ]:
        for old_file in simulation_dir.glob(pattern):
            old_file.unlink()

    total_boss_teams = 0
    total_player_teams = 0
    total_team_members = 0
    total_team_member_moves = 0
    team_metadata_values: dict[str, set[str]] = {
        "team_id": set(),
        "game_version": set(),
    }
    team_member_values: dict[str, set[str]] = {
        "team_member_id": set(),
        "team_id": set(),
        "game_version": set(),
    }
    team_member_move_values: dict[str, set[str]] = {
        "team_member_id": set(),
        "team_id": set(),
    }

    boss_teams_by_game: dict[str, list[dict[str, Any]]] = defaultdict(list)
    learnable_reference_df = read_parquet(learnable_moves_path) if learnable_moves_path.exists() else pd.DataFrame()

    for team in boss_teams:
        game_version = str(team.get("game_version") or "").strip().lower()
        if game_version:
            boss_teams_by_game[game_version].append(team)

    for game_key, records in records_with_game_keys:
        logger.info("[silver] building teams for game=%s records=%s", game_key, len(records))
        game_started_at = time.perf_counter()

        boss_teams_game = boss_teams_by_game.get(game_key, [])
        progression_source_teams = build_progression_source_teams(records, boss_teams_game)

        combat_pool_rows = _build_combat_pool_rows_for_game(progression_source_teams)
        paths = _game_output_paths(simulation_dir, game_key)
        write_parquet(paths["combat_pool"], combat_pool_rows)

        player_teams, player_move_data = build_player_teams_from_progression_context(
            progression_source_teams,
            reference_context=reference_context,
        )
        all_move_data.update(player_move_data)
        current_move_count = len(all_move_data)
        peak_counts["all_move_data_size"] = max(peak_counts["all_move_data_size"], current_move_count)
        if current_move_count % 10_000 == 0 or len(player_move_data) >= 2_000:
            logger.info(
                "[silver] all_move_data growth game=%s size=%s added_this_game=%s",
                game_key,
                current_move_count,
                len(player_move_data),
            )

        teams_data_game = boss_teams_game + player_teams
        if not teams_data_game:
            logger.warning("[silver] no teams for game=%s", game_key)
            continue

        validated_teams = validate_team_payloads(teams_data_game)
        team_metadata_rows = _build_team_metadata_rows(validated_teams)
        team_members = build_team_members_table(validated_teams)
        team_member_moves = build_team_member_moves_table(validated_teams, all_move_data)

        for row in team_metadata_rows:
            team_id = str(row.get("team_id") or "").strip().lower()
            game_version = str(row.get("game_version") or "").strip().lower()
            if team_id:
                team_metadata_values["team_id"].add(team_id)
            if game_version:
                team_metadata_values["game_version"].add(game_version)

        for row in team_members:
            team_member_id = str(row.get("team_member_id") or "").strip().lower()
            team_id = str(row.get("team_id") or "").strip().lower()
            game_version = str(row.get("game_version") or "").strip().lower()
            if team_member_id:
                team_member_values["team_member_id"].add(team_member_id)
            if team_id:
                team_member_values["team_id"].add(team_id)
            if game_version:
                team_member_values["game_version"].add(game_version)

        for row in team_member_moves:
            team_member_id = str(row.get("team_member_id") or "").strip().lower()
            team_id = str(row.get("team_id") or "").strip().lower()
            if team_member_id:
                team_member_move_values["team_member_id"].add(team_member_id)
            if team_id:
                team_member_move_values["team_id"].add(team_id)

        write_parquet(paths["teams"], team_metadata_rows)
        write_parquet(paths["team_members"], team_members)
        write_parquet(paths["team_member_moves"], team_member_moves)
        peak_counts["max_team_members_per_game"] = max(peak_counts["max_team_members_per_game"], len(team_members))
        peak_counts["max_team_member_moves_per_game"] = max(peak_counts["max_team_member_moves_per_game"], len(team_member_moves))

        total_boss_teams += len(boss_teams_game)
        total_player_teams += len(player_teams)
        total_team_members += len(team_members)
        total_team_member_moves += len(team_member_moves)

        logger.info(
            "[silver] wrote team shards game=%s boss_teams=%s player_teams=%s team_members=%s elapsed_s=%.2f",
            game_key,
            len(boss_teams_game),
            len(player_teams),
            len(team_members),
            time.perf_counter() - game_started_at,
        )

        del progression_source_teams
        del player_teams
        del player_move_data
        del teams_data_game
        del validated_teams
        del team_metadata_rows
        del team_members
        del team_member_moves
        del combat_pool_rows
        gc.collect()

    move_write_started_at = time.perf_counter()
    write_validated_move_data(
        simulation_dir / "move_data.parquet",
        all_move_data,
        chunk_threshold=120_000,
        chunk_size=40_000,
    )
    stage_durations["move_write_s"] = time.perf_counter() - move_write_started_at
    del all_move_data
    gc.collect()
    move_reference_df = read_parquet(move_reference_path) if move_reference_path.exists() else pd.DataFrame()

    relational_started_at = time.perf_counter()
    relational_report = validate_normalized_silver_tables(
        {
            "games": pd.DataFrame(games_table),
            "bosses": pd.DataFrame(bosses_table),
            "locations": pd.DataFrame(locations_table),
            "encounters": encounters_frame,
            "teams": _validation_profile(team_metadata_values, total_boss_teams + total_player_teams),
            "team_members": _validation_profile(team_member_values, total_team_members),
            "team_member_moves": _validation_profile(team_member_move_values, total_team_member_moves),
            "move_reference": move_reference_df,
            "learnable_moves": learnable_reference_df,
        }
    )
    stage_durations["relational_validation_s"] = time.perf_counter() - relational_started_at
    write_json(diagnostics_dir / "relational_validation.json", relational_report.as_dict())
    del move_reference_df
    del learnable_reference_df
    del encounters_frame
    gc.collect()
    stage_started_at = time.perf_counter()
    logger.info(
        "[silver] stage=write_validated_move_data start total_player_teams=%s total_team_members=%s move_records=%s",
        total_player_teams,
        total_team_members,
        len(all_move_data),
    )
    try:
        write_validated_move_data(simulation_dir / "move_data.parquet", all_move_data)
    except Exception:
        logger.exception(
            "[silver] stage=write_validated_move_data error total_player_teams=%s total_team_members=%s move_records=%s",
            total_player_teams,
            total_team_members,
            len(all_move_data),
        )
        raise
    logger.info(
        "[silver] stage=write_validated_move_data done elapsed_s=%.2f rows=%s total_player_teams=%s total_team_members=%s",
        time.perf_counter() - stage_started_at,
        len(all_move_data),
        total_player_teams,
        total_team_members,
    )

    stage_started_at = time.perf_counter()
    logger.info(
        "[silver] stage=read_move_reference_parquet start total_player_teams=%s total_team_members=%s move_records=%s",
        total_player_teams,
        total_team_members,
        len(all_move_data),
    )
    try:
        move_reference_df = read_parquet(move_reference_path) if move_reference_path.exists() else pd.DataFrame()
    except Exception:
        logger.exception(
            "[silver] stage=read_move_reference_parquet error total_player_teams=%s total_team_members=%s move_records=%s",
            total_player_teams,
            total_team_members,
            len(all_move_data),
        )
        raise
    logger.info(
        "[silver] stage=read_move_reference_parquet done elapsed_s=%.2f rows=%s total_player_teams=%s total_team_members=%s",
        time.perf_counter() - stage_started_at,
        len(move_reference_df),
        total_player_teams,
        total_team_members,
    )

    stage_started_at = time.perf_counter()
    logger.info(
        "[silver] stage=validate_normalized_silver_tables start total_player_teams=%s total_team_members=%s move_records=%s",
        total_player_teams,
        total_team_members,
        len(all_move_data),
    )
    try:
        relational_report = validate_normalized_silver_tables(
            {
                "games": pd.DataFrame(games_table),
                "bosses": pd.DataFrame(bosses_table),
                "locations": pd.DataFrame(locations_table),
                "encounters": encounters_frame,
                "teams": _series_frame(team_metadata_values),
                "team_members": _series_frame(team_member_values),
                "team_member_moves": _series_frame(team_member_move_values),
                "move_reference": move_reference_df,
                "learnable_moves": learnable_reference_df,
            }
        )
    except Exception:
        logger.exception(
            "[silver] stage=validate_normalized_silver_tables error total_player_teams=%s total_team_members=%s move_records=%s",
            total_player_teams,
            total_team_members,
            len(all_move_data),
        )
        raise
    logger.info(
        "[silver] stage=validate_normalized_silver_tables done elapsed_s=%.2f rows=%s total_player_teams=%s total_team_members=%s",
        time.perf_counter() - stage_started_at,
        len(relational_report.issues),
        total_player_teams,
        total_team_members,
    )

    stage_started_at = time.perf_counter()
    logger.info(
        "[silver] stage=write_relational_validation_diagnostics start total_player_teams=%s total_team_members=%s move_records=%s",
        total_player_teams,
        total_team_members,
        len(all_move_data),
    )
    try:
        write_json(diagnostics_dir / "relational_validation.json", relational_report.as_dict())
    except Exception:
        logger.exception(
            "[silver] stage=write_relational_validation_diagnostics error total_player_teams=%s total_team_members=%s move_records=%s",
            total_player_teams,
            total_team_members,
            len(all_move_data),
        )
        raise
    logger.info(
        "[silver] stage=write_relational_validation_diagnostics done elapsed_s=%.2f rows=%s total_player_teams=%s total_team_members=%s",
        time.perf_counter() - stage_started_at,
        len(relational_report.issues),
        total_player_teams,
        total_team_members,
    )
    if not relational_report.is_valid:
        raise ValueError("Silver relational validation failed; see diagnostics/relational_validation.json")

    logger.info(
        "[silver] team extraction done boss_teams=%s player_teams=%s team_members=%s move_records=%s elapsed_s=%.2f",
        total_boss_teams,
        total_player_teams,
        total_team_members,
        peak_counts["all_move_data_size"],
        time.perf_counter() - teams_started_at,
    )

    stage_started_at = time.perf_counter()
    logger.info(
        "[silver] stage=create_silver_manifest start total_player_teams=%s total_team_members=%s move_records=%s",
        total_player_teams,
        total_team_members,
        len(all_move_data),
    )
    try:
        create_silver_manifest(silver_dir)
    except Exception:
        logger.exception(
            "[silver] stage=create_silver_manifest error total_player_teams=%s total_team_members=%s move_records=%s",
            total_player_teams,
            total_team_members,
            len(all_move_data),
        )
        raise
    logger.info(
        "[silver] stage=create_silver_manifest done elapsed_s=%.2f rows=%s total_player_teams=%s total_team_members=%s",
        time.perf_counter() - stage_started_at,
        len(records_with_game_keys),
        total_player_teams,
        total_team_members,
    )

    save_state(
        state_path,
        {
            "input_signature": current_signature,
            "updated_at": time.time(),
            "games_processed": len(records_with_game_keys),
            "boss_teams": total_boss_teams,
            "player_teams": total_player_teams,
            "move_records": peak_counts["all_move_data_size"],
            "runtime_team_config": runtime_team_config,
            "runtime_simulation_config": runtime_simulation_config,
            "pipeline_code_fingerprint": code_fingerprint,
        },
    stage_started_at = time.perf_counter()
    logger.info(
        "[silver] stage=save_state start total_player_teams=%s total_team_members=%s move_records=%s",
        total_player_teams,
        total_team_members,
        len(all_move_data),
    )
    try:
        save_state(
            state_path,
            {
                "input_signature": current_signature,
                "updated_at": time.time(),
                "games_processed": len(records_with_game_keys),
                "boss_teams": total_boss_teams,
                "player_teams": total_player_teams,
                "move_records": len(all_move_data),
                "runtime_team_config": runtime_team_config,
                "runtime_simulation_config": runtime_simulation_config,
                "pipeline_code_fingerprint": code_fingerprint,
            },
        )
    except Exception:
        logger.exception(
            "[silver] stage=save_state error total_player_teams=%s total_team_members=%s move_records=%s",
            total_player_teams,
            total_team_members,
            len(all_move_data),
        )
        raise
    logger.info(
        "[silver] stage=save_state done elapsed_s=%.2f rows=%s total_player_teams=%s total_team_members=%s",
        time.perf_counter() - stage_started_at,
        len(records_with_game_keys),
        total_player_teams,
        total_team_members,
    )

    write_json(
        diagnostics_dir / "performance_summary.json",
        {
            "generated_at_epoch_s": time.time(),
            "stage_durations_s": stage_durations,
            "peak_counts": peak_counts,
            "totals": {
                "games_processed": len(records_with_game_keys),
                "boss_teams": total_boss_teams,
                "player_teams": total_player_teams,
                "team_members": total_team_members,
                "team_member_moves": total_team_member_moves,
            },
        },
    )

    logger.info(
        "[silver] build finished unmapped_events=%s records=%s elapsed_s=%.2f",
        len(mapper.misses),
        len(all_records),
        time.perf_counter() - started_at,
    )


if __name__ == "__main__":
    build_silver_from_bronze()
