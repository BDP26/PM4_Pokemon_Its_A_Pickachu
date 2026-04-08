from collections import Counter, defaultdict
import logging
import time
from pathlib import Path
from typing import Any, cast

import pandas as pd

from src.pipeline.common.io import read_json, read_jsonl, write_json, write_jsonl, write_parquet
from src.pipeline.settings import BRONZE_DIR, SILVER_DIR, ensure_medallion_dirs, get_silver_subdirs
from src.pipeline.bronze.inputs.create_type_chart import build_type_chart, save_as_json
from src.pipeline.silver.config.game_config import get_games_config
from src.pipeline.silver.inputs.kaggle_boss_mapping import (
    build_boss_mapping_payload,
    build_harmonized_candidates_by_boss,
    enrich_boss_records,
    load_kaggle_rows_by_game,
)
from src.pipeline.silver.inputs.location_mapper import LocationMapper
from src.pipeline.silver.enrichment.location_pokemon_enrichment import (
    enrich_records_with_location_pokemon,
    get_location_area_and_pokemon_maps,
)
from src.pipeline.silver.inputs.parser import extract_game_data
from src.pipeline.silver.inputs.builders.player_teams import build_player_teams_from_progression_context
from src.pipeline.silver.inputs.sources.boss_teams import extract_boss_teams_from_kaggle_source
from src.pipeline.silver.reporting.silver_manifest import create_silver_manifest
from src.pipeline.silver.enrichment.schema_normalizer import (
    write_normalized_silver,
    create_pokemon_reference_index,
    create_encounter_methods_reference,
)
from src.pipeline.silver.writers.outputs import (
    build_input_signature,
    fingerprint_path,
    load_state,
    save_state,
    write_validated_move_data,
    write_validated_teams,
)
from src.pipeline.silver.transforms.normalized_tables import (
    build_bosses_table,
    build_games_table,
    build_learnable_moves_table,
    build_locations_table,
    build_move_reference_table,
    build_snapshot_available_pokemon_table,
    build_team_member_moves_table,
    build_team_members_table,
)
from src.pipeline.silver.schemas.relational_checks import validate_normalized_silver_tables


logger = logging.getLogger(__name__)


def _cleanup_legacy_silver_artifacts(silver_dir: Path, silver_subdirs: dict[str, Path]) -> None:
    """Remove deprecated artifacts that are no longer part of current silver outputs."""
    removed: list[Path] = []

    simulation_dir = silver_subdirs["simulation"]
    current_simulation_outputs = {
        "teams.parquet",
        "teams.jsonl",
        "boss_teams.parquet",
        "player_teams.parquet",
        "team_members.parquet",
        "team_member_moves.parquet",
        "move_data.json",
    }
    for artifact in simulation_dir.iterdir():
        if artifact.is_file() and artifact.name not in current_simulation_outputs:
            artifact.unlink()
            removed.append(artifact)

    # Legacy root-level snapshot layout (kept for old fallback paths only).
    for root_snapshot in silver_dir.glob("*_boss_snapshots.jsonl"):
        root_snapshot.unlink()
        removed.append(root_snapshot)

    if removed:
        logger.info("[silver] hard cleanup removed legacy artifacts count=%s", len(removed))


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


def build_silver_from_bronze(
    bronze_dir: Path = BRONZE_DIR,
    silver_dir: Path = SILVER_DIR,
    hard_cleanup: bool = False,
) -> None:
    started_at = time.perf_counter()
    ensure_medallion_dirs()

    # Ensure type chart is available
    type_chart_path = bronze_dir / "type_chart.json"
    if not type_chart_path.exists():
        chart = build_type_chart()
        save_as_json(chart, type_chart_path)

    silver_dir.mkdir(parents=True, exist_ok=True)
    silver_subdirs = get_silver_subdirs(silver_dir)
    for directory in silver_subdirs.values():
        directory.mkdir(parents=True, exist_ok=True)

    if hard_cleanup:
        _cleanup_legacy_silver_artifacts(silver_dir=silver_dir, silver_subdirs=silver_subdirs)

    snapshots_dir = silver_subdirs["snapshots"]
    mappings_dir = silver_subdirs["mappings"]
    references_dir = silver_subdirs["references"]
    diagnostics_dir = silver_subdirs["diagnostics"]
    simulation_dir = silver_subdirs["simulation"]

    location_index_path = bronze_dir / "pokeapi" / "location_index.json"
    bulbapedia_dir = bronze_dir / "bulbapedia"

    if not location_index_path.exists() or not bulbapedia_dir.exists():
        raise FileNotFoundError(
            "Bronze inputs are missing. Run the bronze step first: python -m src.pipeline.run_pipeline layers bronze"
        )

    # Get allowed versions from game config
    games_config = get_games_config()
    allowed_versions = {game["game_key"] for game in games_config}

    location_index = cast(dict[str, Any], read_json(location_index_path))
    mapper = LocationMapper(location_index)
    kaggle_rows_by_game = load_kaggle_rows_by_game(bronze_dir)

    all_records: list[dict] = []
    all_slugs: list[str] = []
    boss_mapping_by_version: dict[str, dict] = {}
    records_with_game_keys: list[tuple[str, list[dict]]] = []

    game_files = sorted(bulbapedia_dir.glob("*.json"))
    state_dir = silver_dir / "_state"
    state_dir.mkdir(parents=True, exist_ok=True)
    state_path = state_dir / "silver_state.json"
    kaggle_csv_path = bronze_dir / "kagglehub" / "gym_leaders_elite_four.csv"
    current_signature = build_input_signature(
        {
            "location_index": fingerprint_path(location_index_path),
            "bulbapedia": fingerprint_path(bulbapedia_dir),
            "kaggle": fingerprint_path(kaggle_csv_path) if kaggle_csv_path.exists() else None,
            "type_chart": fingerprint_path(type_chart_path),
            "allowed_versions": sorted(allowed_versions),
        }
    )
    previous_state = load_state(state_path)
    if previous_state.get("input_signature") == current_signature:
        expected_outputs = [
            snapshots_dir,
            mappings_dir / "location_to_area_map.json",
            mappings_dir / "location_to_pokemon_map.json",
            mappings_dir / "boss_mapping_by_version.json",
            references_dir / "pokemon_reference.json",
            references_dir / "encounter_methods_reference.json",
            references_dir / "games.parquet",
            references_dir / "bosses.parquet",
            references_dir / "locations.parquet",
            references_dir / "encounters.parquet",
            references_dir / "snapshot_available_pokemon.parquet",
            references_dir / "move_reference.parquet",
            references_dir / "learnable_moves.parquet",
            references_dir / "pokemon_learnable_moves.parquet",
            simulation_dir / "teams.parquet",
            simulation_dir / "boss_teams.parquet",
            simulation_dir / "player_teams.parquet",
            simulation_dir / "team_members.parquet",
            simulation_dir / "team_member_moves.parquet",
            simulation_dir / "move_data.json",
        ]
        if all(path.exists() for path in expected_outputs):
            logger.info("[silver] incremental skip; input signature unchanged")
            return
    logger.info("[silver] processing %s bulbapedia game files", len(game_files))

    for game_file in game_files:
        game_payload = cast(dict[str, Any], read_json(game_file))
        records = extract_game_data(game_payload, mapper)
        game_key = game_payload["game_key"]
        expected_bosses = game_payload.get("bosses", [])

        harmonized_candidates_by_boss = build_harmonized_candidates_by_boss(
            game_key=game_key,
            expected_bosses=expected_bosses,
            kaggle_rows_by_game=kaggle_rows_by_game,
        )
        records = enrich_boss_records(records, expected_bosses, harmonized_candidates_by_boss)
        boss_mapping_by_version[game_key] = build_boss_mapping_payload(
            game_key,
            expected_bosses,
            harmonized_candidates_by_boss,
        )

        if not records:
            logger.warning("[silver] skipped %s: no boss records extracted", game_file.name)
            continue

        all_records.extend(records)
        records_with_game_keys.append((game_key, records))
        for record in records:
            all_slugs.extend(record["reachable_locations"])

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
    )
    logger.info(
        "[silver] mapping locations+pokemon done elapsed_s=%.2f locations=%s pokemon_locations=%s",
        time.perf_counter() - mapping_started_at,
        len(area_map),
        len(location_pokemon_map),
    )
    write_json(mappings_dir / "location_to_area_map.json", area_map)
    write_json(mappings_dir / "location_to_pokemon_map.json", location_pokemon_map)

    # Clear encounters file if exists (for fresh aggregation)
    encounters_file = references_dir / "encounters.jsonl"
    if encounters_file.exists():
        encounters_file.unlink()

    # Clear snapshot files to avoid accumulating duplicate rows across reruns.
    removed_snapshots = 0
    for snapshot_file in snapshots_dir.glob("*_boss_snapshots.jsonl"):
        snapshot_file.unlink()
        removed_snapshots += 1

    all_pokemon_references = {}

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

    # Create centralized references
    create_pokemon_reference_index(all_pokemon_references, references_dir)
    create_encounter_methods_reference(all_records, references_dir)

    # Build normalized reference/progression tables.
    games_table = build_games_table(games_config)
    bosses_table = build_bosses_table(boss_mapping_by_version)
    locations_table = build_locations_table(all_records, area_map, mapper.misses)
    snapshot_available_pokemon = build_snapshot_available_pokemon_table(all_records)

    write_parquet(references_dir / "games.parquet", games_table, partition_cols=["region"])
    write_parquet(references_dir / "bosses.parquet", bosses_table, partition_cols=["game_version", "boss_role"])
    write_parquet(references_dir / "locations.parquet", locations_table, partition_cols=["game_version", "mapping_status"])
    write_parquet(
        references_dir / "snapshot_available_pokemon.parquet",
        snapshot_available_pokemon,
        partition_cols=["game_version", "boss_id"],
    )

    encounters_frame = pd.DataFrame()
    if encounters_file.exists():
        encounters_frame = read_jsonl(encounters_file)
        write_parquet(references_dir / "encounters.parquet", encounters_frame, partition_cols=["game"])

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

    logger.info(
        "[silver] diagnostics written unmapped_events=%s top_reasons=%s",
        len(mapper.misses),
        _top_counts([str(miss.get("reason", "unknown")) for miss in mapper.misses], limit=3),
    )

    write_json(mappings_dir / "boss_mapping_by_version.json", boss_mapping_by_version)

    # Build battle simulation data in separate stages (boss -> player)
    logger.info("[silver] extracting boss teams for simulation")
    teams_started_at = time.perf_counter()
    boss_teams, boss_move_data = extract_boss_teams_from_kaggle_source(
        bronze_dir,
        allowed_versions=allowed_versions,
    )
    player_teams, player_move_data = build_player_teams_from_progression_context(boss_teams)
    teams_data = boss_teams + player_teams
    all_move_data = {**boss_move_data, **player_move_data}
    logger.info(
        "[silver] team extraction done boss_teams=%s player_teams=%s teams_total=%s move_records=%s elapsed_s=%.2f",
        len(boss_teams),
        len(player_teams),
        len(teams_data),
        len(all_move_data),
        time.perf_counter() - teams_started_at,
    )

    if teams_data:
        validated_teams = write_validated_teams(
            simulation_dir / "teams.parquet",
            teams_data,
            partition_cols=["game_version", "team_role"],
        )
        write_jsonl(simulation_dir / "teams.jsonl", validated_teams)
        write_validated_teams(
            simulation_dir / "boss_teams.parquet",
            boss_teams,
            partition_cols=["game_version", "boss_name"],
        )
        write_validated_teams(
            simulation_dir / "player_teams.parquet",
            player_teams,
            partition_cols=["game_version"],
        )
        validated_move_data = write_validated_move_data(simulation_dir / "move_data.json", all_move_data)

        team_members = build_team_members_table(validated_teams)
        team_member_moves = build_team_member_moves_table(validated_teams, all_move_data)
        move_reference = build_move_reference_table(all_move_data)
        learnable_moves = build_learnable_moves_table(all_move_data)

        write_parquet(simulation_dir / "team_members.parquet", team_members, partition_cols=["game_version"])
        write_parquet(simulation_dir / "team_member_moves.parquet", team_member_moves, partition_cols=["game_version"])
        write_parquet(references_dir / "move_reference.parquet", move_reference)
        write_parquet(references_dir / "learnable_moves.parquet", learnable_moves, partition_cols=["game_version"])
        write_parquet(references_dir / "pokemon_learnable_moves.parquet", learnable_moves, partition_cols=["game_version", "pokemon_species"])

        relational_report = validate_normalized_silver_tables(
            {
                "games": pd.DataFrame(games_table),
                "bosses": pd.DataFrame(bosses_table),
                "locations": pd.DataFrame(locations_table),
                "encounters": encounters_frame,
                "snapshot_available_pokemon": pd.DataFrame(snapshot_available_pokemon),
                "teams": pd.DataFrame(validated_teams),
                "team_members": pd.DataFrame(team_members),
                "team_member_moves": pd.DataFrame(team_member_moves),
                "move_reference": pd.DataFrame(move_reference),
                "learnable_moves": pd.DataFrame(learnable_moves),
            }
        )
        write_json(diagnostics_dir / "relational_validation.json", relational_report.as_dict())
        if not relational_report.is_valid:
            raise ValueError("Silver relational validation failed; see diagnostics/relational_validation.json")

        logger.info(
            "[silver] wrote teams.parquet boss_teams=%s player_teams=%s team_members=%s move_records=%s",
            len(boss_teams),
            len(player_teams),
            len(team_members),
            len(validated_move_data),
        )
    else:
        logger.warning("[silver] no teams found; simulation outputs will be skipped")

    # Create manifest of available data
    create_silver_manifest(silver_dir)

    save_state(
        state_path,
        {
            "input_signature": current_signature,
            "updated_at": time.time(),
            "games_processed": len(records_with_game_keys),
            "boss_teams": len(boss_teams),
            "player_teams": len(player_teams),
            "move_records": len(all_move_data),
        },
    )

    logger.info("[silver] build finished unmapped_events=%s records=%s elapsed_s=%.2f", len(mapper.misses), len(all_records), time.perf_counter() - started_at)




if __name__ == "__main__":
    build_silver_from_bronze()
