from collections import Counter, defaultdict
import logging
import time
from pathlib import Path
from typing import Any, cast

from src.pipeline.common.io import read_json, write_json, write_jsonl, write_parquet
from src.pipeline.settings import BRONZE_DIR, SILVER_DIR, ensure_medallion_dirs, get_silver_subdirs
from src.pipeline.bronze.inputs.create_type_chart import build_type_chart, save_as_json
from src.pipeline.silver.inputs.game_config import get_games_config
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
from src.pipeline.silver.inputs.kaggle_teams import build_member_movesets_dataset, extract_kaggle_teams
from src.pipeline.silver.simulation.team_moveset_combinations import build_team_moveset_combinations
from src.pipeline.silver.reporting.silver_manifest import create_silver_manifest
from src.pipeline.silver.enrichment.schema_normalizer import (
    write_normalized_silver,
    create_pokemon_reference_index,
    create_encounter_methods_reference,
)


logger = logging.getLogger(__name__)


def _cleanup_legacy_silver_artifacts(silver_dir: Path, silver_subdirs: dict[str, Path]) -> None:
    """Remove deprecated artifacts that are no longer part of current silver outputs."""
    removed: list[Path] = []

    simulation_dir = silver_subdirs["simulation"]
    current_simulation_outputs = {
        "teams.parquet",
        "teams.jsonl",
        "member_movesets.parquet",
        "starter_team_moveset_combinations.parquet",
        "boss_teams.parquet",
        "player_teams.parquet",
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


def _split_team_roles(teams_data: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    boss_teams: list[dict[str, Any]] = []
    player_teams: list[dict[str, Any]] = []
    fallback_to_boss_count = 0
    for team in teams_data:
        role = str(team.get("team_role") or "").strip().lower()
        if role == "player" or bool(team.get("is_player_candidate")):
            player_teams.append(team)
            continue
        if role == "boss" or isinstance(team.get("boss_name"), str):
            boss_teams.append(team)
            continue
        fallback_to_boss_count += 1
        boss_teams.append(team)

    return boss_teams, player_teams


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
            "Bronze inputs are missing. Run the bronze step first: python -m pipeline.run_pipeline --layer bronze"
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
    logger.info("[silver] processing %s bulbapedia game files", len(game_files))

    for game_index, game_file in enumerate(game_files, start=1):
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
    area_map, location_pokemon_map = get_location_area_and_pokemon_maps(all_slugs, allowed_versions=allowed_versions)
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

    # Clear simulation outputs before writing to avoid stale datasets from prior runs.
    for simulation_output in [
        "teams.parquet",
        "teams.jsonl",
        "member_movesets.parquet",
        "member_movesets.jsonl",
        "starter_team_moveset_combinations.parquet",
        "boss_teams.parquet",
        "player_teams.parquet",
    ]:
        simulation_output_path = simulation_dir / simulation_output
        if simulation_output_path.exists():
            simulation_output_path.unlink()

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

    # Build battle simulation data
    # Extract teams from Kaggle dataset (primary source)
    logger.info("[silver] extracting teams for simulation")
    teams_started_at = time.perf_counter()
    teams_data = extract_kaggle_teams(
        bronze_dir,
        allowed_versions=allowed_versions,
    )
    logger.info(
        "[silver] team extraction done teams=%s elapsed_s=%.2f",
        len(teams_data),
        time.perf_counter() - teams_started_at,
    )

    # If no Kaggle teams, try to extract from boss_mapping (fallback)
    if not teams_data:
        fallback_started_at = time.perf_counter()
        logger.warning("[silver] kaggle team extraction returned empty; building fallback teams from boss mapping")
        for game_version, bosses_dict in boss_mapping_by_version.items():
            for boss_name, boss_info in bosses_dict.items():
                if isinstance(boss_info, dict) and "teams" in boss_info:
                    for team_idx, team_info in enumerate(boss_info["teams"]):
                        team_id = f"TEAM_{game_version}_{boss_name}_{team_idx}"
                        teams_data.append({
                            "team_id": team_id,
                            "boss_name": boss_name,
                            "game_version": game_version,
                            "pokemon": team_info.get("pokemon", []),
                            "level": team_info.get("level", 20),
                            "team_role": "boss",
                            "is_player_candidate": False,
                        })
        logger.info(
            "[silver] fallback team build done teams=%s elapsed_s=%.2f",
            len(teams_data),
            time.perf_counter() - fallback_started_at,
        )
    if teams_data:
        boss_teams, player_teams = _split_team_roles(teams_data)
        write_parquet(simulation_dir / "teams.parquet", teams_data)
        write_jsonl(simulation_dir / "teams.jsonl", teams_data)
        write_parquet(simulation_dir / "boss_teams.parquet", boss_teams)
        write_parquet(simulation_dir / "player_teams.parquet", player_teams)
        member_movesets = build_member_movesets_dataset(player_teams)
        write_parquet(simulation_dir / "member_movesets.parquet", member_movesets)
        team_moveset_rows = build_team_moveset_combinations(silver_dir=silver_dir)
        logger.info(
            "[silver] wrote teams.parquet boss_teams=%s player_teams=%s member_movesets=%s",
            len(boss_teams),
            len(player_teams),
            len(member_movesets),
        )
    else:
        logger.warning("[silver] no teams found; simulation outputs will be skipped")

    # Create manifest of available data
    create_silver_manifest(silver_dir)

    logger.info("[silver] build finished unmapped_events=%s records=%s elapsed_s=%.2f", len(mapper.misses), len(all_records), time.perf_counter() - started_at)




if __name__ == "__main__":
    build_silver_from_bronze()
