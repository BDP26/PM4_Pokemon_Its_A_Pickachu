from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, cast

from src.pipeline.common.io import read_json, read_jsonl, write_json
from src.pipeline.settings import BRONZE_DIR, SILVER_DIR, ensure_medallion_dirs, get_silver_subdirs
from src.pipeline.silver.game_config import get_games_config
from src.pipeline.silver.kaggle_boss_mapping import (
    build_boss_mapping_payload,
    build_harmonized_candidates_by_boss,
    enrich_boss_records,
    load_kaggle_rows_by_game,
)
from src.pipeline.silver.location_mapper import LocationMapper
from src.pipeline.silver.location_pokemon_enrichment import (
    enrich_records_with_location_pokemon,
    get_location_area_and_pokemon_maps,
)
from src.pipeline.silver.parser import extract_game_data
from src.pipeline.silver.type_matchups import build_type_matchups
from src.pipeline.silver.battle_seeds import build_battle_seeds
from src.pipeline.silver.silver_manifest import create_silver_manifest
from src.pipeline.silver.schema_normalizer import (
    write_normalized_silver,
    create_pokemon_reference_index,
    create_encounter_methods_reference,
)


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


def build_silver_from_bronze(bronze_dir: Path = BRONZE_DIR, silver_dir: Path = SILVER_DIR) -> None:
    ensure_medallion_dirs()
    silver_dir.mkdir(parents=True, exist_ok=True)
    silver_subdirs = get_silver_subdirs(silver_dir)
    for directory in silver_subdirs.values():
        directory.mkdir(parents=True, exist_ok=True)

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

    for game_file in sorted(bulbapedia_dir.glob("*.json")):
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
            print(f"[silver] skipped {game_file.name}: no boss records extracted")
            continue

        all_records.extend(records)
        records_with_game_keys.append((game_key, records))
        for record in records:
            all_slugs.extend(record["reachable_locations"])

    area_map, location_pokemon_map = get_location_area_and_pokemon_maps(all_slugs, allowed_versions=allowed_versions)
    write_json(mappings_dir / "location_to_area_map.json", area_map)
    write_json(mappings_dir / "location_to_pokemon_map.json", location_pokemon_map)

    # Clear encounters file if exists (for fresh aggregation)
    encounters_file = references_dir / "encounters.jsonl"
    if encounters_file.exists():
        encounters_file.unlink()
    
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

        print(f"[silver] wrote {game_key}_boss_snapshots.jsonl with {len(records)} records")

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

    write_json(mappings_dir / "boss_mapping_by_version.json", boss_mapping_by_version)

    # Build battle simulation data
    # Extract teams from boss_mapping
    teams_data = []
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
                    })
    
    # Build type matchups if we have teams
    if teams_data:
        build_type_matchups(teams_data, simulation_dir, bronze_dir)
        build_battle_seeds(simulation_dir)
    
    # Create manifest of available data
    create_silver_manifest(silver_dir)

    print(f"[silver] unmapped location events: {len(mapper.misses)}")
    print(f"[silver] done: {len(all_records)} boss snapshots across {len(set(r['game'] for r in all_records))} games")


def build_silver_from_existing_files(silver_dir: Path = SILVER_DIR) -> None:
    all_slugs: list[str] = []
    silver_subdirs = get_silver_subdirs(silver_dir)
    snapshots_dir = silver_subdirs["snapshots"]
    mappings_dir = silver_subdirs["mappings"]

    game_files = sorted(snapshots_dir.glob("*_boss_snapshots.jsonl"))
    if not game_files:
        game_files = sorted(silver_dir.glob("*_boss_snapshots.jsonl"))
    if not game_files:
        raise FileNotFoundError(f"No *_boss_snapshots.jsonl files found in {silver_dir}")

    for game_file in game_files:
        dataframe = read_jsonl(game_file)
        for locations in dataframe["reachable_locations"]:
            all_slugs.extend(locations)

    area_map, location_pokemon_map = get_location_area_and_pokemon_maps(all_slugs)
    write_json(mappings_dir / "location_to_area_map.json", area_map)
    write_json(mappings_dir / "location_to_pokemon_map.json", location_pokemon_map)
    print(
        f"[silver] refreshed location_to_area_map.json and location_to_pokemon_map.json "
        f"from {len(game_files)} game files in {snapshots_dir}"
    )


if __name__ == "__main__":
    build_silver_from_bronze()