"""Silver Layer manifest for harmonized and enriched intermediate data."""
from pathlib import Path

from src.pipeline.common.io import read_jsonl, read_parquet, write_json
from src.pipeline.settings import SILVER_DIR, get_silver_subdirs


def _relative_to(base: Path, target: Path) -> str:
    return str(target.relative_to(base))


def create_silver_manifest(silver_dir: Path = SILVER_DIR) -> None:
    """Create manifest of available normalized and enriched Silver datasets."""

    silver_subdirs = get_silver_subdirs(silver_dir)
    snapshots_dir = silver_subdirs["snapshots"]
    mappings_dir = silver_subdirs["mappings"]
    references_dir = silver_subdirs["references"]
    simulation_dir = silver_subdirs["simulation"]

    manifest = {
        "layer": "silver",
        "purpose": "Harmonized intermediate data and simulation inputs for gold",
        "datasets": {}
    }

    # Boss records
    game_files = list(snapshots_dir.glob("*_boss_snapshots.jsonl"))
    if game_files:
        total_records = 0
        for f in game_files:
            try:
                records = read_jsonl(f)
                total_records += len(records)
            except Exception:
                pass

        manifest["datasets"]["boss_records"] = {
            "files": [_relative_to(silver_dir, f) for f in game_files],
            "total_records": total_records,
            "format": "JSONL",
            "description": "Boss team data with reachable locations and Pokemon"
        }

    encounters_file = references_dir / "encounters.jsonl"
    if encounters_file.exists():
        try:
            encounters = read_jsonl(encounters_file)
            encounter_count = len(encounters)
        except Exception:
            encounter_count = 0
        manifest["datasets"]["encounters"] = {
            "file": _relative_to(silver_dir, encounters_file),
            "count": encounter_count,
            "format": "JSONL",
            "description": "Normalized location-pokemon encounter rows"
        }

    pokemon_reference_file = references_dir / "pokemon_reference.json"
    if pokemon_reference_file.exists():
        manifest["datasets"]["pokemon_reference"] = {
            "file": _relative_to(silver_dir, pokemon_reference_file),
            "format": "JSON",
            "description": "Centralized Pokemon URL and name reference"
        }

    encounter_methods_file = references_dir / "encounter_methods_reference.json"
    if encounter_methods_file.exists():
        manifest["datasets"]["encounter_methods_reference"] = {
            "file": _relative_to(silver_dir, encounter_methods_file),
            "format": "JSON",
            "description": "Deduplicated encounter-method lookup table"
        }

    # Teams
    teams_file = simulation_dir / "teams.parquet"
    if teams_file.exists():
        teams = read_parquet(teams_file)
        manifest["datasets"]["simulation_inputs_teams"] = {
            "file": _relative_to(silver_dir, teams_file),
            "count": len(teams),
            "format": "Parquet",
            "description": "Prepared team compositions consumed by gold simulation"
        }

    teams_jsonl_file = simulation_dir / "teams.jsonl"
    if teams_jsonl_file.exists():
        try:
            teams_jsonl = read_jsonl(teams_jsonl_file)
            teams_jsonl_count = len(teams_jsonl)
        except Exception:
            teams_jsonl_count = 0
        manifest["datasets"]["simulation_inputs_teams_jsonl"] = {
            "file": _relative_to(silver_dir, teams_jsonl_file),
            "count": teams_jsonl_count,
            "format": "JSONL",
            "description": "Line-delimited view of prepared team compositions"
        }

    for filename, dataset_name, description in [
        ("team_battle_simulations.parquet", "team_battle_simulations", "Deterministic team-vs-team battle matrix"),
        ("battle_seeds.parquet", "battle_seeds", "Monte-Carlo seed scenarios derived from battle matrix"),
        ("monte_carlo_results.parquet", "monte_carlo_results", "Monte-Carlo simulation outcomes and win rates"),
    ]:
        file_path = simulation_dir / filename
        if file_path.exists():
            try:
                row_count = len(read_parquet(file_path))
            except Exception:
                row_count = 0
            manifest["datasets"][dataset_name] = {
                "file": _relative_to(silver_dir, file_path),
                "count": row_count,
                "format": "Parquet",
                "description": description,
            }

    # Location maps
    location_area_file = mappings_dir / "location_to_area_map.json"
    if location_area_file.exists():
        manifest["datasets"]["location_to_area_map"] = {
            "file": _relative_to(silver_dir, location_area_file),
            "format": "JSON",
            "description": "Maps location slugs to game areas"
        }

    location_pokemon_file = mappings_dir / "location_to_pokemon_map.json"
    if location_pokemon_file.exists():
        manifest["datasets"]["location_to_pokemon_map"] = {
            "file": _relative_to(silver_dir, location_pokemon_file),
            "format": "JSON",
            "description": "Maps locations to available Pokemon species"
        }

    # Boss mapping
    boss_mapping_file = mappings_dir / "boss_mapping_by_version.json"
    if boss_mapping_file.exists():
        manifest["datasets"]["boss_mapping_by_version"] = {
            "file": _relative_to(silver_dir, boss_mapping_file),
            "format": "JSON",
            "description": "Boss team configurations per game version"
        }

    write_json(silver_dir / "manifest.json", manifest)

    print("[silver_manifest] created manifest.json")
    print(f"  Available datasets: {len(manifest['datasets'])}")
    for dataset_name in manifest["datasets"]:
        print(f"    - {dataset_name}")



