"""Silver Layer manifest for battle simulation."""
from pathlib import Path

from src.pipeline.common.io import read_jsonl, read_parquet, write_json
from src.pipeline.settings import SILVER_DIR, get_silver_subdirs


def _relative_to(base: Path, target: Path) -> str:
    return str(target.relative_to(base))


def create_silver_manifest(silver_dir: Path = SILVER_DIR) -> None:
    """Create manifest of available battle simulation data in Silver layer."""
    
    silver_subdirs = get_silver_subdirs(silver_dir)
    snapshots_dir = silver_subdirs["snapshots"]
    mappings_dir = silver_subdirs["mappings"]
    simulation_dir = silver_subdirs["simulation"]

    manifest = {
        "layer": "silver",
        "purpose": "Prepared data for 6v6 boss battle simulation",
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
    
    # Teams
    teams_file = simulation_dir / "teams.parquet"
    if teams_file.exists():
        teams = read_parquet(teams_file)
        manifest["datasets"]["teams"] = {
            "file": _relative_to(silver_dir, teams_file),
            "count": len(teams),
            "format": "Parquet",
            "description": "All team compositions (boss teams + variations)"
        }
    
    # Sequential team battle simulations (primary artifact)
    team_battles_file = simulation_dir / "team_battle_simulations.parquet"
    if team_battles_file.exists():
        team_battles = read_parquet(team_battles_file)
        manifest["datasets"]["team_battle_simulations"] = {
            "file": _relative_to(silver_dir, team_battles_file),
            "count": len(team_battles),
            "format": "Parquet",
            "description": "Deterministic sequential team-vs-team battle outcomes"
        }
    
    # Battle seeds
    seeds_file = simulation_dir / "battle_seeds.parquet"
    if seeds_file.exists():
        seeds = read_parquet(seeds_file)
        manifest["datasets"]["battle_seeds"] = {
            "file": _relative_to(silver_dir, seeds_file),
            "count": len(seeds),
            "format": "Parquet",
            "description": "Pre-computed battle scenarios with predicted outcomes"
        }

    # Monte-Carlo simulation results
    monte_carlo_file = simulation_dir / "monte_carlo_results.parquet"
    if monte_carlo_file.exists():
        monte_carlo = read_parquet(monte_carlo_file)
        manifest["datasets"]["monte_carlo_results"] = {
            "file": _relative_to(silver_dir, monte_carlo_file),
            "count": len(monte_carlo),
            "format": "Parquet",
            "description": "Monte-Carlo outcomes per scenario based on predicted win probabilities"
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


