"""Silver Layer manifest for battle simulation."""
from pathlib import Path

from src.pipeline.common.io import read_jsonl, write_json
from src.pipeline.settings import SILVER_DIR


def create_silver_manifest(silver_dir: Path = SILVER_DIR) -> None:
    """Create manifest of available battle simulation data in Silver layer."""
    
    manifest = {
        "layer": "silver",
        "purpose": "Prepared data for 6v6 boss battle simulation",
        "datasets": {}
    }
    
    # Boss records
    game_files = list(silver_dir.glob("*_data.jsonl"))
    if game_files:
        total_records = 0
        for f in game_files:
            try:
                records = read_jsonl(f)
                total_records += len(records)
            except Exception:
                pass
        
        manifest["datasets"]["boss_records"] = {
            "files": [f.name for f in game_files],
            "total_records": total_records,
            "format": "JSONL",
            "description": "Boss team data with reachable locations and Pokemon"
        }
    
    # Teams
    teams_file = silver_dir / "teams.jsonl"
    if teams_file.exists():
        teams = read_jsonl(teams_file)
        manifest["datasets"]["teams"] = {
            "file": "teams.jsonl",
            "count": len(teams),
            "format": "JSONL",
            "description": "All team compositions (boss teams + variations)"
        }
    
    # Pokemon instances
    pokemon_file = silver_dir / "pokemon_instances.jsonl"
    if pokemon_file.exists():
        pokemon_instances = read_jsonl(pokemon_file)
        manifest["datasets"]["pokemon_instances"] = {
            "file": "pokemon_instances.jsonl",
            "count": len(pokemon_instances),
            "format": "JSONL",
            "description": "Individual Pokemon with calculated stats for each level/team"
        }
    
    # Type matchups
    matchups_file = silver_dir / "type_matchups.jsonl"
    if matchups_file.exists():
        matchups = read_jsonl(matchups_file)
        manifest["datasets"]["type_matchups"] = {
            "file": "type_matchups.jsonl",
            "count": len(matchups),
            "format": "JSONL",
            "description": "Type advantage calculations between all team pairs"
        }
    
    # Battle seeds
    seeds_file = silver_dir / "battle_seeds.jsonl"
    if seeds_file.exists():
        seeds = read_jsonl(seeds_file)
        manifest["datasets"]["battle_seeds"] = {
            "file": "battle_seeds.jsonl",
            "count": len(seeds),
            "format": "JSONL",
            "description": "Pre-computed battle scenarios with predicted outcomes"
        }
    
    # Location maps
    location_area_file = silver_dir / "location_to_area_map.json"
    if location_area_file.exists():
        manifest["datasets"]["location_to_area_map"] = {
            "file": "location_to_area_map.json",
            "format": "JSON",
            "description": "Maps location slugs to game areas"
        }
    
    location_pokemon_file = silver_dir / "location_to_pokemon_map.json"
    if location_pokemon_file.exists():
        manifest["datasets"]["location_to_pokemon_map"] = {
            "file": "location_to_pokemon_map.json",
            "format": "JSON",
            "description": "Maps locations to available Pokemon species"
        }
    
    # Boss mapping
    boss_mapping_file = silver_dir / "boss_mapping_by_version.json"
    if boss_mapping_file.exists():
        manifest["datasets"]["boss_mapping_by_version"] = {
            "file": "boss_mapping_by_version.json",
            "format": "JSON",
            "description": "Boss team configurations per game version"
        }
    
    write_json(silver_dir / "manifest.json", manifest)
    
    print("[silver_manifest] created manifest.json")
    print(f"  Available datasets: {len(manifest['datasets'])}")
    for dataset_name in manifest["datasets"]:
        print(f"    - {dataset_name}")


