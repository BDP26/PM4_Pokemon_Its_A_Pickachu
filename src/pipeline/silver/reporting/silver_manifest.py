"""Silver Layer manifest for harmonized and enriched intermediate data."""
from pathlib import Path

from src.pipeline.common.io import read_json, read_jsonl, read_parquet, write_json
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
        "purpose": "Harmonized intermediate data with reusable team and move contracts for gold",
        "contracts": {
            "gold_strict": {
                "required_dataset_keys": [
                    "boss_records",
                    "simulation_inputs_teams",
                    "team_members",
                    "team_member_moves",
                    "pokemon_reference",
                    "snapshot_available_pokemon",
                    "encounters",
                ]
            }
        },
        "datasets": {},
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

    for reference_name, description in [
        ("games.parquet", "Game dimension with region, generation, and version groups"),
        ("bosses.parquet", "Boss dimension with canonical names and deterministic IDs"),
        ("locations.parquet", "Location dimension with mapping status"),
        ("encounters.parquet", "Encounter fact table normalized by location and species"),
        ("snapshot_available_pokemon.parquet", "Pokemon availability fact per boss snapshot"),
        ("move_reference.parquet", "Move reference dimension"),
        ("learnable_moves.parquet", "Learnable moves fact by game and species"),
        ("pokemon_learnable_moves.parquet", "Explicit pokemon->learnable moves table by game version"),
    ]:
        path = references_dir / reference_name
        if not path.exists():
            continue
        count = 0
        try:
            count = len(read_parquet(path))
        except Exception:
            count = 0
        manifest["datasets"][reference_name.replace(".parquet", "")] = {
            "file": _relative_to(silver_dir, path),
            "count": count,
            "format": "Parquet",
            "description": description,
        }

    # Teams
    teams_file = simulation_dir / "teams.parquet"
    if teams_file.exists():
        teams = read_parquet(teams_file)
        manifest["datasets"]["simulation_inputs_teams"] = {
            "file": _relative_to(silver_dir, teams_file),
            "count": len(teams),
            "format": "Parquet",
            "description": "Partitioned team compositions consumed by gold"
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
            "description": "Line-delimited view of normalized team compositions"
        }

    boss_teams_file = simulation_dir / "boss_teams.parquet"
    if boss_teams_file.exists():
        try:
            boss_teams = read_parquet(boss_teams_file)
            boss_teams_count = len(boss_teams)
        except Exception:
            boss_teams_count = 0
        manifest["datasets"]["simulation_boss_teams"] = {
            "file": _relative_to(silver_dir, boss_teams_file),
            "count": boss_teams_count,
            "format": "Parquet",
            "description": "Boss-controlled teams separated from player candidates"
        }

    player_teams_file = simulation_dir / "player_teams.parquet"
    if player_teams_file.exists():
        try:
            player_teams = read_parquet(player_teams_file)
            player_teams_count = len(player_teams)
        except Exception:
            player_teams_count = 0
        manifest["datasets"]["simulation_player_teams"] = {
            "file": _relative_to(silver_dir, player_teams_file),
            "count": player_teams_count,
            "format": "Parquet",
            "description": "Player-candidate teams separated from boss teams"
        }

    move_data_file = simulation_dir / "move_data.json"
    if move_data_file.exists():
        try:
            move_data = read_json(move_data_file)
            move_count = len(move_data) if isinstance(move_data, dict) else 0
        except Exception:
            move_count = 0
        manifest["datasets"]["move_data"] = {
            "file": _relative_to(silver_dir, move_data_file),
            "count": move_count,
            "format": "JSON",
            "description": "Validated move metadata stored separately from team records",
        }

    for simulation_name, description in [
        ("team_members.parquet", "Team member fact table (one row per team slot)"),
        ("team_member_moves.parquet", "Team-member move fact table (one row per move slot)"),
    ]:
        path = simulation_dir / simulation_name
        if not path.exists():
            continue
        count = 0
        try:
            count = len(read_parquet(path))
        except Exception:
            count = 0
        manifest["datasets"][simulation_name.replace(".parquet", "")] = {
            "file": _relative_to(silver_dir, path),
            "count": count,
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

    relational_validation_file = silver_subdirs["diagnostics"] / "relational_validation.json"
    if relational_validation_file.exists():
        manifest["datasets"]["relational_validation"] = {
            "file": _relative_to(silver_dir, relational_validation_file),
            "format": "JSON",
            "description": "FK/PK validation report for normalized silver tables",
        }

    write_json(silver_dir / "manifest.json", manifest)

    print("[silver_manifest] created manifest.json")
    print(f"  Available datasets: {len(manifest['datasets'])}")
    for dataset_name in manifest["datasets"]:
        print(f"    - {dataset_name}")



