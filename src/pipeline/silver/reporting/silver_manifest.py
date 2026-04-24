"""Silver Layer manifest for harmonized and enriched intermediate data."""
from pathlib import Path
import logging

from src.pipeline.common.io import read_json, read_jsonl, read_parquet, write_json
from src.pipeline.settings import SILVER_DIR, get_silver_subdirs

logger = logging.getLogger(__name__)


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
                    "source_team_members",
                    "member_move_options",
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

    logger.info("[silver_manifest] found %s boss records", len(game_files))

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

    pokemon_reference_file = references_dir / "pokemon_reference.parquet"
    pokemon_reference_count = 0
    if pokemon_reference_file.exists():
        try:
            pokemon_reference_count = len(read_parquet(pokemon_reference_file))
        except Exception:
            pokemon_reference_count = 0
        manifest["datasets"]["pokemon_reference"] = {
            "file": _relative_to(silver_dir, pokemon_reference_file),
            "count": pokemon_reference_count,
            "format": "Parquet",
            "description": "Centralized Pokemon URL and name reference"
        }

    logger.info("[silver_manifest] found %s pokemon reference entries", pokemon_reference_count)

    encounter_methods_file = references_dir / "encounter_methods_reference.json"
    encounter_methods_count = 0
    if encounter_methods_file.exists():
        try:
            encounter_methods_count = len(read_json(encounter_methods_file))
        except Exception:
            encounter_methods_count = 0
        manifest["datasets"]["encounter_methods_reference"] = {
            "file": _relative_to(silver_dir, encounter_methods_file),
            "format": "JSON",
            "description": "Deduplicated encounter-method lookup table"
        }

    logger.info("[silver_manifest] found %s encounter methods reference entries", encounter_methods_count)

    reference_count = 0
    for reference_name, description in [
        ("games.parquet", "Game dimension with region, generation, and version groups"),
        ("bosses.parquet", "Boss dimension with canonical names and deterministic IDs"),
        ("locations.parquet", "Location dimension with mapping status"),
        ("encounters.parquet", "Encounter fact table normalized by location and species"),
        ("snapshot_available_pokemon.parquet", "Pokemon availability fact per boss snapshot"),
        ("pokemon_stats.parquet", "Pokemon base stats and typing reference (if available)"),
        ("move_reference.parquet", "Move reference dimension"),
        ("learnable_moves.parquet", "Unified learnable moves fact by game and species (authoritative move source)"),
    ]:
        path = references_dir / reference_name
        if not path.exists():
            continue
        try:
            reference_count = len(read_parquet(path))
        except Exception:
            reference_count = 0
        manifest["datasets"][reference_name.replace(".parquet", "")] = {
            "file": _relative_to(silver_dir, path),
            "count": reference_count,
            "format": "Parquet",
            "description": description,
        }

    logger.info("[silver_manifest] found %s reference entries", reference_count)

    # Teams (sharded parquet-first contract)
    teams_shards = sorted(simulation_dir.glob("source_teams_*.parquet"))
    teams_count = 0
    if teams_shards:
        teams_count = len(teams_shards)
        manifest["datasets"]["simulation_inputs_teams"] = {
            "files": [_relative_to(silver_dir, shard) for shard in teams_shards],
            "glob": "source_teams_*.parquet",
            "count": teams_count,
            "format": "Parquet",
            "description": "Sharded team compositions consumed by gold",
        }
    else:
        teams_file = simulation_dir / "source_teams.parquet"
        if teams_file.exists():
            teams = []
            try:
                teams = read_parquet(teams_file)
            except Exception:
                teams = []
            teams_count = len(teams)
            manifest["datasets"]["simulation_inputs_teams"] = {
                "file": _relative_to(silver_dir, teams_file),
                "count": teams_count,
                "format": "Parquet",
                "description": "Partitioned team compositions consumed by gold",
            }

    logger.info("[silver_manifest] found %s teams", teams_count)

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

    move_data_file = simulation_dir / "move_data.parquet"
    move_count = 0
    if move_data_file.exists():
        try:
            move_count = len(read_parquet(move_data_file))
        except Exception:
            move_count = 0
        manifest["datasets"]["move_data"] = {
            "file": _relative_to(silver_dir, move_data_file),
            "count": move_count,
            "format": "Parquet",
            "description": "Validated move metadata stored separately from team records",
        }

    logger.info("[silver_manifest] found %s move data", move_count)

    sharded_team_members = sorted(simulation_dir.glob("source_team_members_*.parquet"))
    team_members_count = 0
    if sharded_team_members:
        team_members_count = len(sharded_team_members)
        manifest["datasets"]["source_team_members"] = {
            "files": [_relative_to(silver_dir, shard) for shard in sharded_team_members],
            "glob": "source_team_members_*.parquet",
            "count": team_members_count,
            "format": "Parquet",
            "description": "Sharded team member fact table (one row per team slot)",
        }
    else:
        members_file = simulation_dir / "source_team_members.parquet"
        if members_file.exists():
            try:
                team_members_count = len(read_parquet(members_file))
            except Exception:
                team_members_count = 0
            manifest["datasets"]["source_team_members"] = {
                "file": _relative_to(silver_dir, members_file),
                "count": team_members_count,
                "format": "Parquet",
                "description": "Team member fact table (one row per team slot)",
            }

    logger.info("[silver_manifest] found %s team members", team_members_count)

    sharded_member_moves = sorted(simulation_dir.glob("member_move_options_*.parquet"))
    team_member_moves_count = 0
    if sharded_member_moves:
        team_member_moves_count = len(sharded_member_moves)
        manifest["datasets"]["member_move_options"] = {
            "files": [_relative_to(silver_dir, shard) for shard in sharded_member_moves],
            "glob": "member_move_options_*.parquet",
            "count": team_member_moves_count,
            "format": "Parquet",
            "description": "Sharded team-member move fact table (one row per move slot)",
        }
    else:
        member_moves_file = simulation_dir / "member_move_options.parquet"
        if member_moves_file.exists():
            try:
                team_member_moves_count = len(read_parquet(member_moves_file))
            except Exception:
                team_member_moves_count = 0
            manifest["datasets"]["member_move_options"] = {
                "file": _relative_to(silver_dir, member_moves_file),
                "count": team_member_moves_count,
                "format": "Parquet",
                "description": "Team-member move fact table (one row per move slot)",
            }
    logger.info("[silver_manifest] found %s team member moves", team_member_moves_count)

    # Location maps
    location_area_file = mappings_dir / "location_to_area_map.json"
    location_area_count = 0
    if location_area_file.exists():
        try:
            location_area_count = len(read_json(location_area_file))
        except Exception:
            location_area_count = 0
        manifest["datasets"]["location_to_area_map"] = {
            "file": _relative_to(silver_dir, location_area_file),
            "format": "JSON",
            "description": "Maps location slugs to game areas"
        }

        logger.info("[silver_manifest] found %s location area mappings", location_area_count)

    location_pokemon_file = mappings_dir / "location_to_pokemon_map.json"
    location_pokemon_count = 0
    if location_pokemon_file.exists():
        try:
            location_pokemon_count = len(read_json(location_pokemon_file))
        except Exception:
            location_pokemon_count = 0
        manifest["datasets"]["location_to_pokemon_map"] = {
            "file": _relative_to(silver_dir, location_pokemon_file),
            "format": "JSON",
            "description": "Maps locations to available Pokemon species"
        }

        logger.info("[silver_manifest] found %s location-pokemon mappings", location_pokemon_count)


    # Boss mapping
    boss_mapping_file = mappings_dir / "boss_mapping_by_version.json"
    boss_mapping_count = 0
    if boss_mapping_file.exists():
        try:
            boss_mapping_count = len(read_json(boss_mapping_file))
        except Exception:
            boss_mapping_count = 0
        manifest["datasets"]["boss_mapping_by_version"] = {
            "file": _relative_to(silver_dir, boss_mapping_file),
            "format": "JSON",
            "description": "Boss team configurations per game version"
        }

        logger.info("[silver_manifest] found %s boss mappings", boss_mapping_count)

    relational_validation_file = silver_subdirs["diagnostics"] / "relational_validation.json"
    relational_validation_count = 0
    if relational_validation_file.exists():
        try:
            relational_validation_count = len(read_json(relational_validation_file))
        except Exception:
            relational_validation_count = 0
        manifest["datasets"]["relational_validation"] = {
            "file": _relative_to(silver_dir, relational_validation_file),
            "format": "JSON",
            "description": "FK/PK validation report for normalized silver tables",
        }

        logger.info("[silver_manifest] found %s relational validation report", relational_validation_count)

    write_json(silver_dir / "manifest.json", manifest)

    print("[silver_manifest] created manifest.json")
    print(f"  Available datasets: {len(manifest['datasets'])}")
    for dataset_name in manifest["datasets"]:
        print(f"    - {dataset_name}")
