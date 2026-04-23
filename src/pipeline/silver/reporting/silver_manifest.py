"""Silver Layer manifest for harmonized and enriched intermediate data."""
from pathlib import Path
import logging

from src.pipeline.common.io import read_jsonl, read_parquet, write_json
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
    if pokemon_reference_file.exists():
        count = 0
        try:
            count = len(read_parquet(pokemon_reference_file))
        except Exception:
            count = 0
        manifest["datasets"]["pokemon_reference"] = {
            "file": _relative_to(silver_dir, pokemon_reference_file),
            "count": count,
            "format": "Parquet",
            "description": "Centralized Pokemon URL and name reference"
        }

    logger.info("[silver_manifest] found %s pokemon reference entries", len(read_json(pokemon_reference_file)))

    encounter_methods_file = references_dir / "encounter_methods_reference.json"
    if encounter_methods_file.exists():
        manifest["datasets"]["encounter_methods_reference"] = {
            "file": _relative_to(silver_dir, encounter_methods_file),
            "format": "JSON",
            "description": "Deduplicated encounter-method lookup table"
        }

    logger.info("[silver_manifest] found %s encounter methods reference entries", len(read_json(encounter_methods_file)))

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

    logger.info("[silver_manifest] found %s reference entries", count)

    # Teams (sharded parquet-first contract)
    teams_shards = sorted(simulation_dir.glob("teams_*.parquet"))
    if teams_shards:
        manifest["datasets"]["simulation_inputs_teams"] = {
            "files": [_relative_to(silver_dir, shard) for shard in teams_shards],
            "glob": "teams_*.parquet",
            "count": len(teams_shards),
            "format": "Parquet",
            "description": "Sharded team compositions consumed by gold",
        }
    else:
        teams_file = simulation_dir / "teams.parquet"
        if teams_file.exists():
            teams = read_parquet(teams_file)
            manifest["datasets"]["simulation_inputs_teams"] = {
                "file": _relative_to(silver_dir, teams_file),
                "count": len(teams),
                "format": "Parquet",
                "description": "Partitioned team compositions consumed by gold",
            }

    logger.info("[silver_manifest] found %s teams", len(teams) if isinstance(teams, list) else 0)

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

    sharded_team_members = sorted(simulation_dir.glob("team_members_*.parquet"))
    if sharded_team_members:
        manifest["datasets"]["team_members"] = {
            "files": [_relative_to(silver_dir, shard) for shard in sharded_team_members],
            "glob": "team_members_*.parquet",
            "count": len(sharded_team_members),
            "format": "Parquet",
            "description": "Sharded team member fact table (one row per team slot)",
        }
    else:
        members_file = simulation_dir / "team_members.parquet"
        if members_file.exists():
            count = 0
            try:
                count = len(read_parquet(members_file))
            except Exception:
                count = 0
            manifest["datasets"]["team_members"] = {
                "file": _relative_to(silver_dir, members_file),
                "count": count,
                "format": "Parquet",
                "description": "Team member fact table (one row per team slot)",
            }

    logger.info("[silver_manifest] found %s team members", count)

    sharded_member_moves = sorted(simulation_dir.glob("team_member_moves_*.parquet"))
    if sharded_member_moves:
        manifest["datasets"]["team_member_moves"] = {
            "files": [_relative_to(silver_dir, shard) for shard in sharded_member_moves],
            "glob": "team_member_moves_*.parquet",
            "count": len(sharded_member_moves),
            "format": "Parquet",
            "description": "Sharded team-member move fact table (one row per move slot)",
        }
    else:
        member_moves_file = simulation_dir / "team_member_moves.parquet"
        if member_moves_file.exists():
            count = 0
            try:
                count = len(read_parquet(member_moves_file))
            except Exception:
                count = 0
            manifest["datasets"]["team_member_moves"] = {
                "file": _relative_to(silver_dir, member_moves_file),
                "count": count,
                "format": "Parquet",
                "description": "Team-member move fact table (one row per move slot)",
            }
    logger.info("[silver_manifest] found %s team member moves", count)

    # Location maps
    location_area_file = mappings_dir / "location_to_area_map.json"
    if location_area_file.exists():
        manifest["datasets"]["location_to_area_map"] = {
            "file": _relative_to(silver_dir, location_area_file),
            "format": "JSON",
            "description": "Maps location slugs to game areas"
        }

        logger.info("[silver_manifest] found %s location area mappings", len(read_json(location_area_file)))

    location_pokemon_file = mappings_dir / "location_to_pokemon_map.json"
    if location_pokemon_file.exists():
        manifest["datasets"]["location_to_pokemon_map"] = {
            "file": _relative_to(silver_dir, location_pokemon_file),
            "format": "JSON",
            "description": "Maps locations to available Pokemon species"
        }

        logger.info("[silver_manifest] found %s location-pokemon mappings", len(read_json(location_pokemon_file)))


    # Boss mapping
    boss_mapping_file = mappings_dir / "boss_mapping_by_version.json"
    if boss_mapping_file.exists():
        manifest["datasets"]["boss_mapping_by_version"] = {
            "file": _relative_to(silver_dir, boss_mapping_file),
            "format": "JSON",
            "description": "Boss team configurations per game version"
        }

        logger.info("[silver_manifest] found %s boss mappings", len(read_json(boss_mapping_file)))

    relational_validation_file = silver_subdirs["diagnostics"] / "relational_validation.json"
    if relational_validation_file.exists():
        manifest["datasets"]["relational_validation"] = {
            "file": _relative_to(silver_dir, relational_validation_file),
            "format": "JSON",
            "description": "FK/PK validation report for normalized silver tables",
        }

        logger.info("[silver_manifest] found %s relational validation report", len(read_json(relational_validation_file)))

    write_json(silver_dir / "manifest.json", manifest)

    print("[silver_manifest] created manifest.json")
    print(f"  Available datasets: {len(manifest['datasets'])}")
    for dataset_name in manifest["datasets"]:
        print(f"    - {dataset_name}")
