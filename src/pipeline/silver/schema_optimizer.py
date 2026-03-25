"""
Silver Layer Schema Optimizer
Converts denormalized boss records to optimized, normalized structure
"""

import json
from pathlib import Path
from typing import Any


def normalize_boss_records(records: list[dict]) -> tuple[list[dict], dict[str, Any], list[dict]]:
    """
    Convert denormalized boss records to optimized normalized structure.
    
    Returns:
        - boss_snapshots: Lightweight boss metadata
        - pokemon_reference: Centralized pokemon URL mapping
        - encounters: Normalized encounter records (one per location-pokemon pair)
    """
    pokemon_reference = {}  # pokemon_id -> {"url": "...", "canonical_name": "..."}
    encounters = []  # Flat list of encounters
    boss_snapshots = []
    
    for record in records:
        # Extract boss-level metadata (lightweight)
        boss_snapshot = {
            "boss_id": record["boss_id"],
            "boss_slug": record["boss_slug"],
            "boss_name": record["boss_name_canonical"],
            "game": record["game"],
            "version": record["version"],
            "boss_order": record["boss_order"],
            "heading": record["heading"],
            "part": record["part"],
            "reachable_location_count": record["location_count"],
            "reachable_locations": record["reachable_locations"],
            "reachable_pokemon_count": record["reachable_pokemon_count"],
        }
        boss_snapshots.append(boss_snapshot)
        
        # Normalize encounters and extract pokemon references
        location_encounters = record.get("reachable_location_encounters", {})
        
        for location_slug, encounter_list in location_encounters.items():
            for encounter in encounter_list:
                species = encounter.get("species", "")
                pokemon_url = encounter.get("pokemon_url", "")
                
                # Build centralized pokemon reference
                if species and pokemon_url:
                    if species not in pokemon_reference:
                        pokemon_reference[species] = {
                            "url": pokemon_url,
                            "name": species,
                        }
                
                # Create normalized encounter record
                normalized_encounter = {
                    "boss_id": record["boss_id"],
                    "game": record["game"],
                    "location": location_slug,
                    "pokemon": species,
                    "level_min": encounter.get("level_min"),
                    "level_max": encounter.get("level_max"),
                    "methods": encounter.get("encounter_methods", []),
                }
                encounters.append(normalized_encounter)
    
    return boss_snapshots, pokemon_reference, encounters


def write_optimized_silver(
    records: list[dict],
    output_dir: Path,
    game_key: str,
) -> dict[str, dict[str, str]]:
    """
    Write records in optimized normalized format.
    
    Returns:
        dict: pokemon_reference mapping
    
    Files created:
    - {game_key}_boss_snapshots.jsonl: Lightweight boss metadata
    - encounters.jsonl: Normalized encounters (appended across all games)
    - pokemon_reference.json: Centralized pokemon info (once per dataset)
    """
    boss_snapshots, pokemon_reference, encounters = normalize_boss_records(records)
    
    # Write boss snapshots
    boss_output = output_dir / f"{game_key}_boss_snapshots.jsonl"
    with boss_output.open("a", encoding="utf-8") as f:
        for snapshot in boss_snapshots:
            f.write(json.dumps(snapshot, ensure_ascii=False) + "\n")
    
    # Write encounters (append mode for multi-game aggregation)
    encounters_output = output_dir / "encounters.jsonl"
    with encounters_output.open("a", encoding="utf-8") as f:
        for encounter in encounters:
            f.write(json.dumps(encounter, ensure_ascii=False) + "\n")
    
    return pokemon_reference


def create_pokemon_reference_index(
    all_pokemon_references: dict[str, dict],
    output_dir: Path,
) -> None:
    """Create centralized pokemon reference (deduped across all games)."""
    output_file = output_dir / "pokemon_reference.json"
    
    # Deduplicate pokemon references
    unique_pokemon = {}
    for species, info in all_pokemon_references.items():
        if species not in unique_pokemon:
            unique_pokemon[species] = info
    
    with output_file.open("w", encoding="utf-8") as f:
        json.dump(unique_pokemon, f, ensure_ascii=False, indent=2)


def create_encounter_methods_reference(
    records: list[dict],
    output_dir: Path,
) -> None:
    """Extract and deduplicate encounter methods."""
    methods_map = {}  # method_name -> method_url
    
    for record in records:
        location_encounters = record.get("reachable_location_encounters", {})
        for encounter_list in location_encounters.values():
            for encounter in encounter_list:
                methods = encounter.get("encounter_methods", [])
                method_urls = encounter.get("encounter_method_urls", [])
                
                for method_name, method_url in zip(methods, method_urls):
                    if method_name not in methods_map:
                        methods_map[method_name] = method_url
    
    output_file = output_dir / "encounter_methods_reference.json"
    with output_file.open("w", encoding="utf-8") as f:
        json.dump(methods_map, f, ensure_ascii=False, indent=2)


def analyze_schema_efficiency(records: list[dict]) -> dict[str, Any]:
    """Analyze current vs optimized schema sizes."""
    # Current size (denormalized)
    current_json = json.dumps(records)
    current_size_bytes = len(current_json.encode('utf-8'))
    
    # Optimized size
    boss_snapshots, pokemon_ref, encounters = normalize_boss_records(records)
    optimized_size = (
        len(json.dumps(boss_snapshots).encode('utf-8')) +
        len(json.dumps(pokemon_ref).encode('utf-8')) +
        len(json.dumps(encounters).encode('utf-8'))
    )
    
    reduction = ((current_size_bytes - optimized_size) / current_size_bytes) * 100
    
    return {
        "current_size_bytes": current_size_bytes,
        "optimized_size_bytes": optimized_size,
        "reduction_percentage": round(reduction, 2),
        "space_saved_bytes": current_size_bytes - optimized_size,
        "num_boss_snapshots": len(boss_snapshots),
        "num_encounters": len(encounters),
        "num_unique_pokemon": len(pokemon_ref),
    }




