"""
Boss team enrichment and extraction.

Combines:
- Boss team rosters from Kaggle dataset
- Pokemon stats from PokeAPI
- Reachable locations and available Pokemon
- Level progression based on game flow

Produces enriched boss encounters with complete team compositions.
"""

from typing import Optional
from pathlib import Path
import json


def extract_boss_team_from_kaggle(
    kaggle_row: dict,
    boss_name: str,
) -> Optional[list[str]]:
    """
    Extract team species from Kaggle boss mapping row.
    
    Expected Kaggle format: columns for each Pokemon slot (1-6)
    
    Args:
        kaggle_row: Row from Kaggle boss mapping dataset
        boss_name: Canonical boss name to match
    
    Returns:
        List of Pokemon species slugs, or None if not found
    """
    # Standard Kaggle format has Pokemon in slots: pokemon_1, pokemon_2, ..., pokemon_6
    team_slots = []
    
    for slot in range(1, 7):
        key_variants = [
            f"pokemon_{slot}",
            f"Pokemon {slot}",
            f"Slot {slot}",
        ]
        
        for key in key_variants:
            if key in kaggle_row and kaggle_row[key]:
                species = str(kaggle_row[key]).strip().lower()
                if species and species != "nan":
                    team_slots.append(species)
                    break
    
    return team_slots if team_slots else None


def get_pokemon_level_for_team(
    pokemon_index: int,
    boss_level: int,
    team_size: int = 6,
) -> int:
    """
    Calculate level for individual Pokemon in team.
    
    Typically: lead Pokemon is highest level, last Pokemon is lowest.
    
    Args:
        pokemon_index: Position in team (0-indexed)
        boss_level: Average boss team level
        team_size: Number of Pokemon in team
    
    Returns:
        Level for this Pokemon
    """
    # Lead Pokemon is slightly lower, team balance
    position_modifier = (pokemon_index - team_size / 2) * 0.5
    level = max(1, min(100, boss_level + int(position_modifier)))
    return level


class BossTeamEnricher:
    """Enrich boss encounters with complete team rosters."""
    
    def __init__(
        self,
        total_bosses_per_game: dict[str, int],
        pokemon_stats: Optional[dict] = None,
    ):
        """
        Initialize BossTeamEnricher.
        
        Args:
            total_bosses_per_game: Map of game_key -> total boss count
            pokemon_stats: Optional dict of Pokemon stats from PokeAPI
        """
        self.total_bosses_per_game = total_bosses_per_game
        self.pokemon_stats = pokemon_stats or {}
    
    def enrich_boss_record(
        self,
        record: dict,
        boss_roster: Optional[list[str]] = None,
    ) -> dict:
        """
        Enrich a silver layer boss record with team composition.
        
        Args:
            record: Silver layer boss encounter record
            boss_roster: Optional list of Pokemon species for boss team
        
        Returns:
            Enriched record with boss_team field
        """
        enriched = record.copy()
        game = record.get("game", "")
        boss_order = record.get("boss_order", 1)
        
        # Calculate base level from progression
        total_bosses = self.total_bosses_per_game.get(game, 13)
        min_level = 12
        max_level = 60
        progression_ratio = (boss_order - 1) / max(total_bosses - 1, 1)
        base_level = int(min_level + (max_level - min_level) * progression_ratio)
        
        # Build team
        team = []
        if boss_roster:
            for i, species in enumerate(boss_roster):
                level = get_pokemon_level_for_team(i, base_level, len(boss_roster))
                member = {
                    "slot": i + 1,
                    "species": species,
                    "level": level,
                }
                
                # Add stats if available
                if species in self.pokemon_stats:
                    member["stats"] = self.pokemon_stats[species]
                
                team.append(member)
        
        enriched["boss_team"] = {
            "boss_name": record.get("boss_name", ""),
            "team_size": len(team),
            "average_level": base_level,
            "members": team,
        }
        
        enriched["reachable_team_building"] = {
            "available_pokemon_count": record.get("reachable_pokemon_count", 0),
            "available_locations_count": record.get("location_count", 0),
            "level_range_for_team": [max(1, base_level - 5), min(100, base_level + 5)],
            "suggested_team_size": 6,
        }
        
        return enriched


def merge_encounter_with_team_data(
    silver_record: dict,
    boss_team_data: dict,
) -> dict:
    """
    Merge silver layer encounter data with prepared boss team data.
    
    Args:
        silver_record: Record from silver layer
        boss_team_data: Boss team roster and enrichment data
    
    Returns:
        Merged record ready for gold layer
    """
    merged = silver_record.copy()
    
    # Add team composition
    merged["boss_team"] = boss_team_data.get("team", [])
    merged["boss_team_metadata"] = {
        "team_size": len(boss_team_data.get("team", [])),
        "base_level": boss_team_data.get("base_level"),
    }
    
    # Add player progression guidance
    merged["player_progression"] = {
        "available_for_pickup": silver_record.get("reachable_location_pokemon", {}),
        "available_pokemon_count": silver_record.get("reachable_pokemon_count", 0),
        "suggested_player_level": boss_team_data.get("base_level", 20),
    }
    
    return merged

