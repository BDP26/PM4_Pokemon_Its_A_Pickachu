"""
Team builder for boss encounters and player progression.

Constructs and enriches team rosters for:
- Boss teams (from Kaggle dataset)
- Player progression teams (from reachable Pokemon)

Handles:
- Level calculation based on boss order and game progression
- Type diversity and team balance
- Integration of Pokemon stats and moves
"""

from typing import Optional
import random


def calculate_boss_team_level(boss_order: int, total_bosses: int) -> int:
    """
    Calculate average level for boss team based on progression.
    
    Args:
        boss_order: Position in gym leader sequence (1-indexed)
        total_bosses: Total number of bosses in game (including Elite Four + Champion)
    
    Returns:
        Suggested average level for the team
    """
    # Typical Pokemon game progression: 
    # First gym ~12-15, Last gym ~40-45, Champion ~50-60
    min_level = 12
    max_level = 60
    progression_ratio = (boss_order - 1) / max(total_bosses - 1, 1)
    level = min_level + (max_level - min_level) * progression_ratio
    return int(round(level))


def build_team_from_species(
    species_list: list[str],
    levels: Optional[list[int]] = None,
    total_bosses: int = 13,
    boss_order: int = 1,
) -> list[dict]:
    """
    Build a team from a list of Pokemon species.
    
    Args:
        species_list: List of Pokemon species slugs (e.g. ["pikachu", "charizard"])
        levels: Optional list of levels for each Pokemon
        total_bosses: Total bosses in game for level calculation
        boss_order: Position in gym leader sequence for level calculation
    
    Returns:
        List of team member dicts with species, level, and metadata
    """
    if levels is None:
        avg_level = calculate_boss_team_level(boss_order, total_bosses)
        levels = [max(1, avg_level - 2 + (i % 3)) for i in range(len(species_list))]
    
    team = []
    for i, species in enumerate(species_list):
        level = levels[i] if i < len(levels) else levels[-1]
        team.append({
            "slot": i + 1,  # 1-indexed slot in team
            "species": species,
            "level": min(100, max(1, level)),  # Clamp level to valid range
            "position_in_team": i,
        })
    
    return team


def generate_team_from_available_pokemon(
    available_pokemon: list[str],
    team_size: int = 6,
    avg_level: int = 30,
    seed: Optional[int] = None,
) -> list[dict]:
    """
    Generate a diverse team from available Pokemon in reachable locations.
    Prioritizes type diversity when possible.
    
    Args:
        available_pokemon: List of Pokemon species available on routes
        team_size: Size of team to generate (default 6)
        avg_level: Average level for team members
        seed: Optional random seed for reproducibility
    
    Returns:
        List of team member dicts
    """
    if seed is not None:
        random.seed(seed)
    
    # If we don't have enough Pokemon, use what we have
    if len(available_pokemon) <= team_size:
        selected = available_pokemon
    else:
        # Try to select diverse Pokemon (basic random selection)
        selected = random.sample(available_pokemon, team_size)
    
    # Generate levels with slight variance
    levels = [
        max(1, avg_level - 2 + random.randint(0, 4))
        for _ in range(len(selected))
    ]
    
    return build_team_from_species(selected, levels)


class TeamBuilder:
    """Helper class for building and enriching teams for battle simulation."""
    
    def __init__(self, pokemon_stats: Optional[dict] = None):
        """
        Initialize TeamBuilder with optional Pokemon stats.
        
        Args:
            pokemon_stats: Dict mapping species slug to stats
        """
        self.pokemon_stats = pokemon_stats or {}
    
    def enrich_team(self, team: list[dict]) -> list[dict]:
        """
        Enrich team with Pokemon stats if available.
        
        Args:
            team: List of team member dicts
        
        Returns:
            Enriched team with stats attached
        """
        enriched = []
        for member in team:
            enriched_member = member.copy()
            species = member.get("species", "")
            
            if species in self.pokemon_stats:
                enriched_member["stats"] = self.pokemon_stats[species].copy()
            
            enriched.append(enriched_member)
        
        return enriched
    
    def team_to_dict(self, team: list[dict], team_name: str = "") -> dict:
        """
        Convert team list to structured dict for battle simulation.
        
        Args:
            team: List of team member dicts
            team_name: Name of team (e.g., "Brock's Team")
        
        Returns:
            Structured team dict
        """
        return {
            "team_name": team_name,
            "team_size": len(team),
            "average_level": sum(m.get("level", 1) for m in team) / max(len(team), 1),
            "members": team,
        }

