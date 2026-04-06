"""
Team builder for boss encounters and player progression.

Constructs and enriches team rosters for:
- Boss teams (from Kaggle dataset)
- Player progression teams (from reachable Pokemon)

Handles:
- Level calculation based on boss order and game progression
- Type diversity and team balance
- Integration of Pokemon stats and moves
- Starter locking across a walkthrough
- One evolution family per team
"""

from functools import lru_cache
from typing import Optional, cast
import random

import pokebase as pb

from src.pipeline.silver.inputs.game_config import get_starter_choices


def _norm_species(value: str) -> str:
    return value.strip().lower().replace(" ", "-")


@lru_cache(maxsize=2048)
def get_evolution_family_root(species: str) -> str:
    """Return the root species of an evolution family (e.g. raichu -> pikachu).

    Falls back to the normalized species name if PokeAPI lookup fails.
    """
    species_name = _norm_species(species)
    try:
        current = pb.pokemon_species(species_name)
        while getattr(current, "evolves_from_species", None):
            current = current.evolves_from_species
        root_name = getattr(current, "name", "") or species_name
        return _norm_species(root_name)
    except Exception:
        return species_name


def _dedupe_species_by_family(species_list: list[str]) -> list[str]:
    seen_families: set[str] = set()
    result: list[str] = []
    for species in species_list:
        family_root = get_evolution_family_root(species)
        if family_root in seen_families:
            continue
        seen_families.add(family_root)
        result.append(species)
    return result


def get_walkthrough_starter_choices(game_version: str) -> list[str]:
    return get_starter_choices(game_version)


def choose_walkthrough_starter(
    game_version: str,
    available_pokemon: list[str],
    preferred_starter: Optional[str] = None,
) -> Optional[str]:
    """Pick one starter for the whole walkthrough and keep it stable.

    Preference order:
    1. preferred_starter if provided and available
    2. first starter choice for the version that is available
    3. the first starter choice for the version
    """
    starter_pool = get_walkthrough_starter_choices(game_version)
    available_set = {_norm_species(species) for species in available_pokemon}

    if preferred_starter:
        preferred = _norm_species(preferred_starter)
        if preferred in available_set:
            return preferred

    for starter in starter_pool:
        if _norm_species(starter) in available_set:
            return _norm_species(starter)

    return _norm_species(preferred_starter) if preferred_starter else (starter_pool[0] if starter_pool else None)


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
    starter_species: Optional[str] = None,
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
    levels_list = levels
    if levels_list is None:
        avg_level = calculate_boss_team_level(boss_order, total_bosses)
        levels_list = [max(1, avg_level - 2 + (i % 3)) for i in range(len(species_list))]

    # Guard against malformed callers passing an empty levels list.
    if not levels_list:
        levels_list = [calculate_boss_team_level(boss_order, total_bosses)]

    starter_family = get_evolution_family_root(starter_species) if starter_species else None
    team = []
    for i, species in enumerate(species_list):
        level = levels_list[i] if i < len(levels_list) else levels_list[-1]
        family_root = get_evolution_family_root(species)
        team.append({
            "slot": i + 1,  # 1-indexed slot in team
            "species": species,
            "level": min(100, max(1, level)),  # Clamp level to valid range
            "position_in_team": i,
            "family_root": family_root,
            "is_starter": starter_family is not None and family_root == starter_family,
        })

    return team


def generate_team_from_available_pokemon(
    available_pokemon: list[str],
    team_size: int = 6,
    avg_level: int = 30,
    seed: Optional[int] = None,
    starter_species: Optional[str] = None,
    game_version: Optional[str] = None,
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

    selected_pool: list[str] = _dedupe_species_by_family([_norm_species(species) for species in available_pokemon])
    if game_version and starter_species is None:
        starter_species = choose_walkthrough_starter(game_version, selected_pool)
    selected: list[str]
    if starter_species:
        starter = _norm_species(cast(str, starter_species))
        starter_family = get_evolution_family_root(starter)
        selected_pool = cast(
            list[str],
            [species for species in selected_pool if get_evolution_family_root(species) != starter_family],
        )
        selected = [starter]
        remaining_slots = max(team_size - 1, 0)
        if remaining_slots > 0:
            if len(selected_pool) <= remaining_slots:
                selected.extend(selected_pool)
            else:
                selected.extend(random.sample(selected_pool, remaining_slots))
        starter_species = starter
    else:
        # If we don't have enough Pokemon, use what we have
        if len(selected_pool) <= team_size:
            selected = selected_pool
        else:
            # Try to select diverse Pokemon (basic random selection)
            selected = random.sample(selected_pool, team_size)

    selected = [species for species in selected if isinstance(species, str)]

    # Generate levels with slight variance
    levels = [
        max(1, avg_level - 2 + random.randint(0, 4))
        for _ in range(len(selected))
    ]

    return build_team_from_species(selected, levels, starter_species=starter_species)


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


