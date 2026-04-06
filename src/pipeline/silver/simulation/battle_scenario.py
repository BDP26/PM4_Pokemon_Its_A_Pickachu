"""
Battle simulation data schema and preparation.

Defines data structures for:
- Boss encounters with team rosters
- Player progression state
- Route availability
- Battle simulation scenarios

Prepares silver layer data for gold layer battle simulator.
"""

from typing import Optional, TypedDict
from dataclasses import dataclass, asdict


class PokemonStats(TypedDict, total=False):
    """Pokemon base stats."""
    hp: int
    attack: int
    defense: int
    sp_attack: int
    sp_defense: int
    speed: int


class TeamMember(TypedDict, total=False):
    """A Pokemon in a team."""
    slot: int  # 1-indexed position in team
    species: str  # Pokemon species slug
    level: int
    stats: Optional[PokemonStats]


@dataclass
class BossTeam:
    """Boss team for a specific encounter."""
    boss_name: str
    boss_order: int  # 1-indexed position in game
    game: str
    team_size: int
    average_level: int
    members: list[dict]  # TeamMember dicts

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class RouteProgression:
    """Progression state for a specific point in game."""
    boss_order: int
    boss_name: str
    reachable_locations: list[str]
    available_pokemon: list[str]
    suggested_player_level: int
    suggested_team_size: int = 6

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class BattleScenario:
    """A single battle encounter scenario for simulation."""
    game: str
    boss_name: str
    boss_order: int
    boss_team: BossTeam
    progression_state: RouteProgression

    def to_dict(self) -> dict:
        return {
            "game": self.game,
            "boss_name": self.boss_name,
            "boss_order": self.boss_order,
            "boss_team": self.boss_team.to_dict(),
            "progression_state": self.progression_state.to_dict(),
        }


def create_boss_team_from_roster(
    boss_name: str,
    boss_order: int,
    game: str,
    species_list: list[str],
    base_level: int,
) -> BossTeam:
    """
    Create a BossTeam from a species roster.

    Args:
        boss_name: Name of boss trainer
        boss_order: Position in sequence (1-indexed)
        game: Game version slug
        species_list: List of Pokemon species
        base_level: Base level for team

    Returns:
        BossTeam instance
    """
    members = []
    for i, species in enumerate(species_list):
        # Slight level variance in team
        level_variance = (i - len(species_list) / 2) * 0.5
        level = max(1, min(100, base_level + int(level_variance)))

        members.append({
            "slot": i + 1,
            "species": species,
            "level": level,
        })

    return BossTeam(
        boss_name=boss_name,
        boss_order=boss_order,
        game=game,
        team_size=len(members),
        average_level=base_level,
        members=members,
    )


def create_progression_state(
    boss_order: int,
    boss_name: str,
    reachable_locations: list[str],
    available_pokemon: list[str],
    base_level: int,
) -> RouteProgression:
    """
    Create a RouteProgression state.

    Args:
        boss_order: Position in sequence (1-indexed)
        boss_name: Name of this boss
        reachable_locations: List of location slugs available
        available_pokemon: List of Pokemon species available
        base_level: Suggested player level

    Returns:
        RouteProgression instance
    """
    return RouteProgression(
        boss_order=boss_order,
        boss_name=boss_name,
        reachable_locations=reachable_locations,
        available_pokemon=available_pokemon,
        suggested_player_level=base_level,
        suggested_team_size=6,
    )


def silver_record_to_battle_scenario(
    silver_record: dict,
    base_level: int,
) -> Optional[BattleScenario]:
    """
    Convert a silver layer record to a battle scenario.

    Args:
        silver_record: Record from silver layer JSONL
        base_level: Base level for boss team

    Returns:
        BattleScenario if data is complete, else None
    """
    try:
        game = silver_record.get("game")
        boss_name = silver_record.get("boss_name")
        boss_order = silver_record.get("boss_order", 1)
        boss_team_roster = silver_record.get("boss_team", {}).get("members", [])

        if not all([game, boss_name]):
            return None

        # Extract species from boss team
        species_list = [m.get("species") for m in boss_team_roster if m.get("species")]

        if not species_list:
            return None

        # Create team
        boss_team = create_boss_team_from_roster(
            boss_name=boss_name,
            boss_order=boss_order,
            game=game,
            species_list=species_list,
            base_level=base_level,
        )

        # Create progression state
        progression = create_progression_state(
            boss_order=boss_order,
            boss_name=boss_name,
            reachable_locations=silver_record.get("reachable_locations", []),
            available_pokemon=silver_record.get("reachable_location_pokemon", {}).keys(),
            base_level=base_level,
        )

        return BattleScenario(
            game=game,
            boss_name=boss_name,
            boss_order=boss_order,
            boss_team=boss_team,
            progression_state=progression,
        )

    except Exception as e:
        print(f"Error converting record to scenario: {e}")
        return None


def validate_battle_scenario(scenario: BattleScenario) -> tuple[bool, list[str]]:
    """
    Validate a battle scenario has all required data.

    Returns:
        (is_valid, list of error messages)
    """
    errors = []

    if not scenario.game:
        errors.append("Missing game")

    if not scenario.boss_name:
        errors.append("Missing boss_name")

    if scenario.boss_order < 1:
        errors.append("Invalid boss_order (must be >= 1)")

    if not scenario.boss_team.members:
        errors.append("Boss team has no members")

    if scenario.boss_team.team_size > 6:
        errors.append(f"Boss team too large ({scenario.boss_team.team_size} > 6)")

    if scenario.boss_team.average_level < 1 or scenario.boss_team.average_level > 100:
        errors.append(f"Invalid boss team level ({scenario.boss_team.average_level})")

    if not scenario.progression_state.reachable_locations:
        errors.append("No reachable locations")

    if not scenario.progression_state.available_pokemon:
        errors.append("No available Pokemon")

    return len(errors) == 0, errors


