from __future__ import annotations

import pandas as pd

from src.pipeline.silver.inputs.builders.evolution_normalization import (
    build_level_up_evolution_index_from_species_rules,
    normalize_candidate_pool_for_level,
    normalize_species_for_level,
)
from src.pipeline.silver.inputs.builders.player_teams import (
    build_player_team_compact_tables,
    build_progression_source_teams_from_encounters,
)
from src.pipeline.silver.inputs.reference_context import MoveReferenceContext


def test_level_up_evolution_normalization_rules() -> None:
    evolution_rules = {
        "geodude": [{"to_species": "graveler", "trigger": "level-up", "min_level": 25}],
        "graveler": [{"to_species": "golem", "trigger": "trade"}],
        "zubat": [{"to_species": "golbat", "trigger": "level-up", "min_level": 22}],
    }

    assert normalize_species_for_level(
        "geodude",
        member_level=50,
        evolution_rules=evolution_rules,
    )[0] == "graveler"
    assert normalize_species_for_level(
        "zubat",
        member_level=50,
        evolution_rules=evolution_rules,
    )[0] == "golbat"
    assert normalize_species_for_level(
        "graveler",
        member_level=50,
        evolution_rules=evolution_rules,
    )[0] == "graveler"
    assert normalize_species_for_level(
        "zubat",
        member_level=15,
        evolution_rules=evolution_rules,
    )[0] == "zubat"


def test_candidate_pool_post_normalization_drops_species_missing_in_game() -> None:
    evolution_rules = {
        "geodude": [{"to_species": "graveler", "trigger": "level-up", "min_level": 25}],
    }
    normalized, diagnostics = normalize_candidate_pool_for_level(
        [("geodude", 40, 30, 120)],
        member_level=50,
        evolution_rules=evolution_rules,
        legal_species={"geodude"},
    )
    assert normalized == []
    assert diagnostics["removed_after_validation"] == 1


def test_pokeapi_species_rules_can_be_used_directly_for_normalization() -> None:
    pokeapi_species_rules = {
        "geodude": {
            "species_name": "geodude",
            "base_species": "geodude",
            "evolution_stage": 1,
            "min_valid_level": None,
            "min_level_from_previous": None,
            "special_evolution_conditions": [],
        },
        "graveler": {
            "species_name": "graveler",
            "base_species": "geodude",
            "evolution_stage": 2,
            "min_valid_level": 25,
            "min_level_from_previous": 25,
            "special_evolution_conditions": [],
        },
        "golem": {
            "species_name": "golem",
            "base_species": "geodude",
            "evolution_stage": 3,
            "min_valid_level": 25,
            "min_level_from_previous": None,
            "special_evolution_conditions": [{"trigger": "trade"}],
        },
    }

    transition_index = build_level_up_evolution_index_from_species_rules(pokeapi_species_rules)
    assert normalize_species_for_level("geodude", member_level=50, evolution_rules=transition_index)[0] == "graveler"
    # Also validate direct use of pokeapi flattened rules without pre-conversion.
    assert normalize_species_for_level("geodude", member_level=50, evolution_rules=pokeapi_species_rules)[0] == "graveler"


def _reference_context() -> MoveReferenceContext:
    return MoveReferenceContext(
        move_profiles={
            "tackle": {"power": 40, "damage_class": "physical"},
            "quick-attack": {"power": 40, "damage_class": "physical"},
            "thunder-shock": {"power": 40, "damage_class": "special"},
            "vine-whip": {"power": 45, "damage_class": "physical"},
        },
        learnable_by_game_species={
            ("red", "bulbasaur"): {"tackle": 1, "vine-whip": 7},
            ("red", "charmander"): {"scratch": 1, "tackle": 1},
            ("red", "squirtle"): {"tackle": 1},
            ("red", "pidgey"): {"tackle": 1, "quick-attack": 9},
            ("red", "rattata"): {"tackle": 1, "quick-attack": 7},
            ("red", "zapdos"): {"thunder-shock": 1},
        },
    )


def test_generated_player_candidates_use_encounters_single_source_of_truth() -> None:
    encounters_df = pd.DataFrame(
        [
            {"boss_id": "red-brock", "location": "route-1", "pokemon": "pidgey", "level_min": 2, "level_max": 5, "methods": [], "game": "red"},
            {"boss_id": "red-brock", "location": "route-1", "pokemon": "rattata", "level_min": 2, "level_max": 4, "methods": [], "game": "red"},
            {"boss_id": "red-misty", "location": "power-plant", "pokemon": "zapdos", "level_min": 50, "level_max": 50, "methods": [], "game": "red"},
        ]
    )
    bosses_df = pd.DataFrame(
        [
            {"boss_id": "red-brock", "game_version": "red", "boss_name_canonical": "Brock", "boss_order": 1},
            {"boss_id": "red-misty", "game_version": "red", "boss_name_canonical": "Misty", "boss_order": 2},
        ]
    )
    boss_teams = [
        {"game_version": "red", "boss_name": "brock", "avg_level": 12},
        {"game_version": "red", "boss_name": "misty", "avg_level": 20},
    ]

    source_teams = build_progression_source_teams_from_encounters(
        encounters_df=encounters_df,
        bosses_df=bosses_df,
        boss_teams=boss_teams,
        catch_pool_size=2,
    )
    brock_teams = [row for row in source_teams if row.get("boss_id") == "red-brock"]
    misty_teams = [row for row in source_teams if row.get("boss_id") == "red-misty"]
    assert brock_teams
    assert misty_teams
    assert "zapdos" not in {species for team in brock_teams for species in team.get("pokemon", [])}
    assert "articuno" not in {species for team in source_teams for species in team.get("pokemon", [])}


def test_articuno_and_reference_only_species_cannot_appear_in_player_source_team_members() -> None:
    encounters_df = pd.DataFrame(
        [
            {"boss_id": "red-brock", "location": "route-1", "pokemon": "pidgey", "level_min": 2, "level_max": 5, "methods": [], "game": "red"},
            {"boss_id": "red-brock", "location": "route-1", "pokemon": "rattata", "level_min": 2, "level_max": 4, "methods": [], "game": "red"},
        ]
    )
    bosses_df = pd.DataFrame(
        [
            {"boss_id": "red-brock", "game_version": "red", "boss_name_canonical": "Brock", "boss_order": 1},
        ]
    )
    # "mew" acts as a pokemon_reference-only species for this test; it is absent from encounters.
    pokemon_reference_only = {"mew"}
    boss_teams = [{"game_version": "red", "boss_name": "brock", "avg_level": 12}]

    source_teams = build_progression_source_teams_from_encounters(encounters_df, bosses_df, boss_teams, catch_pool_size=2)
    compact = build_player_team_compact_tables(source_teams, _reference_context())
    generated_species = {row["pokemon_species"] for row in compact["source_team_members"]}

    assert "articuno" not in generated_species
    assert generated_species.isdisjoint(pokemon_reference_only)
