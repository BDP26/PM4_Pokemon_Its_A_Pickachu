from __future__ import annotations

import pandas as pd
import pytest

from src.pipeline.silver.inputs.builders.evolution_normalization import (
    build_level_up_evolution_index_from_species_rules,
    legal_species_pool_for_level,
    normalize_candidate_pool_for_level,
    normalize_species_for_level,
)
from src.pipeline.silver.inputs.builders.player_teams import (
    _rank_candidate_pool,
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


def test_legal_species_pool_for_level_tracks_forced_evolutions() -> None:
    evolution_rules = {
        "geodude": [{"to_species": "graveler", "trigger": "level-up", "min_level": 25}],
    }

    legal = legal_species_pool_for_level(
        [("geodude", 40, 46, 120)],
        member_level=53,
        evolution_rules=evolution_rules,
    )

    assert legal == {"graveler"}


def test_legal_species_pool_for_level_respects_encounter_level_cap() -> None:
    evolution_rules = {
        "geodude": [{"to_species": "graveler", "trigger": "level-up", "min_level": 25}],
    }

    legal = legal_species_pool_for_level(
        [("geodude", 40, 20, 120)],
        member_level=53,
        evolution_rules=evolution_rules,
    )

    assert legal == {"geodude"}


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


def test_generation_aware_normalization_blocks_future_evolution_forms() -> None:
    pokeapi_species_rules = {
        "mr-mime": {
            "species_name": "mr-mime",
            "base_species": "mr-mime",
            "evolution_stage": 1,
            "species_id": 122,
            "min_valid_level": None,
            "min_level_from_previous": None,
            "special_evolution_conditions": [],
        },
        "mr-rime": {
            "species_name": "mr-rime",
            "base_species": "mr-mime",
            "evolution_stage": 2,
            "species_id": 866,
            "min_valid_level": 42,
            "min_level_from_previous": 42,
            "special_evolution_conditions": [],
        },
    }

    normalized_species, _ = normalize_species_for_level(
        "mr-mime",
        member_level=50,
        evolution_rules=pokeapi_species_rules,
        target_generation=1,
    )

    assert normalized_species == "mr-mime"


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


@pytest.fixture(autouse=True)
def _stub_restricted_species(monkeypatch) -> None:
    restricted = {"articuno", "zapdos", "moltres", "mew", "mewtwo"}
    monkeypatch.setattr(
        "src.pipeline.silver.inputs.builders.player_teams.is_restricted_encounter_species",
        lambda species: str(species or "").strip().lower() in restricted,
    )


def test_generated_player_candidates_use_encounters_single_source_of_truth() -> None:
    encounters_df = pd.DataFrame(
        [
            {"boss_id": "red-brock", "location": "route-1", "pokemon": "pidgey", "level_min": 2, "level_max": 5, "methods": [], "game": "red"},
            {"boss_id": "red-brock", "location": "route-1", "pokemon": "rattata", "level_min": 2, "level_max": 4, "methods": [], "game": "red"},
            {"boss_id": "red-misty", "location": "power-plant", "pokemon": "zapdos", "level_min": 50, "level_max": 50, "methods": [], "game": "red"},
            {"boss_id": "red-misty", "location": "route-24", "pokemon": "pidgey", "level_min": 18, "level_max": 20, "methods": [], "game": "red"},
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


def test_restricted_legendary_encounters_are_removed_from_player_candidates() -> None:
    encounters_df = pd.DataFrame(
        [
            {"boss_id": "red-lance", "location": "victory-road", "pokemon": "articuno", "level_min": 50, "level_max": 50, "encounter_chance_max": 100, "capture_rate": 3, "methods": [], "game": "red"},
            {"boss_id": "red-lance", "location": "victory-road", "pokemon": "graveler", "level_min": 45, "level_max": 47, "encounter_chance_max": 20, "capture_rate": 120, "methods": [], "game": "red"},
        ]
    )
    bosses_df = pd.DataFrame(
        [
            {"boss_id": "red-lance", "game_version": "red", "boss_name_canonical": "Lance", "boss_order": 12},
        ]
    )
    boss_teams = [{"game_version": "red", "boss_name": "lance", "avg_level": 53}]

    source_teams = build_progression_source_teams_from_encounters(
        encounters_df=encounters_df,
        bosses_df=bosses_df,
        boss_teams=boss_teams,
        catch_pool_size=1,
    )

    generated_species = {species for team in source_teams for species in team.get("pokemon", [])}
    assert "articuno" not in generated_species
    assert generated_species == {"graveler"}


def test_restricted_encounter_methods_are_removed_from_player_candidates() -> None:
    encounters_df = pd.DataFrame(
        [
            {"boss_id": "red-lance", "location": "gift-house", "pokemon": "eevee", "level_min": 25, "level_max": 25, "encounter_chance_max": 100, "capture_rate": 45, "methods": ["Gift"], "game": "red"},
            {"boss_id": "red-lance", "location": "trade-npc", "pokemon": "mr-mime", "level_min": 20, "level_max": 20, "encounter_chance_max": 100, "capture_rate": 45, "methods": ["NPC Trade"], "game": "red"},
            {"boss_id": "red-lance", "location": "power-plant", "pokemon": "electrode", "level_min": 43, "level_max": 43, "encounter_chance_max": 100, "capture_rate": 60, "methods": ["Only One"], "game": "red"},
            {"boss_id": "red-lance", "location": "victory-road", "pokemon": "graveler", "level_min": 45, "level_max": 47, "encounter_chance_max": 20, "capture_rate": 120, "methods": ["walk"], "game": "red"},
        ]
    )
    bosses_df = pd.DataFrame(
        [
            {"boss_id": "red-lance", "game_version": "red", "boss_name_canonical": "Lance", "boss_order": 12},
        ]
    )
    boss_teams = [{"game_version": "red", "boss_name": "lance", "avg_level": 53}]

    source_teams = build_progression_source_teams_from_encounters(
        encounters_df=encounters_df,
        bosses_df=bosses_df,
        boss_teams=boss_teams,
        catch_pool_size=2,
    )

    generated_species = {species for team in source_teams for species in team.get("pokemon", [])}
    assert generated_species == {"graveler"}


def test_restricted_encounter_method_variants_are_removed_from_player_candidates() -> None:
    encounters_df = pd.DataFrame(
        [
            {"boss_id": "red-lance", "location": "gift-house", "pokemon": "eevee", "level_min": 25, "level_max": 25, "encounter_chance_max": 100, "capture_rate": 45, "methods": ["gift-pokemon"], "game": "red"},
            {"boss_id": "red-lance", "location": "trade-npc", "pokemon": "mr-mime", "level_min": 20, "level_max": 20, "encounter_chance_max": 100, "capture_rate": 45, "methods": ["npc-trade"], "game": "red"},
            {"boss_id": "red-lance", "location": "power-plant", "pokemon": "electrode", "level_min": 43, "level_max": 43, "encounter_chance_max": 100, "capture_rate": 60, "methods": ["only_one"], "game": "red"},
            {"boss_id": "red-lance", "location": "victory-road", "pokemon": "graveler", "level_min": 45, "level_max": 47, "encounter_chance_max": 20, "capture_rate": 120, "methods": ["walk"], "game": "red"},
        ]
    )
    bosses_df = pd.DataFrame(
        [
            {"boss_id": "red-lance", "game_version": "red", "boss_name_canonical": "Lance", "boss_order": 12},
        ]
    )
    boss_teams = [{"game_version": "red", "boss_name": "lance", "avg_level": 53}]

    source_teams = build_progression_source_teams_from_encounters(
        encounters_df=encounters_df,
        bosses_df=bosses_df,
        boss_teams=boss_teams,
        catch_pool_size=2,
    )
    generated_species = {species for team in source_teams for species in team.get("pokemon", [])}
    assert generated_species == {"graveler"}


def test_restricted_encounter_methods_filter_handles_tuple_method_values() -> None:
    encounters_df = pd.DataFrame(
        [
            {"boss_id": "red-lance", "location": "dragonspiral", "pokemon": "reshiram", "level_min": 50, "level_max": 50, "encounter_chance_max": 100, "capture_rate": 3, "methods": ("only-one",), "game": "red"},
            {"boss_id": "red-lance", "location": "victory-road", "pokemon": "graveler", "level_min": 45, "level_max": 47, "encounter_chance_max": 20, "capture_rate": 120, "methods": ["walk"], "game": "red"},
        ]
    )
    bosses_df = pd.DataFrame(
        [
            {"boss_id": "red-lance", "game_version": "red", "boss_name_canonical": "Lance", "boss_order": 12},
        ]
    )
    boss_teams = [{"game_version": "red", "boss_name": "lance", "avg_level": 53}]

    source_teams = build_progression_source_teams_from_encounters(
        encounters_df=encounters_df,
        bosses_df=bosses_df,
        boss_teams=boss_teams,
        catch_pool_size=2,
    )
    generated_species = {species for team in source_teams for species in team.get("pokemon", [])}
    assert generated_species == {"graveler"}


def test_rank_candidate_pool_early_stage_prefers_less_rare_species() -> None:
    candidates = [
        ("rare-high-level", 5, 25, 45),
        ("common-lower-level", 60, 20, 255),
    ]
    ranked, _ = _rank_candidate_pool(
        candidates,
        boss_level=25,
        pool_size=2,
        progression_depth=0.05,
    )
    assert ranked[0][0] == "common-lower-level"


def test_progression_based_team_size_is_smaller_early_and_larger_late() -> None:
    encounters_df = pd.DataFrame(
        [
            {"boss_id": "red-brock", "location": "route-1", "pokemon": "pidgey", "level_min": 2, "level_max": 5, "encounter_chance_max": 60, "capture_rate": 255, "methods": ["walk"], "game": "red"},
            {"boss_id": "red-brock", "location": "route-1", "pokemon": "rattata", "level_min": 2, "level_max": 5, "encounter_chance_max": 55, "capture_rate": 255, "methods": ["walk"], "game": "red"},
            {"boss_id": "red-brock", "location": "route-2", "pokemon": "caterpie", "level_min": 3, "level_max": 6, "encounter_chance_max": 50, "capture_rate": 255, "methods": ["walk"], "game": "red"},
            {"boss_id": "red-brock", "location": "route-2", "pokemon": "weedle", "level_min": 3, "level_max": 6, "encounter_chance_max": 45, "capture_rate": 255, "methods": ["walk"], "game": "red"},
            {"boss_id": "red-brock", "location": "viridian-forest", "pokemon": "pikachu", "level_min": 4, "level_max": 7, "encounter_chance_max": 10, "capture_rate": 190, "methods": ["walk"], "game": "red"},
            {"boss_id": "red-mewtwo", "location": "victory-road", "pokemon": "golbat", "level_min": 42, "level_max": 45, "encounter_chance_max": 40, "capture_rate": 90, "methods": ["walk"], "game": "red"},
            {"boss_id": "red-mewtwo", "location": "victory-road", "pokemon": "onix", "level_min": 42, "level_max": 45, "encounter_chance_max": 35, "capture_rate": 45, "methods": ["walk"], "game": "red"},
            {"boss_id": "red-mewtwo", "location": "seafoam", "pokemon": "seel", "level_min": 35, "level_max": 40, "encounter_chance_max": 50, "capture_rate": 190, "methods": ["surf"], "game": "red"},
            {"boss_id": "red-mewtwo", "location": "seafoam", "pokemon": "slowpoke", "level_min": 32, "level_max": 38, "encounter_chance_max": 45, "capture_rate": 190, "methods": ["surf"], "game": "red"},
            {"boss_id": "red-mewtwo", "location": "power-plant", "pokemon": "magneton", "level_min": 38, "level_max": 44, "encounter_chance_max": 25, "capture_rate": 60, "methods": ["walk"], "game": "red"},
            {"boss_id": "red-mewtwo", "location": "power-plant", "pokemon": "electabuzz", "level_min": 38, "level_max": 43, "encounter_chance_max": 20, "capture_rate": 45, "methods": ["walk"], "game": "red"},
        ]
    )
    bosses_df = pd.DataFrame(
        [
            {"boss_id": "red-brock", "game_version": "red", "boss_name_canonical": "Brock", "boss_order": 1},
            {"boss_id": "red-mewtwo", "game_version": "red", "boss_name_canonical": "Mewtwo", "boss_order": 99},
        ]
    )
    boss_teams = [
        {"game_version": "red", "boss_name": "brock", "avg_level": 12},
        {"game_version": "red", "boss_name": "mewtwo", "avg_level": 70},
    ]

    source_teams = build_progression_source_teams_from_encounters(
        encounters_df=encounters_df,
        bosses_df=bosses_df,
        boss_teams=boss_teams,
        catch_pool_size=5,
    )
    early_sizes = {len(team.get("pokemon", [])) for team in source_teams if team.get("boss_id") == "red-brock"}
    late_sizes = {len(team.get("pokemon", [])) for team in source_teams if team.get("boss_id") == "red-mewtwo"}
    assert early_sizes
    assert late_sizes
    assert max(early_sizes) < max(late_sizes)


def test_progression_source_teams_preserve_colon_boss_ids() -> None:
    encounters_df = pd.DataFrame(
        [
            {"boss_id": "blue:lt-surge", "location": "route-11", "pokemon": "drowzee", "level_min": 11, "level_max": 15, "encounter_chance_max": 30, "capture_rate": 190, "methods": [], "game": "blue"},
            {"boss_id": "blue:lt-surge", "location": "route-11", "pokemon": "rattata", "level_min": 12, "level_max": 14, "encounter_chance_max": 40, "capture_rate": 255, "methods": [], "game": "blue"},
        ]
    )
    bosses_df = pd.DataFrame(
        [
            {"boss_id": "blue:lt-surge", "game_version": "blue", "boss_name_canonical": "Lt. Surge", "boss_order": 3},
        ]
    )
    boss_teams = [{"game_version": "blue", "boss_name": "lt-surge", "avg_level": 24}]

    source_teams = build_progression_source_teams_from_encounters(
        encounters_df=encounters_df,
        bosses_df=bosses_df,
        boss_teams=boss_teams,
        catch_pool_size=2,
    )

    assert source_teams
    assert {row["boss_id"] for row in source_teams} == {"blue:lt-surge"}


def test_progression_source_teams_map_legacy_encounter_boss_ids_to_canonical_ids() -> None:
    encounters_df = pd.DataFrame(
        [
            {"boss_id": "black:chili", "location": "unova-route-1", "pokemon": "patrat", "level_min": 2, "level_max": 4, "encounter_chance_max": 40, "capture_rate": 255, "methods": [], "game": "black"},
            {"boss_id": "black:chili", "location": "unova-route-1", "pokemon": "lillipup", "level_min": 2, "level_max": 4, "encounter_chance_max": 35, "capture_rate": 255, "methods": [], "game": "black"},
        ]
    )
    bosses_df = pd.DataFrame(
        [
            {"boss_id": "boss:black:chili:abc123", "game_version": "black", "boss_name_canonical": "Chili", "boss_order": 1, "gym_index": 1},
        ]
    )
    boss_teams = [{"game_version": "black", "boss_name": "chili", "avg_level": 14}]

    source_teams = build_progression_source_teams_from_encounters(
        encounters_df=encounters_df,
        bosses_df=bosses_df,
        boss_teams=boss_teams,
        catch_pool_size=2,
    )

    assert source_teams
    assert {row["boss_id"] for row in source_teams} == {"boss:black:chili:abc123"}


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


def test_evolved_forms_from_encounter_pool_remain_legal_for_generated_teams() -> None:
    encounters_df = pd.DataFrame(
        [
            {"boss_id": "red-lance", "location": "victory-road", "pokemon": "geodude", "level_min": 41, "level_max": 46, "methods": [], "game": "red"},
        ]
    )
    bosses_df = pd.DataFrame(
        [
            {"boss_id": "red-lance", "game_version": "red", "boss_name_canonical": "Lance", "boss_order": 12},
        ]
    )
    boss_teams = [{"game_version": "red", "boss_name": "lance", "avg_level": 53}]
    evolution_rules_by_game = {
        "red": {
            "geodude": [{"to_species": "graveler", "trigger": "level-up", "min_level": 25}],
        }
    }

    source_teams = build_progression_source_teams_from_encounters(
        encounters_df=encounters_df,
        bosses_df=bosses_df,
        boss_teams=boss_teams,
        catch_pool_size=1,
        evolution_rules_by_game=evolution_rules_by_game,
    )

    generated_species = {species for team in source_teams for species in team.get("pokemon", [])}
    assert generated_species == {"graveler"}
    assert {level for team in source_teams for level in team.get("levels", [])} == {46}


def test_encounter_level_cap_prevents_forced_evolution_in_generated_teams() -> None:
    encounters_df = pd.DataFrame(
        [
            {"boss_id": "red-test", "location": "mt-moon", "pokemon": "geodude", "level_min": 18, "level_max": 20, "methods": [], "game": "red"},
        ]
    )
    bosses_df = pd.DataFrame(
        [
            {"boss_id": "red-test", "game_version": "red", "boss_name_canonical": "Test", "boss_order": 99},
        ]
    )
    boss_teams = [{"game_version": "red", "boss_name": "test", "avg_level": 53}]
    evolution_rules_by_game = {
        "red": {
            "geodude": [{"to_species": "graveler", "trigger": "level-up", "min_level": 25}],
        }
    }

    source_teams = build_progression_source_teams_from_encounters(
        encounters_df=encounters_df,
        bosses_df=bosses_df,
        boss_teams=boss_teams,
        catch_pool_size=1,
        evolution_rules_by_game=evolution_rules_by_game,
    )

    generated_species = {species for team in source_teams for species in team.get("pokemon", [])}
    generated_levels = {level for team in source_teams for level in team.get("levels", [])}
    assert generated_species == {"geodude"}
    assert generated_levels == {20}


def test_build_progression_source_teams_from_encounters_uses_chance_and_capture_for_ranking() -> None:
    encounters_df = pd.DataFrame(
        [
            {"boss_id": "red-misty", "location": "route-24", "pokemon": "psyduck", "level_min": 18, "level_max": 20, "encounter_chance_max": 65, "capture_rate": 190, "methods": [], "game": "red"},
            {"boss_id": "red-misty", "location": "route-24", "pokemon": "oddish", "level_min": 19, "level_max": 21, "encounter_chance_max": 5, "capture_rate": 45, "methods": [], "game": "red"},
        ]
    )
    bosses_df = pd.DataFrame(
        [
            {"boss_id": "red-misty", "game_version": "red", "boss_name_canonical": "Misty", "boss_order": 2},
        ]
    )
    boss_teams = [{"game_version": "red", "boss_name": "misty", "avg_level": 20}]

    source_teams = build_progression_source_teams_from_encounters(
        encounters_df=encounters_df,
        bosses_df=bosses_df,
        boss_teams=boss_teams,
        catch_pool_size=1,
    )

    generated_species = [species for team in source_teams for species in team.get("pokemon", [])]
    assert generated_species
    assert generated_species[0] == "psyduck"


def test_build_progression_source_teams_from_encounters_defaults_missing_ranking_columns(caplog) -> None:
    encounters_df = pd.DataFrame(
        [
            {"boss_id": "red-brock", "location": "route-1", "pokemon": "pidgey", "level_min": 2, "level_max": 5, "methods": [], "game": "red"},
        ]
    )
    bosses_df = pd.DataFrame(
        [
            {"boss_id": "red-brock", "game_version": "red", "boss_name_canonical": "Brock", "boss_order": 1},
        ]
    )
    boss_teams = [{"game_version": "red", "boss_name": "brock", "avg_level": 12}]

    with caplog.at_level("WARNING"):
        source_teams = build_progression_source_teams_from_encounters(
            encounters_df=encounters_df,
            bosses_df=bosses_df,
            boss_teams=boss_teams,
            catch_pool_size=1,
        )

    assert source_teams
    assert "missing optional ranking columns" in caplog.text


def test_build_progression_source_teams_filters_species_without_damaging_moves_before_combo_generation() -> None:
    encounters_df = pd.DataFrame(
        [
            {"boss_id": "red-brock", "location": "route-1", "pokemon": "magikarp", "level_min": 5, "level_max": 10, "encounter_chance_max": 70, "capture_rate": 255, "methods": [], "game": "red"},
            {"boss_id": "red-brock", "location": "route-1", "pokemon": "pidgey", "level_min": 4, "level_max": 8, "encounter_chance_max": 30, "capture_rate": 255, "methods": [], "game": "red"},
        ]
    )
    bosses_df = pd.DataFrame(
        [
            {"boss_id": "red-brock", "game_version": "red", "boss_name_canonical": "Brock", "boss_order": 1},
        ]
    )
    boss_teams = [{"game_version": "red", "boss_name": "brock", "avg_level": 12}]
    reference_context = MoveReferenceContext(
        move_profiles={
            "splash": {"effective_power": 0, "damage_class": "status"},
            "tackle": {"effective_power": 40, "damage_class": "physical"},
        },
        learnable_by_game_species={
            ("red", "magikarp"): {"splash": 1},
            ("red", "pidgey"): {"tackle": 1},
        },
    )

    source_teams = build_progression_source_teams_from_encounters(
        encounters_df=encounters_df,
        bosses_df=bosses_df,
        boss_teams=boss_teams,
        catch_pool_size=1,
        reference_context=reference_context,
    )

    generated_species = {species for team in source_teams for species in team.get("pokemon", [])}
    assert generated_species == {"pidgey"}


def test_build_progression_source_teams_blocks_cross_generation_future_species() -> None:
    encounters_df = pd.DataFrame(
        [
            {"boss_id": "red-blaine", "location": "pokemon-mansion", "pokemon": "mr-mime", "level_min": 42, "level_max": 45, "encounter_chance_max": 100, "capture_rate": 45, "methods": [], "game": "red"},
        ]
    )
    bosses_df = pd.DataFrame(
        [
            {"boss_id": "red-blaine", "game_version": "red", "boss_name_canonical": "Blaine", "boss_order": 7},
        ]
    )
    boss_teams = [{"game_version": "red", "boss_name": "blaine", "avg_level": 47}]
    evolution_rules_by_game = {
        "red": {
            "mr-mime": {
                "species_name": "mr-mime",
                "base_species": "mr-mime",
                "evolution_stage": 1,
                "species_id": 122,
                "min_valid_level": None,
                "min_level_from_previous": None,
                "special_evolution_conditions": [],
            },
            "mr-rime": {
                "species_name": "mr-rime",
                "base_species": "mr-mime",
                "evolution_stage": 2,
                "species_id": 866,
                "min_valid_level": 42,
                "min_level_from_previous": 42,
                "special_evolution_conditions": [],
            },
        }
    }

    source_teams = build_progression_source_teams_from_encounters(
        encounters_df=encounters_df,
        bosses_df=bosses_df,
        boss_teams=boss_teams,
        catch_pool_size=1,
        evolution_rules_by_game=evolution_rules_by_game,
    )

    generated_species = {species for team in source_teams for species in team.get("pokemon", [])}
    assert generated_species == {"mr-mime"}
