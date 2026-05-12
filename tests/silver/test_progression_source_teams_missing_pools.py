from __future__ import annotations

import pandas as pd

from src.pipeline.silver.inputs.builders.player_teams import (
    build_progression_source_teams_from_encounters,
)
from src.pipeline.silver.transforms.progression_depth import build_progression_depth_context


def test_build_progression_source_teams_skips_boss_without_encounter_pool() -> None:
    encounters_df = pd.DataFrame(
        [
            {
                "game": "gold",
                "boss_id": "boss:gold:falkner:aaa",
                "location": "route-29",
                "pokemon": "pidgey",
                "level_min": 2,
                "level_max": 4,
                "encounter_chance_max": 60,
                "capture_rate": 255,
            }
        ]
    )
    bosses_df = pd.DataFrame(
        [
            {
                "game_version": "gold",
                "boss_id": "boss:gold:falkner:aaa",
                "boss_name_canonical": "Falkner",
                "boss_order": 1,
            },
            {
                "game_version": "gold",
                "boss_id": "boss:gold:lt-surge:bbb",
                "boss_name_canonical": "Lt. Surge",
                "boss_order": 2,
            },
        ]
    )
    progression_depth_df = pd.DataFrame(
        [
            {
                "game_version": "gold",
                "boss_id": "boss:gold:falkner:aaa",
                "boss_name": "falkner",
                "boss_index": 1,
                "max_boss_index": 1,
                "available_species_count": 1,
                "max_species_count": 1,
                "progression_depth": 1.0,
            }
        ]
    )
    boss_level_df = pd.DataFrame(
        [
            {
                "game_version": "gold",
                "boss_name": "falkner",
                "boss_ace_level": 9,
                "boss_avg_level": 8,
            }
        ]
    )
    progression_depth_context = build_progression_depth_context(
        progression_depth_df=progression_depth_df,
        boss_level_df=boss_level_df,
    )

    teams = build_progression_source_teams_from_encounters(
        encounters_df=encounters_df,
        bosses_df=bosses_df,
        progression_depth_context=progression_depth_context,
        catch_pool_size=2,
    )

    assert teams
    assert all(str(row.get("boss_id")) == "boss:gold:falkner:aaa" for row in teams)


def test_build_progression_source_teams_filters_species_above_level_cap_by_obtainable_level() -> None:
    encounters_df = pd.DataFrame(
        [
            {
                "game": "diamond",
                "boss_id": "boss:diamond:roark:aaa",
                "location": "route-203",
                "pokemon": "magikarp",
                "level_min": 3,
                "level_max": 25,
                "encounter_chance_max": 60,
                "capture_rate": 255,
            },
            {
                "game": "diamond",
                "boss_id": "boss:diamond:roark:aaa",
                "location": "route-203",
                "pokemon": "gyarados",
                "level_min": 30,
                "level_max": 55,
                "encounter_chance_max": 40,
                "capture_rate": 45,
            },
            {
                "game": "diamond",
                "boss_id": "boss:diamond:roark:aaa",
                "location": "route-203",
                "pokemon": "goldeen",
                "level_min": 4,
                "level_max": 25,
                "encounter_chance_max": 40,
                "capture_rate": 225,
            },
            {
                "game": "diamond",
                "boss_id": "boss:diamond:roark:aaa",
                "location": "route-203",
                "pokemon": "seaking",
                "level_min": 20,
                "level_max": 50,
                "encounter_chance_max": 40,
                "capture_rate": 60,
            },
        ]
    )
    bosses_df = pd.DataFrame(
        [
            {
                "game_version": "diamond",
                "boss_id": "boss:diamond:roark:aaa",
                "boss_name_canonical": "Roark",
                "boss_order": 1,
            }
        ]
    )
    progression_depth_df = pd.DataFrame(
        [
            {
                "game_version": "diamond",
                "boss_id": "boss:diamond:roark:aaa",
                "boss_name": "roark",
                "boss_index": 1,
                "max_boss_index": 10,
                "available_species_count": 2,
                "max_species_count": 20,
                "progression_depth": 0.1,
            }
        ]
    )
    boss_level_df = pd.DataFrame(
        [
            {
                "game_version": "diamond",
                "boss_name": "roark",
                "boss_ace_level": 12,
                "boss_avg_level": 12,
            }
        ]
    )
    progression_depth_context = build_progression_depth_context(
        progression_depth_df=progression_depth_df,
        boss_level_df=boss_level_df,
    )

    teams = build_progression_source_teams_from_encounters(
        encounters_df=encounters_df,
        bosses_df=bosses_df,
        progression_depth_context=progression_depth_context,
        catch_pool_size=3,
        evolution_rules_by_game={
            "diamond": {
                "magikarp": [{"to_species": "gyarados", "trigger": "level-up", "min_level": 20}],
                "goldeen": [{"to_species": "seaking", "trigger": "level-up", "min_level": 33}],
            }
        },
    )

    all_species = {species for team in teams for species in list(team.get("pokemon") or [])}
    assert "magikarp" in all_species
    assert "goldeen" in all_species
    assert "gyarados" not in all_species
    assert "seaking" not in all_species


