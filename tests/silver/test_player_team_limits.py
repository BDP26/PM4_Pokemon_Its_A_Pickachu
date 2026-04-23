from __future__ import annotations

import importlib


def test_effective_limit_unbounded_when_large_variants_enabled(monkeypatch) -> None:
    module = importlib.import_module("src.pipeline.silver.inputs.builders.player_teams")

    monkeypatch.setattr(module, "ALLOW_LARGE_TEAM_VARIANTS", True)
    monkeypatch.setattr(module, "DEFAULT_TEAM_VARIANT_LIMIT", 0)
    monkeypatch.setattr(module, "DEFAULT_MOVESET_VARIANT_LIMIT_PER_TEAM", 60)

    assert module._effective_team_variant_limit(variant_space_size=5000) is None


def test_effective_limit_conservative_mode_uses_smallest_active_cap(monkeypatch) -> None:
    module = importlib.import_module("src.pipeline.silver.inputs.builders.player_teams")

    monkeypatch.setattr(module, "ALLOW_LARGE_TEAM_VARIANTS", False)
    monkeypatch.setattr(module, "DEFAULT_TEAM_VARIANT_LIMIT", 120)
    monkeypatch.setattr(module, "DEFAULT_MOVESET_VARIANT_LIMIT_PER_TEAM", 60)

    assert module._effective_team_variant_limit(variant_space_size=5000) == 60


def test_progression_pools_use_cumulative_location_deltas() -> None:
    module = importlib.import_module("src.pipeline.silver.inputs.builders.player_teams")
    records = [
        {
            "game": "red",
            "boss_name": "brock",
            "part": "part1",
            "reachable_locations": ["route-1"],
            "reachable_location_encounters": {
                "route-1": [{"species": "pidgey", "encounter_chance_max": 60, "level_max": 5, "capture_rate": 255}]
            },
        },
        {
            "game": "red",
            "boss_name": "misty",
            "part": "part2",
            "reachable_locations": ["route-1", "route-2"],
            "reachable_location_encounters": {
                "route-1": [{"species": "pidgey", "encounter_chance_max": 60, "level_max": 5, "capture_rate": 255}],
                "route-2": [{"species": "caterpie", "encounter_chance_max": 40, "level_max": 7, "capture_rate": 255}],
            },
        },
    ]

    pools = module.build_boss_progression_pools(records)

    assert len(pools) == 2
    assert pools[0]["delta_location_count"] == 1
    assert pools[0]["delta_species_count"] == 1
    assert pools[0]["pool_species_count"] == 1
    assert pools[1]["delta_location_count"] == 1
    assert pools[1]["delta_species_count"] == 1
    assert pools[1]["pool_species_count"] == 2


def test_source_teams_are_capped_to_five_members(monkeypatch) -> None:
    module = importlib.import_module("src.pipeline.silver.inputs.builders.player_teams")
    monkeypatch.setattr(module, "DEFAULT_SOURCE_TEAM_COMBO_LIMIT", 1)

    records = [
        {
            "game": "red",
            "boss_name": "brock",
            "part": "part1",
            "reachable_locations": ["route-1"],
            "reachable_location_encounters": {
                "route-1": [
                    {"species": f"species-{idx}", "encounter_chance_max": 100 - idx, "level_max": 10, "capture_rate": 200}
                    for idx in range(8)
                ]
            },
        }
    ]
    boss_teams = [{"game_version": "red", "boss_name": "brock", "avg_level": 12}]

    source_teams = module.build_progression_source_teams(records, boss_teams, catch_pool_size=9)

    assert source_teams
    assert max(len(team["pokemon"]) for team in source_teams) <= 5


def test_member_moveset_variants_are_capped_at_moveset_width() -> None:
    module = importlib.import_module("src.pipeline.silver.inputs.builders.player_teams")
    variants = module._member_moveset_variants(["a", "b", "c", "d", "e", "f"])

    assert variants
    assert max(len(variant) for variant in variants) <= module.MOVESET_WIDTH
