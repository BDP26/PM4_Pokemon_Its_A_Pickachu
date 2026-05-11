from __future__ import annotations

from src.pipeline.silver.inputs.builders.player_teams import (
    _generate_diverse_species_combos,
    _respects_team_type_weight_cap,
    _team_type_weight_totals,
)


def test_weighted_team_type_totals_use_dual_type_weighting() -> None:
    pokemon_types_by_species = {
        "gengar": ("ghost", "poison"),
        "haunter": ("ghost", "poison"),
        "gastly": ("ghost", "poison"),
        "psyduck": ("water", None),
    }
    totals = _team_type_weight_totals(
        ["gengar", "haunter", "gastly", "psyduck"],
        pokemon_types_by_species=pokemon_types_by_species,
    )

    assert totals["ghost"] == 2.25
    assert totals["poison"] == 2.25
    assert totals["water"] == 1.0
    assert _respects_team_type_weight_cap(
        ["gengar", "haunter", "psyduck"],
        pokemon_types_by_species=pokemon_types_by_species,
        cap=2.0,
    )
    assert not _respects_team_type_weight_cap(
        ["gengar", "haunter", "gastly"],
        pokemon_types_by_species=pokemon_types_by_species,
        cap=2.0,
    )


def test_generate_diverse_species_combos_respects_weighted_type_cap() -> None:
    candidates = [
        ("magikarp", 90, 10, 255),
        ("goldeen", 80, 10, 255),
        ("psyduck", 70, 10, 255),
        ("pidgey", 60, 10, 255),
    ]
    pokemon_types_by_species = {
        "magikarp": ("water", None),
        "goldeen": ("water", None),
        "psyduck": ("water", None),
        "pidgey": ("normal", "flying"),
    }
    combos = _generate_diverse_species_combos(
        candidates,
        team_fill_size=3,
        combo_limit=10,
        progression_depth=0.1,
        pokemon_types_by_species=pokemon_types_by_species,
        game_type_target_distribution={"water": 0.7, "normal": 0.2, "flying": 0.1},
        boss_type_profile={"rock": 1.0},
    )

    assert combos
    for combo in combos:
        totals = _team_type_weight_totals(combo, pokemon_types_by_species=pokemon_types_by_species)
        assert totals.get("water", 0.0) <= 2.0
