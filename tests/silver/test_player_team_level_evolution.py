from __future__ import annotations

from typing import Any

import pytest

from src.pipeline.silver.inputs.builders.evolution_normalization import (
    normalize_candidate_pool_for_level,
    normalize_species_for_level,
)
from src.pipeline.silver.inputs.builders.player_teams import (
    _propagate_obtainable_candidates,
    build_player_team_compact_tables,
)


class _StubReferenceContext:
    def damaging_moves(self, species: str, level: int, game_version: str) -> list[str]:
        return ["tackle"]


def test_compact_builder_evolves_level_up_species_at_member_level() -> None:
    progression_source_teams: list[dict[str, Any]] = [
        {
            "team_id": "prog:red:test",
            "game_version": "red",
            "team_role": "player",
            "origin": "generated",
            "is_player_candidate": True,
            "boss_name": "giovanni",
            "boss_id": "red:giovanni",
            "starter_condition": "grass",
            "avg_level": 50,
            "pokemon": ["pidgey"],
            "levels": [50],
        }
    ]
    evolution_rules_by_game = {
        "red": {
            "pidgey": [{"to_species": "pidgeotto", "trigger": "level-up", "min_level": 18}],
            "pidgeotto": [{"to_species": "pidgeot", "trigger": "level-up", "min_level": 36}],
        }
    }

    compact = build_player_team_compact_tables(
        progression_source_teams,
        _StubReferenceContext(),
        evolution_rules_by_game=evolution_rules_by_game,
    )
    members = compact["source_team_members"]
    evolved_non_starter = [
        row for row in members if int(row.get("slot") or 0) == 2 and str(row.get("pokemon_species") or "") == "pidgeot"
    ]
    assert evolved_non_starter


def test_normalize_species_for_level_allows_item_evolution_without_min_level() -> None:
    normalized, applied = normalize_species_for_level(
        "staryu",
        member_level=32,
        evolution_rules={
            "staryu": [
                {"to_species": "starmie", "trigger": "use-item", "required_item": "water-stone", "min_level": None}
            ]
        },
        allow_item_evolutions=True,
    )

    assert normalized == "starmie"
    assert applied
    assert applied[0]["trigger"] == "use-item"
    assert int(applied[0]["min_level"]) == 32


def test_normalize_species_for_level_blocks_item_evolution_when_disabled() -> None:
    normalized, applied = normalize_species_for_level(
        "staryu",
        member_level=32,
        evolution_rules={
            "staryu": [
                {"to_species": "starmie", "trigger": "use-item", "required_item": "water-stone", "min_level": None}
            ]
        },
        allow_item_evolutions=False,
    )

    assert normalized == "staryu"
    assert applied == []


def test_propagate_obtainable_candidates_includes_item_evolution_from_direct_base() -> None:
    rows, meta, direct_species = _propagate_obtainable_candidates(
        [("staryu", 55, 30, 200)],
        evolution_rules={
            "staryu": [
                {"to_species": "starmie", "trigger": "use-item", "required_item": "water-stone", "min_level": None}
            ]
        },
        allow_trade_evolutions=False,
        allow_item_evolutions=True,
        item_evolution_default_level=1,
    )

    rows_by_species = {species: (chance, lvl_max, capture) for species, chance, lvl_max, capture in rows}
    assert direct_species == {"staryu"}
    assert "staryu" in rows_by_species
    assert "starmie" in rows_by_species
    assert meta["staryu"]["directly_catchable"] is True
    assert meta["starmie"]["directly_catchable"] is False
    assert meta["starmie"]["obtain_method"] == "item_evolution"
    assert meta["starmie"]["required_item"] == "water-stone"
    assert int(meta["starmie"]["min_level"]) == 30


def test_normalize_candidate_pool_keeps_item_evolved_species_in_level_filter() -> None:
    normalized, diagnostics = normalize_candidate_pool_for_level(
        [("staryu", 55, 30, 200)],
        member_level=30,
        evolution_rules={
            "staryu": [
                {"to_species": "starmie", "trigger": "use-item", "required_item": "water-stone", "min_level": None}
            ]
        },
        legal_species={"starmie"},
        allow_item_evolutions=True,
    )

    assert [row[0] for row in normalized] == ["starmie"]
    assert diagnostics["removed_after_validation"] == 0


@pytest.mark.parametrize(
    ("base_species", "evolved_species", "required_item"),
    [
        ("staryu", "starmie", "water-stone"),
        ("shellder", "cloyster", "water-stone"),
        ("growlithe", "arcanine", "fire-stone"),
        ("vulpix", "ninetales", "fire-stone"),
        ("pikachu", "raichu", "thunder-stone"),
        ("nidorino", "nidoking", "moon-stone"),
        ("nidorina", "nidoqueen", "moon-stone"),
    ],
)
def test_item_evolution_species_without_min_level_are_generically_supported(
    base_species: str,
    evolved_species: str,
    required_item: str,
) -> None:
    normalized, applied = normalize_species_for_level(
        base_species,
        member_level=40,
        evolution_rules={
            base_species: [{"to_species": evolved_species, "trigger": "use-item", "required_item": required_item, "min_level": None}]
        },
        allow_item_evolutions=True,
    )

    assert normalized == evolved_species
    assert applied


