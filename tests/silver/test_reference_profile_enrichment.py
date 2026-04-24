from __future__ import annotations

import pandas as pd

from src.pipeline.silver.orchestration.build_silver import (
    _collect_kaggle_boss_species_and_moves,
    _ensure_moves_in_combat_profiles,
    _profile_from_pokemon_payload,
    _move_profiles_from_reference,
)
from src.pipeline.silver.inputs.reference_context import normalize_species_slug


def test_collects_kaggle_species_and_moves_from_boss_teams() -> None:
    species, moves = _collect_kaggle_boss_species_and_moves(
        [
            {
                "pokemon": ["pikachu", "Aegislash"],
                "moves": [["thunderbolt", "quick-attack"], ["shadow-ball", "kings-shield"]],
            }
        ]
    )
    assert "pikachu" in species
    assert "aegislash" in species
    assert "kings-shield" in moves


def test_enrichment_adds_missing_kaggle_only_moves() -> None:
    move_reference_df = pd.DataFrame(
        [
            {
                "move_name": "kings-shield",
                "type": "steel",
                "power": 0,
                "damage_class": "status",
                "accuracy": None,
                "pp": 10,
            }
        ]
    )
    move_profiles = _move_profiles_from_reference(move_reference_df)
    move_data = {
        "team:1:member:1": {
            "pokemon_instance_id": "team:1:member:1",
            "team_id": "team:1",
            "species": "pikachu",
            "level": 50,
            "game_version": "silver",
            "provided_moves": ["thunderbolt"],
            "learnable_moves": ["thunderbolt"],
            "move_details": {
                "thunderbolt": {
                    "move_name": "thunderbolt",
                    "type": "electric",
                    "power": 90,
                    "damage_class": "special",
                }
            },
            "slot_index": 1,
        }
    }
    enriched_move_data = _ensure_moves_in_combat_profiles(
        move_data,
        {"thunderbolt", "kings-shield"},
        move_profiles,
    )
    cached_moves = set()
    for payload in enriched_move_data.values():
        cached_moves.update(payload.get("move_details", {}).keys())
    assert "kings-shield" in cached_moves


def test_profile_flattening_maps_stats_and_types() -> None:
    payload = {
        "id": 122,
        "name": "mr-mime",
        "species": {"name": "mr-mime"},
        "types": [{"slot": 2, "type": {"name": "fairy"}}, {"slot": 1, "type": {"name": "psychic"}}],
        "stats": [
            {"stat": {"name": "hp"}, "base_stat": 40},
            {"stat": {"name": "attack"}, "base_stat": 45},
            {"stat": {"name": "defense"}, "base_stat": 65},
            {"stat": {"name": "special-attack"}, "base_stat": 100},
            {"stat": {"name": "special-defense"}, "base_stat": 120},
            {"stat": {"name": "speed"}, "base_stat": 90},
        ],
        "height": 13,
        "weight": 545,
        "base_experience": 161,
        "is_default": True,
    }
    profile = _profile_from_pokemon_payload(payload)
    assert profile["pokemon_species"] == "mr-mime"
    assert profile["type_1"] == "psychic"
    assert profile["type_2"] == "fairy"
    assert profile["base_special_attack"] == 100
    assert profile["pokeapi_id"] == 122


def test_species_alias_normalization_for_mr_mime() -> None:
    assert normalize_species_slug("mr. mime") == "mr-mime"
