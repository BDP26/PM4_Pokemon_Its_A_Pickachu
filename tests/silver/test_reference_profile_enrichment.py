from __future__ import annotations

import pandas as pd

from src.pipeline.silver.orchestration.build_silver import (
    _collect_kaggle_boss_species_and_moves,
    _ensure_moves_in_combat_profiles,
    _ensure_species_in_pokemon_profiles,
    _move_profiles_from_reference,
)


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


def test_enrichment_adds_missing_kaggle_only_species_and_moves() -> None:
    pokemon_data_df = pd.DataFrame([{"name": "pikachu", "pokemon_species": "pikachu"}])
    enriched_pokemon_df = _ensure_species_in_pokemon_profiles(pokemon_data_df, {"pikachu", "aegislash"})
    enriched_species = set(enriched_pokemon_df["pokemon_species"].tolist())
    assert "aegislash" in enriched_species

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
