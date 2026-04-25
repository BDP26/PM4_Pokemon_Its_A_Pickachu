from __future__ import annotations

import pandas as pd

from src.pipeline.silver.orchestration.build_silver import (
    _collect_kaggle_boss_species_and_moves,
    _ensure_moves_in_combat_profiles,
    _resolve_requested_pokemon_profile,
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


def _fake_payload(name: str, poke_id: int, species: str | None = None, is_default: bool = True) -> dict:
    return {
        "id": poke_id,
        "name": name,
        "species": {"name": species or name},
        "types": [{"slot": 1, "type": {"name": "normal"}}],
        "stats": [
            {"stat": {"name": "hp"}, "base_stat": 60},
            {"stat": {"name": "attack"}, "base_stat": 60},
            {"stat": {"name": "defense"}, "base_stat": 60},
            {"stat": {"name": "special-attack"}, "base_stat": 60},
            {"stat": {"name": "special-defense"}, "base_stat": 60},
            {"stat": {"name": "speed"}, "base_stat": 60},
        ],
        "height": 10,
        "weight": 100,
        "base_experience": 100,
        "is_default": is_default,
    }


def test_species_default_resolution_examples() -> None:
    species_payloads = {
        "aegislash": {"name": "aegislash", "varieties": [{"is_default": True, "pokemon": {"name": "aegislash-shield"}}]},
        "gourgeist": {"name": "gourgeist", "varieties": [{"is_default": True, "pokemon": {"name": "gourgeist-average"}}]},
        "meowstic": {"name": "meowstic", "varieties": [{"is_default": True, "pokemon": {"name": "meowstic-male"}}]},
        "pyroar": {"name": "pyroar", "varieties": [{"is_default": True, "pokemon": {"name": "pyroar-male"}}]},
        "pumpkaboo": {"name": "pumpkaboo", "varieties": [{"is_default": True, "pokemon": {"name": "pumpkaboo-average"}}]},
        "raichu": {"name": "raichu", "varieties": [{"is_default": True, "pokemon": {"name": "raichu"}}]},
        "frillish": {"name": "frillish", "varieties": [{"is_default": True, "pokemon": {"name": "frillish"}}]},
        "jellicent": {"name": "jellicent", "varieties": [{"is_default": True, "pokemon": {"name": "jellicent"}}]},
    }
    pokemon_payloads = {
        "aegislash-shield": _fake_payload("aegislash-shield", 681, species="aegislash"),
        "gourgeist-average": _fake_payload("gourgeist-average", 711, species="gourgeist"),
        "meowstic-male": _fake_payload("meowstic-male", 678, species="meowstic"),
        "pyroar-male": _fake_payload("pyroar-male", 668, species="pyroar"),
        "pumpkaboo-average": _fake_payload("pumpkaboo-average", 710, species="pumpkaboo"),
        "raichu": _fake_payload("raichu", 26, species="raichu"),
        "frillish": _fake_payload("frillish", 592, species="frillish"),
        "jellicent": _fake_payload("jellicent", 593, species="jellicent"),
    }

    def _fetch(url: str) -> dict | None:
        if "/pokemon-species/" in url:
            key = url.rstrip("/").split("/")[-1]
            return species_payloads.get(key)
        if "/pokemon/" in url:
            key = url.rstrip("/").split("/")[-1]
            return pokemon_payloads.get(key)
        return None

    expected = {
        "aegislash": "aegislash-shield",
        "gourgeist": "gourgeist-average",
        "meowstic": "meowstic-male",
        "pyroar": "pyroar-male",
        "pumpkaboo-large": "pumpkaboo-average",
        "pumpkaboo-small": "pumpkaboo-average",
        "pumpkaboo-super": "pumpkaboo-average",
        "raichu-alola": "raichu",
        "frillish-male": "frillish",
        "jellicent-male": "jellicent",
    }
    for requested, resolved in expected.items():
        profile, failure = _resolve_requested_pokemon_profile(requested, _fetch)
        assert failure is None
        assert profile is not None
        assert profile["resolved_pokemon_name"] == resolved
        assert profile["resolved_pokeapi_id"] is not None


def test_exact_resolution_examples_for_variant_forms() -> None:
    pokemon_payloads = {
        "basculin-blue-striped": _fake_payload("basculin-blue-striped", 550, species="basculin", is_default=False),
        "basculin-red-striped": _fake_payload("basculin-red-striped", 550, species="basculin", is_default=True),
        "zygarde-50": _fake_payload("zygarde-50", 718, species="zygarde", is_default=False),
    }

    def _fetch(url: str) -> dict | None:
        if "/pokemon/" in url:
            key = url.rstrip("/").split("/")[-1]
            return pokemon_payloads.get(key)
        return None

    for requested in ("basculin-blue-striped", "basculin-red-striped", "zygarde-50"):
        profile, failure = _resolve_requested_pokemon_profile(requested, _fetch)
        assert failure is None
        assert profile is not None
        assert profile["resolution_method"] == "pokemon_exact"
        assert profile["resolved_pokeapi_id"] is not None
