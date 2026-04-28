from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from src.pipeline.silver.orchestration.build_silver import (
    _build_enriched_pokemon_profiles,
    _build_evolution_rules_by_game_from_encounters,
    _collect_kaggle_boss_species_and_moves,
    _ensure_moves_in_combat_profiles,
    _validate_boss_team_targets,
    _validate_progression_source_team_boss_targets,
    _validate_and_persist_pokemon_data_contract,
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


def test_pokemon_data_contract_ignores_nan_required_species(tmp_path: Path) -> None:
    pokemon_data_df = pd.DataFrame(
        [
            {
                "pokemon_species": "snivy",
                "name": "snivy",
                "type_1": "grass",
                "base_hp": 45,
                "base_attack": 45,
                "base_defense": 55,
                "base_special_attack": 45,
                "base_special_defense": 55,
                "base_speed": 63,
                "resolved_pokemon_name": "snivy",
                "resolved_pokeapi_id": 495,
            }
        ]
    )

    _validate_and_persist_pokemon_data_contract(pokemon_data_df, tmp_path, {"snivy", float("nan")})

    diagnostics_path = tmp_path / "incomplete_pokemon_profiles.csv"
    assert diagnostics_path.exists()
    diagnostics_df = pd.read_csv(diagnostics_path)
    assert diagnostics_df.empty


def test_enriched_profiles_ignore_nan_required_species(tmp_path: Path) -> None:
    cached_profiles = pd.DataFrame(
        [
            {
                "pokemon_species": "snivy",
                "name": "snivy",
                "type_1": "grass",
                "type_2": None,
                "base_hp": 45,
                "base_attack": 45,
                "base_defense": 55,
                "base_special_attack": 45,
                "base_special_defense": 55,
                "base_speed": 63,
                "resolved_pokemon_name": "snivy",
                "resolved_pokeapi_id": 495,
                "resolution_method": "cached_profile",
            }
        ]
    )
    references_dir = tmp_path / "references"
    references_dir.mkdir(parents=True, exist_ok=True)
    cached_profiles.to_parquet(references_dir / "pokemon_data.parquet", index=False)

    profiles_df, diagnostics = _build_enriched_pokemon_profiles({"snivy": {"name": "snivy"}}, {"snivy", float("nan")}, silver_dir=tmp_path)

    assert set(profiles_df["pokemon_species"].tolist()) == {"snivy"}
    assert diagnostics == []


def test_progression_source_team_boss_target_validation_rejects_unknown_boss_ids() -> None:
    bosses_reference_df = pd.DataFrame(
        [
            {"boss_id": "blue:lt-surge", "game_version": "blue"},
            {"boss_id": "black-white:chili", "game_version": "black-white"},
        ]
    )

    with pytest.raises(ValueError, match="Player progression source teams reference bosses missing from bosses.parquet"):
        _validate_progression_source_team_boss_targets(
            [
                {"game_version": "blue", "boss_id": "blue-lt-surge"},
                {"game_version": "black-white", "boss_id": "black-white:chili"},
            ],
            bosses_reference_df,
        )


def test_boss_team_target_validation_rejects_unknown_boss_ids() -> None:
    bosses_reference_df = pd.DataFrame(
        [
            {"boss_id": "gold:falkner", "game_version": "gold"},
            {"boss_id": "blue:blue", "game_version": "blue"},
        ]
    )

    with pytest.raises(ValueError, match="Canonicalized boss teams reference bosses missing from bosses.parquet"):
        _validate_boss_team_targets(
            [
                {"game_version": "gold", "boss_id": "gold:blaine"},
                {"game_version": "blue", "boss_id": "blue:blue"},
            ],
            bosses_reference_df,
        )


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


def test_resolved_profiles_include_species_classification_flags() -> None:
    species_payloads = {
        "articuno": {
            "name": "articuno",
            "is_legendary": True,
            "is_mythical": False,
            "varieties": [{"is_default": True, "pokemon": {"name": "articuno"}}],
        }
    }
    pokemon_payloads = {
        "articuno": {
            **_fake_payload("articuno", 144, species="articuno"),
            "species": {"name": "articuno", "url": "https://pokeapi.co/api/v2/pokemon-species/articuno/"},
        }
    }

    def _fetch(url: str) -> dict | None:
        if "/pokemon-species/" in url:
            key = url.rstrip("/").split("/")[-1]
            return species_payloads.get(key)
        if "/pokemon/" in url:
            key = url.rstrip("/").split("/")[-1]
            return pokemon_payloads.get(key)
        return None

    profile, failure = _resolve_requested_pokemon_profile("articuno", _fetch)
    assert failure is None
    assert profile is not None
    assert profile["is_legendary"] is True
    assert profile["is_mythical"] is False


def test_enriched_profiles_prefer_cached_parquet_without_http(monkeypatch, tmp_path: Path) -> None:
    references_dir = tmp_path / "references"
    references_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "name": "aegislash-shield",
                "pokemon_species": "aegislash",
                "pokeapi_id": 681,
                "source_url": "https://pokeapi.co/api/v2/pokemon/681/",
                "type_1": "steel",
                "type_2": "ghost",
                "base_hp": 60,
                "base_attack": 50,
                "base_defense": 140,
                "base_special_attack": 50,
                "base_special_defense": 140,
                "base_speed": 60,
                "height": 17,
                "weight": 530,
                "base_experience": 234,
                "is_default": True,
                "requested_pokemon_name": "aegislash",
                "normalized_requested_name": "aegislash",
                "normalized_species": "aegislash",
                "resolved_pokemon_name": "aegislash-shield",
                "resolved_pokeapi_id": 681,
                "is_default_variety": True,
                "is_legendary": False,
                "is_mythical": False,
                "resolution_method": "species_default_variety",
                "resolution_warning": None,
            }
        ]
    ).to_parquet(references_dir / "pokemon_data.parquet", index=False)

    def _fail_build_session():
        raise AssertionError("HTTP session should not be created when cached profiles cover all required species")

    monkeypatch.setattr("src.pipeline.silver.orchestration.build_silver.build_session", _fail_build_session)

    profiles_df, diagnostics = _build_enriched_pokemon_profiles(
        all_pokemon_references={},
        required_species={"aegislash"},
        silver_dir=tmp_path,
    )

    assert diagnostics == []
    assert len(profiles_df) == 1
    row = profiles_df.to_dict(orient="records")[0]
    assert row["pokemon_species"] == "aegislash"
    assert row["requested_pokemon_name"] == "aegislash"
    assert row["resolved_pokemon_name"] == "aegislash-shield"


def test_build_evolution_rules_by_game_from_encounters_collects_species_rules(monkeypatch) -> None:
    encounters_df = pd.DataFrame(
        [
            {"game": "red", "pokemon": "geodude"},
            {"game": "red", "pokemon": "graveler"},
            {"game": "blue", "pokemon": "zubat"},
        ]
    )

    def _fake_rules(species: str) -> dict:
        mapping = {
            "geodude": {
                "geodude": {"species_name": "geodude", "base_species": "geodude", "evolution_stage": 1},
                "graveler": {"species_name": "graveler", "base_species": "geodude", "evolution_stage": 2},
            },
            "graveler": {
                "geodude": {"species_name": "geodude", "base_species": "geodude", "evolution_stage": 1},
                "graveler": {"species_name": "graveler", "base_species": "geodude", "evolution_stage": 2},
            },
            "zubat": {
                "zubat": {"species_name": "zubat", "base_species": "zubat", "evolution_stage": 1},
                "golbat": {"species_name": "golbat", "base_species": "zubat", "evolution_stage": 2},
            },
        }
        return mapping.get(species, {})

    monkeypatch.setattr("src.pipeline.silver.orchestration.build_silver.get_species_evolution_rules", _fake_rules)

    rules = _build_evolution_rules_by_game_from_encounters(encounters_df)

    assert {"geodude", "graveler"} <= set(rules["red"].keys())
    assert {"zubat", "golbat"} <= set(rules["blue"].keys())
