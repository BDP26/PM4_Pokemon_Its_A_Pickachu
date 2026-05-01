from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from src.pipeline.silver.orchestration import build_silver
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


def test_enriched_profiles_ignore_nan_required_species(tmp_path: Path, monkeypatch) -> None:
    def _pokebase_payload(endpoint: str, resource_name_or_id: str | int):
        if endpoint == "pokemon" and resource_name_or_id == "snivy":
            return _fake_payload("snivy", 495, species="snivy", is_default=True)
        if endpoint == "pokemon-species" and resource_name_or_id == "snivy":
            return {"name": "snivy", "is_legendary": False, "is_mythical": False, "varieties": []}
        return {}

    monkeypatch.setattr(build_silver, "pokebase_get_data", _pokebase_payload)

    profiles_df, diagnostics = _build_enriched_pokemon_profiles({"snivy": {"name": "snivy"}}, {"snivy", float("nan")}, silver_dir=tmp_path)

    assert set(profiles_df["pokemon_species"].tolist()) == {"snivy"}
    assert diagnostics == []


def test_enriched_profiles_use_pokebase_payloads(tmp_path: Path, monkeypatch) -> None:
    def _pokebase_payload(endpoint: str, resource_name_or_id=None):
        if endpoint == "pokemon" and resource_name_or_id == "snivy":
            return _fake_payload("snivy", 495, species="snivy", is_default=True)
        if endpoint == "pokemon-species" and resource_name_or_id == "snivy":
            return {
                "name": "snivy",
                "is_legendary": False,
                "is_mythical": False,
                "varieties": [{"is_default": True, "pokemon": {"name": "snivy"}}],
            }
        return None

    monkeypatch.setattr(build_silver, "pokebase_get_data", _pokebase_payload)

    profiles_df, diagnostics = _build_enriched_pokemon_profiles(
        {"snivy": {"name": "snivy"}},
        {"snivy"},
        silver_dir=tmp_path,
    )

    assert diagnostics == []
    profile = profiles_df.iloc[0].to_dict()
    assert profile["pokemon_species"] == "snivy"
    assert profile["resolved_pokeapi_id"] == 495



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

    def _fetch(endpoint: str, resource_name_or_id: str | int) -> dict | None:
        key = str(resource_name_or_id).strip().lower()
        if endpoint == "pokemon-species":
            return species_payloads.get(key)
        if endpoint == "pokemon":
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

    def _fetch(endpoint: str, resource_name_or_id: str | int) -> dict | None:
        key = str(resource_name_or_id).strip().lower()
        if endpoint == "pokemon":
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

    def _fetch(endpoint: str, resource_name_or_id: str | int) -> dict | None:
        key = str(resource_name_or_id).strip().lower()
        if endpoint == "pokemon-species":
            return species_payloads.get(key)
        if endpoint == "pokemon":
            return pokemon_payloads.get(key)
        return None

    profile, failure = _resolve_requested_pokemon_profile("articuno", _fetch)
    assert failure is None
    assert profile is not None
    assert profile["is_legendary"] is True
    assert profile["is_mythical"] is False


def test_resolve_requested_pokemon_profile_marks_reshiram_as_legendary() -> None:
    def _fetch(endpoint: str, resource_name_or_id: str | int) -> dict | None:
        key = str(resource_name_or_id).strip().lower()
        if endpoint == "pokemon" and key == "reshiram":
            return _fake_payload("reshiram", 643, species="reshiram", is_default=True)
        if endpoint == "pokemon-species" and key == "reshiram":
            return {
                "name": "reshiram",
                "is_legendary": True,
                "is_mythical": False,
                "varieties": [{"is_default": True, "pokemon": {"name": "reshiram"}}],
            }
        return None

    profile, failure = _resolve_requested_pokemon_profile("reshiram", _fetch)

    assert failure is None
    assert profile == {
        "name": "reshiram",
        "pokemon_species": "reshiram",
        "pokeapi_id": 643,
        "source_url": "pokebase://pokemon/643",
        "type_1": "normal",
        "type_2": None,
        "base_hp": 60,
        "base_attack": 60,
        "base_defense": 60,
        "base_special_attack": 60,
        "base_special_defense": 60,
        "base_speed": 60,
        "height": 10,
        "weight": 100,
        "base_experience": 100,
        "is_default": True,
        "requested_pokemon_name": "reshiram",
        "normalized_requested_name": "reshiram",
        "normalized_species": "reshiram",
        "resolved_pokemon_name": "reshiram",
        "resolved_pokeapi_id": 643,
        "is_default_variety": True,
        "is_legendary": True,
        "is_mythical": False,
        "resolution_method": "pokemon_exact",
        "resolution_warning": None,
    }


def test_resolve_requested_pokemon_profile_uses_alias_fallback_when_exact_payload_incomplete() -> None:
    def _fetch(endpoint: str, resource_name_or_id: str | int) -> dict | None:
        key = str(resource_name_or_id).strip().lower()
        if endpoint == "pokemon" and key == "aegislash":
            # Mimics incomplete Pokebase object metadata (no combat payload).
            return {"name": "aegislash", "id": None}
        if endpoint == "pokemon-species" and key == "aegislash-shield":
            return {
                "name": "aegislash",
                "is_legendary": False,
                "is_mythical": False,
                "varieties": [{"is_default": True, "pokemon": {"name": "aegislash-shield"}}],
            }
        if endpoint == "pokemon" and key == "aegislash-shield":
            return _fake_payload("aegislash-shield", 681, species="aegislash", is_default=True)
        return None

    profile, failure = _resolve_requested_pokemon_profile("aegislash", _fetch)

    assert failure is None
    assert profile is not None
    assert profile["resolved_pokemon_name"] == "aegislash-shield"
    assert profile["resolved_pokeapi_id"] == 681
    assert profile["resolution_method"] == "alias_species_default_variety"


def test_build_enriched_profiles_fetches_reshiram_from_pokebase_and_uses_species_flags(monkeypatch, tmp_path: Path) -> None:
    fetch_calls: list[tuple[str, str | int]] = []

    def _pokebase_payload(endpoint: str, resource_name_or_id: str | int):
        fetch_calls.append((endpoint, resource_name_or_id))
        key = str(resource_name_or_id).strip().lower()
        if endpoint == "pokemon" and key == "reshiram":
            return _fake_payload("reshiram", 643, species="reshiram", is_default=True)
        if endpoint == "pokemon-species" and key == "reshiram":
            return {
                "name": "reshiram",
                "is_legendary": True,
                "is_mythical": False,
                "varieties": [{"is_default": True, "pokemon": {"name": "reshiram"}}],
            }
        return {}

    monkeypatch.setattr(build_silver, "pokebase_get_data", _pokebase_payload)

    profiles_df, diagnostics = _build_enriched_pokemon_profiles(
        all_pokemon_references={"reshiram": {"name": "reshiram"}},
        required_species={"reshiram"},
        silver_dir=tmp_path,
    )

    assert diagnostics == []
    assert ("pokemon", "reshiram") in fetch_calls
    assert ("pokemon-species", "reshiram") in fetch_calls
    profile = profiles_df.iloc[0].to_dict()
    assert profile["pokemon_species"] == "reshiram"
    assert profile["resolved_pokeapi_id"] == 643
    assert profile["is_legendary"] is True
    assert profile["is_mythical"] is False


def test_enriched_profiles_resolve_via_pokebase(monkeypatch, tmp_path: Path) -> None:
    def _pokebase_payload(endpoint: str, resource_name_or_id: str | int):
        if endpoint == "pokemon" and resource_name_or_id == "aegislash":
            return {}
        if endpoint == "pokemon-species" and resource_name_or_id == "aegislash":
            return {
                "name": "aegislash",
                "is_legendary": False,
                "is_mythical": False,
                "varieties": [{"is_default": True, "pokemon": {"name": "aegislash-shield"}}],
            }
        if endpoint == "pokemon" and resource_name_or_id == "aegislash-shield":
            return _fake_payload("aegislash-shield", 681, species="aegislash")
        return {}

    monkeypatch.setattr(build_silver, "pokebase_get_data", _pokebase_payload)

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


def test_enriched_profiles_resolve_default_forms_via_pokebase(monkeypatch, tmp_path: Path) -> None:
    species_defaults = {
        "aegislash": "aegislash-shield",
        "gourgeist": "gourgeist-average",
        "jellicent": "jellicent",
        "meowstic": "meowstic-male",
        "pyroar": "pyroar-male",
    }

    def _pokebase_payload(endpoint: str, resource_name_or_id: str | int):
        key = str(resource_name_or_id).strip().lower()
        if endpoint == "pokemon" and key in species_defaults and key != species_defaults[key]:
            return {}
        if endpoint == "pokemon-species" and key in species_defaults:
            return {
                "name": key,
                "is_legendary": False,
                "is_mythical": False,
                "varieties": [{"is_default": True, "pokemon": {"name": species_defaults[key]}}],
            }
        if endpoint == "pokemon":
            for species, default_form in species_defaults.items():
                if key == default_form:
                    return _fake_payload(default_form, 100 + len(default_form), species=species)
        return {}

    monkeypatch.setattr(build_silver, "pokebase_get_data", _pokebase_payload)

    profiles_df, diagnostics = _build_enriched_pokemon_profiles(
        all_pokemon_references={},
        required_species={"aegislash", "gourgeist", "jellicent", "meowstic", "pyroar"},
        silver_dir=tmp_path,
    )

    assert diagnostics == []
    assert set(profiles_df["pokemon_species"]) == {"aegislash", "gourgeist", "jellicent", "meowstic", "pyroar"}
    rows = {row["pokemon_species"]: row for row in profiles_df.to_dict(orient="records")}
    assert rows["aegislash"]["resolved_pokemon_name"] == "aegislash-shield"
    assert rows["gourgeist"]["resolved_pokemon_name"] == "gourgeist-average"
    assert rows["jellicent"]["resolved_pokemon_name"] == "jellicent"
    assert rows["meowstic"]["resolved_pokemon_name"] == "meowstic-male"
    assert rows["pyroar"]["resolved_pokemon_name"] == "pyroar-male"


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
