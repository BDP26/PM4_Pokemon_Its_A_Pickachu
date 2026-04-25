from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.pipeline.common.io import write_parquet
from src.pipeline.silver.simulation import type_matchups


def test_load_reference_profiles_prefers_move_reference_schema(tmp_path: Path, monkeypatch) -> None:
    silver_dir = tmp_path / "silver"
    references_dir = silver_dir / "references"
    simulation_dir = silver_dir / "simulation"
    references_dir.mkdir(parents=True)
    simulation_dir.mkdir(parents=True)

    write_parquet(
        references_dir / "pokemon_data.parquet",
        [
            {
                "pokemon_species": "pikachu",
                "name": "pikachu",
                "type_1": "electric",
                "base_hp": 35,
                "base_attack": 55,
                "base_defense": 40,
                "base_special_attack": 50,
                "base_special_defense": 50,
                "base_speed": 90,
            }
        ],
    )
    write_parquet(
        simulation_dir / "move_data.parquet",
        [
            {
                "pokemon_instance_id": "junk",
                "team_id": "junk",
                "species": "pikachu",
            }
        ],
    )
    write_parquet(
        references_dir / "move_reference.parquet",
        [
            {
                "move_name": "thunderbolt",
                "type": "electric",
                "damage_class": "special",
                "power": 90,
                "accuracy": 100,
                "priority": 0,
                "pp": 15,
            }
        ],
    )

    captured: dict[str, dict] = {}

    def _capture_install(pokemon_profiles: dict, move_profiles: dict) -> None:
        captured["pokemon"] = pokemon_profiles
        captured["moves"] = move_profiles

    monkeypatch.setattr(type_matchups, "_install_reference_profiles", _capture_install)
    type_matchups._load_reference_profiles_from_parquet(silver_dir)

    assert "pikachu" in captured["pokemon"]
    assert "thunderbolt" in captured["moves"]


def test_load_reference_profiles_skips_incomplete_pokemon_rows(tmp_path: Path, monkeypatch) -> None:
    silver_dir = tmp_path / "silver"
    references_dir = silver_dir / "references"
    references_dir.mkdir(parents=True)

    write_parquet(
        references_dir / "pokemon_data.parquet",
        [
            {
                "pokemon_species": "aegislash",
                "name": "aegislash",
                "type_1": None,
                "base_hp": float("nan"),
                "base_attack": 50,
                "base_defense": 140,
                "base_special_attack": 50,
                "base_special_defense": 140,
                "base_speed": 60,
            }
        ],
    )
    write_parquet(references_dir / "move_reference.parquet", pd.DataFrame(columns=["move_name"]))

    captured: dict[str, dict] = {}

    def _capture_install(pokemon_profiles: dict, move_profiles: dict) -> None:
        captured["pokemon"] = pokemon_profiles
        captured["moves"] = move_profiles

    monkeypatch.setattr(type_matchups, "_install_reference_profiles", _capture_install)
    type_matchups._load_reference_profiles_from_parquet(silver_dir)

    assert captured["pokemon"] == {}


def test_team_members_filters_invalid_move_placeholders() -> None:
    team = {
        "team_id": "t1",
        "pokemon": ["pikachu"],
        "levels": [12],
        "moves": [[None, float("nan"), "nan", "NA", "", "thunderbolt", "THUNDERBOLT", "<NA>"]],
    }
    members = type_matchups._team_members(team)
    assert members[0]["moves"] == ["thunderbolt"]


def test_profile_cache_completeness_ignores_invalid_move_placeholders() -> None:
    type_matchups._install_reference_profiles(
        pokemon_profiles={"pikachu": {"name": "pikachu", "types": ["Electric"], "stats": {"hp": 35}, "moves": []}},
        move_profiles={"thunderbolt": {"name": "thunderbolt", "type": "electric", "damage_class": "special"}},
    )
    teams = [
        {
            "team_id": "t1",
            "pokemon": ["pikachu"],
            "levels": [10],
            "moves": [[None, "", "NaN", "<NA>", "thunderbolt"]],
        }
    ]
    type_matchups._assert_profile_cache_completeness(teams)


def test_profile_cache_completeness_still_fails_for_unknown_real_move() -> None:
    type_matchups._install_reference_profiles(
        pokemon_profiles={"pikachu": {"name": "pikachu", "types": ["Electric"], "stats": {"hp": 35}, "moves": []}},
        move_profiles={"thunderbolt": {"name": "thunderbolt", "type": "electric", "damage_class": "special"}},
    )
    teams = [
        {
            "team_id": "t1",
            "pokemon": ["pikachu"],
            "levels": [10],
            "moves": [["thunderbolt", "mystery-move"]],
        }
    ]
    try:
        type_matchups._assert_profile_cache_completeness(teams)
        raise AssertionError("Expected missing move validation failure")
    except ValueError as exc:
        message = str(exc)
        assert "missing_moves=1" in message
        assert "mystery-move" in message
