from __future__ import annotations

from pathlib import Path
from typing import Any

from src.pipeline.common.io import write_json
from src.pipeline.silver.simulation import type_matchups
from src.pipeline.silver.simulation.type_matchups import BattleSimulationConfig, build_team_battle_simulations, filter_simulation_teams


def _install_test_profiles() -> None:
    type_matchups._install_reference_profiles(
        pokemon_profiles={
            "magikarp": {
                "name": "magikarp",
                "species": "magikarp",
                "types": ["Water"],
                "stats": {"hp": 20, "attack": 10, "defense": 55, "sp_attack": 15, "sp_defense": 20, "speed": 80},
                "moves": [
                    {
                        "move_name": "splash",
                        "version_group_details": [{"version_group": "red-blue", "learn_method": "level-up", "level_learned_at": 1}],
                    }
                ],
            },
            "pikachu": {
                "name": "pikachu",
                "species": "pikachu",
                "types": ["Electric"],
                "stats": {"hp": 35, "attack": 55, "defense": 40, "sp_attack": 50, "sp_defense": 50, "speed": 90},
                "moves": [
                    {
                        "move_name": "thunder-shock",
                        "version_group_details": [{"version_group": "red-blue", "learn_method": "level-up", "level_learned_at": 1}],
                    }
                ],
            },
            "geodude": {
                "name": "geodude",
                "species": "geodude",
                "types": ["Rock", "Ground"],
                "stats": {"hp": 40, "attack": 80, "defense": 100, "sp_attack": 30, "sp_defense": 30, "speed": 20},
                "moves": [
                    {
                        "move_name": "tackle",
                        "version_group_details": [{"version_group": "red-blue", "learn_method": "level-up", "level_learned_at": 1}],
                    }
                ],
            },
        },
        move_profiles={
            "splash": {
                "name": "splash",
                "type": "Water",
                "power": None,
                "raw_power": None,
                "effective_power": 0.0,
                "power_handling": "status_no_damage",
                "is_status_move": True,
                "is_damage_move": False,
                "is_null_power": True,
                "damage_class": "status",
                "accuracy": None,
                "pp": 40,
                "level_learned_at": 1,
                "version_group": "red-blue",
                "degraded_data": False,
            },
            "thunder-shock": {
                "name": "thunder-shock",
                "type": "Electric",
                "power": 40,
                "raw_power": 40,
                "effective_power": 40.0,
                "power_handling": "direct_power",
                "is_status_move": False,
                "is_damage_move": True,
                "is_null_power": False,
                "damage_class": "special",
                "accuracy": 100,
                "pp": 30,
                "level_learned_at": 1,
                "version_group": "red-blue",
                "degraded_data": False,
            },
            "tackle": {
                "name": "tackle",
                "type": "Normal",
                "power": 40,
                "raw_power": 40,
                "effective_power": 40.0,
                "power_handling": "direct_power",
                "is_status_move": False,
                "is_damage_move": True,
                "is_null_power": False,
                "damage_class": "physical",
                "accuracy": 100,
                "pp": 35,
                "level_learned_at": 1,
                "version_group": "red-blue",
                "degraded_data": False,
            },
        },
    )


def test_filter_simulation_teams_skips_magikarp_and_keeps_valid_members() -> None:
    _install_test_profiles()
    teams = [
        {
            "team_id": "PLAYER_1",
            "is_player_candidate": True,
            "game_version": "red",
            "gym": "brock",
            "pokemon": ["magikarp", "pikachu"],
            "levels": [11, 11],
            "moves": [["splash"], ["thunder-shock"]],
        },
        {
            "team_id": "BROCK_red",
            "boss_name": "Brock",
            "game_version": "red",
            "gym": "brock",
            "pokemon": ["geodude"],
            "levels": [11],
            "moves": [["tackle"]],
        },
    ]

    filtered, skipped_members, dropped_teams = filter_simulation_teams(teams)

    assert skipped_members == 1
    assert dropped_teams == 0
    assert len(filtered) == 2
    assert filtered[0]["pokemon"] == ["pikachu"]
    assert filtered[0]["moves"] == [["thunder-shock"]]


def test_filter_simulation_teams_drops_all_invalid_team() -> None:
    _install_test_profiles()
    teams = [
        {
            "team_id": "PLAYER_2",
            "is_player_candidate": True,
            "game_version": "red",
            "gym": "brock",
            "pokemon": ["magikarp"],
            "levels": [11],
            "moves": [["splash"]],
        }
    ]

    filtered, skipped_members, dropped_teams = filter_simulation_teams(teams)
    assert filtered == []
    assert skipped_members == 1
    assert dropped_teams == 1


def test_filtered_teams_are_used_by_local_and_spark_paths(tmp_path: Path, monkeypatch) -> None:
    _install_test_profiles()
    bronze_dir = tmp_path / "bronze"
    silver_dir = tmp_path / "silver"
    write_json(bronze_dir / "type_chart.json", {"Water": {"Rock": 2.0}, "Electric": {"Rock": 1.0}, "Normal": {"Rock": 0.5}})

    teams = [
        {
            "team_id": "PLAYER_3",
            "is_player_candidate": True,
            "game_version": "red",
            "gym": "brock",
            "pokemon": ["magikarp", "pikachu"],
            "levels": [11, 11],
            "moves": [["splash"], ["thunder-shock"]],
        },
        {
            "team_id": "BROCK_red",
            "boss_name": "Brock",
            "game_version": "red",
            "gym": "brock",
            "pokemon": ["geodude"],
            "levels": [11],
            "moves": [["tackle"]],
        },
    ]
    seen: dict[str, list[dict[str, Any]]] = {}

    monkeypatch.setattr(type_matchups, "_load_move_and_pokemon_profiles_from_disk", lambda _silver_dir: None)

    def _fake_local(teams_data, _type_chart, _config):
        seen["local"] = teams_data
        return []

    def _fake_spark(teams_data, _type_chart, _config):
        seen["spark"] = teams_data
        return []

    monkeypatch.setattr(type_matchups, "_run_local_simulations", _fake_local)
    monkeypatch.setattr(type_matchups, "_run_spark_simulations", _fake_spark)

    build_team_battle_simulations(
        teams_data=teams,
        silver_dir=silver_dir,
        bronze_dir=bronze_dir,
        force_spark=False,
        runtime_config=BattleSimulationConfig(n_battle_trials=1),
    )
    build_team_battle_simulations(
        teams_data=teams,
        silver_dir=silver_dir,
        bronze_dir=bronze_dir,
        force_spark=True,
        runtime_config=BattleSimulationConfig(n_battle_trials=1),
    )

    assert seen["local"][0]["pokemon"] == ["pikachu"]
    assert seen["spark"][0]["pokemon"] == ["pikachu"]


def test_spark_is_default_engine(monkeypatch) -> None:
    monkeypatch.delenv("PIPELINE_USE_PYSPARK", raising=False)
    assert type_matchups._should_use_spark() is True
    monkeypatch.setenv("PIPELINE_USE_PYSPARK", "0")
    assert type_matchups._should_use_spark() is False
