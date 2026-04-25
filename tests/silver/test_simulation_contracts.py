from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.pipeline.common.io import write_json, write_parquet
from src.pipeline.silver.simulation.type_matchups import BattleSimulationConfig, _is_version_compatible, build_team_battle_simulations
from src.pipeline.silver.simulation.monte_carlo_optimizer import run_monte_carlo_team_optimizer


def _write_reference_profiles(silver_dir: Path) -> None:
    references_dir = silver_dir / "references"
    references_dir.mkdir(parents=True, exist_ok=True)
    write_parquet(
        references_dir / "pokemon_data.parquet",
        [
            {"name": "pikachu", "pokemon_species": "pikachu", "type_1": "electric", "type_2": None, "base_hp": 35, "base_attack": 55, "base_defense": 40, "base_special_attack": 50, "base_special_defense": 50, "base_speed": 90},
            {"name": "geodude", "pokemon_species": "geodude", "type_1": "rock", "type_2": "ground", "base_hp": 40, "base_attack": 80, "base_defense": 100, "base_special_attack": 30, "base_special_defense": 30, "base_speed": 20},
            {"name": "staryu", "pokemon_species": "staryu", "type_1": "water", "type_2": None, "base_hp": 30, "base_attack": 45, "base_defense": 55, "base_special_attack": 70, "base_special_defense": 55, "base_speed": 85},
            {"name": "patrat", "pokemon_species": "patrat", "type_1": "normal", "type_2": None, "base_hp": 45, "base_attack": 55, "base_defense": 39, "base_special_attack": 35, "base_special_defense": 39, "base_speed": 42},
        ],
    )
    write_parquet(
        references_dir / "move_reference.parquet",
        [
            {"move_name": "tackle", "type": "normal", "damage_class": "physical", "power": 40, "raw_power": 40, "effective_power": 40, "accuracy": 100, "pp": 35, "power_handling": "direct_power", "is_status_move": False, "is_damage_move": True, "is_null_power": False}
        ],
    )


def _teams() -> list[dict[str, object]]:
    return [
        {
            "team_id": "STARTER_red",
            "is_player_candidate": True,
            "game_version": "red",
            "gym": "brock",
            "avg_level": 10,
            "pokemon": ["pikachu"],
            "levels": [10],
            "moves": [["tackle"]],
        },
        {
            "team_id": "BROCK_red",
            "boss_name": "Brock",
            "game_version": "red",
            "gym": "brock",
            "avg_level": 10,
            "pokemon": ["geodude"],
            "levels": [10],
            "moves": [["tackle"]],
        },
        {
            "team_id": "MISTY_red",
            "boss_name": "Misty",
            "game_version": "red",
            "gym": "misty",
            "avg_level": 10,
            "pokemon": ["staryu"],
            "levels": [10],
            "moves": [["tackle"]],
        },
        {
            "team_id": "BROCK_black",
            "boss_name": "Brock",
            "game_version": "black",
            "gym": "brock",
            "avg_level": 10,
            "pokemon": ["patrat"],
            "levels": [10],
            "moves": [["tackle"]],
        },
    ]


def test_cross_version_pairing_is_blocked(tmp_path: Path) -> None:
    bronze_dir = tmp_path / "bronze"
    silver_dir = tmp_path / "silver"
    write_json(bronze_dir / "type_chart.json", {"Normal": {"Normal": 1.0}})
    _write_reference_profiles(silver_dir)

    build_team_battle_simulations(
        teams_data=_teams(),
        silver_dir=silver_dir,
        bronze_dir=bronze_dir,
        runtime_config=BattleSimulationConfig(n_battle_trials=3, require_exact_version_match=True),
    )

    df = pd.read_parquet(silver_dir / "simulation" / "team_battle_simulations.parquet")
    assert set(df["team_id_defender"].tolist()) == {"BROCK_red"}
    assert bool(df["is_compatible_version"].all())


def test_only_intended_gym_matchups_are_simulated(tmp_path: Path) -> None:
    bronze_dir = tmp_path / "bronze"
    silver_dir = tmp_path / "silver"
    write_json(bronze_dir / "type_chart.json", {"Normal": {"Normal": 1.0}})
    _write_reference_profiles(silver_dir)

    build_team_battle_simulations(
        teams_data=_teams(),
        silver_dir=silver_dir,
        bronze_dir=bronze_dir,
        runtime_config=BattleSimulationConfig(n_battle_trials=1, require_exact_version_match=True),
    )

    df = pd.read_parquet(silver_dir / "simulation" / "team_battle_simulations.parquet")
    assert set(df["team_id_defender"].tolist()) == {"BROCK_red"}


def test_player_without_gym_context_is_not_simulated(tmp_path: Path) -> None:
    bronze_dir = tmp_path / "bronze"
    silver_dir = tmp_path / "silver"
    write_json(bronze_dir / "type_chart.json", {"Normal": {"Normal": 1.0}})
    _write_reference_profiles(silver_dir)
    teams = _teams()
    teams[0] = dict(teams[0])
    teams[0].pop("gym", None)

    build_team_battle_simulations(
        teams_data=teams,
        silver_dir=silver_dir,
        bronze_dir=bronze_dir,
        runtime_config=BattleSimulationConfig(n_battle_trials=1, require_exact_version_match=True),
    )

    df = pd.read_parquet(silver_dir / "simulation" / "team_battle_simulations.parquet")
    assert df.empty


def test_runtime_n_trials_propagates_to_outputs(tmp_path: Path) -> None:
    simulation_dir = tmp_path / "simulation"
    write_parquet(
        simulation_dir / "battle_seeds.parquet",
        [
            {
                "scenario_id": "A_vs_B",
                "player_team_id": "A",
                "boss_team_id": "B",
                "boss_name": "Brock",
                "game_version": "red",
                "boss_level": 10,
            }
        ],
    )
    write_parquet(
        simulation_dir / "team_battle_simulations.parquet",
        [
            {
                "team_id_attacker": "A",
                "team_id_defender": "B",
                "predicted_player_win_chance": 0.6,
                "simulation_score": 1.0,
                "attacker_win": True,
                "degraded_data": False,
                "n_trials": 7,
            }
        ],
    )
    run_monte_carlo_team_optimizer(silver_dir=tmp_path, n_trials=50, rng_seed=11)
    out = pd.read_parquet(simulation_dir / "monte_carlo_results.parquet")
    assert set(["scenario_id", "player_team_id", "boss_team_id", "mc_win_rate"]).issubset(set(out.columns))
    assert out.iloc[0]["scenario_id"] == "A_vs_B"
    assert int(out.iloc[0]["n_trials"]) == 7
    assert int(out.iloc[0]["mc_resamples"]) == 50


def test_version_compatibility_contract() -> None:
    cfg = BattleSimulationConfig(require_exact_version_match=True)
    assert _is_version_compatible("red", "red", cfg)
    assert not _is_version_compatible("red", "black", cfg)


def test_spark_pairing_uses_keyed_join_not_cross_join() -> None:
    source = Path("src/pipeline/gold/simulation/team_battle_simulations.py").read_text(encoding="utf-8")
    assert ".crossJoin(" not in source
    assert 'on=["game_version", "target"]' in source
