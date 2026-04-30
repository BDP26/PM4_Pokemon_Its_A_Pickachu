from __future__ import annotations

from pathlib import Path

import numpy as np

from src.pipeline.common.io import read_parquet, write_parquet
from src.pipeline.gold.simulation import team_battle_simulations as sim_module
from src.pipeline.gold.simulation.battle_seeds import build_battle_seeds
from src.pipeline.gold.simulation.monte_carlo_optimizer import run_monte_carlo_team_optimizer


def test_battle_seeds_reads_gold_team_simulations_schema(tmp_path: Path) -> None:
    gold_sim = tmp_path / "gold" / "simulation"
    silver_sim = tmp_path / "silver" / "simulation"
    gold_sim.mkdir(parents=True, exist_ok=True)
    silver_sim.mkdir(parents=True, exist_ok=True)

    write_parquet(
        gold_sim / "teams.parquet",
        [
            {"team_id": "player_team", "boss_name": None, "game_version": "gold", "avg_level": 19},
            {"team_id": "boss_team", "boss_name": "Falkner", "game_version": "gold", "avg_level": 12},
        ],
    )
    write_parquet(
        gold_sim / "team_battle_simulations.parquet",
        [
            {
                "team_id_attacker": "player_team",
                "team_id_defender": "boss_team",
                "attacker_win": True,
                "predicted_player_win_chance": 0.73,
                "simulation_score": 0.88,
                "attacker_wins": 11,
                "attacker_losses": 4,
                "n_trials": 15,
                "attacker_game_version": "gold",
                "defender_game_version": "gold",
                "is_compatible_version": True,
            }
        ],
    )

    build_battle_seeds(gold_dir=tmp_path / "gold", silver_dir=tmp_path / "silver")

    seeds = read_parquet(gold_sim / "battle_seeds.parquet")
    assert len(seeds) == 1
    row = seeds.iloc[0].to_dict()
    assert row["player_team_id"] == "player_team"
    assert row["boss_team_id"] == "boss_team"


def test_monte_carlo_consumes_gold_simulation_rows(tmp_path: Path) -> None:
    gold_sim = tmp_path / "gold" / "simulation"
    gold_sim.mkdir(parents=True, exist_ok=True)

    write_parquet(
        gold_sim / "team_battle_simulations.parquet",
        [
            {
                "team_id_attacker": "player_team",
                "team_id_defender": "boss_team",
                "attacker_win": True,
                "predicted_player_win_chance": 0.73,
                "simulation_score": 0.88,
                "attacker_wins": 11,
                "attacker_losses": 4,
                "n_trials": 15,
                "degraded_data": False,
            }
        ],
    )

    count = run_monte_carlo_team_optimizer(gold_dir=tmp_path / "gold", n_trials=30, rng_seed=7)
    assert count == 1
    results = read_parquet(gold_sim / "monte_carlo_results.parquet")
    assert len(results) == 1
    row = results.iloc[0].to_dict()
    assert "adaptive_rerun" in row
    assert "final_mc_win_rate" in row
    assert float(row["mc_win_rate"]) == float(row["final_mc_win_rate"])


def test_gold_simulation_scenario_ids_distinguish_boss_and_gauntlet_rows(tmp_path: Path) -> None:
    gold_sim = tmp_path / "gold" / "simulation"
    silver_sim = tmp_path / "silver" / "simulation"
    gold_sim.mkdir(parents=True, exist_ok=True)
    silver_sim.mkdir(parents=True, exist_ok=True)

    write_parquet(
        gold_sim / "teams.parquet",
        [
            {"team_id": "player_team", "boss_name": None, "game_version": "black", "avg_level": 50},
            {"team_id": "boss_team", "boss_name": "Shauntal", "game_version": "black", "avg_level": 48},
        ],
    )
    write_parquet(
        gold_sim / "team_battle_simulations.parquet",
        [
            {
                "team_id_attacker": "player_team",
                "team_id_defender": "boss_team",
                "attacker_win": True,
                "predicted_player_win_chance": 0.8,
                "simulation_score": 1.1,
                "attacker_wins": 8,
                "attacker_losses": 2,
                "n_trials": 10,
                "simulation_mode": "boss",
                "boss_sequence_id": None,
                "sequence_position": None,
                "degraded_data": False,
            },
            {
                "team_id_attacker": "player_team",
                "team_id_defender": "boss_team",
                "attacker_win": False,
                "predicted_player_win_chance": 0.1,
                "simulation_score": -0.4,
                "attacker_wins": 1,
                "attacker_losses": 9,
                "n_trials": 10,
                "simulation_mode": "gauntlet",
                "boss_sequence_id": "black:elite_four_champion",
                "sequence_position": 1,
                "gauntlet_success": False,
                "gauntlet_success_rate": 0.0,
                "degraded_data": False,
            },
        ],
    )

    build_battle_seeds(gold_dir=tmp_path / "gold", silver_dir=tmp_path / "silver")
    seeds = read_parquet(gold_sim / "battle_seeds.parquet")
    assert len(seeds) == 2
    assert seeds["scenario_id"].nunique() == 2

    count = run_monte_carlo_team_optimizer(gold_dir=tmp_path / "gold", silver_dir=tmp_path / "silver", n_trials=20, rng_seed=5)
    assert count == 2

    results = read_parquet(gold_sim / "monte_carlo_results.parquet").sort_values(["simulation_mode", "scenario_id"]).reset_index(drop=True)
    assert results["simulation_mode"].tolist() == ["boss", "gauntlet"]
    assert results["scenario_id"].nunique() == 2


def test_adaptive_rerun_applies_only_to_borderline_simulated_loss(tmp_path: Path) -> None:
    gold_sim = tmp_path / "gold" / "simulation"
    gold_sim.mkdir(parents=True, exist_ok=True)

    write_parquet(
        gold_sim / "team_battle_simulations.parquet",
        [
            {
                "team_id_attacker": "player_team_1",
                "team_id_defender": "boss_team_1",
                "attacker_win": False,
                "predicted_player_win_chance": 0.0,
                "simulation_score": -0.4,
                "attacker_wins": 0,
                "attacker_losses": 10,
                "n_trials": 10,
                "degraded_data": False,
                "outcome_cause": "simulated_loss",
            },
            {
                "team_id_attacker": "player_team_2",
                "team_id_defender": "boss_team_2",
                "attacker_win": False,
                "predicted_player_win_chance": 0.0,
                "simulation_score": -0.6,
                "attacker_wins": 0,
                "attacker_losses": 10,
                "n_trials": 10,
                "degraded_data": False,
                "outcome_cause": "level_filter",
            },
        ],
    )

    count = run_monte_carlo_team_optimizer(
        gold_dir=tmp_path / "gold",
        n_trials=20,
        rng_seed=13,
        adaptive_rerun_threshold_low=0.0,
        adaptive_rerun_threshold_high=0.02,
        adaptive_rerun_resamples=200,
    )
    assert count == 2
    results = read_parquet(gold_sim / "monte_carlo_results.parquet").sort_values("player_team_id").reset_index(drop=True)
    row1 = results.iloc[0].to_dict()
    row2 = results.iloc[1].to_dict()
    assert bool(row1["adaptive_rerun"]) is True
    assert int(row1["mc_resamples"]) == 200
    assert bool(row2["adaptive_rerun"]) is False
    assert int(row2["mc_resamples"]) == 20


def test_team_members_accept_numpy_array_columns() -> None:
    team = {
        "team_id": "player-team",
        "avg_level": 10,
        "pokemon": np.array(["squirtle", "pidgey"], dtype=object),
        "levels": np.array([9, 8]),
        "moves": np.array(
            [
                np.array(["bubble", "tackle"], dtype=object),
                np.array(["gust"], dtype=object),
            ],
            dtype=object,
        ),
        "pokemon_instance_ids": np.array(["m1", "m2"], dtype=object),
    }

    members = sim_module._team_members(team)

    assert len(members) == 2
    assert members[0]["species"] == "squirtle"
    assert members[0]["moves"] == ["bubble", "tackle"]
    assert members[1]["pokemon_instance_id"] == "m2"
