from __future__ import annotations

from pathlib import Path

from src.pipeline.common.io import read_parquet, write_parquet
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
