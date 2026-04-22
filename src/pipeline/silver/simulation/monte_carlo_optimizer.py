"""Aggregate results from real round-based team simulations.

This file no longer performs fake Bernoulli resampling from an already-known
probability. It summarizes the probabilities produced by the actual battle engine.
"""

from __future__ import annotations

from pathlib import Path

from src.pipeline.common.io import read_parquet, write_parquet
from src.pipeline.settings import SILVER_DIR, SILVER_SIMULATION_DIRNAME


def run_monte_carlo_team_optimizer(
    silver_dir: Path = SILVER_DIR,
    simulation_dirname: str = SILVER_SIMULATION_DIRNAME,
    n_trials: int = 500,
    rng_seed: int = 42,
) -> int:
    simulation_dir = silver_dir / simulation_dirname
    simulations_path = simulation_dir / "team_battle_simulations.parquet"
    output_path = simulation_dir / "monte_carlo_results.parquet"

    if not simulations_path.exists():
        print("[monte_carlo] no team simulations found, skipping")
        return 0

    simulations_df = read_parquet(simulations_path)
    if simulations_df.empty:
        print("[monte_carlo] team_battle_simulations.parquet is empty, skipping")
        return 0

    required_cols = {
        "team_id_attacker",
        "team_id_defender",
        "predicted_player_win_chance",
        "simulation_score",
        "attacker_win",
        "degraded_data",
        "n_trials",
    }
    missing = required_cols - set(simulations_df.columns)
    if missing:
        raise ValueError(f"team_battle_simulations.parquet missing required columns: {sorted(missing)}")

    result_df = simulations_df.copy()
    result_df["mc_win_rate"] = result_df["predicted_player_win_chance"].astype(float)
    result_df["wins"] = (result_df["mc_win_rate"] * result_df["n_trials"].astype(int)).round().astype(int)
    result_df["losses"] = result_df["n_trials"].astype(int) - result_df["wins"]
    result_df["rng_seed"] = int(rng_seed)

    output_columns = [
        "team_id_attacker",
        "team_id_defender",
        "predicted_player_win_chance",
        "simulation_score",
        "attacker_win",
        "degraded_data",
        "n_trials",
        "rng_seed",
        "wins",
        "losses",
        "mc_win_rate",
    ]
    records = result_df[output_columns].to_dict(orient="records")
    write_parquet(output_path, records)

    print(f"[monte_carlo] summarized {len(records)} scenarios from real battle trials")
    return len(records)