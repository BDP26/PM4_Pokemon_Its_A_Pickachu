"""Run Monte Carlo resampling over simulated matchup win probabilities."""

from __future__ import annotations

import random
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
    rng = random.Random(int(rng_seed))

    mc_win_rates: list[float] = []
    wins_list: list[int] = []
    losses_list: list[int] = []
    ci_low: list[float] = []
    ci_high: list[float] = []

    for _, row in result_df.iterrows():
        p = float(row.get("predicted_player_win_chance", 0.0) or 0.0)
        p = max(0.0, min(1.0, p))
        battle_trials = max(1, int(row.get("n_trials", 1) or 1))
        draws: list[float] = []
        for _ in range(max(1, int(n_trials))):
            wins = sum(1 for _ in range(battle_trials) if rng.random() <= p)
            draws.append(wins / battle_trials)
        draws.sort()
        mean_rate = sum(draws) / len(draws)
        wins_mc = int(round(mean_rate * battle_trials))
        wins_list.append(wins_mc)
        losses_list.append(battle_trials - wins_mc)
        mc_win_rates.append(round(mean_rate, 6))
        lower_idx = int(0.025 * (len(draws) - 1))
        upper_idx = int(0.975 * (len(draws) - 1))
        ci_low.append(round(draws[lower_idx], 6))
        ci_high.append(round(draws[upper_idx], 6))

    result_df["mc_win_rate"] = mc_win_rates
    result_df["wins"] = wins_list
    result_df["losses"] = losses_list
    result_df["mc_ci95_low"] = ci_low
    result_df["mc_ci95_high"] = ci_high
    result_df["rng_seed"] = int(rng_seed)
    result_df["mc_resamples"] = int(n_trials)

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
        "mc_ci95_low",
        "mc_ci95_high",
        "mc_resamples",
    ]
    records = result_df[output_columns].to_dict(orient="records")
    write_parquet(output_path, records)

    print(f"[monte_carlo] resampled {len(records)} scenarios")
    return len(records)
