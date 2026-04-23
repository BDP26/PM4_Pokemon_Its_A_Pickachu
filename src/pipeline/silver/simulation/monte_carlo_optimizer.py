"""Summarize simulated matchup win probabilities with analytic uncertainty bounds."""

from __future__ import annotations

import math
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
    wins_list: list[int] = []
    losses_list: list[int] = []
    ci_low: list[float] = []
    ci_high: list[float] = []
    empirical_rates: list[float] = []

    def _wilson_interval(successes: int, trials: int, z: float = 1.96) -> tuple[float, float]:
        if trials <= 0:
            return 0.0, 0.0
        p_hat = successes / trials
        denom = 1 + (z * z / trials)
        center = (p_hat + (z * z) / (2 * trials)) / denom
        margin = (z / denom) * math.sqrt((p_hat * (1 - p_hat) / trials) + ((z * z) / (4 * trials * trials)))
        return max(0.0, center - margin), min(1.0, center + margin)

    for _, row in result_df.iterrows():
        p = float(row.get("predicted_player_win_chance", 0.0) or 0.0)
        p = max(0.0, min(1.0, p))
        battle_trials = max(1, int(row.get("n_trials", 1) or 1))
        wins = int(round(p * battle_trials))
        losses = battle_trials - wins
        lower, upper = _wilson_interval(wins, battle_trials)
        wins_list.append(wins)
        losses_list.append(losses)
        empirical_rates.append(round(wins / battle_trials, 6))
        ci_low.append(round(lower, 6))
        ci_high.append(round(upper, 6))

    result_df["empirical_win_rate"] = empirical_rates
    result_df["wins"] = wins_list
    result_df["losses"] = losses_list
    result_df["win_rate_ci95_low"] = ci_low
    result_df["win_rate_ci95_high"] = ci_high
    result_df["rng_seed"] = int(rng_seed)
    result_df["mc_resamples"] = int(n_trials)
    result_df["interval_method"] = "wilson_95"

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
        "empirical_win_rate",
        "win_rate_ci95_low",
        "win_rate_ci95_high",
        "mc_resamples",
        "interval_method",
    ]
    records = result_df[output_columns].to_dict(orient="records")
    write_parquet(output_path, records)

    print(f"[monte_carlo] resampled {len(records)} scenarios")
    return len(records)
