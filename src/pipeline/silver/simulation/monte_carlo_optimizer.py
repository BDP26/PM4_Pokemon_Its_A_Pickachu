"""Monte-Carlo resampling for battle seeds."""
from __future__ import annotations

from pathlib import Path

import numpy as np

from src.pipeline.common.io import read_parquet, write_parquet
from src.pipeline.settings import SILVER_DIR, SILVER_SIMULATION_DIRNAME


def run_monte_carlo_team_optimizer(
    silver_dir: Path = SILVER_DIR,
    simulation_dirname: str = SILVER_SIMULATION_DIRNAME,
    n_trials: int = 500,
    rng_seed: int = 42,
) -> int:
    """
    Run downstream Bernoulli resampling for already-estimated battle win probabilities.

    This does not simulate battle mechanics directly.
    It samples scenario outcomes from `predicted_player_win_chance`.
    """
    simulation_dir = silver_dir / simulation_dirname
    seeds_path = simulation_dir / "battle_seeds.parquet"
    output_path = simulation_dir / "monte_carlo_results.parquet"

    if not seeds_path.exists():
        print("[monte_carlo] no battle seeds found, skipping")
        return 0

    seeds_df = read_parquet(seeds_path)
    if seeds_df.empty:
        print("[monte_carlo] battle_seeds.parquet is empty, skipping")
        return 0

    required_cols = {
        "scenario_id",
        "player_team_id",
        "boss_team_id",
        "boss_name",
        "game_version",
        "predicted_player_win_chance",
    }
    missing = required_cols - set(seeds_df.columns)
    if missing:
        raise ValueError(f"battle_seeds.parquet missing required columns: {sorted(missing)}")

    probs = seeds_df["predicted_player_win_chance"].astype(float).clip(lower=0.0, upper=1.0).to_numpy()
    rng = np.random.default_rng(rng_seed)
    wins = rng.binomial(n_trials, probs)

    seeds_df = seeds_df.copy()
    seeds_df["mc_trials"] = int(n_trials)
    seeds_df["mc_rng_seed"] = int(rng_seed)
    seeds_df["wins"] = wins
    seeds_df["losses"] = n_trials - wins
    seeds_df["mc_win_rate"] = seeds_df["wins"] / float(n_trials)

    output_columns = [
        "scenario_id",
        "player_team_id",
        "boss_team_id",
        "boss_name",
        "game_version",
        "predicted_player_win_chance",
        "simulation_score",
        "simulated_attacker_win",
        "degraded_data",
        "n_trials",
        "mc_trials",
        "mc_rng_seed",
        "wins",
        "losses",
        "mc_win_rate",
    ]
    available_columns = [column for column in output_columns if column in seeds_df.columns]
    records = seeds_df[available_columns].to_dict(orient="records")
    write_parquet(output_path, records)

    print(f"[monte_carlo] simulated {len(records)} scenarios with mc_trials={n_trials}")
    return len(records)