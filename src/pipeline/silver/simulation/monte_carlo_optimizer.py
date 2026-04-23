"""Summarize simulated matchup win probabilities with analytic uncertainty bounds."""

from __future__ import annotations

import math
from pathlib import Path
from typing import Any

from src.pipeline.common.io import read_parquet, write_parquet
from src.pipeline.silver.simulation.schema_contract import canonical_scenario_id, row_player_boss_ids
from src.pipeline.settings import SILVER_DIR, SILVER_SIMULATION_DIRNAME


def run_monte_carlo_team_optimizer(
    silver_dir: Path = SILVER_DIR,
    simulation_dirname: str = SILVER_SIMULATION_DIRNAME,
    n_trials: int = 500,
    rng_seed: int = 42,
) -> int:
    simulation_dir = silver_dir / simulation_dirname
    simulations_path = simulation_dir / "team_battle_simulations.parquet"
    seeds_path = simulation_dir / "battle_seeds.parquet"
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

    seed_by_pair: dict[tuple[str, str], dict[str, Any]] = {}
    if seeds_path.exists():
        seeds_df = read_parquet(seeds_path)
        for row in seeds_df.to_dict(orient="records"):
            player_team_id, boss_team_id = row_player_boss_ids(row)
            if not player_team_id or not boss_team_id:
                continue
            seed_by_pair[(player_team_id, boss_team_id)] = row

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

    records: list[dict[str, Any]] = []
    for row in result_df.to_dict(orient="records"):
        player_team_id, boss_team_id = row_player_boss_ids(row)
        seed_row = seed_by_pair.get((player_team_id, boss_team_id), {})
        scenario_id = str(seed_row.get("scenario_id") or canonical_scenario_id(player_team_id, boss_team_id)).strip()
        mc_win_rate = round(float(row.get("empirical_win_rate") or 0.0), 6)

        records.append(
            {
                "scenario_id": scenario_id,
                "player_team_id": player_team_id,
                "boss_team_id": boss_team_id,
                "boss_name": seed_row.get("boss_name"),
                "game_version": seed_row.get("game_version"),
                "boss_level": seed_row.get("boss_level"),
                "predicted_player_win_chance": float(row.get("predicted_player_win_chance") or 0.0),
                "simulation_score": float(row.get("simulation_score") or 0.0),
                "simulated_attacker_win": bool(row.get("attacker_win", False)),
                "degraded_data": bool(row.get("degraded_data", False)),
                "n_trials": int(row.get("n_trials") or 1),
                "rng_seed": int(row.get("rng_seed") or rng_seed),
                "wins": int(row.get("wins") or 0),
                "losses": int(row.get("losses") or 0),
                "mc_win_rate": mc_win_rate,
                "win_rate_ci95_low": float(row.get("win_rate_ci95_low") or 0.0),
                "win_rate_ci95_high": float(row.get("win_rate_ci95_high") or 0.0),
                "mc_resamples": int(row.get("mc_resamples") or n_trials),
                "interval_method": row.get("interval_method") or "wilson_95",
            }
        )
    write_parquet(output_path, records)

    print(f"[monte_carlo] resampled {len(records)} scenarios")
    return len(records)
