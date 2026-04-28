"""Run Monte Carlo resampling over Gold simulation outcomes."""

from __future__ import annotations

import logging
from pathlib import Path
import random
from typing import Any

from src.pipeline.common.io import read_parquet, write_parquet
from src.pipeline.silver.simulation.schema_contract import canonical_scenario_id, row_player_boss_ids
from src.pipeline.settings import GOLD_DIR, GOLD_SIMULATION_DIRNAME, SILVER_DIR, SILVER_SIMULATION_DIRNAME

logger = logging.getLogger(__name__)


def _normalize_simulation_schema_columns(records_df):
    column_aliases = {
        "player_team_id": "team_id_attacker",
        "boss_team_id": "team_id_defender",
        "win_probability": "predicted_player_win_chance",
        "score": "simulation_score",
    }
    return records_df.rename(columns={k: v for k, v in column_aliases.items() if k in records_df.columns})


def run_monte_carlo_team_optimizer(
    gold_dir: Path = GOLD_DIR,
    simulation_dirname: str = GOLD_SIMULATION_DIRNAME,
    silver_dir: Path = SILVER_DIR,
    n_trials: int = 500,
    rng_seed: int = 42,
) -> int:
    simulation_dir = gold_dir / simulation_dirname
    simulations_path = simulation_dir / "team_battle_simulations.parquet"
    seeds_path = simulation_dir / "battle_seeds.parquet"
    output_path = simulation_dir / "monte_carlo_results.parquet"

    logger.info("[monte_carlo] reading simulations path=%s", simulations_path)
    if not simulations_path.exists():
        fallback = silver_dir / SILVER_SIMULATION_DIRNAME / "team_battle_simulations.parquet"
        if fallback.exists():
            logger.warning("[gold/simulation] deprecated silver simulation path used")
            simulations_path = fallback
        else:
            logger.warning("[monte_carlo] no team simulations found, skipping")
            return 0

    simulations_df = _normalize_simulation_schema_columns(read_parquet(simulations_path))
    logger.info("[monte_carlo] input rows=%s", len(simulations_df))
    if simulations_df.empty:
        logger.warning("[monte_carlo] team_battle_simulations.parquet is empty, skipping")
        return 0

    required_cols = {
        "team_id_attacker",
        "team_id_defender",
        "predicted_player_win_chance",
        "simulation_score",
        "attacker_win",
        "n_trials",
    }
    missing = required_cols - set(simulations_df.columns)
    if missing:
        raise ValueError(f"team_battle_simulations.parquet missing required columns: {sorted(missing)}")

    seed_by_pair: dict[tuple[str, str], dict[str, Any]] = {}
    if seeds_path.exists():
        seeds_df = read_parquet(seeds_path)
        logger.info("[monte_carlo] seed rows=%s", len(seeds_df))
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

    rng = random.Random(rng_seed)

    for _, row in result_df.iterrows():
        p = float(row.get("predicted_player_win_chance", 0.0) or 0.0)
        p = max(0.0, min(1.0, p))
        battle_trials = max(1, int(row.get("n_trials", 1) or 1))
        if row.get("attacker_wins") is not None:
            wins = max(0, min(battle_trials, int(row.get("attacker_wins") or 0)))
        else:
            wins = int(round(p * battle_trials))
        losses = battle_trials - wins
        posterior_alpha = 1 + wins
        posterior_beta = 1 + losses
        samples = [rng.betavariate(posterior_alpha, posterior_beta) for _ in range(max(1, n_trials))]
        samples.sort()
        lower_idx = int(0.025 * (len(samples) - 1))
        upper_idx = int(0.975 * (len(samples) - 1))
        lower = samples[lower_idx]
        upper = samples[upper_idx]
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
    result_df["interval_method"] = "beta_posterior_mc_95"

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
                "interval_method": row.get("interval_method") or "beta_posterior_mc_95",
                "boss_sequence_id": seed_row.get("boss_sequence_id"),
                "sequence_position": seed_row.get("sequence_position"),
                "remaining_team_state": seed_row.get("remaining_team_state", []),
                "gauntlet_success": bool(seed_row.get("gauntlet_success", False)),
                "simulation_mode": seed_row.get("simulation_mode") or row.get("simulation_mode") or "gym",
            }
        )

    if len(records) == 0 and len(simulations_df) > 0:
        raise ValueError(
            "monte_carlo generated 0 records despite non-empty team_battle_simulations.parquet; "
            f"input_rows={len(simulations_df)}"
        )

    write_parquet(output_path, records)
    logger.info("[monte_carlo] output rows=%s", len(records))
    return len(records)
