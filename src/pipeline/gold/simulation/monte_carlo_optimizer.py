"""Run Monte Carlo resampling over Gold simulation outcomes."""

from __future__ import annotations

import logging
from pathlib import Path
import random
from typing import Any

from src.pipeline.common.env import env_float, env_int
from src.pipeline.common.io import read_parquet, write_parquet
from src.pipeline.common.simulation_schema import normalize_team_battle_simulation_schema
from src.pipeline.silver.simulation.schema_contract import canonical_scenario_context_id, canonical_scenario_id, row_player_boss_ids
from src.pipeline.settings import GOLD_DIR, GOLD_SIMULATION_DIRNAME, SILVER_DIR

logger = logging.getLogger(__name__)


def _posterior_ci_and_rate(
    *,
    wins: int,
    losses: int,
    battle_trials: int,
    n_resamples: int,
    rng: random.Random,
) -> tuple[float, float, float]:
    posterior_alpha = 1 + wins
    posterior_beta = 1 + losses
    samples = [rng.betavariate(posterior_alpha, posterior_beta) for _ in range(max(1, n_resamples))]
    samples.sort()
    lower_idx = int(0.025 * (len(samples) - 1))
    upper_idx = int(0.975 * (len(samples) - 1))
    lower = round(samples[lower_idx], 6)
    upper = round(samples[upper_idx], 6)
    empirical_rate = round(wins / max(1, battle_trials), 6)
    return empirical_rate, lower, upper


def run_monte_carlo_team_optimizer(
    gold_dir: Path = GOLD_DIR,
    simulation_dirname: str = GOLD_SIMULATION_DIRNAME,
    silver_dir: Path = SILVER_DIR,
    n_trials: int = 500,
    rng_seed: int = 42,
    adaptive_rerun_threshold_low: float | None = None,
    adaptive_rerun_threshold_high: float | None = None,
    adaptive_rerun_resamples: int | None = None,
) -> int:
    simulation_dir = gold_dir / simulation_dirname
    simulations_path = simulation_dir / "team_battle_simulations.parquet"
    seeds_path = simulation_dir / "battle_seeds.parquet"
    output_path = simulation_dir / "monte_carlo_results.parquet"

    logger.info("[monte_carlo] reading simulations path=%s", simulations_path)
    if not simulations_path.exists():
        logger.warning("[monte_carlo] no team simulations found, skipping")
        return 0

    simulations_df = normalize_team_battle_simulation_schema(read_parquet(simulations_path))
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

    seed_by_scenario: dict[str, dict[str, Any]] = {}
    seed_by_pair: dict[tuple[str, str], dict[str, Any]] = {}
    if seeds_path.exists():
        seeds_df = read_parquet(seeds_path)
        logger.info("[monte_carlo] seed rows=%s", len(seeds_df))
        for row in seeds_df.to_dict(orient="records"):
            player_team_id, boss_team_id = row_player_boss_ids(row)
            if not player_team_id or not boss_team_id:
                continue
            scenario_id = canonical_scenario_context_id(
                player_team_id,
                boss_team_id,
                simulation_mode=row.get("simulation_mode"),
                boss_sequence_id=row.get("boss_sequence_id"),
                sequence_position=row.get("sequence_position"),
            )
            seed_by_scenario[scenario_id] = row
            seed_by_pair[(player_team_id, boss_team_id)] = row

    result_df = simulations_df.copy()
    wins_list: list[int] = []
    losses_list: list[int] = []
    ci_low: list[float] = []
    ci_high: list[float] = []
    empirical_rates: list[float] = []
    adaptive_rerun_flags: list[bool] = []
    final_rates: list[float] = []
    final_ci_low: list[float] = []
    final_ci_high: list[float] = []
    final_resamples: list[int] = []
    final_interval_methods: list[str] = []

    rerun_low = max(0.0, min(1.0, adaptive_rerun_threshold_low if adaptive_rerun_threshold_low is not None else env_float("PM4_SIM_ADAPTIVE_RERUN_LOW", 0.0)))
    rerun_high = max(rerun_low, min(1.0, adaptive_rerun_threshold_high if adaptive_rerun_threshold_high is not None else env_float("PM4_SIM_ADAPTIVE_RERUN_HIGH", 0.02)))
    rerun_resamples = max(
        int(n_trials),
        int(
            adaptive_rerun_resamples
            if adaptive_rerun_resamples is not None
            else env_int("PM4_SIM_ADAPTIVE_RERUN_RESAMPLES", 5000)
        ),
    )

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
        empirical_rate, lower, upper = _posterior_ci_and_rate(
            wins=wins,
            losses=losses,
            battle_trials=battle_trials,
            n_resamples=int(n_trials),
            rng=rng,
        )
        wins_list.append(wins)
        losses_list.append(losses)
        empirical_rates.append(empirical_rate)
        ci_low.append(lower)
        ci_high.append(upper)

        outcome_cause = str(row.get("outcome_cause") or "").strip().lower()
        should_rerun = outcome_cause == "simulated_loss" and rerun_low <= empirical_rate <= rerun_high
        adaptive_rerun_flags.append(should_rerun)
        if should_rerun:
            rerun_rate, rerun_low_ci, rerun_high_ci = _posterior_ci_and_rate(
                wins=wins,
                losses=losses,
                battle_trials=battle_trials,
                n_resamples=rerun_resamples,
                rng=rng,
            )
            final_rates.append(rerun_rate)
            final_ci_low.append(rerun_low_ci)
            final_ci_high.append(rerun_high_ci)
            final_resamples.append(rerun_resamples)
            final_interval_methods.append("beta_posterior_mc_95_adaptive")
        else:
            final_rates.append(empirical_rate)
            final_ci_low.append(lower)
            final_ci_high.append(upper)
            final_resamples.append(int(n_trials))
            final_interval_methods.append("beta_posterior_mc_95")

    result_df["empirical_win_rate"] = empirical_rates
    result_df["wins"] = wins_list
    result_df["losses"] = losses_list
    result_df["win_rate_ci95_low"] = ci_low
    result_df["win_rate_ci95_high"] = ci_high
    result_df["rng_seed"] = int(rng_seed)
    result_df["mc_resamples"] = int(n_trials)
    result_df["interval_method"] = "beta_posterior_mc_95"
    result_df["adaptive_rerun"] = adaptive_rerun_flags
    result_df["final_mc_win_rate"] = final_rates
    result_df["final_win_rate_ci95_low"] = final_ci_low
    result_df["final_win_rate_ci95_high"] = final_ci_high
    result_df["final_mc_resamples"] = final_resamples
    result_df["final_interval_method"] = final_interval_methods

    records: list[dict[str, Any]] = []
    for row in result_df.to_dict(orient="records"):
        player_team_id, boss_team_id = row_player_boss_ids(row)
        # Fallback to attacker/defender if player/boss IDs not available
        if not player_team_id:
            player_team_id = str(row.get("team_id_attacker") or "").strip()
        if not boss_team_id:
            boss_team_id = str(row.get("team_id_defender") or "").strip()
        # Skip rows with null or empty team IDs to match battle_seeds.py filtering
        if not player_team_id or not boss_team_id:
            continue
        scenario_id = canonical_scenario_context_id(
            player_team_id,
            boss_team_id,
            simulation_mode=row.get("simulation_mode"),
            boss_sequence_id=row.get("boss_sequence_id"),
            sequence_position=row.get("sequence_position"),
        )
        seed_row = seed_by_scenario.get(scenario_id) or seed_by_pair.get((player_team_id, boss_team_id), {})
        scenario_id = str(seed_row.get("scenario_id") or scenario_id or canonical_scenario_id(player_team_id, boss_team_id)).strip()
        mc_win_rate = round(float(row.get("final_mc_win_rate") or row.get("empirical_win_rate") or 0.0), 6)

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
                "n_trials": int(row.get("n_trials") or 1),
                "rng_seed": int(row.get("rng_seed") or rng_seed),
                "wins": int(row.get("wins") or 0),
                "losses": int(row.get("losses") or 0),
                "mc_win_rate": mc_win_rate,
                "win_rate_ci95_low": float(row.get("win_rate_ci95_low") or 0.0),
                "win_rate_ci95_high": float(row.get("win_rate_ci95_high") or 0.0),
                "mc_resamples": int(row.get("final_mc_resamples") or row.get("mc_resamples") or n_trials),
                "interval_method": row.get("final_interval_method") or row.get("interval_method") or "beta_posterior_mc_95",
                "adaptive_rerun": bool(row.get("adaptive_rerun", False)),
                "base_mc_win_rate": float(row.get("empirical_win_rate") or 0.0),
                "base_win_rate_ci95_low": float(row.get("win_rate_ci95_low") or 0.0),
                "base_win_rate_ci95_high": float(row.get("win_rate_ci95_high") or 0.0),
                "final_mc_win_rate": float(row.get("final_mc_win_rate") or row.get("empirical_win_rate") or 0.0),
                "final_win_rate_ci95_low": float(row.get("final_win_rate_ci95_low") or row.get("win_rate_ci95_low") or 0.0),
                "final_win_rate_ci95_high": float(row.get("final_win_rate_ci95_high") or row.get("win_rate_ci95_high") or 0.0),
                "boss_sequence_id": row.get("boss_sequence_id") if row.get("boss_sequence_id") is not None else seed_row.get("boss_sequence_id"),
                "sequence_position": row.get("sequence_position") if row.get("sequence_position") is not None else seed_row.get("sequence_position"),
                "remaining_team_state": row.get("remaining_team_state", seed_row.get("remaining_team_state", [])),
                "gauntlet_success": bool(row.get("gauntlet_success", seed_row.get("gauntlet_success", False))),
                "gauntlet_success_rate": row.get("gauntlet_success_rate") if row.get("gauntlet_success_rate") is not None else seed_row.get("gauntlet_success_rate"),
                "simulation_mode": row.get("simulation_mode") or seed_row.get("simulation_mode") or "gym",
                "outcome_cause": row.get("outcome_cause") or "simulated_loss",
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
