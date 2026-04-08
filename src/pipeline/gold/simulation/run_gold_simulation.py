from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any, cast

import pandas as pd

from src.pipeline.common.io import read_parquet, write_parquet
from src.pipeline.settings import (
    BRONZE_DIR,
    GOLD_DIR,
    GOLD_SIMULATION_DIRNAME,
    SILVER_DIR,
    SILVER_SIMULATION_DIRNAME,
)
from src.pipeline.silver.simulation.battle_seeds import build_battle_seeds
from src.pipeline.silver.simulation.monte_carlo_optimizer import run_monte_carlo_team_optimizer
from src.pipeline.silver.simulation.type_matchups import build_team_battle_simulations


logger = logging.getLogger(__name__)


def run_gold_simulation_from_silver(
    silver_dir: Path = SILVER_DIR,
    gold_dir: Path = GOLD_DIR,
    bronze_dir: Path = BRONZE_DIR,
    n_trials: int = 500,
    rng_seed: int = 42,
) -> None:
    """Run full battle simulation pipeline in gold using silver prepared team inputs."""
    started_at = time.perf_counter()
    silver_simulation_dir = silver_dir / SILVER_SIMULATION_DIRNAME
    gold_simulation_dir = gold_dir / GOLD_SIMULATION_DIRNAME
    gold_simulation_dir.mkdir(parents=True, exist_ok=True)
    logger.info("[gold/simulation] start silver_dir=%s gold_dir=%s", silver_dir, gold_dir)

    teams_path = silver_simulation_dir / "teams.parquet"
    boss_teams_path = silver_simulation_dir / "boss_teams.parquet"
    player_teams_path = silver_simulation_dir / "player_teams.parquet"
    combos_path = silver_simulation_dir / "starter_team_moveset_combinations.parquet"

    if boss_teams_path.exists() and player_teams_path.exists():
        boss_df = read_parquet(boss_teams_path)
        player_df = read_parquet(player_teams_path)
        teams_df = read_parquet(teams_path) if teams_path.exists() else None

        expanded_players: list[dict[str, Any]] = []
        if combos_path.exists():
            combos_df = read_parquet(combos_path)
            if not combos_df.empty:
                expanded_players = _build_combo_player_teams(player_df=player_df, combos_df=combos_df)
                logger.info(
                    "[gold/simulation] expanded combo player teams=%s from combos=%s",
                    len(expanded_players),
                    len(combos_df),
                )

        if expanded_players:
            teams_df = pd.concat([boss_df, pd.DataFrame(expanded_players)], ignore_index=True)
        elif teams_df is None or teams_df.empty:
            teams_df = pd.concat([boss_df, player_df], ignore_index=True)
    else:
        if not teams_path.exists():
            raise FileNotFoundError(
                f"Simulation input missing in silver: {teams_path}. Run silver layer first."
            )
        teams_df = read_parquet(teams_path)
    if teams_df.empty:
        logger.warning("[gold/simulation] teams.parquet is empty; skipping simulation")
        return

    teams_data = cast(list[dict[str, Any]], teams_df.to_dict(orient="records"))
    write_parquet(gold_simulation_dir / "teams.parquet", teams_data)
    logger.info("[gold/simulation] loaded teams count=%s", len(teams_data))

    sims_started_at = time.perf_counter()
    logger.info("[gold/simulation] running team battle simulations with pyspark")
    build_team_battle_simulations(
        teams_data=teams_data,
        silver_dir=gold_dir,
        bronze_dir=bronze_dir,
        force_spark=True,
    )
    logger.info("[gold/simulation] team battle simulations done elapsed_s=%.2f", time.perf_counter() - sims_started_at)

    seeds_started_at = time.perf_counter()
    logger.info("[gold/simulation] building battle seeds")
    build_battle_seeds(
        silver_dir=gold_dir,
        simulation_dirname=GOLD_SIMULATION_DIRNAME,
    )
    logger.info("[gold/simulation] battle seeds done elapsed_s=%.2f", time.perf_counter() - seeds_started_at)

    mc_started_at = time.perf_counter()
    logger.info("[gold/simulation] running monte carlo optimizer trials=%s seed=%s", n_trials, rng_seed)
    run_monte_carlo_team_optimizer(
        silver_dir=gold_dir,
        simulation_dirname=GOLD_SIMULATION_DIRNAME,
        n_trials=n_trials,
        rng_seed=rng_seed,
    )
    logger.info("[gold/simulation] monte carlo optimizer done elapsed_s=%.2f", time.perf_counter() - mc_started_at)
    logger.info("[gold/simulation] finished elapsed_s=%.2f", time.perf_counter() - started_at)


def _build_combo_player_teams(
    player_df: pd.DataFrame,
    combos_df: pd.DataFrame,
) -> list[dict[str, Any]]:
    base_by_team_id = {
        str(row.get("team_id")): row
        for row in player_df.to_dict(orient="records")
        if row.get("team_id") is not None
    }

    combo_players: list[dict[str, Any]] = []
    for combo in combos_df.to_dict(orient="records"):
        base_team_id = str(combo.get("player_team_id") or "")
        candidate_team_id = str(combo.get("candidate_team_id") or "")
        if not base_team_id or not candidate_team_id:
            continue
        base = base_by_team_id.get(base_team_id)
        if base is None:
            continue

        team_size = int(combo.get("team_size") or 0)
        details: list[dict[str, Any]] = []
        pokemon: list[str] = []
        levels = list(base.get("levels", [])) if isinstance(base.get("levels"), list) else []

        for slot_idx in range(1, max(1, team_size) + 1):
            species = str(combo.get(f"slot_{slot_idx}_species") or "").strip().lower()
            if not species:
                continue
            member_moves = [
                str(combo.get(f"slot_{slot_idx}_move_{move_idx}") or "").strip().lower()
                for move_idx in range(1, 5)
            ]
            member_moves = [move for move in member_moves if move]
            raw_level = levels[slot_idx - 1] if slot_idx - 1 < len(levels) else base.get("avg_level", 20)
            try:
                member_level = int(raw_level)
            except Exception:
                member_level = 20
            details.append(
                {
                    "name": species,
                    "level": int(member_level),
                    "moves": member_moves,
                    "required_moves": member_moves,
                    "origin": "starter_combo",
                }
            )
            pokemon.append(species)

        if not details:
            continue

        avg_level = int(sum(int(member.get("level", 1)) for member in details) / len(details))
        combo_players.append(
            {
                **base,
                "team_id": candidate_team_id,
                "source_team_id": base_team_id,
                "details": details,
                "pokemon": pokemon,
                "levels": [int(member["level"]) for member in details],
                "avg_level": avg_level,
                "team_role": "player",
                "is_player_candidate": True,
                "candidate_moveset_key": combo.get("moveset_key"),
                "catchable_species_key": combo.get("catchable_species_key"),
            }
        )

    return combo_players
