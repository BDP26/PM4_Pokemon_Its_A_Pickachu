"""Prepare battle seeds from Gold team battle simulation outputs."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import pandas as pd

from src.pipeline.common.io import read_many_parquet, read_parquet, write_parquet
from src.pipeline.common.simulation_schema import normalize_team_battle_simulation_schema
from src.pipeline.silver.simulation.schema_contract import canonical_scenario_context_id
from src.pipeline.settings import GOLD_DIR, GOLD_SIMULATION_DIRNAME, SILVER_DIR

logger = logging.getLogger(__name__)


def build_battle_seeds(
    gold_dir: Path = GOLD_DIR,
    simulation_dirname: str = GOLD_SIMULATION_DIRNAME,
    silver_dir: Path = SILVER_DIR,
) -> None:
    simulation_dir = gold_dir / simulation_dirname
    simulations_file = simulation_dir / "team_battle_simulations.parquet"
    logger.info("[battle_seeds] reading simulations path=%s", simulations_file)

    teams_shards = sorted(simulation_dir.glob("teams_*.parquet"))
    if teams_shards:
        teams_df = read_many_parquet(teams_shards)
    else:
        teams_file = simulation_dir / "teams.parquet"
        if teams_file.exists():
            teams_df = read_parquet(teams_file)
        else:
            logger.warning("[battle_seeds] no teams found in gold simulation directory; skipping")
            return

    logger.info("[battle_seeds] input teams rows=%s", len(teams_df))
    boss_teams_df = teams_df[teams_df["boss_name"].notna()].copy()
    logger.info("[battle_seeds] rows after boss filter=%s", len(boss_teams_df))

    if simulations_file.exists():
        simulations_df = normalize_team_battle_simulation_schema(
            read_parquet(simulations_file),
            required_columns={"team_id_attacker", "team_id_defender", "predicted_player_win_chance", "simulation_score"},
        )
    else:
        simulations_df = pd.DataFrame()

    logger.info("[battle_seeds] input simulation rows=%s", len(simulations_df))

    scenarios: list[dict[str, Any]] = []
    skipped_examples: list[dict[str, Any]] = []

    for boss_team in boss_teams_df.to_dict(orient="records"):
        boss_id = boss_team.get("team_id")
        boss_name = boss_team.get("boss_name")
        game_version = boss_team.get("game_version")
        boss_level = boss_team.get("avg_level", 20)

        if not simulations_df.empty:
            boss_matchups = simulations_df[simulations_df["team_id_defender"] == boss_id]
        else:
            boss_matchups = pd.DataFrame()

        for _, player_match in boss_matchups.iterrows():
            player_id = player_match.get("team_id_attacker")
            # Filter out null/empty team IDs to prevent downstream inconsistencies
            if not player_id or not boss_id or str(player_id).strip() == "" or str(boss_id).strip() == "":
                if len(skipped_examples) < 5:
                    skipped_examples.append({"reason": "null_team_id", "row": player_match.to_dict()})
                continue

            predicted_win_chance = round(float(player_match.get("predicted_player_win_chance", 0.5) or 0.5), 4)
            simulation_score = float(player_match.get("simulation_score", 0.0) or 0.0)
            attacker_win = bool(player_match.get("attacker_win", False))
            n_trials = int(player_match.get("n_trials", 1) or 1)

            scenarios.append(
                {
                    "scenario_id": canonical_scenario_context_id(
                        player_id,
                        boss_id,
                        simulation_mode=player_match.get("simulation_mode"),
                        boss_sequence_id=player_match.get("boss_sequence_id"),
                        sequence_position=player_match.get("sequence_position"),
                    ),
                    "player_team_id": player_id,
                    "boss_team_id": boss_id,
                    "boss_name": boss_name,
                    "game_version": game_version,
                    "boss_level": boss_level,
                    "predicted_player_win_chance": predicted_win_chance,
                    "simulation_score": round(simulation_score, 3),
                    "simulated_attacker_win": attacker_win,
                    "n_trials": n_trials,
                    "boss_sequence_id": player_match.get("boss_sequence_id"),
                    "sequence_position": player_match.get("sequence_position"),
                    "remaining_team_state": player_match.get("remaining_team_state", []),
                    "gauntlet_success": bool(player_match.get("gauntlet_success", False)),
                    "gauntlet_success_rate": player_match.get("gauntlet_success_rate"),
                    "simulation_mode": player_match.get("simulation_mode") or "gym",
                }
            )

    write_parquet(simulation_dir / "battle_seeds.parquet", scenarios)
    logger.info("[battle_seeds] output battle seed rows=%s", len(scenarios))

    if not simulations_df.empty and len(scenarios) == 0:
        logger.error("[battle_seeds] zero output rows despite simulation input rows=%s skipped_examples=%s", len(simulations_df), skipped_examples)
        raise ValueError(
            "battle_seeds produced 0 rows even though team_battle_simulations.parquet has data; "
            f"simulation_rows={len(simulations_df)} boss_rows={len(boss_teams_df)} skipped_examples={skipped_examples}"
        )
