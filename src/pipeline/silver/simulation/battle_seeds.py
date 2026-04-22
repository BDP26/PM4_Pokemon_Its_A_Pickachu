"""Prepare battle simulation seeds from sharded Silver simulation outputs."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.pipeline.common.io import read_many_parquet, write_parquet
from src.pipeline.settings import SILVER_DIR, SILVER_SIMULATION_DIRNAME


def build_battle_seeds(
    silver_dir: Path = SILVER_DIR,
    simulation_dirname: str = SILVER_SIMULATION_DIRNAME,
) -> None:
    simulation_dir = silver_dir / simulation_dirname

    teams_df = read_many_parquet(sorted(simulation_dir.glob("teams_*.parquet")))
    if teams_df.empty:
        print("[battle_seeds] no teams found, skipping")
        return

    simulations_df = read_many_parquet(sorted(simulation_dir.glob("team_battle_simulations_*.parquet")))
    if simulations_df.empty:
        print("[battle_seeds] no team battle simulations found, skipping")
        return

    boss_teams = teams_df[teams_df["boss_name"].notna()].to_dict(orient="records")
    scenarios = []

    for boss_team in boss_teams:
        boss_id = boss_team.get("team_id")
        boss_name = boss_team.get("boss_name")
        game_version = boss_team.get("game_version")
        boss_level = boss_team.get("avg_level", 20)

        boss_matchups = simulations_df[simulations_df["team_id_defender"] == boss_id]

        for _, player_match in boss_matchups.iterrows():
            player_id = player_match.get("team_id_attacker")
            predicted_win_chance = round(float(player_match.get("predicted_player_win_chance", 0.5) or 0.5), 3)
            simulation_score = float(player_match.get("simulation_score", 0.0) or 0.0)
            attacker_win = bool(player_match.get("attacker_win", False))
            degraded_data = bool(player_match.get("degraded_data", False))
            n_trials = int(player_match.get("n_trials", 1) or 1)

            scenarios.append(
                {
                    "scenario_id": f"{player_id}_vs_{boss_id}",
                    "player_team_id": player_id,
                    "boss_team_id": boss_id,
                    "boss_name": boss_name,
                    "game_version": game_version,
                    "boss_level": boss_level,
                    "predicted_player_win_chance": predicted_win_chance,
                    "simulation_score": round(simulation_score, 3),
                    "simulated_attacker_win": attacker_win,
                    "degraded_data": degraded_data,
                    "n_trials": n_trials,
                }
            )

    write_parquet(simulation_dir / "battle_seeds.parquet", scenarios)
    print(f"[battle_seeds] created {len(scenarios)} battle scenarios")