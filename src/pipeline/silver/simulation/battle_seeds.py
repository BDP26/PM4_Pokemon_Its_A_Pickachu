"""Prepare battle simulation seeds from silver layer data."""
from __future__ import annotations

import math
from pathlib import Path

import pandas as pd

from src.pipeline.common.io import read_parquet, write_parquet
from src.pipeline.settings import SILVER_DIR, SILVER_SIMULATION_DIRNAME


def _probability_from_simulation(row: pd.Series) -> float:
    if "predicted_player_win_chance" in row and pd.notna(row["predicted_player_win_chance"]):
        return round(float(row["predicted_player_win_chance"]), 3)

    # Legacy fallback
    score = float(row.get("simulation_score", 0.0) or 0.0)
    attacker_win = bool(row.get("attacker_win", False))
    degraded = bool(row.get("degraded_data", False))

    probability = 0.5 + 0.5 * math.tanh(score / 120.0)

    if attacker_win:
        probability = max(probability, 0.55)
    else:
        probability = min(probability, 0.45)

    if degraded:
        probability = 0.5 + (probability - 0.5) * 0.6

    return round(min(0.99, max(0.01, probability)), 3)


def build_battle_seeds(
    silver_dir: Path = SILVER_DIR,
    simulation_dirname: str = SILVER_SIMULATION_DIRNAME,
) -> None:
    simulation_dir = silver_dir / simulation_dirname

    teams_file = simulation_dir / "teams.parquet"
    simulations_file = simulation_dir / "team_battle_simulations.parquet"

    if not teams_file.exists():
        print("[battle_seeds] no teams found, skipping")
        return

    teams_df = read_parquet(teams_file)
    boss_teams = teams_df[teams_df["boss_name"].notna()].to_dict(orient="records")

    if simulations_file.exists():
        simulations_df = read_parquet(simulations_file)
    else:
        simulations_df = pd.DataFrame()

    scenarios = []

    for boss_team in boss_teams:
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
            predicted_win_chance = _probability_from_simulation(player_match)
            simulation_score = float(player_match.get("simulation_score", 0.0) or 0.0)
            attacker_win = bool(player_match.get("attacker_win", False))
            degraded_data = bool(player_match.get("degraded_data", False))
            n_trials = int(player_match.get("n_trials", 1) or 1)

            scenarios.append({
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
            })

    simulation_dir.mkdir(parents=True, exist_ok=True)
    write_parquet(simulation_dir / "battle_seeds.parquet", scenarios)

    print(f"[battle_seeds] created {len(scenarios)} battle scenarios")