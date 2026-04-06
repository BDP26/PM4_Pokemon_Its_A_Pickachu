"""Prepare battle simulation seeds from silver layer data."""
import math
from pathlib import Path

import pandas as pd

from src.pipeline.common.io import read_parquet, write_parquet
from src.pipeline.settings import SILVER_DIR, SILVER_SIMULATION_DIRNAME


def _probability_from_simulation(row: pd.Series) -> float:
    """Map deterministic simulation outputs to a stable [0, 1] probability."""
    score = float(row.get("simulation_score", 0.0) or 0.0)
    attacker_win = bool(row.get("attacker_win", False))
    degraded = bool(row.get("degraded_data", False))

    # Keep mapping deterministic and monotonic while avoiding 0/1 extremes.
    probability = 0.5 + 0.5 * math.tanh(score / 120.0)

    # Add a tiny deterministic prior so explicit simulated winner nudges ties.
    if attacker_win:
        probability = max(probability, 0.55)
    else:
        probability = min(probability, 0.45)

    if degraded:
        # Degraded simulations get pulled closer to neutral confidence.
        probability = 0.5 + (probability - 0.5) * 0.6

    return round(min(0.99, max(0.01, probability)), 3)


def build_battle_seeds(
    silver_dir: Path = SILVER_DIR,
    simulation_dirname: str = SILVER_SIMULATION_DIRNAME,
) -> None:
    """
    Create pre-computed battle scenarios for quick simulation lookups.

    Seeds map: player_team_id -> boss_team_id -> simulated win probability
    """

    simulation_dir = silver_dir / simulation_dirname

    # Read teams and sequential battle simulations.
    teams_file = simulation_dir / "teams.parquet"
    simulations_file = simulation_dir / "team_battle_simulations.parquet"

    if not teams_file.exists():
        print("[battle_seeds] no teams found, skipping")
        return

    teams_df = read_parquet(teams_file)

    # Identify boss teams
    boss_teams = teams_df[teams_df["boss_name"].notna()].to_dict(orient="records")

    if simulations_file.exists():
        simulations_df = read_parquet(simulations_file)
    else:
        simulations_df = pd.DataFrame()

    # Create battle scenarios
    scenarios = []

    for boss_team in boss_teams:
        boss_id = boss_team.get("team_id")
        boss_name = boss_team.get("boss_name")
        game_version = boss_team.get("game_version")
        boss_level = boss_team.get("avg_level", 20)

        # Find simulated battles where the boss is the defending side.
        if not simulations_df.empty:
            boss_matchups = simulations_df[
                simulations_df["team_id_defender"] == boss_id
            ]
        else:
            boss_matchups = pd.DataFrame()

        for _, player_match in boss_matchups.iterrows():
            player_id = player_match.get("team_id_attacker")
            predicted_win_chance = _probability_from_simulation(player_match)
            simulation_score = float(player_match.get("simulation_score", 0.0) or 0.0)
            attacker_win = bool(player_match.get("attacker_win", False))
            degraded_data = bool(player_match.get("degraded_data", False))

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
            })

    simulation_dir.mkdir(parents=True, exist_ok=True)
    write_parquet(simulation_dir / "battle_seeds.parquet", scenarios)

    print(f"[battle_seeds] created {len(scenarios)} battle scenarios")



