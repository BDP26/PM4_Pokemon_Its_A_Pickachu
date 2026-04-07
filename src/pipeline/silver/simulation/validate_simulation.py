"""Smoke checks for simulation artifacts in data/silver/simulation."""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, cast

import numpy as np
import pandas as pd

from src.pipeline.settings import SILVER_DIR, SILVER_SIMULATION_DIRNAME


def _read_parquet(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    dataframe = pd.read_parquet(path)
    return cast(list[dict[str, Any]], dataframe.to_dict(orient="records"))


def validate_simulation_artifacts(silver_dir: Path = SILVER_DIR) -> list[str]:
    issues: list[str] = []
    simulation_dir = silver_dir / SILVER_SIMULATION_DIRNAME

    teams_path = simulation_dir / "teams.parquet"
    team_battles_path = simulation_dir / "team_battle_simulations.parquet"
    seeds_path = simulation_dir / "battle_seeds.parquet"
    monte_carlo_path = simulation_dir / "monte_carlo_results.parquet"

    required_files = [teams_path, team_battles_path, seeds_path, monte_carlo_path]
    for file_path in required_files:
        if not file_path.exists():
            issues.append(f"Missing file: {file_path}")

    if issues:
        return issues

    teams = _read_parquet(teams_path)
    team_battles = _read_parquet(team_battles_path)
    seeds = _read_parquet(seeds_path)
    monte_carlo = _read_parquet(monte_carlo_path)

    if not teams:
        issues.append("teams.parquet is empty")
    if not team_battles:
        issues.append("team_battle_simulations.parquet is empty")
    if not seeds:
        issues.append("battle_seeds.parquet is empty")
    if not monte_carlo:
        issues.append("monte_carlo_results.parquet is empty")

    team_ids: set[str] = set()

    def _is_sequence_like(value: Any) -> bool:
        return isinstance(value, (list, tuple, np.ndarray))

    def _is_numeric(value: Any) -> bool:
        return isinstance(value, (int, float, np.integer, np.floating))

    for idx, team in enumerate(teams, start=1):
        for key in ["team_id", "boss_name", "game_version", "pokemon"]:
            if key not in team:
                issues.append(f"teams.parquet row {idx}: missing '{key}'")
        team_id = team.get("team_id")
        pokemon = team.get("pokemon")
        avg_level = team.get("avg_level")

        if isinstance(team_id, str):
            if team_id in team_ids:
                issues.append(f"Duplicate team_id: {team_id}")
            team_ids.add(team_id)
        else:
            issues.append(f"teams.parquet row {idx}: team_id is not a string")

        pokemon_items = cast(list[Any] | tuple[Any, ...] | np.ndarray, pokemon) if _is_sequence_like(pokemon) else []
        if len(pokemon_items) == 0:
            issues.append(f"teams.parquet row {idx}: pokemon must be a non-empty sequence")

        if avg_level is not None and not _is_numeric(avg_level):
            issues.append(f"teams.parquet row {idx}: avg_level must be numeric")

        details = team.get("details")
        detail_items = cast(list[Any] | tuple[Any, ...] | np.ndarray, details) if _is_sequence_like(details) else []
        for member_idx, member in enumerate(detail_items, start=1):
            if not isinstance(member, dict):
                continue
            required_moves = member.get("required_moves")
            required_items = (
                cast(list[Any] | tuple[Any, ...] | np.ndarray, required_moves)
                if _is_sequence_like(required_moves)
                else []
            )
            if len(required_items) == 0:
                issues.append(
                    f"teams.parquet row {idx} member {member_idx}: required_moves must be a non-empty sequence"
                )

    matchup_pairs: set[tuple[str, str]] = set()
    for idx, matchup in enumerate(team_battles, start=1):
        atk = matchup.get("team_id_attacker")
        deff = matchup.get("team_id_defender")
        attacker_win = matchup.get("attacker_win")
        battle_turns = matchup.get("battle_turns")
        simulation_score = matchup.get("simulation_score")

        if not isinstance(atk, str) or atk not in team_ids:
            issues.append(f"team_battle_simulations.parquet row {idx}: invalid attacker team reference")
        if not isinstance(deff, str) or deff not in team_ids:
            issues.append(f"team_battle_simulations.parquet row {idx}: invalid defender team reference")
        if isinstance(atk, str) and isinstance(deff, str):
            if atk == deff:
                issues.append(f"team_battle_simulations.parquet row {idx}: self-matchup is not allowed")
            matchup_pairs.add((cast(str, atk), cast(str, deff)))

        if not isinstance(attacker_win, bool):
            issues.append(f"team_battle_simulations.parquet row {idx}: attacker_win must be boolean")
        if not _is_numeric(battle_turns) or int(cast(int | float, battle_turns)) <= 0:
            issues.append(f"team_battle_simulations.parquet row {idx}: battle_turns must be > 0")
        if not isinstance(simulation_score, (int, float)):
            issues.append(f"team_battle_simulations.parquet row {idx}: simulation_score must be numeric")

    scenario_ids: set[str] = set()
    for idx, seed in enumerate(seeds, start=1):
        scenario_id = seed.get("scenario_id")
        player_id = seed.get("player_team_id")
        boss_id = seed.get("boss_team_id")
        win_chance = seed.get("predicted_player_win_chance")

        for key in ["scenario_id", "player_team_id", "boss_team_id", "predicted_player_win_chance"]:
            if key not in seed:
                issues.append(f"battle_seeds.parquet row {idx}: missing '{key}'")

        if isinstance(scenario_id, str):
            if scenario_id in scenario_ids:
                issues.append(f"Duplicate scenario_id: {scenario_id}")
            scenario_ids.add(scenario_id)
        else:
            issues.append(f"battle_seeds.parquet row {idx}: scenario_id is not a string")

        if not isinstance(player_id, str) or player_id not in team_ids:
            issues.append(f"battle_seeds.parquet row {idx}: invalid player_team_id")
        if not isinstance(boss_id, str) or boss_id not in team_ids:
            issues.append(f"battle_seeds.parquet row {idx}: invalid boss_team_id")

        if isinstance(player_id, str) and isinstance(boss_id, str) and (player_id, boss_id) not in matchup_pairs:
            issues.append(
                f"battle_seeds.parquet row {idx}: no matching team_battle_simulations entry for ({player_id}, {boss_id})"
            )

        if not _is_numeric(win_chance) or not (0.0 <= float(cast(int | float, win_chance)) <= 1.0):
            issues.append(f"battle_seeds.parquet row {idx}: predicted_player_win_chance out of [0, 1]")

        simulation_score = seed.get("simulation_score")
        if simulation_score is not None and not _is_numeric(simulation_score):
            issues.append(f"battle_seeds.parquet row {idx}: simulation_score must be numeric when provided")

    seed_by_scenario: dict[str, dict[str, Any]] = {}
    for row in seeds:
        scenario_id = row.get("scenario_id")
        if isinstance(scenario_id, str):
            seed_by_scenario[scenario_id] = row

    for idx, mc_row in enumerate(monte_carlo, start=1):
        for key in [
            "scenario_id",
            "player_team_id",
            "boss_team_id",
            "predicted_player_win_chance",
            "n_trials",
            "wins",
            "losses",
            "mc_win_rate",
        ]:
            if key not in mc_row:
                issues.append(f"monte_carlo_results.parquet row {idx}: missing '{key}'")

        scenario_id = mc_row.get("scenario_id")
        player_id = mc_row.get("player_team_id")
        boss_id = mc_row.get("boss_team_id")
        n_trials = mc_row.get("n_trials")
        wins = mc_row.get("wins")
        losses = mc_row.get("losses")
        mc_win_rate = mc_row.get("mc_win_rate")
        base_prob = mc_row.get("predicted_player_win_chance")

        if not isinstance(scenario_id, str) or scenario_id not in seed_by_scenario:
            issues.append(f"monte_carlo_results.parquet row {idx}: invalid scenario_id reference")

        if not isinstance(player_id, str) or player_id not in team_ids:
            issues.append(f"monte_carlo_results.parquet row {idx}: invalid player_team_id")
        if not isinstance(boss_id, str) or boss_id not in team_ids:
            issues.append(f"monte_carlo_results.parquet row {idx}: invalid boss_team_id")

        if not _is_numeric(base_prob) or not (0.0 <= float(cast(int | float, base_prob)) <= 1.0):
            issues.append(f"monte_carlo_results.parquet row {idx}: predicted_player_win_chance out of [0, 1]")

        if not _is_numeric(n_trials) or int(cast(int | float, n_trials)) <= 0:
            issues.append(f"monte_carlo_results.parquet row {idx}: n_trials must be > 0")
            continue

        n_trials_int = int(cast(int | float, n_trials))
        if not _is_numeric(wins) or not _is_numeric(losses):
            issues.append(f"monte_carlo_results.parquet row {idx}: wins/losses must be numeric")
        else:
            wins_int = int(cast(int | float, wins))
            losses_int = int(cast(int | float, losses))
            if wins_int < 0 or losses_int < 0:
                issues.append(f"monte_carlo_results.parquet row {idx}: wins/losses must be >= 0")
            if wins_int + losses_int != n_trials_int:
                issues.append(
                    f"monte_carlo_results.parquet row {idx}: wins + losses must equal n_trials"
                )

        if not _is_numeric(mc_win_rate) or not (0.0 <= float(cast(int | float, mc_win_rate)) <= 1.0):
            issues.append(f"monte_carlo_results.parquet row {idx}: mc_win_rate out of [0, 1]")

    expected_matchups = len(team_ids) * max(len(team_ids) - 1, 0)
    if team_battles and len(team_battles) != expected_matchups:
        issues.append(
            f"team_battle_simulations.parquet row count mismatch: got {len(team_battles)}, expected {expected_matchups}"
        )

    if seeds and len(monte_carlo) != len(seeds):
        issues.append(
            f"monte_carlo_results.parquet row count mismatch: got {len(monte_carlo)}, expected {len(seeds)}"
        )

    if len(issues) == 0:
        print("[validate_simulation] OK")
        print(f"  teams: {len(teams)}")
        print(f"  team_battle_simulations: {len(team_battles)}")
        print(f"  battle_seeds: {len(seeds)}")
        print(f"  monte_carlo_results: {len(monte_carlo)}")

    return issues


def main() -> int:
    issues = validate_simulation_artifacts()
    if issues:
        print("[validate_simulation] FAILED")
        for issue in issues[:200]:
            print(f"  - {issue}")
        if len(issues) > 200:
            print(f"  ... and {len(issues) - 200} more")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())




