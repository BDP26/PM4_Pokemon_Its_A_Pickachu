"""
Elite Four + Champion battle gauntlet simulation.

Simulates a player team progressing through a full Elite Four gauntlet:
- Elite Four Trainer 1
- Elite Four Trainer 2
- Elite Four Trainer 3
- Elite Four Trainer 4
- Champion

The player wins only if ALL 5 battles are won.
Win probability is the product of individual battle win chances.
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import pandas as pd

from src.pipeline.common.io import read_parquet, write_parquet
from src.pipeline.settings import SILVER_DIR, SILVER_SIMULATION_DIRNAME

logger = logging.getLogger(__name__)


def _get_elite_four_sequence(
    game_version: str,
    teams_df: pd.DataFrame,
) -> list[dict[str, Any]]:
    """
    Get the Elite Four + Champion sequence for a game version.
    Returns list of boss teams in order: [E4_1, E4_2, E4_3, E4_4, Champion]
    """
    # Filter for this game version
    game_teams = teams_df[teams_df["game_version"] == game_version]

    # Get elite four and champion teams
    elite_four = game_teams[
        (game_teams["gym"].str.contains("Elite Four", case=False, na=False)) &
        (~game_teams["gym"].str.contains("Champion", case=False, na=False))
    ].sort_values("team_id")

    champions = game_teams[
        game_teams["gym"].str.contains("Champion", case=False, na=False)
    ].sort_values("team_id")

    # For Elite Four: group by boss_name to get unique members
    elite_four_sequence = []
    for boss_name in elite_four["boss_name"].unique():
        boss_teams = elite_four[elite_four["boss_name"] == boss_name]
        # Take the first one (they should all have same team)
        if not boss_teams.empty:
            elite_four_sequence.append(boss_teams.iloc[0].to_dict())

    # Champions: each starter variant is separate
    champion_sequence = champions.to_dict(orient="records")

    return elite_four_sequence + champion_sequence


def _get_champion_for_starter(
    game_version: str,
    starter_base: str,
    teams_df: pd.DataFrame,
) -> dict[str, Any] | None:
    """
    Get the champion team for a specific starter base.
    Returns the champion team dict or None if not found.
    """
    champions = teams_df[
        (teams_df["game_version"] == game_version) &
        (teams_df["gym"].str.contains("Champion", case=False, na=False))
    ]

    # Try to match starter
    if starter_base:
        starter_champions = champions[
            champions["starter_base"].str.lower() == starter_base.lower()
        ]
        if not starter_champions.empty:
            return starter_champions.iloc[0].to_dict()

    # Fallback: return first champion
    if not champions.empty:
        return champions.iloc[0].to_dict()

    return None


def calculate_elite_four_gauntlet_win_probability(
    player_team_id: str,
    game_version: str,
    starter_base: str,
    battle_seeds_df: pd.DataFrame,
    teams_df: pd.DataFrame,
) -> dict[str, Any]:
    """
    Calculate win probability for a player team through the full Elite Four gauntlet.

    Args:
        player_team_id: Player team identifier
        game_version: Game version (e.g., "red", "blue", "black")
        starter_base: Base starter Pokémon (e.g., "bulbasaur")
        battle_seeds_df: DataFrame with battle win probabilities (scenario_id, predicted_player_win_chance)
        teams_df: DataFrame with all teams

    Returns:
        Dict with:
        - gauntlet_id: Unique identifier
        - player_team_id: Player team
        - game_version: Game version
        - starter_base: Starter Pokémon
        - elite_four_sequence: List of Elite Four boss team IDs
        - champion_team_id: Champion team ID
        - battle_win_probabilities: List of individual win chances
        - cumulative_gauntlet_win_probability: Product of all win chances
        - is_viable: True if win chance > 0 (at least one complete path possible)
    """

    # Get Elite Four sequence + Champion for this game
    elite_four_teams = _get_elite_four_sequence(game_version, teams_df)
    if not elite_four_teams:
        logger.warning(
            "[gauntlet] no elite four sequence found for game=%s", game_version
        )
        return None

    # Get champion for this starter
    champion = _get_champion_for_starter(game_version, starter_base, teams_df)
    if not champion:
        logger.warning(
            "[gauntlet] no champion found for game=%s starter=%s",
            game_version,
            starter_base,
        )
        return None

    # Build full sequence: Elite Four 1-4, then Champion
    boss_sequence = elite_four_teams + [champion]

    # Look up win probability for each battle
    battle_win_probs: list[float] = []
    boss_team_ids: list[str] = []

    for idx, boss_team in enumerate(boss_sequence, start=1):
        boss_team_id = boss_team.get("team_id")
        boss_team_ids.append(boss_team_id)

        # Look up this matchup in battle_seeds
        scenario_id = f"{player_team_id}_vs_{boss_team_id}"
        matching = battle_seeds_df[battle_seeds_df["scenario_id"] == scenario_id]

        if not matching.empty:
            win_prob = float(matching.iloc[0]["predicted_player_win_chance"])
        else:
            # No simulation data - assume 50% chance
            logger.warning(
                "[gauntlet] no simulation data for %s vs %s, using 0.5",
                player_team_id,
                boss_team_id,
            )
            win_prob = 0.5

        battle_win_probs.append(win_prob)

    # Calculate cumulative probability (product of all individual probabilities)
    cumulative_prob = 1.0
    for prob in battle_win_probs:
        cumulative_prob *= prob

    gauntlet_id = f"{player_team_id}__gauntlet__{game_version}__{starter_base}"

    return {
        "gauntlet_id": gauntlet_id,
        "player_team_id": player_team_id,
        "game_version": game_version,
        "starter_base": starter_base,
        "elite_four_count": len(elite_four_teams),
        "elite_four_team_ids": "|".join(str(tid) for tid in boss_team_ids[:-1]),
        "champion_team_id": boss_team_ids[-1],
        "elite_four_win_prob_1": battle_win_probs[0] if len(battle_win_probs) > 0 else None,
        "elite_four_win_prob_2": battle_win_probs[1] if len(battle_win_probs) > 1 else None,
        "elite_four_win_prob_3": battle_win_probs[2] if len(battle_win_probs) > 2 else None,
        "elite_four_win_prob_4": battle_win_probs[3] if len(battle_win_probs) > 3 else None,
        "champion_win_prob": battle_win_probs[4] if len(battle_win_probs) > 4 else None,
        "cumulative_gauntlet_win_probability": round(cumulative_prob, 4),
        "is_viable": cumulative_prob > 0.0,
    }


def build_elite_four_gauntlet_results(
    silver_dir: Path = SILVER_DIR,
    simulation_dirname: str = SILVER_SIMULATION_DIRNAME,
) -> int:
    """
    Build Elite Four gauntlet win probabilities for all player teams per starter.

    For each unique (game_version, starter_base) pair, calculates the probability
    of beating the full Elite Four + Champion sequence.

    Returns:
        Number of gauntlet scenarios calculated
    """
    simulation_dir = silver_dir / simulation_dirname
    teams_path = simulation_dir / "teams.parquet"
    battle_seeds_path = simulation_dir / "battle_seeds.parquet"
    output_path = simulation_dir / "elite_four_gauntlet_results.parquet"

    if not teams_path.exists():
        logger.warning("[gauntlet] teams file not found: %s", teams_path)
        return 0

    if not battle_seeds_path.exists():
        logger.warning("[gauntlet] battle_seeds file not found: %s", battle_seeds_path)
        return 0

    teams_df = read_parquet(teams_path)
    battle_seeds_df = read_parquet(battle_seeds_path)

    # Get all player teams per starter per game
    player_teams = teams_df[teams_df["is_player_candidate"] == True]

    results: list[dict[str, Any]] = []

    for _, player_team in player_teams.iterrows():
        player_team_id = str(player_team.get("team_id") or "")
        game_version = str(player_team.get("game_version") or "").lower().strip()
        starter_base = str(player_team.get("starter_base") or "").lower().strip()

        if not player_team_id or not game_version:
            continue

        gauntlet_result = calculate_elite_four_gauntlet_win_probability(
            player_team_id,
            game_version,
            starter_base,
            battle_seeds_df,
            teams_df,
        )

        if gauntlet_result:
            results.append(gauntlet_result)

    write_parquet(output_path, results)
    logger.info(
        "[gauntlet] calculated gauntlet results for %s player teams, wrote to %s",
        len(results),
        output_path,
    )
    return len(results)

