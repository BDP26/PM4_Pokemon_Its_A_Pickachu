"""Shared schema contract helpers for Silver/Gold simulation artifacts."""

from __future__ import annotations

from typing import Any

SIMULATION_MATCHUP_TEAM_COLUMNS: tuple[str, str] = ("team_id_attacker", "team_id_defender")
SIMULATION_MATCHUP_CANONICAL_COLUMNS: tuple[str, str, str] = (
    "scenario_id",
    "player_team_id",
    "boss_team_id",
)

MONTE_CARLO_REQUIRED_COLUMNS: tuple[str, ...] = (
    "scenario_id",
    "player_team_id",
    "boss_team_id",
    "predicted_player_win_chance",
    "n_trials",
    "wins",
    "losses",
    "mc_win_rate",
)


def canonical_scenario_id(player_team_id: Any, boss_team_id: Any) -> str:
    player = str(player_team_id or "").strip()
    boss = str(boss_team_id or "").strip()
    return f"{player}_vs_{boss}"


def row_player_boss_ids(row: dict[str, Any]) -> tuple[str, str]:
    """Extract canonical player/boss ids from either canonical or legacy matchup keys."""
    player_team_id = str(
        row.get("player_team_id")
        or row.get("team_id_attacker")
        or ""
    ).strip()
    boss_team_id = str(
        row.get("boss_team_id")
        or row.get("team_id_defender")
        or ""
    ).strip()
    return player_team_id, boss_team_id

