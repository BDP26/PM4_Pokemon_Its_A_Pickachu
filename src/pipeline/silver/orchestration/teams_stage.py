"""Team-stage helpers for Silver orchestration."""

from __future__ import annotations

from collections import defaultdict
from typing import Any


def group_teams_by_game(teams: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    by_game: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for team in teams:
        game_version = str(team.get("game_version") or "").strip().lower()
        if game_version:
            by_game[game_version].append(team)
    return by_game

