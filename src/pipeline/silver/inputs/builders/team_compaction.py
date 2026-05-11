"""Team compaction facade.

This module isolates compact table generation from progression team selection.
"""

from __future__ import annotations

from typing import Any

from src.pipeline.silver.inputs.builders.player_teams import (
    build_player_team_compact_tables,
    build_player_teams_from_progression_context,
    estimate_team_moveset_space,
)

__all__ = [
    "build_player_team_compact_tables",
    "build_player_teams_from_progression_context",
    "estimate_team_moveset_space",
]

