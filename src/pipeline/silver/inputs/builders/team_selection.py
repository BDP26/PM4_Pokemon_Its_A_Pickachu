"""Team selection facade.

This module isolates progression candidate/team selection entry points from
compaction/output table generation.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from src.pipeline.silver.inputs.builders.player_teams import (
    build_progression_source_teams_from_encounters,
)

__all__ = [
    "build_progression_source_teams_from_encounters",
]

