from __future__ import annotations

from typing import Iterable

import pandas as pd

TEAM_BATTLE_COLUMN_ALIASES: dict[str, str] = {
    "player_team_id": "team_id_attacker",
    "boss_team_id": "team_id_defender",
    "win_probability": "predicted_player_win_chance",
    "score": "simulation_score",
}


def normalize_team_battle_simulation_schema(
    frame: pd.DataFrame,
    *,
    required_columns: Iterable[str] | None = None,
) -> pd.DataFrame:
    renamed = frame.rename(columns={k: v for k, v in TEAM_BATTLE_COLUMN_ALIASES.items() if k in frame.columns})
    if required_columns is None:
        return renamed
    missing = sorted(set(required_columns) - set(renamed.columns))
    if missing:
        raise ValueError(f"missing required columns after schema normalization: {missing}")
    return renamed
