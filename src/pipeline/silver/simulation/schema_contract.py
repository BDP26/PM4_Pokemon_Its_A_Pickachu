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


def _normalized_optional_value(value: Any) -> str:
    if value is None:
        return ""
    try:
        if value != value:
            return ""
    except Exception:
        pass
    text = str(value).strip()
    if text.lower() in {"", "none", "nan", "<na>"}:
        return ""
    return text


def canonical_scenario_context_id(
    player_team_id: Any,
    boss_team_id: Any,
    *,
    simulation_mode: Any = None,
    boss_sequence_id: Any = None,
    sequence_position: Any = None,
) -> str:
    scenario_id = canonical_scenario_id(player_team_id, boss_team_id)
    mode = _normalized_optional_value(simulation_mode).lower()
    if mode in {"", "gym"}:
        return scenario_id

    parts = [f"mode={mode}"]
    sequence_id = _normalized_optional_value(boss_sequence_id)
    if sequence_id:
        parts.append(f"sequence={sequence_id}")
    position = _normalized_optional_value(sequence_position)
    if position:
        parts.append(f"position={position}")
    return f"{scenario_id}__{'|'.join(parts)}"


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
