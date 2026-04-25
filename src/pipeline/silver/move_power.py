from __future__ import annotations

import re
from typing import Any

FIXED_DAMAGE_LEVEL_MOVES = {
    "seismic-toss",
    "night-shade",
}

FIXED_DAMAGE_CONSTANT_MOVES = {
    "dragon-rage": 40.0,
    "sonic-boom": 20.0,
}

VARIABLE_POWER_MOVES = {
    "grass-knot",
    "low-kick",
    "gyro-ball",
    "flail",
    "reversal",
    "eruption",
    "water-spout",
    "crush-grip",
    "wring-out",
    "heavy-slam",
    "heat-crash",
    "electro-ball",
    "stored-power",
    "punishment",
    "magnitude",
    "natural-gift",
    "trump-card",
    "weather-ball",
    "facade",
    "hex",
    "acrobatics",
    "payback",
    "venoshock",
    "brine",
    "assurance",
    "retaliate",
    "round",
    "echoed-voice",
}


def normalize_move_power_name(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    normalized = normalized.replace("_", "-").replace(" ", "-").replace("'", "")
    normalized = re.sub(r"-{2,}", "-", normalized)
    return normalized.strip("-")


def resolve_effective_power(
    move_name: str,
    power: int | float | None,
    damage_class: str | None,
    level: int | None = None,
) -> tuple[float, str]:
    normalized_move = normalize_move_power_name(move_name)
    normalized_damage_class = str(damage_class or "").strip().lower()

    if power is not None:
        return float(power), "direct_power"
    if normalized_damage_class == "status":
        return 0.0, "status_no_damage"
    if normalized_move in FIXED_DAMAGE_LEVEL_MOVES:
        return float(level or 50), "fixed_damage_level"
    if normalized_move in FIXED_DAMAGE_CONSTANT_MOVES:
        return FIXED_DAMAGE_CONSTANT_MOVES[normalized_move], f"fixed_damage_{int(FIXED_DAMAGE_CONSTANT_MOVES[normalized_move])}"
    if normalized_move in VARIABLE_POWER_MOVES:
        return 60.0, "variable_power_proxy"
    return 60.0, "unknown_null_power_damage_proxy"
