from __future__ import annotations

from typing import Any


def clamp_progression_depth(value: Any) -> float:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        numeric = 0.0
    return max(0.0, min(1.0, numeric))


def dynamic_level_gap_limits(
    progression_depth: Any,
    *,
    base_max_overlevel: int,
    base_max_underlevel: int,
) -> tuple[int, int]:
    depth = clamp_progression_depth(progression_depth)
    max_overlevel = max(1, int(round(base_max_overlevel * (0.5 + (0.5 * depth)))))
    max_underlevel = max(2, int(round(base_max_underlevel * (0.35 + (0.65 * depth)))))
    return max_overlevel, max_underlevel
