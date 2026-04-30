from __future__ import annotations

import math
from typing import Any


def is_nullish(value: Any, *, include_pandas_na: bool = True) -> bool:
    if value is None:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    if include_pandas_na:
        try:
            import pandas as pd  # local import to avoid hard dependency at import time

            if pd.isna(value):
                return True
        except Exception:
            pass
    return False


def to_int(value: Any, *, default: int | None = None) -> int | None:
    if is_nullish(value):
        return default
    try:
        return int(value)
    except (TypeError, ValueError, OverflowError):
        return default


def to_float(value: Any, *, default: float | None = None, finite_only: bool = False) -> float | None:
    if is_nullish(value):
        return default
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return default
    if finite_only and (math.isnan(parsed) or math.isinf(parsed)):
        return default
    return parsed


def to_bool(
    value: Any,
    *,
    default: bool = False,
    truthy: set[str] | None = None,
    falsy: set[str] | None = None,
) -> bool:
    if is_nullish(value):
        return default
    if isinstance(value, str):
        normalized = value.strip().lower()
        if not normalized:
            return default
        truthy_values = truthy or {"true", "1", "yes", "y", "on"}
        falsy_values = falsy or {"false", "0", "no", "n", "off"}
        if normalized in truthy_values:
            return True
        if normalized in falsy_values:
            return False
    return bool(value)


def to_list(value: Any, *, drop_nullish: bool = False) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        values = list(value)
    elif hasattr(value, "tolist"):
        converted = value.tolist()
        if isinstance(converted, list):
            values = converted
        elif converted is None:
            values = []
        else:
            values = [converted]
    else:
        values = [value]

    if not drop_nullish:
        return values
    return [item for item in values if not is_nullish(item)]
