from __future__ import annotations

import re
from typing import Any

from src.pipeline.common.cast import is_nullish


def normalize_text(value: Any, *, lower: bool = True) -> str:
    if is_nullish(value):
        return ""
    text = str(value).strip()
    return text.lower() if lower else text


def normalize_optional_text(value: Any, *, lower: bool = True) -> str | None:
    normalized = normalize_text(value, lower=lower)
    return normalized or None


def normalize_slug(value: Any) -> str:
    text = normalize_text(value)
    slug = re.sub(r"[^a-z0-9]+", "-", text).strip("-")
    return re.sub(r"-+", "-", slug)
