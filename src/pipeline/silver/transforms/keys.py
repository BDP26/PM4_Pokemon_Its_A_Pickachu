"""Deterministic key helpers for the Silver layer."""

from __future__ import annotations

import hashlib
import re
from typing import Any


_NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")


def normalize_key_part(value: Any) -> str:
    text = str(value or "").strip().lower()
    text = text.replace("&", " and ")
    text = _NON_ALNUM_RE.sub("-", text)
    return re.sub(r"-+", "-", text).strip("-")


def stable_digest(*parts: Any, length: int = 12) -> str:
    raw = "|".join(normalize_key_part(part) for part in parts)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:length]


def make_boss_snapshot_id(game_version: str, boss_id: str, part: Any, heading: Any) -> str:
    return f"{normalize_key_part(game_version)}:{normalize_key_part(boss_id)}:{stable_digest(part, heading)}"


def make_team_id(
    team_role: str,
    game_version: str,
    source_name: str,
    *,
    source_team_id: str | None = None,
    variant: str | None = None,
) -> str:
    pieces = [team_role, game_version, source_name, source_team_id or "", variant or ""]
    return f"{normalize_key_part(team_role)}:{normalize_key_part(game_version)}:{stable_digest(*pieces)}"


def make_pokemon_instance_id(team_id: str, slot_index: int, species: str) -> str:
    return f"{normalize_key_part(team_id)}:m{slot_index}:{normalize_key_part(species)}:{stable_digest(team_id, slot_index, species)}"

