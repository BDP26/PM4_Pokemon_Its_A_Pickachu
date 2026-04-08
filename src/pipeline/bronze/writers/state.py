"""State helpers for Bronze source-level incremental processing."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.pipeline.common.io import read_json, write_json
from src.pipeline.settings import BRONZE_DIR, get_bronze_subdirs


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def stable_signature(payload: Any) -> str:
    normalized = json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def bronze_state_path(bronze_dir: Path = BRONZE_DIR) -> Path:
    return get_bronze_subdirs(bronze_dir)["state"] / "source_state.json"


def load_source_state(bronze_dir: Path = BRONZE_DIR) -> dict[str, Any]:
    path = bronze_state_path(bronze_dir)
    if not path.exists():
        return {}
    loaded = read_json(path)
    return loaded if isinstance(loaded, dict) else {}


def save_source_state(state: dict[str, Any], bronze_dir: Path = BRONZE_DIR) -> None:
    path = bronze_state_path(bronze_dir)
    write_json(path, state)

