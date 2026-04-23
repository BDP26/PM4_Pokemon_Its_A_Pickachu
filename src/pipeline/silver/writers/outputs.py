"""Silver-layer output writers, validation, and incremental state helpers."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict
from pathlib import Path
from typing import Any

import pandas as pd

from src.pipeline.common.io import read_json, write_json, write_jsonl, write_parquet
from src.pipeline.silver.schemas.contracts import (
    validate_boss_snapshots,
    validate_move_payloads,
    validate_team_payloads,
)


def _stable_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, ensure_ascii=False, default=str)


def fingerprint_path(path: Path) -> str:
    hasher = hashlib.sha256()
    if path.is_dir():
        for file_path in sorted(p for p in path.rglob("*") if p.is_file()):
            hasher.update(file_path.relative_to(path).as_posix().encode("utf-8"))
            hasher.update(file_path.read_bytes())
    else:
        hasher.update(path.read_bytes())
    return hasher.hexdigest()


def build_input_signature(inputs: dict[str, Any]) -> str:
    return hashlib.sha256(_stable_json(inputs).encode("utf-8")).hexdigest()


def fingerprint_python_files(paths: list[Path]) -> str:
    hasher = hashlib.sha256()
    for base in paths:
        if base.is_file() and base.suffix == ".py":
            hasher.update(base.as_posix().encode("utf-8"))
            hasher.update(base.read_bytes())
            continue
        if not base.exists() or not base.is_dir():
            continue
        for file_path in sorted(p for p in base.rglob("*.py") if p.is_file()):
            hasher.update(file_path.relative_to(base).as_posix().encode("utf-8"))
            hasher.update(file_path.read_bytes())
    return hasher.hexdigest()


def load_state(state_path: Path) -> dict[str, Any]:
    if not state_path.exists():
        return {}
    return read_json(state_path) if state_path.exists() else {}


def save_state(state_path: Path, payload: dict[str, Any]) -> None:
    write_json(state_path, payload)


def write_validated_boss_snapshots(path: Path, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    validated = validate_boss_snapshots(records)
    write_jsonl(path, validated)
    return validated


def write_validated_teams(path: Path, records: list[dict[str, Any]], *, partition_cols: list[str] | tuple[str, ...] | None = None) -> list[dict[str, Any]]:
    validated = validate_team_payloads(records)
    write_parquet(path, pd.DataFrame(validated), partition_cols=partition_cols)
    return validated


def write_validated_move_data(path: Path, records: dict[str, dict[str, Any]] | list[dict[str, Any]]) -> list[dict[str, Any]]:
    payload = list(records.values()) if isinstance(records, dict) else list(records)
    validated = validate_move_payloads(payload)
    write_parquet(path, pd.DataFrame(validated))
    return validated

def write_simulation_run_metadata(
    output_dir: Path,
    *,
    engine: str,
    config: Any,
    extra: dict[str, Any] | None = None,
) -> None:
    payload: dict[str, Any] = {
        "engine": engine,
        "config": asdict(config) if hasattr(config, "__dataclass_fields__") else dict(config),
    }
    if extra:
        payload["extra"] = extra
    write_json(output_dir / "run_metadata.json", payload)
