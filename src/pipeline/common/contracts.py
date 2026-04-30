from __future__ import annotations

from pathlib import Path


def format_contract_error(
    *,
    prefix: str,
    code: str,
    message: str,
    dataset: str | None = None,
    path: Path | None = None,
) -> str:
    parts: list[str] = [f"[{prefix}] {code}"]
    if dataset:
        parts.append(f"dataset={dataset}")
    if path is not None:
        parts.append(f"path={path}")
    parts.append(f"action=\"{message}\"")
    return " ".join(parts)
