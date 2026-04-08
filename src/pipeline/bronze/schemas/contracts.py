"""v2 contracts for Bronze source metadata/state."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


@dataclass(frozen=True)
class BronzeSourceState:
    source: str
    signature: str
    updated_at_utc: str
    output_paths: list[str]

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class BronzeRunManifest:
    run_started_at_utc: str
    run_finished_at_utc: str
    updated_sources: list[str]
    unchanged_sources: list[str]
    errors: list[str]

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)

