"""Bronze run-manifest utilities."""

from __future__ import annotations

from pathlib import Path

from src.pipeline.bronze.schemas.contracts import BronzeRunManifest
from src.pipeline.common.io import write_json
from src.pipeline.settings import BRONZE_DIR, get_bronze_subdirs


def write_bronze_run_manifest(
    *,
    started_at_utc: str,
    finished_at_utc: str,
    updated_sources: list[str],
    unchanged_sources: list[str],
    errors: list[str],
    bronze_dir: Path = BRONZE_DIR,
) -> Path:
    manifests_dir = get_bronze_subdirs(bronze_dir)["manifests"]
    manifests_dir.mkdir(parents=True, exist_ok=True)

    payload = BronzeRunManifest(
        run_started_at_utc=started_at_utc,
        run_finished_at_utc=finished_at_utc,
        updated_sources=sorted(updated_sources),
        unchanged_sources=sorted(unchanged_sources),
        errors=errors,
    ).as_dict()

    output_path = manifests_dir / "bronze_run_manifest.json"
    write_json(output_path, payload)
    return output_path


