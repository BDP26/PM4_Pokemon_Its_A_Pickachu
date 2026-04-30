from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

from src.pipeline.common.io import read_json


def load_manifest(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(path)
    payload = read_json(path)
    if not isinstance(payload, dict):
        raise ValueError("manifest must be a JSON object")
    return cast(dict[str, Any], payload)


def manifest_datasets(manifest: dict[str, Any]) -> dict[str, Any]:
    datasets = manifest.get("datasets")
    if not isinstance(datasets, dict):
        raise ValueError("manifest missing top-level datasets object")
    return cast(dict[str, Any], datasets)


@dataclass(frozen=True)
class ManifestResolutionError(ValueError):
    code: str
    dataset: str
    path: Path | None = None
    detail: str | None = None


def resolve_manifest_dataset_path(
    *,
    base_dir: Path,
    manifest: dict[str, Any],
    dataset_key: str,
    strict_sharded: bool = False,
) -> Path | list[Path]:
    datasets = manifest_datasets(manifest)
    dataset_entry = datasets.get(dataset_key)
    if not isinstance(dataset_entry, dict):
        raise ManifestResolutionError("missing_dataset_entry", dataset_key)

    files_entry = dataset_entry.get("files")
    if isinstance(files_entry, list) and files_entry:
        resolved_files: list[Path] = []
        for index, rel in enumerate(cast(list[Any], files_entry)):
            if not isinstance(rel, str) or not rel.strip():
                raise ManifestResolutionError(
                    "invalid_dataset_files_entry",
                    dataset_key,
                    detail=f"files[{index}] must be a non-empty string path",
                )
            candidate = base_dir / rel
            if strict_sharded and (not candidate.exists() or not candidate.is_file()):
                raise ManifestResolutionError("missing_dataset_file", dataset_key, path=candidate)
            if not strict_sharded and not candidate.exists():
                raise ManifestResolutionError("missing_dataset_file", dataset_key, path=candidate)
            resolved_files.append(candidate)
        return sorted(set(resolved_files))

    if strict_sharded:
        raise ManifestResolutionError("missing_dataset_files", dataset_key)

    rel_path = dataset_entry.get("file")
    if not isinstance(rel_path, str) or not rel_path.strip():
        raise ManifestResolutionError("missing_dataset_file_path", dataset_key)
    path = base_dir / cast(str, rel_path)
    if not path.exists():
        raise ManifestResolutionError("missing_dataset_file", dataset_key, path=path)
    return path
