from __future__ import annotations

from pathlib import Path

from src.pipeline.common.io import read_json
from src.pipeline.gold.orchestration.build_gold import _REQUIRED_MANIFEST_DATASET_FILES
from src.pipeline.silver.reporting.silver_manifest import create_silver_manifest


def test_gold_required_manifest_keys_match_declared_strict_contract(tmp_path: Path) -> None:
    create_silver_manifest(tmp_path)
    manifest = read_json(tmp_path / "manifest.json")
    declared = set(
        manifest["contracts"]["gold_strict"]["required_dataset_keys"]
    ) - {"boss_records"}
    assert declared == set(_REQUIRED_MANIFEST_DATASET_FILES)
