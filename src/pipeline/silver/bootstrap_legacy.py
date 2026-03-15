import shutil
from pathlib import Path

from src.pipeline.settings import LEGACY_SILVER_DIR, SILVER_DIR, ensure_medallion_dirs


def bootstrap_legacy_silver(legacy_dir: Path = LEGACY_SILVER_DIR, silver_dir: Path = SILVER_DIR) -> None:
    ensure_medallion_dirs()
    silver_dir.mkdir(parents=True, exist_ok=True)

    if not legacy_dir.exists():
        raise FileNotFoundError(f"Legacy folder not found: {legacy_dir}")

    copied = 0
    for path in sorted(legacy_dir.glob("*")):
        if path.suffix not in {".json", ".jsonl"}:
            continue
        destination = silver_dir / path.name
        shutil.copy2(path, destination)
        copied += 1

    print(f"[silver-bootstrap] copied {copied} files from {legacy_dir} to {silver_dir}")

