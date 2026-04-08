from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from src.pipeline.common.io import write_json
from src.pipeline.silver.config.game_config import get_games_config
from src.pipeline.settings import BRONZE_DIR, get_bronze_subdirs


def write_bronze_config_snapshot(output_dir: Path = BRONZE_DIR) -> dict[str, Path]:
    """Persist the effective Bronze configuration as reproducible raw snapshots."""
    bronze_subdirs = get_bronze_subdirs(output_dir)
    config_dir = bronze_subdirs["config"]
    overrides_dir = config_dir / "overrides"
    config_dir.mkdir(parents=True, exist_ok=True)
    overrides_dir.mkdir(parents=True, exist_ok=True)

    games_config = get_games_config()
    games_config_path = config_dir / "games_config.json"
    write_json(games_config_path, games_config)

    override_files = sorted(
        str(path.relative_to(config_dir))
        for path in overrides_dir.rglob("*")
        if path.is_file()
    )

    manifest = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source": "src.pipeline.silver.game_config.get_games_config",
        "output_dir": str(config_dir.relative_to(output_dir)),
        "files": {
            "games_config": games_config_path.name,
        },
        "override_files": override_files,
        "game_count": len(games_config),
    }
    write_json(config_dir / "manifest.json", manifest)

    return {
        "config_dir": config_dir,
        "games_config": games_config_path,
        "manifest": config_dir / "manifest.json",
        "overrides_dir": overrides_dir,
    }


