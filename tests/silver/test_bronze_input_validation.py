from __future__ import annotations

from pathlib import Path

import pytest

from src.pipeline.common.io import write_json
from src.pipeline.silver.orchestration.build_silver import _validate_bronze_inputs_or_raise


def _prepare_common_layout(base: Path) -> tuple[Path, Path, Path, Path]:
    bronze_dir = base / "bronze"
    location_index_path = bronze_dir / "pokeapi" / "location_index.json"
    snapshot_path = bronze_dir / "pokeapi" / "location_pokemon_snapshot.json"
    bulbapedia_dir = bronze_dir / "bulbapedia"
    kaggle_csv_path = bronze_dir / "kagglehub" / "gym_leaders_elite_four.csv"

    write_json(location_index_path, {"results": [{"name": "kanto-route-1", "url": "https://pokeapi.co/api/v2/location/1/"}]})
    write_json(snapshot_path, {"location_pokemon_map": {"kanto-route-1": {"all": ["pidgey"]}}})
    write_json(bulbapedia_dir / "red.json", {"game_key": "red", "parts": []})
    kaggle_csv_path.parent.mkdir(parents=True, exist_ok=True)
    kaggle_csv_path.write_text("a,b\n1,2\n", encoding="utf-8")

    return bronze_dir, location_index_path, bulbapedia_dir, kaggle_csv_path


def test_validate_bronze_inputs_rejects_empty_location_snapshot(tmp_path: Path) -> None:
    bronze_dir, location_index_path, bulbapedia_dir, kaggle_csv_path = _prepare_common_layout(tmp_path)
    write_json(bronze_dir / "pokeapi" / "location_pokemon_snapshot.json", {"location_pokemon_map": {}})

    with pytest.raises(ValueError, match="location_pokemon_map has no entries"):
        _validate_bronze_inputs_or_raise(
            bronze_dir=bronze_dir,
            location_index_path=location_index_path,
            bulbapedia_dir=bulbapedia_dir,
            kaggle_csv_path=kaggle_csv_path,
        )


def test_validate_bronze_inputs_accepts_minimal_valid_layout(tmp_path: Path) -> None:
    bronze_dir, location_index_path, bulbapedia_dir, kaggle_csv_path = _prepare_common_layout(tmp_path)

    _validate_bronze_inputs_or_raise(
        bronze_dir=bronze_dir,
        location_index_path=location_index_path,
        bulbapedia_dir=bulbapedia_dir,
        kaggle_csv_path=kaggle_csv_path,
    )

