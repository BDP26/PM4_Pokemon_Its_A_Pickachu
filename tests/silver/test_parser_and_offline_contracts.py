from __future__ import annotations

from pathlib import Path

import pytest

from src.pipeline.common.io import write_json
from src.pipeline.silver.enrichment.location_pokemon_enrichment import get_location_area_and_pokemon_maps
from src.pipeline.silver.inputs.parser import enforce_parser_coverage, extract_game_data
from src.pipeline.silver.inputs.location_mapper import LocationMapper


def test_silver_location_enrichment_uses_bronze_snapshot_only(tmp_path: Path) -> None:
    bronze_dir = tmp_path / "bronze"
    silver_dir = tmp_path / "silver"
    write_json(
        bronze_dir / "pokeapi" / "location_pokemon_snapshot.json",
        {
            "location_pokemon_map": {
                "kanto-route-2": {
                    "all": ["pikachu"],
                    "by_version": {"red-blue": ["pikachu"]},
                    "all_encounters": [{"species": "pikachu", "level_max": 5}],
                    "by_version_encounters": {"red-blue": [{"species": "pikachu", "level_max": 5}]},
                    "areas": ["kanto-route-2-area"],
                    "areas_detail": {},
                }
            }
        },
    )

    area_map, pokemon_map = get_location_area_and_pokemon_maps(
        ["kanto-route-2"],
        allowed_versions={"red"},
        silver_dir=silver_dir,
        bronze_dir=bronze_dir,
    )
    assert area_map["kanto-route-2"] == ["kanto-route-2-area"]
    assert pokemon_map["kanto-route-2"]["by_version"]["red-blue"] == ["pikachu"]


def test_parser_coverage_gate_enforced() -> None:
    with pytest.raises(ValueError, match="coverage gate failed"):
        enforce_parser_coverage(
            game_key="red",
            records=[{"boss_name": "brock"}],
            expected_bosses=["brock", "misty", "surge"],
            min_coverage=0.8,
        )


def test_parser_regression_with_frozen_html_snapshot() -> None:
    mapper = LocationMapper({"results": [{"name": "kanto-route-2"}], "location_area_results": [], "location_area_parent_map": {}})
    payload = {
        "game_key": "red",
        "route_prefix": "kanto-route",
        "bosses": ["Brock"],
        "parts": [
            {
                "part": 1,
                "html": '<div class="mw-parser-output"><a title="Route 2"></a><h2>Brock</h2></div>',
            }
        ],
    }
    records = extract_game_data(payload, mapper)
    assert len(records) == 1
    assert records[0]["boss_name"] == "Brock"
    assert "kanto-route-2" in records[0]["reachable_locations"]
