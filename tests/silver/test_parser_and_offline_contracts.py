from __future__ import annotations

from pathlib import Path

import pytest

from src.pipeline.common.io import write_json
from src.pipeline.silver.enrichment.location_pokemon_enrichment import get_location_area_and_pokemon_maps
from src.pipeline.silver.config.boss_config import BOSS_ALIASES
from src.pipeline.silver.config.game_config import BASE_GAME_GROUPS
from src.pipeline.silver.inputs.parser import enforce_parser_coverage, extract_game_data, normalize_text
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


def test_silver_location_enrichment_rejects_empty_bronze_snapshot(tmp_path: Path) -> None:
    bronze_dir = tmp_path / "bronze"
    silver_dir = tmp_path / "silver"
    write_json(
        bronze_dir / "pokeapi" / "location_pokemon_snapshot.json",
        {"location_pokemon_map": {}},
    )

    with pytest.raises(ValueError, match="location_pokemon_map has no entries"):
        get_location_area_and_pokemon_maps(
            ["kanto-route-2"],
            allowed_versions={"red"},
            silver_dir=silver_dir,
            bronze_dir=bronze_dir,
        )


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


def test_normalize_text_strips_edit_markers() -> None:
    assert normalize_text("Lostlorn Forest[edit]") == "Lostlorn Forest"
    assert normalize_text("Lostlorn Forest [ edit ]") == "Lostlorn Forest"
    assert normalize_text("Lostlorn Forest [edit source]") == "Lostlorn Forest"


def test_parser_maps_kaggle_blue_variant_heading_to_blue() -> None:
    mapper = LocationMapper({"results": [], "location_area_results": [], "location_area_parent_map": {}})
    payload = {
        "game_key": "red",
        "route_prefix": "kanto-route",
        "bosses": ["Blue"],
        "parts": [
            {
                "part": 1,
                "html": '<div class="mw-parser-output"><h2>Champion Blue Bulbasaur</h2></div>',
            }
        ],
    }

    records = extract_game_data(payload, mapper)

    assert len(records) == 1
    assert records[0]["boss_name"] == "Blue"


def test_parser_maps_striaton_heading_to_configured_branching_bosses() -> None:
    mapper = LocationMapper({"results": [], "location_area_results": [], "location_area_parent_map": {}})
    payload = {
        "game_key": "black",
        "route_prefix": "unova-route",
        "bosses": ["Chili", "Cilan", "Cress"],
        "parts": [
            {
                "part": 1,
                "html": '<div class="mw-parser-output"><h2>Chili</h2></div>',
            }
        ],
    }

    records = extract_game_data(payload, mapper)

    assert [record["boss_name"] for record in records] == ["Chili", "Cilan", "Cress"]
    assert len({record["location_count"] for record in records}) == 1


def test_parser_keeps_latest_progress_for_repeated_boss_heading() -> None:
    mapper = LocationMapper(
        {
            "results": [
                {"name": "sinnoh-route-201"},
                {"name": "sinnoh-route-202"},
                {"name": "sinnoh-route-203"},
            ],
            "location_area_results": [],
            "location_area_parent_map": {},
        }
    )
    payload = {
        "game_key": "diamond",
        "route_prefix": "sinnoh-route",
        "bosses": ["Roark", "Gardenia", "Maylene", "Crasher Wake", "Fantina"],
        "parts": [
            {
                "part": 1,
                "html": (
                    '<div class="mw-parser-output">'
                    '<a title="Route 201"></a>'
                    '<h2>Fantina</h2>'
                    '</div>'
                ),
            },
            {
                "part": 2,
                "html": (
                    '<div class="mw-parser-output">'
                    '<a title="Route 202"></a>'
                    '<a title="Route 203"></a>'
                    '<h2>Fantina</h2>'
                    '</div>'
                ),
            },
        ],
    }

    records = extract_game_data(payload, mapper)
    fantina = next(record for record in records if record["boss_name"] == "Fantina")
    assert fantina["location_count"] == 3
    assert fantina["reachable_locations"] == ["sinnoh-route-201", "sinnoh-route-202", "sinnoh-route-203"]


def test_parser_avoids_early_non_battle_person_alias_match() -> None:
    mapper = LocationMapper(
        {
            "results": [{"name": "kanto-route-1"}, {"name": "viridian-city"}],
            "location_area_results": [],
            "location_area_parent_map": {},
        }
    )
    payload = {
        "game_key": "red",
        "route_prefix": "kanto-route",
        "bosses": ["Giovanni"],
        "parts": [
            {
                "part": 1,
                "html": '<div class="mw-parser-output"><a title="Route 1"></a><h2>Team Rocket and Giovanni</h2></div>',
            },
            {
                "part": 2,
                "html": '<div class="mw-parser-output"><a title="Viridian City"></a><h2>Viridian Gym</h2></div>',
            },
        ],
    }

    records = extract_game_data(payload, mapper)
    assert len(records) == 1
    assert records[0]["boss_name"] == "Giovanni"
    assert records[0]["location_count"] == 2


def test_parser_prefers_battle_heading_over_meet_heading_for_person_alias() -> None:
    mapper = LocationMapper(
        {
            "results": [{"name": "kalos-route-5"}, {"name": "shalour-city"}],
            "location_area_results": [],
            "location_area_parent_map": {},
        }
    )
    payload = {
        "game_key": "x",
        "route_prefix": "kalos-route",
        "bosses": ["Korrina"],
        "parts": [
            {
                "part": 1,
                "html": '<div class="mw-parser-output"><a title="Route 5"></a><h2>Meet Korrina</h2></div>',
            },
            {
                "part": 2,
                "html": '<div class="mw-parser-output"><a title="Shalour City"></a><h2>Shalour Gym</h2></div>',
            },
        ],
    }

    records = extract_game_data(payload, mapper)
    assert len(records) == 1
    assert records[0]["boss_name"] == "Korrina"
    assert records[0]["location_count"] == 2


def test_parser_ignores_late_city_alias_heading_when_earlier_gym_heading_exists() -> None:
    mapper = LocationMapper(
        {
            "results": [
                {"name": "johto-route-36"},
                {"name": "johto-route-37"},
            ],
            "location_area_results": [],
            "location_area_parent_map": {},
        }
    )
    payload = {
        "game_key": "gold",
        "route_prefix": "johto-route",
        "bosses": ["Morty"],
        "parts": [
            {
                "part": 1,
                "html": '<div class="mw-parser-output"><a title="Route 36"></a><h2>Ecruteak Gym</h2></div>',
            },
            {
                "part": 2,
                "html": '<div class="mw-parser-output"><a title="Route 37"></a><h2>Ecruteak City</h2></div>',
            },
        ],
    }

    records = extract_game_data(payload, mapper)

    assert len(records) == 1
    assert records[0]["boss_name"] == "Morty"
    assert records[0]["heading"] == "Ecruteak Gym"
    assert records[0]["location_count"] == 1
    assert records[0]["reachable_locations"] == ["johto-route-36"]


def test_parser_keeps_city_alias_coverage_when_no_gym_heading_exists() -> None:
    mapper = LocationMapper(
        {
            "results": [{"name": "kalos-route-13"}],
            "location_area_results": [],
            "location_area_parent_map": {},
        }
    )
    payload = {
        "game_key": "x",
        "route_prefix": "kalos-route",
        "bosses": ["Clemont"],
        "parts": [
            {
                "part": 1,
                "html": '<div class="mw-parser-output"><a title="Route 13"></a><h2>Lumiose City</h2></div>',
            }
        ],
    }

    records = extract_game_data(payload, mapper)

    assert len(records) == 1
    assert records[0]["boss_name"] == "Clemont"
    assert records[0]["heading"] == "Lumiose City"
    assert records[0]["location_count"] == 1


def test_parser_endgame_fallbacks_include_unova_elite_four_and_alder() -> None:
    mapper = LocationMapper({"results": [], "location_area_results": [], "location_area_parent_map": {}})
    payload = {
        "game_key": "black",
        "route_prefix": "unova-route",
        "bosses": ["Shauntal", "Grimsley", "Caitlin", "Marshal", "Alder"],
        "parts": [
            {
                "part": 1,
                "html": '<div class="mw-parser-output"><h2>Pokémon League</h2></div>',
            }
        ],
    }

    records = extract_game_data(payload, mapper)

    assert [record["boss_name"] for record in records] == ["Shauntal", "Grimsley", "Caitlin", "Marshal", "Alder"]


def test_unova_kaggle_facing_configs_exclude_progression_only_story_bosses() -> None:
    unova_group = next(group for group in BASE_GAME_GROUPS if group["versions"] == ["black", "white"])
    assert set(BOSS_ALIASES["black"]) == {
        "Chili", "Cilan", "Cress", "Lenora", "Burgh", "Elesa", "Clay", "Skyla",
        "Brycen", "Drayden", "Shauntal", "Grimsley", "Caitlin", "Marshal", "Alder",
    }
    assert set(BOSS_ALIASES["white"]) == {
        "Chili", "Cilan", "Cress", "Lenora", "Burgh", "Elesa", "Clay", "Skyla",
        "Brycen", "Iris", "Shauntal", "Grimsley", "Caitlin", "Marshal", "Alder",
    }
    assert unova_group["bosses_by_version"]["black"] == [
        "Chili", "Cilan", "Cress", "Lenora", "Burgh", "Elesa", "Clay", "Skyla", "Brycen", "Drayden",
        "Shauntal", "Grimsley", "Caitlin", "Marshal", "Alder",
    ]
    assert unova_group["bosses_by_version"]["white"] == [
        "Chili", "Cilan", "Cress", "Lenora", "Burgh", "Elesa", "Clay", "Skyla", "Brycen", "Iris",
        "Shauntal", "Grimsley", "Caitlin", "Marshal", "Alder",
    ]
