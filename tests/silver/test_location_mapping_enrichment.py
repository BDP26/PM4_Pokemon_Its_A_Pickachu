from __future__ import annotations

from copy import deepcopy

from src.pipeline.silver.enrichment.location_pokemon_enrichment import (
    _pick_encounters_for_version,
    _pick_species_for_version,
    enrich_records_with_location_pokemon,
)
from src.pipeline.silver.inputs.location_mapper import LocationMapper


def _build_location_index() -> dict:
    return {
        "results": [
            {"name": "kanto-route-2"},
            {"name": "viridian-forest"},
            {"name": "kanto-route-5"},
        ],
        "location_area_results": [
            {"name": "viridian-forest-area"},
            {"name": "kanto-route-2-area"},
        ],
        "location_area_parent_map": {
            "viridian-forest-area": "viridian-forest",
            "kanto-route-2-area": "kanto-route-2",
        },
    }


def test_grouped_version_matching_prefers_group_before_all() -> None:
    species = {
        "red-blue": ["pikachu"],
        "all": ["rattata"],
    }
    encounters = {
        "red-blue": [{"species": "pikachu"}],
        "all": [{"species": "rattata"}],
    }

    assert _pick_species_for_version(species, "red") == ["pikachu"]
    assert _pick_encounters_for_version(encounters, "blue") == [{"species": "pikachu"}]


def test_mapper_handles_generic_heading_and_blacklist_edge_case() -> None:
    mapper = LocationMapper(_build_location_index())

    # generic cave is intentionally rejected
    assert mapper.resolve("Cave", "kanto-route") is None

    # entrances should still resolve when they refer to a real catch area
    resolved = mapper.resolve("Viridian Forest Entrance", "kanto-route")
    assert resolved == "viridian-forest"


def test_mapper_parent_fallback_for_location_area_slug() -> None:
    mapper = LocationMapper(_build_location_index())

    resolution = mapper.resolve_with_kind("Viridian Forest Area", "kanto-route")
    assert resolution.kind == "parent_fallback"
    assert resolution.slug == "viridian-forest"


def test_enrichment_preserves_area_level_and_rollup() -> None:
    records = [
        {
            "boss_id": "red:brock",
            "version": "red",
            "reachable_locations": ["kanto-route-2"],
        }
    ]
    location_pokemon_map = {
        "kanto-route-2": {
            "all": ["caterpie", "weedle"],
            "by_version": {"red-blue": ["caterpie"]},
            "all_encounters": [{"species": "weedle", "level_max": 5}],
            "by_version_encounters": {"red-blue": [{"species": "caterpie", "level_max": 6}]},
            "areas": ["kanto-route-2-area"],
            "areas_detail": {
                "kanto-route-2-area": {
                    "all": ["weedle"],
                    "by_version": {"red-blue": ["caterpie"]},
                    "all_encounters": [{"species": "weedle", "level_max": 5}],
                    "by_version_encounters": {"red-blue": [{"species": "caterpie", "level_max": 6}]},
                }
            },
        }
    }

    enrich_records_with_location_pokemon(records, location_pokemon_map)
    enriched = records[0]

    assert enriched["reachable_location_pokemon"]["kanto-route-2"] == ["caterpie"]
    assert enriched["reachable_location_encounters"]["kanto-route-2"] == [{"species": "caterpie", "level_max": 6}]
    assert enriched["reachable_location_area_encounters"]["kanto-route-2"]["kanto-route-2-area"] == [
        {"species": "caterpie", "level_max": 6}
    ]


def test_enrichment_is_deterministic() -> None:
    base_records = [
        {
            "boss_id": "red:misty",
            "version": "red",
            "reachable_locations": ["kanto-route-2", "viridian-forest"],
        }
    ]
    location_pokemon_map = {
        "kanto-route-2": {
            "all": ["caterpie"],
            "by_version": {},
            "all_encounters": [{"species": "caterpie", "level_max": 4}],
            "by_version_encounters": {},
            "areas": [],
            "areas_detail": {},
        },
        "viridian-forest": {
            "all": ["pikachu"],
            "by_version": {"red-blue": ["pikachu"]},
            "all_encounters": [{"species": "pikachu", "level_max": 5}],
            "by_version_encounters": {"red-blue": [{"species": "pikachu", "level_max": 5}]},
            "areas": [],
            "areas_detail": {},
        },
    }

    r1 = deepcopy(base_records)
    r2 = deepcopy(base_records)
    enrich_records_with_location_pokemon(r1, location_pokemon_map)
    enrich_records_with_location_pokemon(r2, location_pokemon_map)

    assert r1 == r2
