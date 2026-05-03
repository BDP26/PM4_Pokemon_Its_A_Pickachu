from __future__ import annotations

import pytest

from src.pipeline.bronze.orchestration import fetch_sources


def test_build_location_pokemon_snapshot_persists_encounter_methods(monkeypatch) -> None:
    monkeypatch.setattr(fetch_sources, "_fetch_capture_rate", lambda *args, **kwargs: 255)
    monkeypatch.setattr(
        fetch_sources,
        "_get_pokebase_payload",
        lambda endpoint, resource_name_or_id=None: (
            {"areas": [{"name": "test-route-area"}]}
            if endpoint == "location" and resource_name_or_id == "test-route"
            else {
                "pokemon_encounters": [
                    {
                        "pokemon": {
                            "name": "pidgey",
                            "url": "pokebase://pokemon/16",
                        },
                        "version_details": [
                            {
                                "version": {"name": "red"},
                                "encounter_details": [
                                    {
                                        "min_level": 2,
                                        "max_level": 4,
                                        "chance": 45,
                                        "method": {
                                            "name": "walk",
                                            "url": "pokebase://encounter-method/1",
                                        },
                                    }
                                ],
                            }
                        ],
                    }
                ]
            }
            if endpoint == "location-area" and resource_name_or_id == "test-route-area"
            else {}
        ),
    )

    snapshot = fetch_sources._build_location_pokemon_snapshot(
        {"results": [{"name": "test-route"}]},
    )

    route_payload = snapshot["location_pokemon_map"]["test-route"]
    encounter = route_payload["by_version_encounters"]["red"][0]
    area_encounter = route_payload["areas_detail"]["test-route-area"]["by_version_encounters"]["red"][0]

    assert encounter["encounter_methods"] == ["walk"]
    assert encounter["encounter_method_urls"] == ["pokebase://encounter-method/1"]
    assert area_encounter["encounter_methods"] == ["walk"]


def test_build_location_pokemon_snapshot_falls_back_to_numeric_ids(monkeypatch) -> None:
    monkeypatch.setattr(fetch_sources, "_fetch_capture_rate", lambda *args, **kwargs: 255)

    def _stub(endpoint, resource_name_or_id=None):
        if endpoint == "location" and resource_name_or_id == "test-route":
            return {}
        if endpoint == "location" and resource_name_or_id == 100:
            return {"areas": [{"name": "test-route-area"}]}
        if endpoint == "location-area" and resource_name_or_id == "test-route-area":
            return {}
        if endpoint == "location-area" and resource_name_or_id == 200:
            return {
                "pokemon_encounters": [
                    {
                        "pokemon": {
                            "name": "pidgey",
                            "url": "pokebase://pokemon/16",
                        },
                        "version_details": [
                            {
                                "version": {"name": "red"},
                                "encounter_details": [],
                            }
                        ],
                    }
                ]
            }
        return {}

    monkeypatch.setattr(fetch_sources, "_get_pokebase_payload", _stub)

    snapshot = fetch_sources._build_location_pokemon_snapshot(
        {
            "results": [{"name": "test-route", "url": "https://pokeapi.co/api/v2/location/100/"}],
            "location_area_results": [{"name": "test-route-area", "url": "https://pokeapi.co/api/v2/location-area/200/"}],
        },
    )

    assert "test-route" in snapshot["location_pokemon_map"]
    assert snapshot["diagnostics"]["location_errors"] == []
    assert snapshot["diagnostics"]["area_errors"] == []


def test_validate_location_snapshot_rejects_empty_map() -> None:
    with pytest.raises(ValueError, match="location_pokemon_map is empty"):
        fetch_sources._validate_location_snapshot({"location_pokemon_map": {}, "diagnostics": {}})


def test_should_write_source_with_force_overrides_signature() -> None:
    source_state = {"pokeapi:location_index": {"signature": "same-signature"}}
    assert fetch_sources._should_write_source_with_force(
        source_state,
        "pokeapi:location_index",
        "same-signature",
        force_write=True,
    )


