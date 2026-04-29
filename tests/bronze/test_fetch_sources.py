from __future__ import annotations

from src.pipeline.bronze.orchestration import fetch_sources


class _FakeResponse:
    def __init__(self, payload: dict, status_code: int = 200) -> None:
        self._payload = payload
        self.status_code = status_code

    def json(self) -> dict:
        return self._payload


class _FakeSession:
    def __init__(self, responses: dict[str, dict]) -> None:
        self._responses = responses

    def get(self, url: str, **_: object) -> _FakeResponse:
        payload = self._responses.get(url)
        if payload is None:
            return _FakeResponse({}, status_code=404)
        return _FakeResponse(payload, status_code=200)


def test_build_location_pokemon_snapshot_persists_encounter_methods(monkeypatch) -> None:
    monkeypatch.setattr(fetch_sources, "_fetch_capture_rate", lambda *args, **kwargs: 255)

    base = fetch_sources.POKEAPI
    session = _FakeSession(
        {
            f"{base}/location/test-route": {
                "areas": [{"name": "test-route-area"}],
            },
            f"{base}/location-area/test-route-area": {
                "pokemon_encounters": [
                    {
                        "pokemon": {
                            "name": "pidgey",
                            "url": f"{base}/pokemon/16/",
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
                                            "url": f"{base}/encounter-method/1/",
                                        },
                                    }
                                ],
                            }
                        ],
                    }
                ]
            },
        }
    )

    snapshot = fetch_sources._build_location_pokemon_snapshot(
        session,
        {"results": [{"name": "test-route"}]},
    )

    route_payload = snapshot["location_pokemon_map"]["test-route"]
    encounter = route_payload["by_version_encounters"]["red"][0]
    area_encounter = route_payload["areas_detail"]["test-route-area"]["by_version_encounters"]["red"][0]

    assert encounter["encounter_methods"] == ["walk"]
    assert encounter["encounter_method_urls"] == [f"{base}/encounter-method/1/"]
    assert area_encounter["encounter_methods"] == ["walk"]
