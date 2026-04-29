from __future__ import annotations

import requests

from src.pipeline.silver.inputs.connectors import pokeapi_evolution


class _FailingResponseSession:
    def get(self, url: str, timeout: int = 0) -> None:
        raise requests.RequestException(f"offline for {url} timeout={timeout}")


def test_get_json_returns_empty_on_request_failure(monkeypatch) -> None:
    pokeapi_evolution._get_json.cache_clear()
    monkeypatch.setattr(pokeapi_evolution, "_SESSION", _FailingResponseSession())

    payload = pokeapi_evolution._get_json("https://pokeapi.co/api/v2/pokemon-species/audino")

    assert payload == {}


def test_get_species_evolution_rules_returns_empty_when_species_lookup_fails(monkeypatch) -> None:
    pokeapi_evolution._get_json.cache_clear()
    monkeypatch.setattr(pokeapi_evolution, "_SESSION", _FailingResponseSession())

    rules = pokeapi_evolution.get_species_evolution_rules("audino")

    assert rules == {}


def test_get_json_uses_readonly_pokebase_cache_before_live_lookup(monkeypatch) -> None:
    def _unexpected_get(url: str, timeout: int = 0) -> None:
        raise AssertionError(f"network should not be called for {url}")

    pokeapi_evolution._get_json.cache_clear()
    monkeypatch.setattr(pokeapi_evolution, "_POKEBASE_CACHE_PATH", pokeapi_evolution.Path("/tmp/fake-home/.cache/pokebase/api.cache"))
    monkeypatch.setattr(
        pokeapi_evolution,
        "_cached_pokebase_payload",
        lambda endpoint, resource_name_or_id=None: {
            "name": "audino",
            "evolution_chain": {"url": "https://pokeapi.co/api/v2/evolution-chain/133/"}
        }
        if endpoint == "pokemon-species" and resource_name_or_id == "audino"
        else {},
    )
    monkeypatch.setattr(pokeapi_evolution, "_SESSION", type("OfflineSession", (), {"get": _unexpected_get})())

    payload = pokeapi_evolution._get_json("https://pokeapi.co/api/v2/pokemon-species/audino")

    assert payload["name"] == "audino"
    assert payload["evolution_chain"]["url"].endswith("/133/")


def test_get_json_returns_empty_without_http_fallback_when_cache_missing(monkeypatch) -> None:
    def _unexpected_get(url: str, timeout: int = 0) -> None:
        raise AssertionError(f"network should not be called for {url}")

    pokeapi_evolution._get_json.cache_clear()
    monkeypatch.setattr(pokeapi_evolution, "_cached_pokebase_payload", lambda endpoint, resource_name_or_id=None: {})
    monkeypatch.setattr(pokeapi_evolution, "_SESSION", type("OfflineSession", (), {"get": _unexpected_get})())

    payload = pokeapi_evolution._get_json("https://pokeapi.co/api/v2/pokemon-species/missingno")

    assert payload == {}
