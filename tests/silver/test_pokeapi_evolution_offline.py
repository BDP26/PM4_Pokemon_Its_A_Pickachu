from __future__ import annotations

from src.pipeline.silver.inputs.connectors import pokeapi_evolution


def test_get_resource_returns_empty_when_cache_missing(monkeypatch) -> None:
    pokeapi_evolution._get_resource.cache_clear()
    monkeypatch.setattr(
        pokeapi_evolution,
        "pokebase_get_data",
        lambda endpoint, resource_name_or_id=None: (_ for _ in ()).throw(RuntimeError("offline")),
    )

    payload = pokeapi_evolution._get_resource("pokemon-species", "missingno")

    assert payload == {}


def test_get_evolution_chain_for_species_uses_pokebase_payload(monkeypatch) -> None:
    pokeapi_evolution._get_resource.cache_clear()
    monkeypatch.setattr(
        pokeapi_evolution,
        "pokebase_get_data",
        lambda endpoint, resource_name_or_id=None: (
            {"name": "audino", "evolution_chain": {"url": "https://pokeapi.co/api/v2/evolution-chain/133/"}}
            if endpoint == "pokemon-species" and resource_name_or_id == "audino"
            else {"id": 133, "chain": {"species": {"name": "audino"}, "evolves_to": []}}
            if endpoint == "evolution-chain" and int(resource_name_or_id) == 133
            else {}
        ),
    )

    payload = pokeapi_evolution.get_evolution_chain_for_species("audino")

    assert payload.get("id") == 133


def test_get_species_evolution_rules_returns_empty_when_species_lookup_fails(monkeypatch) -> None:
    pokeapi_evolution._get_resource.cache_clear()
    monkeypatch.setattr(
        pokeapi_evolution,
        "pokebase_get_data",
        lambda endpoint, resource_name_or_id=None: (_ for _ in ()).throw(RuntimeError("offline")),
    )

    rules = pokeapi_evolution.get_species_evolution_rules("audino")

    assert rules == {}
