from __future__ import annotations

from src.pipeline.common.pokebase_cache import _cache_uri_for_resource


def test_cache_uri_for_resource_resolves_from_paginated_listings() -> None:
    cache = {
        "move/": {
            "count": 400,
            "results": [{"name": "pound", "url": "https://pokeapi.co/api/v2/move/1/"}],
        },
        "move/?offset=200&limit=200": {
            "count": 400,
            "results": [{"name": "tail-glow", "url": "https://pokeapi.co/api/v2/move/356/"}],
        },
    }

    assert (
        _cache_uri_for_resource(
            cache, "move", "tail-glow", resolve_name_via_listing=True
        )
        == "move/356/"
    )
