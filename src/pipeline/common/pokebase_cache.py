from __future__ import annotations

from pathlib import Path
import shelve
from typing import Any

POKEBASE_CACHE_PATH = Path.home() / ".cache" / "pokebase" / "api.cache"


def _cache_available(cache_path: Path = POKEBASE_CACHE_PATH) -> bool:
    candidates = [cache_path, *cache_path.parent.glob(f"{cache_path.name}*")]
    return any(path.exists() for path in candidates)


def _cache_uri_for_resource(
    cache: shelve.Shelf[Any],
    endpoint: str,
    resource_name_or_id: str | int | None,
    *,
    resolve_name_via_listing: bool,
) -> str | None:
    endpoint_key = endpoint.strip("/").lower()
    if resource_name_or_id is None:
        return f"{endpoint_key}/"
    if isinstance(resource_name_or_id, int):
        return f"{endpoint_key}/{resource_name_or_id}/"

    resource_key = str(resource_name_or_id).strip().strip("/").lower()
    direct_uri = f"{endpoint_key}/{resource_key}/"
    if direct_uri in cache:
        return direct_uri
    if not resolve_name_via_listing:
        return direct_uri

    listing = cache.get(f"{endpoint_key}/")
    results = listing.get("results", []) if isinstance(listing, dict) else []
    for row in results:
        if str(row.get("name") or "").strip().lower() != resource_key:
            continue
        url = str(row.get("url") or "")
        parts = [part for part in url.split("/") if part]
        if not parts:
            continue
        try:
            resource_id = int(parts[-1])
        except ValueError:
            continue
        return f"{endpoint_key}/{resource_id}/"
    return None


def get_cached_pokebase_payload(
    endpoint: str,
    resource_name_or_id: str | int | None = None,
    *,
    resolve_name_via_listing: bool = False,
    empty_as_none: bool = False,
    cache_path: Path = POKEBASE_CACHE_PATH,
) -> dict[str, Any] | None:
    if not _cache_available(cache_path):
        return None if empty_as_none else {}
    try:
        with shelve.open(str(cache_path), flag="r") as cache:
            uri = _cache_uri_for_resource(
                cache,
                endpoint,
                resource_name_or_id,
                resolve_name_via_listing=resolve_name_via_listing,
            )
            if not uri:
                return None if empty_as_none else {}
            payload = cache.get(uri)
    except Exception:
        return None if empty_as_none else {}

    if isinstance(payload, dict):
        return payload
    return None if empty_as_none else {}
