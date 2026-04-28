from __future__ import annotations

from functools import lru_cache
from typing import Any

from src.pipeline.common.http import build_session
from src.pipeline.silver.inputs.reference_context import normalize_species_slug


@lru_cache(maxsize=1024)
def get_species_classification(species_name: str) -> dict[str, bool]:
    species = normalize_species_slug(species_name)
    if not species:
        return {"is_legendary": False, "is_mythical": False}

    session = build_session()
    try:
        response = session.get(f"https://pokeapi.co/api/v2/pokemon-species/{species}/", timeout=30)
    except Exception:  # noqa: BLE001
        return {"is_legendary": False, "is_mythical": False}
    if response.status_code >= 400:
        return {"is_legendary": False, "is_mythical": False}
    try:
        payload = response.json()
    except Exception:  # noqa: BLE001
        return {"is_legendary": False, "is_mythical": False}
    if not isinstance(payload, dict):
        return {"is_legendary": False, "is_mythical": False}
    return {
        "is_legendary": bool(payload.get("is_legendary")),
        "is_mythical": bool(payload.get("is_mythical")),
    }


def is_restricted_encounter_species(species_name: str) -> bool:
    classification = get_species_classification(species_name)
    return bool(classification.get("is_legendary")) or bool(classification.get("is_mythical"))
