from __future__ import annotations

from functools import lru_cache
from typing import Any

from src.pipeline.common.io import read_parquet
from src.pipeline.silver.inputs.reference_context import normalize_species_slug
from src.pipeline.settings import SILVER_DIR


@lru_cache(maxsize=1)
def _persisted_species_classification() -> dict[str, dict[str, bool]]:
    pokemon_data_path = SILVER_DIR / "references" / "pokemon_data.parquet"
    if not pokemon_data_path.exists():
        return {}

    lookup: dict[str, dict[str, bool]] = {}
    pokemon_data_df = read_parquet(pokemon_data_path)
    if pokemon_data_df.empty or "pokemon_species" not in pokemon_data_df.columns:
        return lookup

    for row in pokemon_data_df.to_dict(orient="records"):
        species = normalize_species_slug(row.get("pokemon_species") or row.get("name") or "")
        if not species:
            continue
        lookup[species] = {
            "is_legendary": bool(row.get("is_legendary")),
            "is_mythical": bool(row.get("is_mythical")),
        }
    return lookup


@lru_cache(maxsize=1024)
def get_species_classification(species_name: str) -> dict[str, bool]:
    species = normalize_species_slug(species_name)
    if not species:
        return {"is_legendary": False, "is_mythical": False}

    persisted = _persisted_species_classification().get(species)
    if persisted is not None:
        return persisted
    return {"is_legendary": False, "is_mythical": False}


def is_restricted_encounter_species(species_name: str) -> bool:
    classification = get_species_classification(species_name)
    return bool(classification.get("is_legendary")) or bool(classification.get("is_mythical"))
