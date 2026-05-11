"""One-shot script: fetch profiles for species in learnable_moves missing from pokemon_data."""
from __future__ import annotations
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd
from src.pipeline.silver.orchestration.build_silver import (
    _normalize_pokebase_payload,
    _has_complete_pokemon_profile_payload,
    _profile_from_pokemon_payload,
    _species_classification_from_payload,
    pokebase_get_data,
)

PD_PATH = Path("data/silver/references/pokemon_data.parquet")
LM_PATH = Path("data/silver/references/learnable_moves.parquet")


def fetch(endpoint: str, name: str) -> dict | None:
    try:
        payload = pokebase_get_data(endpoint, name)
        result = _normalize_pokebase_payload(payload)
        return result if isinstance(result, dict) and result else None
    except Exception as exc:
        print(f"  ERROR {endpoint}/{name}: {exc}")
        return None


def main() -> None:
    existing = pd.read_parquet(PD_PATH)
    lm = pd.read_parquet(LM_PATH)
    known = set(existing["pokemon_species"].dropna().unique())
    all_lm_species = set(lm["pokemon_species"].dropna().unique())
    missing = sorted(all_lm_species - known)
    print(f"Missing species ({len(missing)}): {missing}")

    profiles = []
    for species in missing:
        print(f"Fetching {species}...")
        poke = fetch("pokemon", species)
        if not _has_complete_pokemon_profile_payload(poke):
            print(f"  SKIP – no complete payload")
            continue
        spec = fetch("pokemon-species", species)
        profile = _profile_from_pokemon_payload(poke)
        profile.update(
            {
                "pokemon_species": species,
                "name": species,
                "requested_pokemon_name": species,
                "normalized_requested_name": species,
                "normalized_species": species,
                "resolved_pokemon_name": species,
                "resolved_pokeapi_id": profile.get("pokeapi_id"),
                "is_default_variety": bool(poke.get("is_default")),
                **_species_classification_from_payload(spec),
                "resolution_method": "manual_patch",
                "resolution_warning": None,
            }
        )
        profiles.append(profile)
        print(f"  OK type={profile.get('type_1')} hp={profile.get('base_hp')}")

    if not profiles:
        print("Nothing to patch.")
        return

    combined = (
        pd.concat([existing, pd.DataFrame(profiles)], ignore_index=True)
        .drop_duplicates(subset=["pokemon_species"])
        .reset_index(drop=True)
    )
    print(f"\nRows before: {len(existing)}  after: {len(combined)}")
    combined.to_parquet(PD_PATH, index=False)
    print("Written.")


if __name__ == "__main__":
    main()

