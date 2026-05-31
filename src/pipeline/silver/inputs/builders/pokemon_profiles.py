"""Pokemon profile fetching and enrichment from PokeAPI."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from src.pipeline.settings import SILVER_DIR
from src.pipeline.silver.inputs.reference_context import normalize_species_slug


def pokebase_get_data(endpoint: str, resource_name_or_id: str | int):
    import pokebase as pb

    loader = getattr(pb, str(endpoint).strip().lower().replace("-", "_"), None)
    if not callable(loader):
        raise ValueError(f"Unsupported pokebase endpoint: {endpoint}")
    return loader(resource_name_or_id)


STAT_NAME_TO_COLUMN = {
    "hp": "base_hp",
    "attack": "base_attack",
    "defense": "base_defense",
    "special-attack": "base_special_attack",
    "special-defense": "base_special_defense",
    "speed": "base_speed",
}

POKEMON_COMBAT_REQUIRED_COLUMNS = [
    "pokemon_species",
    "name",
    "type_1",
    "base_hp",
    "base_attack",
    "base_defense",
    "base_special_attack",
    "base_special_defense",
    "base_speed",
]

POKEMON_RESOLUTION_ALIAS_FALLBACKS: dict[str, str] = {
    "aegislash": "aegislash-shield",
    "gourgeist": "gourgeist-average",
    "meowstic": "meowstic-male",
    "pyroar": "pyroar-male",
    "pumpkaboo-small": "pumpkaboo",
    "pumpkaboo-large": "pumpkaboo",
    "pumpkaboo-super": "pumpkaboo",
    "raichu-alola": "raichu",
    "frillish-male": "frillish",
    "jellicent-male": "jellicent",
}

INVALID_NORMALIZED_POKEMON_TOKENS = {"", "nan", "none", "null", "<na>", "na"}


def _extract_pokeapi_id_from_source_url(source_url: str) -> int | None:
    normalized = str(source_url or "").strip().lower()
    match = re.search(r"(?:/pokemon/|pokebase://pokemon/)(\d+)/?$", normalized)
    if match is None:
        return None
    try:
        return int(match.group(1))
    except (TypeError, ValueError):
        return None


def _normalize_requested_pokemon_name(value: Any) -> str:
    normalized = str(value or "").strip().lower().replace(" ", "-").replace("_", "-")
    normalized = re.sub(r"-{2,}", "-", normalized).strip("-")
    if normalized in INVALID_NORMALIZED_POKEMON_TOKENS:
        return ""
    return normalized


def _species_resolution_candidates(normalized_name: str) -> list[str]:
    parts = [part for part in normalized_name.split("-") if part]
    candidates: list[str] = []
    for length in range(len(parts), 0, -1):
        candidate = "-".join(parts[:length]).strip("-")
        if candidate:
            candidates.append(candidate)
    return list(dict.fromkeys(candidates))


def _normalize_pokebase_payload(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _normalize_pokebase_payload(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_normalize_pokebase_payload(item) for item in value]
    if isinstance(value, tuple):
        return [_normalize_pokebase_payload(item) for item in value]
    if hasattr(value, "__dict__"):
        raw = dict(getattr(value, "__dict__", {}) or {})
        return {key: _normalize_pokebase_payload(item) for key, item in raw.items()}
    return value


def _profile_from_pokemon_payload(payload: dict[str, Any]) -> dict[str, Any]:
    types_sorted = sorted(
        (
            entry for entry in payload.get("types", [])
            if isinstance(entry, dict)
        ),
        key=lambda entry: int(entry.get("slot") or 999),
    )
    type_names = [
        str((entry.get("type") or {}).get("name") or "").strip().lower()
        for entry in types_sorted
        if str((entry.get("type") or {}).get("name") or "").strip()
    ]

    stats_payload = {
        column: None
        for column in STAT_NAME_TO_COLUMN.values()
    }
    for stat_entry in payload.get("stats", []):
        if not isinstance(stat_entry, dict):
            continue
        stat_name = str((stat_entry.get("stat") or {}).get("name") or "").strip().lower()
        column_name = STAT_NAME_TO_COLUMN.get(stat_name)
        if column_name is None:
            continue
        try:
            stats_payload[column_name] = int(stat_entry.get("base_stat"))
        except (TypeError, ValueError):
            stats_payload[column_name] = None

    pokeapi_id = payload.get("id")
    try:
        pokeapi_id_value = int(pokeapi_id)
    except (TypeError, ValueError):
        pokeapi_id_value = None

    species_name = str(((payload.get("species") or {}).get("name")) or payload.get("name") or "").strip().lower()
    species_slug = normalize_species_slug(species_name)
    profile_name = str(payload.get("name") or species_slug).strip().lower()

    return {
        "name": profile_name,
        "pokemon_species": species_slug or profile_name,
        "pokeapi_id": pokeapi_id_value,
        "source_url": f"pokebase://pokemon/{pokeapi_id_value}" if pokeapi_id_value is not None else None,
        "type_1": type_names[0] if type_names else None,
        "type_2": type_names[1] if len(type_names) > 1 else None,
        **stats_payload,
        "height": payload.get("height"),
        "weight": payload.get("weight"),
        "base_experience": payload.get("base_experience"),
        "is_default": payload.get("is_default"),
    }


def _species_classification_from_payload(payload: dict[str, Any] | None) -> dict[str, bool | None]:
    if not isinstance(payload, dict):
        return {
            "is_legendary": None,
            "is_mythical": None,
        }
    return {
        "is_legendary": bool(payload.get("is_legendary")),
        "is_mythical": bool(payload.get("is_mythical")),
    }


def _has_complete_pokemon_profile_payload(payload: dict[str, Any] | None) -> bool:
    if not isinstance(payload, dict):
        return False
    try:
        int(payload.get("id"))
    except (TypeError, ValueError):
        return False
    types = payload.get("types")
    stats = payload.get("stats")
    if not isinstance(types, list) or not types:
        return False
    if not isinstance(stats, list) or not stats:
        return False
    return True


def _resolve_requested_pokemon_profile(
    requested_name: str,
    fetch_resource: Callable[[str, str | int], dict[str, Any] | None],
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    normalized_requested_name = _normalize_requested_pokemon_name(requested_name)
    lineage_base: dict[str, Any] = {
        "requested_pokemon_name": str(requested_name or "").strip(),
        "normalized_requested_name": normalized_requested_name,
        "normalized_species": None,
        "resolved_pokemon_name": None,
        "resolved_pokeapi_id": None,
        "is_default_variety": None,
        "resolution_method": None,
        "resolution_warning": None,
    }
    if not normalized_requested_name:
        lineage = dict(lineage_base)
        lineage["resolution_warning"] = "empty_requested_name"
        return None, lineage

    pokemon_payload = fetch_resource("pokemon", normalized_requested_name)
    if _has_complete_pokemon_profile_payload(pokemon_payload):
        profile = _profile_from_pokemon_payload(pokemon_payload)
        species_payload = None
        species_name = _normalize_requested_pokemon_name(((pokemon_payload.get("species") or {}).get("name")) or "")
        if species_name:
            species_payload = fetch_resource("pokemon-species", species_name)
        normalized_species = normalize_species_slug(((pokemon_payload.get("species") or {}).get("name")) or profile.get("pokemon_species") or "")
        profile.update(
            {
                "pokemon_species": normalized_requested_name,
                "name": normalized_requested_name,
                "requested_pokemon_name": lineage_base["requested_pokemon_name"],
                "normalized_requested_name": normalized_requested_name,
                "normalized_species": normalized_species or normalize_species_slug(normalized_requested_name),
                "resolved_pokemon_name": str(profile.get("name") or "").strip().lower() or normalized_requested_name,
                "resolved_pokeapi_id": profile.get("pokeapi_id"),
                "is_default_variety": bool(pokemon_payload.get("is_default")),
                **_species_classification_from_payload(species_payload),
                "resolution_method": "pokemon_exact",
                "resolution_warning": None,
            }
        )
        return profile, None

    species_payload: dict[str, Any] | None = None
    species_candidate: str | None = None
    for candidate in _species_resolution_candidates(normalized_requested_name):
        species_payload = fetch_resource("pokemon-species", candidate)
        if species_payload is not None:
            species_candidate = candidate
            break
    used_alias_fallback = False
    if species_payload is None:
        alias_candidate = POKEMON_RESOLUTION_ALIAS_FALLBACKS.get(normalized_requested_name)
        if alias_candidate:
            species_payload = fetch_resource("pokemon-species", alias_candidate)
            if species_payload is not None:
                species_candidate = alias_candidate
                used_alias_fallback = True

    if species_payload is None:
        lineage = dict(lineage_base)
        lineage["resolution_warning"] = "unresolved_after_pokemon_species_alias_fallback"
        return None, lineage

    normalized_species = normalize_species_slug(species_payload.get("name") or species_candidate or normalized_requested_name)
    default_variety_name: str | None = None
    for variety in species_payload.get("varieties", []):
        if not isinstance(variety, dict):
            continue
        if bool(variety.get("is_default")):
            default_variety_name = _normalize_requested_pokemon_name(((variety.get("pokemon") or {}).get("name")) or "")
            break
    if not default_variety_name:
        lineage = dict(lineage_base)
        lineage.update(
            {
                "normalized_species": normalized_species,
                "resolution_method": "species_default_variety",
                "resolution_warning": "species_without_default_variety",
            }
        )
        return None, lineage

    default_payload = fetch_resource("pokemon", default_variety_name)
    if default_payload is None:
        lineage = dict(lineage_base)
        lineage.update(
            {
                "normalized_species": normalized_species,
                "resolved_pokemon_name": default_variety_name,
                "resolution_method": "species_default_variety",
                "resolution_warning": "default_variety_profile_lookup_failed",
            }
        )
        return None, lineage

    profile = _profile_from_pokemon_payload(default_payload)
    profile.update(
        {
            "pokemon_species": normalized_requested_name,
            "name": normalized_requested_name,
            "requested_pokemon_name": lineage_base["requested_pokemon_name"],
            "normalized_requested_name": normalized_requested_name,
            "normalized_species": normalized_species or normalize_species_slug(default_variety_name),
            "resolved_pokemon_name": str(profile.get("name") or "").strip().lower() or default_variety_name,
            "resolved_pokeapi_id": profile.get("pokeapi_id"),
            "is_default_variety": bool(default_payload.get("is_default")),
            **_species_classification_from_payload(species_payload),
            "resolution_method": "alias_species_default_variety" if used_alias_fallback else "species_default_variety",
            "resolution_warning": None,
        }
    )
    return profile, None


def _build_enriched_pokemon_profiles(
    all_pokemon_references: dict[str, Any],
    required_species: set[str],
    silver_dir: Path = SILVER_DIR,
) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    profiles_by_requested: dict[str, dict[str, Any]] = {}
    diagnostics: list[dict[str, Any]] = []
    normalized_required_species = sorted(
        {
            normalized
            for species in required_species
            if species is not None and not pd.isna(species)
            for normalized in [_normalize_requested_pokemon_name(species)]
            if normalized
        }
    )

    def _fetch_resource(endpoint: str, resource_name_or_id: str | int) -> dict[str, Any] | None:
        endpoint_name = str(endpoint or "").strip().lower()
        if not endpoint_name:
            return None
        try:
            payload = pokebase_get_data(endpoint_name, resource_name_or_id)
        except Exception:
            return None
        normalized = _normalize_pokebase_payload(payload)
        return normalized if isinstance(normalized, dict) and normalized else None

    for species in normalized_required_species:
        requested_species = _normalize_requested_pokemon_name(species)
        if not requested_species:
            continue
        payload = all_pokemon_references.get(requested_species, {}) if isinstance(all_pokemon_references.get(requested_species), dict) else {}
        source_url = str(payload.get("url") or payload.get("source_url") or "").strip()
        reference_name = _normalize_requested_pokemon_name(payload.get("name") or requested_species)
        profile, failure = _resolve_requested_pokemon_profile(
            reference_name or requested_species,
            fetch_resource=_fetch_resource,
        )
        if profile is None:
            diagnostics.append(
                {
                    "requested_pokemon_name": requested_species,
                    "normalized_requested_name": requested_species,
                    "source_url": source_url or None,
                    **(failure or {"resolution_warning": "unknown_resolution_failure"}),
                }
            )
            continue
        profiles_by_requested[requested_species] = profile

    return pd.DataFrame(list(profiles_by_requested.values())), diagnostics


def _validate_and_persist_pokemon_data_contract(
    pokemon_data_df: pd.DataFrame,
    diagnostics_dir: Path,
    required_species: set[str],
) -> None:
    normalized_required_species = {
        normalized
        for species in required_species
        if species is not None and not pd.isna(species)
        for normalized in [normalize_species_slug(species)]
        if normalized not in INVALID_NORMALIZED_POKEMON_TOKENS
    }
    present_species = {
        normalize_species_slug(row.get("pokemon_species") or row.get("name") or "")
        for row in pokemon_data_df.to_dict(orient="records")
        if normalize_species_slug(row.get("pokemon_species") or row.get("name") or "")
    }
    missing_species = sorted(species for species in normalized_required_species if species not in present_species)
    if missing_species:
        missing_frame = pd.DataFrame(
            [
                {
                    "pokemon_species": species,
                    "requested_pokemon_name": species,
                    "normalized_requested_name": species,
                    "resolution_warning": "missing_profile",
                    "error": "missing_profile",
                }
                for species in missing_species
            ]
        )
        pokemon_data_df = pd.concat([pokemon_data_df, missing_frame], ignore_index=True)

    for column in (
        "requested_pokemon_name",
        "normalized_requested_name",
        "normalized_species",
        "resolved_pokemon_name",
        "resolved_pokeapi_id",
        "is_default_variety",
        "resolution_method",
        "resolution_warning",
    ):
        if column not in pokemon_data_df.columns:
            pokemon_data_df[column] = None

    unresolved_default_species = pokemon_data_df[
        pokemon_data_df["resolution_warning"].astype("string").fillna("").eq("species_without_default_variety")
    ]
    if not unresolved_default_species.empty:
        unresolved = ",".join(sorted(unresolved_default_species["normalized_requested_name"].astype("string").dropna().tolist())[:20])
        raise ValueError(
            "Silver pokemon profile resolution failed: species had no default variety "
            f"count={len(unresolved_default_species)} first_20=[{unresolved}]"
        )

    incomplete_mask = pd.Series(False, index=pokemon_data_df.index)
    for column in POKEMON_COMBAT_REQUIRED_COLUMNS:
        if column not in pokemon_data_df.columns:
            incomplete_mask = pd.Series(True, index=pokemon_data_df.index)
            continue
        incomplete_mask = incomplete_mask | pokemon_data_df[column].isna()
        if pd.api.types.is_object_dtype(pokemon_data_df[column]) or pd.api.types.is_string_dtype(pokemon_data_df[column]):
            stripped = pokemon_data_df[column].astype("string").str.strip()
            incomplete_mask = incomplete_mask | stripped.eq("")
    incomplete_mask = incomplete_mask | pokemon_data_df["resolved_pokeapi_id"].isna()
    incomplete_mask = incomplete_mask | pokemon_data_df["resolved_pokemon_name"].astype("string").str.strip().eq("")

    incomplete_rows = pokemon_data_df[incomplete_mask].copy()
    diagnostics_path = diagnostics_dir / "incomplete_pokemon_profiles.csv"
    diagnostics_path.parent.mkdir(parents=True, exist_ok=True)
    incomplete_rows.to_csv(diagnostics_path, index=False)

    if incomplete_rows.empty:
        return

    impacted_species = [
        normalize_species_slug(value)
        for value in incomplete_rows.get("pokemon_species", pd.Series([], dtype="object")).fillna("")
        if normalize_species_slug(value)
    ]
    preview = ",".join(sorted(dict.fromkeys(impacted_species))[:20])
    raise ValueError(
        "Silver pokemon_data contract violation: "
        f"incomplete_rows={len(incomplete_rows)} first_20_species=[{preview}] "
        f"diagnostics={diagnostics_path}"
    )
