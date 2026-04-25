from __future__ import annotations

from collections import Counter, defaultdict
import gc
import logging
import re
import shutil
import time
from pathlib import Path
from typing import Any, Callable, cast

import pandas as pd

from src.pipeline.bronze.inputs.create_type_chart import build_type_chart, save_as_json
from src.pipeline.common.http import build_session
from src.pipeline.common.io import read_json, read_jsonl, read_parquet, write_json, write_parquet
from src.pipeline.common.simulation_config import load_runtime_battle_policy_config
from src.pipeline.settings import BRONZE_DIR, SILVER_DIR, ensure_medallion_dirs, get_silver_subdirs
from src.pipeline.silver.config.game_config import get_games_config, get_starter_choices, get_starter_family_members
from src.pipeline.silver.config.team_config import resolve_runtime_team_config
from src.pipeline.silver.enrichment.location_pokemon_enrichment import (
    enrich_records_with_location_pokemon,
    get_location_area_and_pokemon_maps,
)
from src.pipeline.silver.enrichment.schema_normalizer import (
    create_encounter_methods_reference,
    create_pokemon_reference_index,
    write_normalized_silver,
)
from src.pipeline.silver.inputs.builders.player_teams import (
    build_player_team_compact_tables,
    build_progression_source_teams_from_encounters,
)
from src.pipeline.silver.inputs.connectors.pokeapi_moves import (
    bootstrap_move_reference_cache,
    persist_move_reference_cache,
)
from src.pipeline.silver.inputs.kaggle_boss_mapping import load_kaggle_rows_by_game
from src.pipeline.silver.inputs.location_mapper import LocationMapper
from src.pipeline.silver.inputs.connectors.pokeapi_evolution import get_species_evolution_rules
from src.pipeline.silver.inputs.reference_context import load_reference_context, normalize_move_name, normalize_species_slug
from src.pipeline.silver.move_power import resolve_effective_power
from src.pipeline.silver.inputs.sources.boss_teams import (
    extract_boss_teams_from_kaggle_source,
    load_kaggle_boss_rows_by_game,
)
from src.pipeline.silver.orchestration.stages import run_parse_stage
from src.pipeline.silver.reporting.silver_manifest import create_silver_manifest
from src.pipeline.silver.schemas.relational_checks import validate_normalized_silver_tables
from src.pipeline.silver.transforms.keys import stable_digest
from src.pipeline.silver.transforms.normalized_tables import build_bosses_table, build_games_table, build_locations_table
from src.pipeline.silver.writers.outputs import (
    build_input_signature,
    fingerprint_path,
    fingerprint_python_files,
    load_state,
    save_state,
    write_validated_move_data,
)

logger = logging.getLogger(__name__)


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


def summarize_unmapped_locations(misses: list[dict]) -> dict:
    by_reason = Counter()
    by_tried_slug = Counter()
    by_raw_title = Counter()
    examples_by_reason = defaultdict(list)

    for miss in misses:
        reason = miss.get("reason", "unknown")
        raw_title = miss.get("raw_title", "")
        tried_slug = miss.get("tried_slug") or ""
        by_reason[reason] += 1
        if tried_slug:
            by_tried_slug[tried_slug] += 1
        if raw_title:
            by_raw_title[raw_title] += 1
        if len(examples_by_reason[reason]) < 10:
            examples_by_reason[reason].append(miss)

    return {
        "total_unmapped_events": len(misses),
        "by_reason": dict(by_reason.most_common()),
        "top_tried_slugs": dict(by_tried_slug.most_common(50)),
        "top_raw_titles": dict(by_raw_title.most_common(50)),
        "examples_by_reason": dict(examples_by_reason),
    }


def _remove_if_exists(path: Path) -> None:
    if not path.exists():
        return
    if path.is_dir():
        shutil.rmtree(path)
    else:
        path.unlink()


def _game_output_paths(simulation_dir: Path, game_key: str) -> dict[str, Path]:
    return {
        "source_teams": simulation_dir / f"source_teams_{game_key}.parquet",
        "source_team_members": simulation_dir / f"source_team_members_{game_key}.parquet",
        "member_moveset_combos": simulation_dir / f"member_moveset_combos_{game_key}.parquet",
        "member_move_options": simulation_dir / f"member_move_options_{game_key}.parquet",
        "pokemon_moveset_options": simulation_dir / f"pokemon_moveset_options_{game_key}.parquet",
        "simulation_sampling_plan": simulation_dir / f"simulation_sampling_plan_{game_key}.parquet",
        "combat_pool": simulation_dir / f"pokemon_combat_pool_{game_key}.parquet",
    }


def _validation_profile(values_by_column: dict[str, set[str]], row_count: int) -> dict[str, Any]:
    return {"row_count": row_count, "columns": {column: sorted(values) for column, values in values_by_column.items()}}


def _collect_kaggle_boss_species_and_moves(boss_teams: list[dict[str, Any]]) -> tuple[set[str], set[str]]:
    species: set[str] = set()
    moves: set[str] = set()
    for team in boss_teams:
        pokemon_entries = team.get("pokemon")
        if isinstance(pokemon_entries, list):
            for pokemon in pokemon_entries:
                species_slug = normalize_species_slug(pokemon)
                if species_slug:
                    species.add(species_slug)
        move_entries = team.get("moves")
        if isinstance(move_entries, list):
            for member_moves in move_entries:
                if not isinstance(member_moves, list):
                    continue
                for move in member_moves:
                    move_slug = normalize_move_name(move)
                    if move_slug:
                        moves.add(move_slug)
    return species, moves


def _extract_pokeapi_id_from_source_url(source_url: str) -> int | None:
    match = re.search(r"/pokemon/(\d+)/?$", str(source_url or "").strip().lower())
    if match is None:
        return None
    try:
        return int(match.group(1))
    except (TypeError, ValueError):
        return None


def _normalize_requested_pokemon_name(value: Any) -> str:
    normalized = str(value or "").strip().lower().replace(" ", "-").replace("_", "-")
    normalized = re.sub(r"-{2,}", "-", normalized).strip("-")
    return normalized


def _species_resolution_candidates(normalized_name: str) -> list[str]:
    parts = [part for part in normalized_name.split("-") if part]
    candidates: list[str] = []
    for length in range(len(parts), 0, -1):
        candidate = "-".join(parts[:length]).strip("-")
        if candidate:
            candidates.append(candidate)
    return list(dict.fromkeys(candidates))


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
        "source_url": f"https://pokeapi.co/api/v2/pokemon/{pokeapi_id_value}/" if pokeapi_id_value is not None else None,
        "type_1": type_names[0] if type_names else None,
        "type_2": type_names[1] if len(type_names) > 1 else None,
        **stats_payload,
        "height": payload.get("height"),
        "weight": payload.get("weight"),
        "base_experience": payload.get("base_experience"),
        "is_default": payload.get("is_default"),
    }


def _resolve_requested_pokemon_profile(
    requested_name: str,
    fetch_json: Callable[[str], dict[str, Any] | None],
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

    pokemon_payload = fetch_json(f"https://pokeapi.co/api/v2/pokemon/{normalized_requested_name}/")
    if pokemon_payload is not None:
        profile = _profile_from_pokemon_payload(pokemon_payload)
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
                "resolution_method": "pokemon_exact",
                "resolution_warning": None,
            }
        )
        return profile, None

    species_payload: dict[str, Any] | None = None
    species_candidate: str | None = None
    for candidate in _species_resolution_candidates(normalized_requested_name):
        species_payload = fetch_json(f"https://pokeapi.co/api/v2/pokemon-species/{candidate}/")
        if species_payload is not None:
            species_candidate = candidate
            break
    used_alias_fallback = False
    if species_payload is None:
        alias_candidate = POKEMON_RESOLUTION_ALIAS_FALLBACKS.get(normalized_requested_name)
        if alias_candidate:
            species_payload = fetch_json(f"https://pokeapi.co/api/v2/pokemon-species/{alias_candidate}/")
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

    default_payload = fetch_json(f"https://pokeapi.co/api/v2/pokemon/{default_variety_name}/")
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
            "resolution_method": "alias_species_default_variety" if used_alias_fallback else "species_default_variety",
            "resolution_warning": None,
        }
    )
    return profile, None


def _build_enriched_pokemon_profiles(
    all_pokemon_references: dict[str, Any],
    required_species: set[str],
) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    session = build_session()
    profiles_by_requested: dict[str, dict[str, Any]] = {}
    diagnostics: list[dict[str, Any]] = []

    def _fetch_json(url: str) -> dict[str, Any] | None:
        try:
            response = session.get(url, timeout=30)
        except Exception:  # noqa: BLE001
            return None
        if response.status_code >= 400:
            return None
        try:
            return cast(dict[str, Any], response.json())
        except Exception:  # noqa: BLE001
            return None

    for species in sorted(required_species):
        requested_species = _normalize_requested_pokemon_name(species)
        if not requested_species:
            continue
        payload = all_pokemon_references.get(requested_species, {}) if isinstance(all_pokemon_references.get(requested_species), dict) else {}
        source_url = str(payload.get("url") or payload.get("source_url") or "").strip()
        reference_name = _normalize_requested_pokemon_name(payload.get("name") or requested_species)
        profile, failure = _resolve_requested_pokemon_profile(reference_name or requested_species, fetch_json=_fetch_json)
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
    present_species = {
        normalize_species_slug(row.get("pokemon_species") or row.get("name") or "")
        for row in pokemon_data_df.to_dict(orient="records")
        if normalize_species_slug(row.get("pokemon_species") or row.get("name") or "")
    }
    missing_species = sorted(species for species in required_species if species and species not in present_species)
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


def _move_profiles_from_reference(move_reference_df: pd.DataFrame) -> dict[str, dict[str, Any]]:
    profiles: dict[str, dict[str, Any]] = {}
    for row in move_reference_df.to_dict(orient="records"):
        move_name = normalize_move_name(row.get("move_name"))
        if not move_name:
            continue
        raw_power = row.get("power")
        if isinstance(raw_power, float) and pd.isna(raw_power):
            raw_power = None
        effective_power, power_handling = resolve_effective_power(
            move_name=move_name,
            power=raw_power,
            damage_class=row.get("damage_class"),
        )
        stored_effective_power = row.get("effective_power", effective_power)
        if isinstance(stored_effective_power, float) and pd.isna(stored_effective_power):
            stored_effective_power = effective_power
        stored_power_handling = row.get("power_handling", power_handling)
        if not isinstance(stored_power_handling, str) or not stored_power_handling.strip():
            stored_power_handling = power_handling
        profiles[move_name] = {
            "move_name": move_name,
            "type": str(row.get("type") or "Normal"),
            "power": raw_power,
            "raw_power": raw_power,
            "damage_class": str(row.get("damage_class") or "status"),
            "accuracy": row.get("accuracy"),
            "pp": row.get("pp"),
            "effective_power": stored_effective_power,
            "power_handling": stored_power_handling,
            "is_status_move": row.get("is_status_move", str(row.get("damage_class") or "").strip().lower() == "status"),
            "is_damage_move": row.get("is_damage_move", effective_power > 0),
            "is_null_power": row.get("is_null_power", raw_power is None),
            "level_learned_at": 0,
            "version_group": "reference",
            "degraded_data": False,
        }
    return profiles


def _ensure_moves_in_combat_profiles(
    move_data: dict[str, dict[str, Any]],
    required_moves: set[str],
    move_reference_profiles: dict[str, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    enriched = {key: dict(value) for key, value in move_data.items()}
    if not required_moves:
        return enriched

    available_moves: set[str] = set()
    for payload in enriched.values():
        details = payload.get("move_details")
        if not isinstance(details, dict):
            continue
        for move in details.keys():
            move_name = normalize_move_name(move)
            if move_name:
                available_moves.add(move_name)

    missing_moves = sorted(move for move in required_moves if move and move not in available_moves)
    if not missing_moves:
        return enriched

    if not enriched:
        enriched["reference:seed"] = {
            "pokemon_instance_id": "reference:seed",
            "team_id": "reference",
            "species": "reference",
            "level": 1,
            "game_version": "reference",
            "provided_moves": [],
            "learnable_moves": [],
            "move_details": {},
            "slot_index": 1,
        }

    first_key = next(iter(enriched))
    first_payload = dict(enriched[first_key])
    move_details = dict(first_payload.get("move_details") or {})
    for move in missing_moves:
        profile = move_reference_profiles.get(move)
        if profile is None:
            continue
        move_details[move] = profile
    first_payload["move_details"] = move_details
    enriched[first_key] = first_payload
    return enriched


def _build_bootstrap_move_entries(
    records_with_game_keys: list[tuple[str, list[dict[str, Any]]]],
) -> list[tuple[str, int, str, list[str]]]:
    bootstrap_entries: list[tuple[str, int, str, list[str]]] = []
    for game_key, records in records_with_game_keys:
        for record in records:
            reachable_encounters = record.get("reachable_location_encounters", {})
            if isinstance(reachable_encounters, dict):
                for encounters in reachable_encounters.values():
                    if not isinstance(encounters, list):
                        continue
                    for encounter in encounters:
                        if not isinstance(encounter, dict):
                            continue
                        species = str(encounter.get("species") or "").strip().lower()
                        if not species:
                            continue
                        try:
                            level = int(encounter.get("level_max") or encounter.get("level") or record.get("boss_avg_level") or 20)
                        except (TypeError, ValueError):
                            level = 20
                        bootstrap_entries.append((species, max(level, 1), game_key, []))

    deduped_entries: list[tuple[str, int, str, list[str]]] = []
    seen: set[tuple[str, int, str]] = set()
    for species, level, game_version, moves in bootstrap_entries:
        key = (str(species).strip().lower(), int(level), str(game_version).strip().lower())
        if key in seen:
            continue
        seen.add(key)
        deduped_entries.append((species, level, game_version, moves))
    return deduped_entries


def _collect_starter_chain_species_by_game(
    games_config: list[dict[str, Any]],
) -> dict[str, set[str]]:
    starter_species_by_game: dict[str, set[str]] = {}
    for game in games_config:
        game_key = str(game.get("game_key") or "").strip().lower()
        if not game_key:
            continue
        species_set = starter_species_by_game.setdefault(game_key, set())
        for starter in get_starter_choices(game_key):
            starter_slug = normalize_species_slug(starter)
            if starter_slug:
                species_set.add(starter_slug)
            try:
                rules = get_species_evolution_rules(starter_slug)
            except Exception:  # noqa: BLE001
                rules = {}
            if rules:
                for species_name in rules.keys():
                    species_slug = normalize_species_slug(species_name)
                    if species_slug:
                        species_set.add(species_slug)
            else:
                for species_name in get_starter_family_members(starter_slug):
                    species_slug = normalize_species_slug(species_name)
                    if species_slug:
                        species_set.add(species_slug)
    return starter_species_by_game


def _starter_chain_bootstrap_entries(
    starter_chain_species_by_game: dict[str, set[str]],
) -> list[tuple[str, int, str, list[str]]]:
    entries: list[tuple[str, int, str, list[str]]] = []
    for game_version in sorted(starter_chain_species_by_game):
        for species in sorted(starter_chain_species_by_game[game_version]):
            entries.append((species, 100, game_version, []))
    return entries


def _dedupe_bootstrap_entries(
    entries: list[tuple[str, int, str, list[str]]],
) -> list[tuple[str, int, str, list[str]]]:
    deduped: list[tuple[str, int, str, list[str]]] = []
    seen: set[tuple[str, int, str]] = set()
    for species, level, game_version, moves in entries:
        key = (normalize_species_slug(species), max(1, int(level)), str(game_version).strip().lower())
        if not key[0] or not key[2] or key in seen:
            continue
        seen.add(key)
        deduped.append((key[0], key[1], key[2], [normalize_move_name(move) for move in moves if normalize_move_name(move)]))
    return deduped


def _validate_starter_chain_move_coverage(
    learnable_moves_df: pd.DataFrame,
    starter_chain_species_by_game: dict[str, set[str]],
    diagnostics_dir: Path,
) -> list[dict[str, Any]]:
    observed_pairs = {
        (
            str(row.get("game_version") or "").strip().lower(),
            normalize_species_slug(row.get("pokemon_species") or ""),
        )
        for row in learnable_moves_df.to_dict(orient="records")
        if str(row.get("game_version") or "").strip() and normalize_species_slug(row.get("pokemon_species") or "")
    }

    missing_rows: list[dict[str, Any]] = []
    for game_version in sorted(starter_chain_species_by_game):
        for species in sorted(starter_chain_species_by_game[game_version]):
            if (game_version, species) not in observed_pairs:
                missing_rows.append(
                    {
                        "species_name": species,
                        "game_version": game_version,
                        "reason": "starter_chain_missing_moves",
                    }
                )

    diagnostics_path = diagnostics_dir / "starter_chain_move_gaps.csv"
    diagnostics_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(missing_rows).to_csv(diagnostics_path, index=False)

    if missing_rows:
        preview = ", ".join(
            f"{row['game_version']}:{row['species_name']}"
            for row in missing_rows[:20]
        )
        raise ValueError(
            "Starter-chain move reference validation failed: "
            f"missing_species_count={len(missing_rows)} "
            f"first_20=[{preview}] "
            f"diagnostics={diagnostics_path}"
        )

    return missing_rows


def _build_kaggle_bootstrap_entries(kaggle_rows_by_game: dict[str, list[dict[str, Any]]]) -> list[tuple[str, int, str, list[str]]]:
    entries: list[tuple[str, int, str, list[str]]] = []
    for game_key, rows in kaggle_rows_by_game.items():
        game_norm = str(game_key or "").strip().lower()
        if not game_norm:
            continue
        for row in rows:
            species = normalize_species_slug(row.get("Pokemon") or "")
            if not species:
                continue
            try:
                level = int(row.get("Level") or 20)
            except (TypeError, ValueError):
                level = 20
            moves = [
                normalize_move_name(row.get("Move 1", "")),
                normalize_move_name(row.get("Move 2", "")),
                normalize_move_name(row.get("Move 3", "")),
                normalize_move_name(row.get("Move 4", "")),
            ]
            entries.append((species, max(level, 1), game_norm, [move for move in moves if move]))
    return entries


def _validate_kaggle_moves_in_move_reference(
    kaggle_rows_by_game: dict[str, list[dict[str, Any]]],
    move_reference_df: pd.DataFrame,
    diagnostics_dir: Path,
) -> None:
    move_reference_moves = {
        normalize_move_name(row.get("move_name"))
        for row in move_reference_df.to_dict(orient="records")
        if normalize_move_name(row.get("move_name"))
    }

    missing_rows: list[dict[str, Any]] = []
    move_columns = ("Move 1", "Move 2", "Move 3", "Move 4")
    for game_key, rows in sorted(kaggle_rows_by_game.items()):
        game_norm = str(game_key or "").strip().lower()
        for row in rows:
            species_slug = normalize_species_slug(row.get("Pokemon") or "")
            for move_column in move_columns:
                normalized_move = normalize_move_name(row.get(move_column) or "")
                if not normalized_move:
                    continue
                if normalized_move in move_reference_moves:
                    continue
                missing_rows.append(
                    {
                        "game_version": game_norm,
                        "pokemon_species": species_slug,
                        "move_name": normalized_move,
                        "source_column": move_column,
                        "reason": "kaggle_move_missing_from_move_reference",
                    }
                )

    diagnostics_path = diagnostics_dir / "kaggle_move_reference_gaps.csv"
    diagnostics_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        missing_rows,
        columns=["game_version", "pokemon_species", "move_name", "source_column", "reason"],
    ).to_csv(diagnostics_path, index=False)

    if missing_rows:
        preview = ", ".join(
            f"{row['game_version']}:{row['pokemon_species']}:{row['move_name']}"
            for row in missing_rows[:20]
        )
        raise ValueError(
            "Kaggle move reference validation failed: "
            f"missing_moves={len(missing_rows)} "
            f"first_20=[{preview}] "
            f"diagnostics={diagnostics_path}"
        )


def _build_boss_compact_tables(
    boss_teams: list[dict[str, Any]],
    move_data: dict[str, Any],
) -> dict[str, list[dict[str, Any]]]:
    del move_data  # Boss teams carry fixed Kaggle moves; they are not expanded into move-option tables.
    source_teams: list[dict[str, Any]] = []
    source_team_members: list[dict[str, Any]] = []
    pokemon_moveset_options: list[dict[str, Any]] = []

    seen_contexts: set[tuple[str, str, int]] = set()

    for team in boss_teams:
        source_team_id = str(team.get("team_id") or "").strip()
        if not source_team_id:
            continue
        game_version = str(team.get("game_version") or "").strip().lower()
        boss_name = str(team.get("boss_name") or team.get("gym") or "").strip().lower()
        source_teams.append(
            {
                "source_team_id": source_team_id,
                "game_version": game_version,
                "team_role": "boss",
                "origin": "kaggle",
                "boss_name": boss_name,
                "starter_base": None,
                "starter_evolved_species": None,
                "progression_source_team_id": None,
                "progression_pool_id": None,
                "avg_level": int(team.get("avg_level") or 0),
                "member_count": len(team.get("pokemon", [])) if isinstance(team.get("pokemon"), list) else 0,
                "is_player_candidate": False,
            }
        )

        members = list(team.get("pokemon", [])) if isinstance(team.get("pokemon"), list) else []
        levels = list(team.get("levels", [])) if isinstance(team.get("levels"), list) else []
        member_ids = list(team.get("pokemon_instance_ids", [])) if isinstance(team.get("pokemon_instance_ids"), list) else []

        for slot, species_raw in enumerate(members, start=1):
            species = str(species_raw or "").strip().lower()
            if not species:
                continue
            level = int(levels[slot - 1] if slot - 1 < len(levels) else team.get("avg_level") or 1)
            member_id = str(member_ids[slot - 1]).strip() if slot - 1 < len(member_ids) else f"{source_team_id}:m{slot}"
            fixed_moves = [
                normalize_move_name(move_name)
                for move_name in (
                    team.get("moves", [])[slot - 1] if slot - 1 < len(team.get("moves", [])) else []
                )
                if normalize_move_name(move_name)
            ]
            source_team_members.append(
                {
                    "team_member_id": member_id,
                    "source_team_id": source_team_id,
                    "game_version": game_version,
                    "team_role": "boss",
                    "origin": "kaggle",
                    "boss_name": boss_name,
                    "slot": slot,
                    "pokemon_species": species,
                    "level": level,
                    "fixed_moves": fixed_moves,
                    "progression_pool_id": None,
                    "is_starter": False,
                }
            )

            context_key = (game_version, species, level)
            context_id = f"ctx:{stable_digest(*context_key, length=20)}"
            if context_key not in seen_contexts:
                seen_contexts.add(context_key)
                pokemon_moveset_options.append(
                    {
                        "moveset_context_id": context_id,
                        "game_version": game_version,
                        "pokemon_species": species,
                        "level": level,
                        "move_policy": "boss-fixed-kaggle-v1",
                        "candidate_move_count": len(fixed_moves),
                    }
                )

    return {
        "source_teams": source_teams,
        "source_team_members": source_team_members,
        "member_move_options": [],
        "pokemon_moveset_options": pokemon_moveset_options,
    }


def _build_boss_team_members_reference_rows(
    boss_teams: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for team in boss_teams:
        boss_id = str(team.get("team_id") or "").strip().lower()
        if not boss_id:
            continue
        game_version = str(team.get("game_version") or "").strip().lower()
        boss_role = str(team.get("team_role") or "boss").strip().lower()
        boss_name = str(team.get("boss_name") or team.get("gym") or "").strip().lower()
        members = list(team.get("pokemon", [])) if isinstance(team.get("pokemon"), list) else []
        levels = list(team.get("levels", [])) if isinstance(team.get("levels"), list) else []
        moves_by_member = list(team.get("moves", [])) if isinstance(team.get("moves"), list) else []

        for slot, species_raw in enumerate(members, start=1):
            species = normalize_species_slug(species_raw)
            if not species:
                continue
            level = int(levels[slot - 1] if slot - 1 < len(levels) else team.get("avg_level") or 1)
            moves = moves_by_member[slot - 1] if slot - 1 < len(moves_by_member) else []
            normalized_moves = [
                normalize_move_name(move)
                for move in (moves if isinstance(moves, list) else [])
                if normalize_move_name(move)
            ]
            if not normalized_moves:
                rows.append(
                    {
                        "boss_id": boss_id,
                        "game_version": game_version,
                        "boss_role": boss_role,
                        "boss_name": boss_name,
                        "slot": slot,
                        "pokemon_species": species,
                        "level": level,
                        "move_name": None,
                        "move_slot": None,
                        "source": "kaggle",
                    }
                )
                continue

            for move_slot, move_name in enumerate(normalized_moves[:4], start=1):
                rows.append(
                    {
                        "boss_id": boss_id,
                        "game_version": game_version,
                        "boss_role": boss_role,
                        "boss_name": boss_name,
                        "slot": slot,
                        "pokemon_species": species,
                        "level": level,
                        "move_name": move_name,
                        "move_slot": move_slot,
                        "source": "kaggle",
                    }
                )
    return rows


def _validate_boss_reference_coverage(
    *,
    boss_team_members_df: pd.DataFrame,
    boss_teams: list[dict[str, Any]],
    pokemon_data_df: pd.DataFrame,
    move_reference_df: pd.DataFrame,
    learnable_moves_df: pd.DataFrame,
    diagnostics_dir: Path,
) -> None:
    pokemon_species = {
        normalize_species_slug(row.get("pokemon_species") or row.get("name") or "")
        for row in pokemon_data_df.to_dict(orient="records")
        if normalize_species_slug(row.get("pokemon_species") or row.get("name") or "")
    }
    moves_in_reference = {
        normalize_move_name(row.get("move_name") or "")
        for row in move_reference_df.to_dict(orient="records")
        if normalize_move_name(row.get("move_name") or "")
    }
    move_profiles = {
        normalize_move_name(row.get("move_name") or ""): row
        for row in move_reference_df.to_dict(orient="records")
        if normalize_move_name(row.get("move_name") or "")
    }
    learnable_pairs = {
        (
            str(row.get("game_version") or "").strip().lower(),
            normalize_species_slug(row.get("pokemon_species") or ""),
            normalize_move_name(row.get("move_name") or ""),
        )
        for row in learnable_moves_df.to_dict(orient="records")
        if str(row.get("game_version") or "").strip()
        and normalize_species_slug(row.get("pokemon_species") or "")
        and normalize_move_name(row.get("move_name") or "")
    }

    coverage_rows: list[dict[str, Any]] = []
    errors: list[str] = []

    if boss_team_members_df.empty:
        errors.append("boss_team_members reference is empty")

    team_member_counts: dict[str, int] = {}
    for team in boss_teams:
        boss_id = str(team.get("team_id") or "").strip().lower()
        members = list(team.get("pokemon", [])) if isinstance(team.get("pokemon"), list) else []
        if boss_id:
            team_member_counts[boss_id] = len([species for species in members if normalize_species_slug(species)])

    missing_member_bosses = sorted(boss_id for boss_id, count in team_member_counts.items() if count <= 0)
    if missing_member_bosses:
        errors.append(f"bosses_without_members={len(missing_member_bosses)} sample={missing_member_bosses[:10]}")

    grouped_move_counts: dict[tuple[str, int, str], int] = defaultdict(int)
    boss_ids_with_member_rows: set[str] = set()
    for row in boss_team_members_df.to_dict(orient="records"):
        boss_id = str(row.get("boss_id") or "").strip().lower()
        slot = int(row.get("slot") or 0)
        species = normalize_species_slug(row.get("pokemon_species") or "")
        move_name = normalize_move_name(row.get("move_name") or "")
        game_version = str(row.get("game_version") or "").strip().lower()
        level = int(row.get("level") or 0)
        if boss_id:
            boss_ids_with_member_rows.add(boss_id)
        if slot <= 0 or not species:
            continue
        member_key = (boss_id, slot, species)

        pokemon_present = species in pokemon_species
        move_present = (move_name in moves_in_reference) if move_name else False
        learnable_present = (game_version, species, move_name) in learnable_pairs if move_name else False

        severity = "OK"
        reason = "complete"
        if not pokemon_present:
            severity = "ERROR"
            reason = "missing_pokemon_data"
        elif not move_present:
            severity = "ERROR"
            reason = "missing_move_reference"
        elif move_name:
            profile = move_profiles.get(move_name, {})
            damage_class = str(profile.get("damage_class") or "").strip().lower()
            move_type = str(profile.get("type") or "").strip().lower()
            if not damage_class or not move_type:
                severity = "ERROR"
                reason = "missing_move_metadata"
            elif not learnable_present:
                severity = "WARN"
                reason = "missing_learnable_pair"

        coverage_rows.append(
            {
                "game_version": game_version,
                "boss_name": str(row.get("boss_name") or "").strip().lower(),
                "pokemon_species": species,
                "level": level if level > 0 else None,
                "move_name": move_name or None,
                "pokemon_in_pokemon_data": pokemon_present,
                "move_in_move_reference": move_present,
                "learnable_pair_present": learnable_present,
                "severity": severity,
                "reason": reason,
            }
        )

        if move_name:
            grouped_move_counts[member_key] += 1

    report_path = diagnostics_dir / "boss_silver_reference_coverage.csv"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        coverage_rows,
        columns=[
            "game_version",
            "boss_name",
            "pokemon_species",
            "level",
            "move_name",
            "pokemon_in_pokemon_data",
            "move_in_move_reference",
            "learnable_pair_present",
            "severity",
            "reason",
        ],
    ).to_csv(report_path, index=False)

    missing_boss_member_rows = sorted(boss_id for boss_id in team_member_counts if boss_id not in boss_ids_with_member_rows)
    if missing_boss_member_rows:
        errors.append(f"bosses_missing_reference_rows={len(missing_boss_member_rows)} sample={missing_boss_member_rows[:10]}")

    invalid_move_count_members = sorted(
        key
        for key, count in grouped_move_counts.items()
        if count < 1 or count > 4
    )
    if invalid_move_count_members:
        errors.append(
            "boss_members_with_invalid_move_count="
            f"{len(invalid_move_count_members)} sample={invalid_move_count_members[:10]}"
        )

    error_rows = [row for row in coverage_rows if row["severity"] == "ERROR"]
    if error_rows:
        sample = ", ".join(
            f"{row['game_version']}:{row['boss_name']}:{row['pokemon_species']}:{row['move_name']}:{row['reason']}"
            for row in error_rows[:20]
        )
        errors.append(f"coverage_errors={len(error_rows)} first_20=[{sample}]")

    if errors:
        raise ValueError(
            "Silver boss reference coverage validation failed: "
            + " | ".join(errors)
            + f" | diagnostics={report_path}"
        )


def build_silver_from_bronze(
    bronze_dir: Path = BRONZE_DIR,
    silver_dir: Path = SILVER_DIR,
    hard_cleanup: bool = False,
) -> None:
    started_at = time.perf_counter()
    stage_durations: dict[str, float] = {}
    ensure_medallion_dirs()

    type_chart_path = bronze_dir / "type_chart.json"
    if not type_chart_path.exists():
        save_as_json(build_type_chart(), type_chart_path)

    silver_dir.mkdir(parents=True, exist_ok=True)
    silver_subdirs = get_silver_subdirs(silver_dir)
    for directory in silver_subdirs.values():
        directory.mkdir(parents=True, exist_ok=True)

    snapshots_dir = silver_subdirs["snapshots"]
    mappings_dir = silver_subdirs["mappings"]
    references_dir = silver_subdirs["references"]
    diagnostics_dir = silver_subdirs["diagnostics"]
    simulation_dir = silver_subdirs["simulation"]

    if hard_cleanup:
        for cleanup_path in (snapshots_dir, mappings_dir, references_dir, diagnostics_dir, simulation_dir):
            _remove_if_exists(cleanup_path)
            cleanup_path.mkdir(parents=True, exist_ok=True)

    location_index_path = bronze_dir / "pokeapi" / "location_index.json"
    bulbapedia_dir = bronze_dir / "bulbapedia"
    if not location_index_path.exists() or not bulbapedia_dir.exists():
        raise FileNotFoundError("Bronze inputs are missing. Run: python -m src.pipeline.run_pipeline layers bronze")

    games_config = get_games_config()
    allowed_versions = {game["game_key"] for game in games_config}
    runtime_team_config = resolve_runtime_team_config()
    runtime_simulation_config = load_runtime_battle_policy_config().__dict__

    state_dir = silver_dir / "_state"
    state_dir.mkdir(parents=True, exist_ok=True)
    state_path = state_dir / "silver_state.json"
    repo_root = Path(__file__).resolve().parents[4]
    code_fingerprint = fingerprint_python_files([
        repo_root / "src" / "pipeline" / "silver",
        repo_root / "src" / "pipeline" / "common" / "simulation_config.py",
        repo_root / "src" / "pipeline" / "settings.py",
    ])

    kaggle_csv_path = bronze_dir / "kagglehub" / "gym_leaders_elite_four.csv"
    current_signature = build_input_signature(
        {
            "location_index": fingerprint_path(location_index_path),
            "bulbapedia": fingerprint_path(bulbapedia_dir),
            "kaggle": fingerprint_path(kaggle_csv_path) if kaggle_csv_path.exists() else None,
            "type_chart": fingerprint_path(type_chart_path),
            "allowed_versions": sorted(allowed_versions),
            "runtime_team_config": runtime_team_config,
            "runtime_simulation_config": runtime_simulation_config,
            "pipeline_code_fingerprint": code_fingerprint,
        }
    )

    previous_state = load_state(state_path)
    expected_outputs = [
        mappings_dir / "location_to_area_map.json",
        mappings_dir / "location_to_pokemon_map.json",
        mappings_dir / "boss_mapping_by_version.json",
        references_dir / "pokemon_reference.parquet",
        references_dir / "pokemon_data.parquet",
        references_dir / "encounter_methods_reference.json",
        references_dir / "games.parquet",
        references_dir / "bosses.parquet",
        references_dir / "locations.parquet",
        references_dir / "encounters.parquet",
        references_dir / "move_reference.parquet",
        references_dir / "learnable_moves.parquet",
        references_dir / "boss_team_members.parquet",
        silver_dir / "manifest.json",
    ]
    expected_snapshot_files = [snapshots_dir / f"{game['game_key']}_boss_snapshots.jsonl" for game in games_config]
    expected_team_shards = [simulation_dir / f"source_teams_{game['game_key']}.parquet" for game in games_config]
    expected_member_shards = [simulation_dir / f"source_team_members_{game['game_key']}.parquet" for game in games_config]
    expected_move_option_shards = [simulation_dir / f"member_moveset_combos_{game['game_key']}.parquet" for game in games_config]

    if previous_state.get("input_signature") == current_signature and all(path.exists() for path in (expected_outputs + expected_snapshot_files + expected_team_shards + expected_member_shards + expected_move_option_shards)):
        logger.info("[silver] incremental skip; input signature unchanged")
        return

    location_index = cast(dict[str, Any], read_json(location_index_path))
    mapper = LocationMapper(location_index)
    kaggle_rows_by_game = load_kaggle_rows_by_game(bronze_dir)
    kaggle_boss_rows_by_game = load_kaggle_boss_rows_by_game(bronze_dir, allowed_versions=allowed_versions)

    parse_started_at = time.perf_counter()
    parse_output = run_parse_stage(game_files=sorted(bulbapedia_dir.glob("*.json")), mapper=mapper, kaggle_rows_by_game=kaggle_rows_by_game)
    stage_durations["parse_stage_s"] = time.perf_counter() - parse_started_at

    all_records = parse_output.all_records
    all_slugs = parse_output.all_slugs
    boss_mapping_by_version = parse_output.boss_mapping_by_version
    records_with_game_keys = parse_output.records_with_game_keys

    mapping_started_at = time.perf_counter()
    area_map, location_pokemon_map = get_location_area_and_pokemon_maps(all_slugs, allowed_versions=allowed_versions, silver_dir=silver_dir, bronze_dir=bronze_dir)
    stage_durations["mapping_stage_s"] = time.perf_counter() - mapping_started_at
    write_json(mappings_dir / "location_to_area_map.json", area_map)
    write_json(mappings_dir / "location_to_pokemon_map.json", location_pokemon_map)

    encounters_file = references_dir / "encounters.jsonl"
    if encounters_file.exists():
        encounters_file.unlink()
    for snapshot_file in snapshots_dir.glob("*_boss_snapshots.jsonl"):
        snapshot_file.unlink()

    all_pokemon_references: dict[str, Any] = {}
    for game_key, records in records_with_game_keys:
        enrich_records_with_location_pokemon(records, location_pokemon_map)
        pokemon_refs = write_normalized_silver(records=records, snapshots_dir=snapshots_dir, encounters_output_path=encounters_file, game_key=game_key)
        if pokemon_refs:
            all_pokemon_references.update(pokemon_refs)

    create_pokemon_reference_index(all_pokemon_references, references_dir)
    create_encounter_methods_reference(all_records, references_dir)
    write_json(mappings_dir / "boss_mapping_by_version.json", boss_mapping_by_version)

    games_table = build_games_table(games_config)
    bosses_table = build_bosses_table(boss_mapping_by_version)
    locations_table = build_locations_table(all_records, area_map, mapper.misses)
    write_parquet(references_dir / "games.parquet", games_table, partition_cols=["region"])
    write_parquet(references_dir / "bosses.parquet", bosses_table, partition_cols=["game_version", "boss_role"])
    write_parquet(references_dir / "locations.parquet", locations_table, partition_cols=["game_version", "mapping_status"])

    encounters_frame = pd.DataFrame()
    if encounters_file.exists():
        encounters_frame = read_jsonl(encounters_file)
        write_parquet(references_dir / "encounters.parquet", encounters_frame, partition_cols=["game"])
    encounters_reference_path = references_dir / "encounters.parquet"
    bosses_reference_path = references_dir / "bosses.parquet"
    encounters_reference_df = read_parquet(encounters_reference_path) if encounters_reference_path.exists() else pd.DataFrame()
    bosses_reference_df = read_parquet(bosses_reference_path) if bosses_reference_path.exists() else pd.DataFrame()

    write_json(diagnostics_dir / "unmapped_locations_detailed.json", mapper.misses)
    write_json(diagnostics_dir / "unmapped_locations_summary.json", summarize_unmapped_locations(mapper.misses))
    write_json(
        diagnostics_dir / "unmapped_locations.json",
        [{"raw_title": miss["raw_title"], "tried_slug": miss["tried_slug"], "reason": miss["reason"]} for miss in mapper.misses],
    )

    move_reference_path = references_dir / "move_reference.parquet"
    learnable_moves_path = references_dir / "learnable_moves.parquet"
    starter_chain_species_by_game = _collect_starter_chain_species_by_game(games_config)
    starter_chain_entries = _starter_chain_bootstrap_entries(starter_chain_species_by_game)
    base_bootstrap_entries = _build_bootstrap_move_entries(records_with_game_keys)
    kaggle_bootstrap_entries = _build_kaggle_bootstrap_entries(kaggle_boss_rows_by_game)
    bootstrap_entries = _dedupe_bootstrap_entries(base_bootstrap_entries + starter_chain_entries + kaggle_bootstrap_entries)
    if not move_reference_path.exists() or not learnable_moves_path.exists():
        bootstrap_stats = bootstrap_move_reference_cache(bootstrap_entries, silver_dir=silver_dir)
        logger.info("[silver] bootstrap move refs entries=%s", bootstrap_stats.get("entry_count", 0))
    if bootstrap_entries:
        persist_move_reference_cache(bootstrap_entries, silver_dir=silver_dir)
    learnable_reference_df = read_parquet(learnable_moves_path) if learnable_moves_path.exists() else pd.DataFrame()
    missing_starter_pairs = _validate_starter_chain_move_coverage(learnable_reference_df, starter_chain_species_by_game, diagnostics_dir)
    if missing_starter_pairs:
        logger.info(
            "[silver/moves] starter coverage gaps detected; refreshing move cache via API missing_pairs=%s",
            len(missing_starter_pairs),
        )
        bootstrap_stats = bootstrap_move_reference_cache(bootstrap_entries, silver_dir=silver_dir)
        logger.info(
            "[silver/moves] starter coverage refresh complete entries=%s target_pairs=%s learnable_rows=%s",
            bootstrap_stats.get("entry_count", 0),
            bootstrap_stats.get("target_pairs", 0),
            bootstrap_stats.get("learnable_rows", 0),
        )
        persist_move_reference_cache(bootstrap_entries, silver_dir=silver_dir)
        learnable_reference_df = read_parquet(learnable_moves_path) if learnable_moves_path.exists() else pd.DataFrame()
        missing_starter_pairs = _validate_starter_chain_move_coverage(learnable_reference_df, starter_chain_species_by_game, diagnostics_dir)

    if missing_starter_pairs:
        preview = ",".join(f"{row['game_version']}:{row['species_name']}" for row in missing_starter_pairs[:20])
        diagnostics_path = diagnostics_dir / "starter_chain_move_gaps.csv"
        raise ValueError(
            "Starter-chain move reference validation failed: "
            f"missing_pairs={len(missing_starter_pairs)} first_20=[{preview}] diagnostics={diagnostics_path}"
        )

    move_reference_df = read_parquet(move_reference_path) if move_reference_path.exists() else pd.DataFrame()
    _validate_kaggle_moves_in_move_reference(kaggle_boss_rows_by_game, move_reference_df, diagnostics_dir)

    reference_context = load_reference_context(silver_dir=silver_dir)
    boss_teams, boss_move_data = extract_boss_teams_from_kaggle_source(bronze_dir, allowed_versions=allowed_versions, reference_context=reference_context)
    _validate_kaggle_boss_move_profiles(boss_move_data, diagnostics_dir)
    all_move_data = dict(boss_move_data)
    kaggle_boss_species, kaggle_boss_moves = _collect_kaggle_boss_species_and_moves(boss_teams)
    boss_team_members_rows = _build_boss_team_members_reference_rows(boss_teams)
    write_parquet(references_dir / "boss_team_members.parquet", boss_team_members_rows, partition_cols=["game_version", "boss_role"])

    for pattern in [
        "source_teams_*.parquet",
        "source_team_members_*.parquet",
        "member_moveset_combos_*.parquet",
        "member_move_options_*.parquet",
        "pokemon_moveset_options_*.parquet",
        "simulation_sampling_plan_*.parquet",
        "pokemon_combat_pool_*.parquet",
    ]:
        for old_file in simulation_dir.glob(pattern):
            old_file.unlink()

    team_values: dict[str, set[str]] = {"team_id": set(), "game_version": set()}
    member_values: dict[str, set[str]] = {"team_member_id": set(), "team_id": set(), "game_version": set()}
    move_values: dict[str, set[str]] = {
        "team_member_id": set(),
        "team_id": set(),
        "move_1": set(),
        "move_2": set(),
        "move_3": set(),
        "move_4": set(),
    }

    total_source_teams = 0
    total_members = 0
    total_moveset_combos = 0
    total_boss_teams = 0
    total_boss_rows_skipped_from_move_options = 0
    total_player_rows_used_for_move_options = 0
    total_fixed_boss_teams_carried_forward = 0
    required_team_species: set[str] = set()
    required_team_moves: set[str] = set()

    boss_teams_by_game: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for team in boss_teams:
        game_version = str(team.get("game_version") or "").strip().lower()
        if game_version:
            boss_teams_by_game[game_version].append(team)

    progression_source_teams_all = build_progression_source_teams_from_encounters(
        encounters_df=encounters_reference_df,
        bosses_df=bosses_reference_df,
        boss_teams=boss_teams,
    )
    progression_source_teams_by_game: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in progression_source_teams_all:
        game_version = str(row.get("game_version") or "").strip().lower()
        if game_version:
            progression_source_teams_by_game[game_version].append(row)

    for game in games_config:
        game_key = str(game.get("game_key") or "").strip().lower()
        if not game_key:
            continue
        paths = _game_output_paths(simulation_dir, game_key)
        boss_teams_game = boss_teams_by_game.get(game_key, [])
        progression_source_teams = progression_source_teams_by_game.get(game_key, [])
        player_compact = build_player_team_compact_tables(progression_source_teams, reference_context)
        boss_compact = _build_boss_compact_tables(boss_teams_game, boss_move_data)

        source_teams_rows = list(boss_compact["source_teams"]) + list(player_compact["source_teams"])
        source_member_rows = list(boss_compact["source_team_members"]) + list(player_compact["source_team_members"])
        source_team_meta = {
            str(row.get("source_team_id") or "").strip(): row
            for row in source_teams_rows
            if str(row.get("source_team_id") or "").strip()
        }
        member_id_to_team_id = {
            str(row.get("team_member_id") or "").strip(): str(row.get("source_team_id") or "").strip()
            for row in source_member_rows
            if str(row.get("team_member_id") or "").strip() and str(row.get("source_team_id") or "").strip()
        }

        boss_rows_skipped = 0
        player_rows_used = 0
        member_move_rows: list[dict[str, Any]] = []
        for row in player_compact["member_move_options"]:
            team_id = str(row.get("source_team_id") or "").strip()
            meta = source_team_meta.get(team_id, {})
            team_role = str(meta.get("team_role") or "").strip().lower()
            origin = str(meta.get("origin") or "").strip().lower()
            if team_role == "boss" or origin == "kaggle":
                boss_rows_skipped += 1
                continue
            if bool(meta.get("is_player_candidate")) and team_role == "player":
                member_move_rows.append(row)
                player_rows_used += 1
                continue
            boss_rows_skipped += 1

        member_moveset_combo_rows: list[dict[str, Any]] = []
        for row in player_compact["member_moveset_combos"]:
            team_id = str(row.get("team_id") or "").strip()
            if not team_id:
                team_id = member_id_to_team_id.get(str(row.get("pokemon_instance_id") or "").strip(), "")
            meta = source_team_meta.get(team_id, {})
            team_role = str(meta.get("team_role") or "").strip().lower()
            origin = str(meta.get("origin") or "").strip().lower()
            if team_role == "boss" or origin == "kaggle" or (meta and not bool(meta.get("is_player_candidate", False))):
                continue
            if meta and team_role != "player":
                continue
            member_moveset_combo_rows.append(row)

        pokemon_moveset_rows = list(boss_compact["pokemon_moveset_options"]) + list(player_compact["pokemon_moveset_options"])
        sampling_rows = list(player_compact["simulation_sampling_plan"])

        write_parquet(paths["source_teams"], source_teams_rows)
        write_parquet(paths["source_team_members"], source_member_rows)
        write_parquet(paths["member_moveset_combos"], member_moveset_combo_rows)
        write_parquet(paths["member_move_options"], member_move_rows)
        write_parquet(paths["pokemon_moveset_options"], pokemon_moveset_rows)
        write_parquet(paths["simulation_sampling_plan"], sampling_rows)

        combat_rows: list[dict[str, Any]] = []
        combat_seen: set[tuple[str, str, int]] = set()
        for row in source_member_rows:
            gv = str(row.get("game_version") or "").strip().lower()
            sp = str(row.get("pokemon_species") or "").strip().lower()
            lv = int(row.get("level") or 0)
            key = (gv, sp, lv)
            if gv and sp and lv > 0 and key not in combat_seen:
                combat_seen.add(key)
                combat_rows.append({"game_version": gv, "pokemon_species": sp, "level": lv})
        write_parquet(paths["combat_pool"], combat_rows)

        team_values["team_id"].update(str(row.get("source_team_id") or "").strip().lower() for row in source_teams_rows if str(row.get("source_team_id") or "").strip())
        team_values["game_version"].update(str(row.get("game_version") or "").strip().lower() for row in source_teams_rows if str(row.get("game_version") or "").strip())

        member_values["team_member_id"].update(str(row.get("team_member_id") or "").strip().lower() for row in source_member_rows if str(row.get("team_member_id") or "").strip())
        member_values["team_id"].update(str(row.get("source_team_id") or "").strip().lower() for row in source_member_rows if str(row.get("source_team_id") or "").strip())
        member_values["game_version"].update(str(row.get("game_version") or "").strip().lower() for row in source_member_rows if str(row.get("game_version") or "").strip())

        move_values["team_member_id"].update(
            str(row.get("pokemon_instance_id") or "").strip().lower()
            for row in member_moveset_combo_rows
            if str(row.get("pokemon_instance_id") or "").strip()
        )
        move_values["team_id"].update(
            str(row.get("team_id") or "").strip().lower()
            for row in member_moveset_combo_rows
            if str(row.get("team_id") or "").strip()
        )
        required_team_species.update(
            normalize_species_slug(row.get("pokemon_species") or "")
            for row in source_member_rows
            if normalize_species_slug(row.get("pokemon_species") or "")
        )
        for move_col in ("move_1", "move_2", "move_3", "move_4"):
            normalized_moves = {
                normalize_move_name(row.get(move_col) or "")
                for row in member_moveset_combo_rows
                if normalize_move_name(row.get(move_col) or "")
            }
            move_values[move_col].update(normalized_moves)
            required_team_moves.update(normalized_moves)

        total_source_teams += len(source_teams_rows)
        total_members += len(source_member_rows)
        total_moveset_combos += len(member_moveset_combo_rows)
        total_boss_teams += len(boss_teams_game)
        total_boss_rows_skipped_from_move_options += boss_rows_skipped
        total_player_rows_used_for_move_options += player_rows_used
        total_fixed_boss_teams_carried_forward += len(boss_compact["source_teams"])

        boss_move_option_rows = sum(
            1
            for row in member_move_rows
            if str(source_team_meta.get(str(row.get("source_team_id") or "").strip(), {}).get("team_role") or "").strip().lower() == "boss"
        )
        boss_moveset_combo_rows = sum(
            1
            for row in member_moveset_combo_rows
            if str(source_team_meta.get(str(row.get("team_id") or "").strip(), {}).get("team_role") or "").strip().lower() == "boss"
        )
        if boss_move_option_rows or boss_moveset_combo_rows:
            raise ValueError(
                f"Boss rows leaked into compact move tables for game={game_key}: "
                f"member_move_options_boss_rows={boss_move_option_rows} "
                f"member_moveset_combos_boss_rows={boss_moveset_combo_rows}"
            )

        logger.info(
            "[silver] wrote compact team shards game=%s source_teams=%s members=%s moveset_combos=%s move_options=%s "
            "boss_rows_skipped_from_move_option_generation=%s player_rows_used_for_move_option_generation=%s fixed_boss_teams_carried_forward=%s",
            game_key,
            len(source_teams_rows),
            len(source_member_rows),
            len(member_moveset_combo_rows),
            len(member_move_rows),
            boss_rows_skipped,
            player_rows_used,
            len(boss_compact["source_teams"]),
        )

        gc.collect()

    restricted_encounter_species = {
        normalize_species_slug(species)
        for species in all_pokemon_references.keys()
        if normalize_species_slug(species)
    }
    total_required_species = restricted_encounter_species | kaggle_boss_species | required_team_species
    total_required_moves = required_team_moves | kaggle_boss_moves

    move_reference_df = read_parquet(move_reference_path) if move_reference_path.exists() else pd.DataFrame()
    move_reference_profiles = _move_profiles_from_reference(move_reference_df)

    logger.info(
        "[silver/reference_enrichment] restricted_encounter_species=%s kaggle_boss_species=%s total_required_species=%s "
        "learnable_reference_moves=%s kaggle_boss_moves=%s total_required_moves=%s",
        len(restricted_encounter_species),
        len(kaggle_boss_species),
        len(total_required_species),
        len(move_reference_profiles),
        len(kaggle_boss_moves),
        len(total_required_moves),
    )

    pokemon_data_path = references_dir / "pokemon_data.parquet"
    pokemon_data_df, pokemon_diagnostics = _build_enriched_pokemon_profiles(all_pokemon_references, total_required_species)
    if pokemon_diagnostics:
        write_parquet(diagnostics_dir / "pokemon_profile_fetch_errors.parquet", pokemon_diagnostics)
    exact_matches = 0
    species_default_matches = 0
    alias_matches = 0
    unresolved_names: list[str] = []
    if not pokemon_data_df.empty and "resolution_method" in pokemon_data_df.columns:
        resolution_counts = pokemon_data_df["resolution_method"].astype("string").value_counts(dropna=True).to_dict()
        exact_matches = int(resolution_counts.get("pokemon_exact", 0))
        species_default_matches = int(resolution_counts.get("species_default_variety", 0))
        alias_matches = int(resolution_counts.get("alias_species_default_variety", 0))
    if pokemon_diagnostics:
        unresolved_names = sorted(
            {
                _normalize_requested_pokemon_name(item.get("normalized_requested_name") or item.get("requested_pokemon_name") or "")
                for item in pokemon_diagnostics
                if _normalize_requested_pokemon_name(item.get("normalized_requested_name") or item.get("requested_pokemon_name") or "")
            }
        )
    logger.info(
        "[silver/reference_enrichment] pokemon_resolution_summary exact=%s species_default=%s alias_fallback=%s unresolved=%s unresolved_names=%s",
        exact_matches,
        species_default_matches,
        alias_matches,
        len(unresolved_names),
        ",".join(unresolved_names[:20]),
    )
    _validate_and_persist_pokemon_data_contract(pokemon_data_df, diagnostics_dir, total_required_species)
    write_parquet(pokemon_data_path, pokemon_data_df)
    create_pokemon_reference_index(
        {
            str(row.get("pokemon_species") or "").strip().lower(): {
                "name": str(row.get("name") or row.get("pokemon_species") or "").strip().lower(),
                "url": str(row.get("source_url") or "").strip() or None,
            }
            for row in pokemon_data_df.to_dict(orient="records")
            if str(row.get("pokemon_species") or "").strip()
        },
        references_dir,
    )

    all_move_data = _ensure_moves_in_combat_profiles(all_move_data, total_required_moves, move_reference_profiles)
    write_validated_move_data(simulation_dir / "move_data.parquet", all_move_data, chunk_threshold=120_000, chunk_size=40_000)

    available_species = {
        normalize_species_slug(row.get("pokemon_species") or row.get("name") or "")
        for row in pokemon_data_df.to_dict(orient="records")
        if normalize_species_slug(row.get("pokemon_species") or row.get("name") or "")
    }
    available_moves: set[str] = set()
    for payload in all_move_data.values():
        move_details = payload.get("move_details")
        if not isinstance(move_details, dict):
            continue
        for move_name in move_details.keys():
            move_slug = normalize_move_name(move_name)
            if move_slug:
                available_moves.add(move_slug)

    missing_profile_species = sorted(species for species in required_team_species if species not in available_species)
    missing_profile_moves = sorted(move for move in required_team_moves if move not in available_moves)
    if missing_profile_species or missing_profile_moves:
        raise ValueError(
            "Silver reference enrichment incomplete for local combat profiles: "
            f"missing_species={len(missing_profile_species)} missing_moves={len(missing_profile_moves)} "
            f"sample_species=[{','.join(missing_profile_species[:10])}] sample_moves=[{','.join(missing_profile_moves[:10])}]"
        )

    logger.info(
        "[silver/reference_enrichment] final_pokemon_profiles=%s final_move_profiles=%s",
        len(available_species),
        len(available_moves),
    )
    learnable_reference_df = read_parquet(learnable_moves_path) if learnable_moves_path.exists() else pd.DataFrame()
    _validate_boss_reference_coverage(
        boss_team_members_df=pd.DataFrame(boss_team_members_rows),
        boss_teams=boss_teams,
        pokemon_data_df=pokemon_data_df,
        move_reference_df=move_reference_df,
        learnable_moves_df=learnable_reference_df,
        diagnostics_dir=diagnostics_dir,
    )

    relational_report = validate_normalized_silver_tables(
        {
            "games": pd.DataFrame(games_table),
            "bosses": pd.DataFrame(bosses_table),
            "locations": pd.DataFrame(locations_table),
            "encounters": encounters_frame,
            "teams": _validation_profile(team_values, total_source_teams),
            "team_members": _validation_profile(member_values, total_members),
            "team_member_moves": _validation_profile(move_values, total_moveset_combos),
            "move_reference": move_reference_df,
            "learnable_moves": learnable_reference_df,
            "pokemon_data": pokemon_data_df,
        }
    )
    write_json(diagnostics_dir / "relational_validation.json", relational_report.as_dict())
    if not relational_report.is_valid:
        raise ValueError("Silver relational validation failed; see diagnostics/relational_validation.json")

    create_silver_manifest(silver_dir)

    save_state(
        state_path,
        {
            "input_signature": current_signature,
            "updated_at": time.time(),
            "games_processed": len(records_with_game_keys),
            "boss_teams": total_boss_teams,
            "source_teams": total_source_teams,
            "source_team_members": total_members,
            "member_moveset_combos": total_moveset_combos,
            "runtime_team_config": runtime_team_config,
            "runtime_simulation_config": runtime_simulation_config,
            "pipeline_code_fingerprint": code_fingerprint,
        },
    )

    write_json(
        diagnostics_dir / "performance_summary.json",
        {
            "generated_at_epoch_s": time.time(),
            "stage_durations_s": stage_durations,
            "totals": {
                "games_processed": len(records_with_game_keys),
                "boss_teams": total_boss_teams,
                "source_teams": total_source_teams,
                "source_team_members": total_members,
                "member_moveset_combos": total_moveset_combos,
            },
        },
    )

    logger.info(
        "[silver] build finished records=%s source_teams=%s source_team_members=%s member_moveset_combos=%s "
        "move_option_generation_boss_rows_skipped=%s move_option_generation_player_rows_used=%s fixed_boss_teams_carried_forward=%s "
        "unmapped=%s elapsed_s=%.2f",
        len(all_records),
        total_source_teams,
        total_members,
        total_moveset_combos,
        total_boss_rows_skipped_from_move_options,
        total_player_rows_used_for_move_options,
        total_fixed_boss_teams_carried_forward,
        len(mapper.misses),
        time.perf_counter() - started_at,
    )

def _validate_kaggle_boss_move_profiles(move_data: dict[str, Any], diagnostics_dir: Path) -> None:
    missing_rows: list[dict[str, Any]] = []

    for pokemon_instance_id, payload in move_data.items():
        provided_moves = payload.get("provided_moves") or []
        move_details = payload.get("move_details") or {}

        for move_name in provided_moves:
            normalized_move = normalize_move_name(move_name)
            if normalized_move and normalized_move not in move_details:
                missing_rows.append(
                    {
                        "pokemon_instance_id": pokemon_instance_id,
                        "species": payload.get("species"),
                        "game_version": payload.get("game_version"),
                        "move_name": normalized_move,
                        "reason": "provided_kaggle_move_missing_profile",
                    }
                )

    diagnostics_path = diagnostics_dir / "kaggle_boss_move_profile_gaps.csv"
    diagnostics_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        missing_rows,
        columns=["pokemon_instance_id", "species", "game_version", "move_name", "reason"],
    ).to_csv(diagnostics_path, index=False)

    if missing_rows:
        preview = ", ".join(
            f"{row['game_version']}:{row['species']}:{row['move_name']}"
            for row in missing_rows[:20]
        )
        raise ValueError(
            "Kaggle boss move reference validation failed: "
            f"missing_move_profiles={len(missing_rows)} "
            f"first_20=[{preview}] "
            f"diagnostics={diagnostics_path}"
        )
if __name__ == "__main__":
    build_silver_from_bronze()
