"""Build a web-friendly payload: best team per walkthrough boss and version."""
from __future__ import annotations

import logging
import math
import re
from pathlib import Path
from typing import Any, NoReturn, cast

import numpy as np
import pandas as pd

from src.pipeline.common.cast import to_bool, to_float, to_int
from src.pipeline.common.contracts import format_contract_error
from src.pipeline.common.io import read_jsonl, read_parquet, write_json
from src.pipeline.common.manifest import (
    ManifestResolutionError,
    load_manifest,
    resolve_manifest_dataset_path,
)
from src.pipeline.common.normalize import normalize_slug
from src.pipeline.gold.inputs.team_tables import load_reconstructed_teams_from_silver
from src.pipeline.silver.config.game_config import get_games_config, get_starter_family_members
from src.pipeline.settings import SILVER_DIR, GOLD_DIR


logger = logging.getLogger(__name__)


class GoldWebContractError(ValueError):
    """Raised when Gold walkthrough inputs violate Silver manifest contract."""


def _raise_web_contract_error(code: str, message: str, *, dataset: str | None = None, path: Path | None = None) -> NoReturn:
    raise GoldWebContractError(
        format_contract_error(
            prefix="gold.contract.web",
            code=code,
            message=message,
            dataset=dataset,
            path=path,
        )
    )


def _norm_name(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.lower())


def _species_slug(value: str) -> str:
    slug = normalize_slug(value)
    # Keep canonical PokeAPI naming for punctuation-heavy species names.
    if slug == "mr-mime":
        return "mr-mime"
    return slug


def _extract_pokeid_from_url(url: str) -> int | None:
    match = re.search(r"(?:/pokemon/|pokebase://pokemon/)(\d+)/?", str(url))
    if not match:
        return None
    try:
        return int(match.group(1))
    except Exception:
        return None


def _safe_int(value: Any) -> int | None:
    return to_int(value, default=None)



def _safe_float(value: Any) -> float | None:
    return to_float(value, default=None, finite_only=True)


def _safe_bool(value: Any, *, default: bool = False) -> bool:
    return to_bool(
        value,
        default=default,
        truthy={"true", "1", "yes", "y", "ja"},
        falsy={"false", "0", "no", "n", "nein"},
    )


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    if isinstance(value, np.ndarray):
        return [_json_safe(v) for v in value.tolist()]
    if isinstance(value, float):
        return None if math.isnan(value) or math.isinf(value) else value
    if isinstance(value, (np.integer, np.floating)):
        scalar = value.item()
        if isinstance(scalar, float) and (math.isnan(scalar) or math.isinf(scalar)):
            return None
        return scalar
    if isinstance(value, np.bool_):
        return bool(value)
    return value


def _build_sprite_fields_from_url(species_name: str, pokemon_url: str | None) -> tuple[str | None, str | None, int | None, str]:
    species_slug = _species_slug(species_name)
    source_url = str(pokemon_url or f"pokebase://pokemon/{species_slug}")
    pokeid = _extract_pokeid_from_url(source_url)
    sprite_url = (
        f"https://raw.githubusercontent.com/PokeAPI/sprites/master/sprites/pokemon/{pokeid}.png"
        if pokeid is not None
        else None
    )
    return sprite_url, source_url, pokeid, species_slug


def _with_sprite_fields(pokemon_entry: Any, pokemon_reference: dict[str, dict[str, Any]]) -> dict[str, Any]:
    if isinstance(pokemon_entry, dict):
        enriched = dict(pokemon_entry)
    else:
        enriched = {"name": str(pokemon_entry)}

    name = enriched.get("name")
    if not isinstance(name, str) or not name.strip():
        enriched["sprite_url"] = None
        enriched["sprite_source_url"] = None
        return enriched

    species_norm = _species_slug(name)
    ref_entry = pokemon_reference.get(species_norm, {}) if isinstance(pokemon_reference, dict) else {}
    pokemon_url = ref_entry.get("url") if isinstance(ref_entry, dict) else None
    sprite_url, source_url, pokeid, species_slug = _build_sprite_fields_from_url(name, pokemon_url)
    enriched["sprite_url"] = sprite_url
    enriched["sprite_source_url"] = source_url
    enriched["pokeid"] = pokeid
    enriched["species_slug"] = species_slug
    return enriched


def _enrich_team_pokemon(team_details: dict[str, Any], pokemon_reference: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    raw_pokemon = team_details.get("details", [])
    if isinstance(raw_pokemon, list) and raw_pokemon:
        enriched_details: list[dict[str, Any]] = []
        for entry in raw_pokemon:
            with_sprite = _with_sprite_fields(entry, pokemon_reference)
            with_sprite["moves"] = [str(move).strip().lower() for move in with_sprite.get("moves", []) if str(move).strip()]
            with_sprite["level"] = _safe_int(with_sprite.get("level"))
            enriched_details.append(with_sprite)
        return enriched_details

    # Fallback for rows that only contain separate arrays.
    names_only = team_details.get("pokemon", [])
    levels = team_details.get("levels", []) if isinstance(team_details.get("levels"), list) else []
    moves = team_details.get("moves", []) if isinstance(team_details.get("moves"), list) else []
    if isinstance(names_only, list) and names_only:
        entries: list[dict[str, Any]] = []
        for idx, name in enumerate(names_only):
            member_moves = moves[idx] if idx < len(moves) and isinstance(moves[idx], list) else []
            entry = {
                "name": str(name),
                "level": _safe_int(levels[idx]) if idx < len(levels) else None,
                "moves": [str(move).strip().lower() for move in member_moves if str(move).strip()],
            }
            entries.append(_with_sprite_fields(entry, pokemon_reference))
        return entries
    return []


def _normalized_team_id(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.lower())


def _candidate_team_ids(team_id: str) -> list[str]:
    candidates = [team_id]
    # STARTER_<version>_<starter>_<base_team_id>
    starter_match = re.match(r"^STARTER_[^_]+_[^_]+_(.+)$", team_id)
    if starter_match:
        candidates.append(starter_match.group(1))
    return candidates


def _starter_name_from_team_id(team_id: str) -> str | None:
    starter_match = re.match(r"^STARTER_[^_]+_([^_]+)_.+$", team_id)
    return starter_match.group(1) if starter_match else None


def _load_pokemon_reference(silver_dir: Path, silver_manifest: dict[str, Any]) -> dict[str, dict[str, Any]]:
    reference_path = _dataset_path_from_manifest(silver_dir, silver_manifest, "pokemon_reference")
    try:
        frame = read_parquet(reference_path)
    except Exception as exc:
        _raise_web_contract_error(
            "invalid_pokemon_reference",
            f"Failed to read pokemon_reference dataset ({exc}).",
            dataset="pokemon_reference",
            path=reference_path,
        )

    normalized: dict[str, dict[str, Any]] = {}
    for row in frame.to_dict(orient="records"):
        species = str(row.get("pokemon_species") or row.get("name") or "").strip().lower()
        key = _species_slug(species)
        if not key:
            continue
        normalized[key] = {
            "url": str(row.get("url") or "").strip() or None,
            "name": str(row.get("name") or species).strip().lower(),
        }
    return normalized


def _load_boss_metadata(silver_dir: Path, silver_manifest: dict[str, Any]) -> dict[tuple[str, str], dict[str, Any]]:
    bosses_path = _dataset_path_from_manifest(silver_dir, silver_manifest, "bosses")
    try:
        frame = read_parquet(bosses_path)
    except Exception as exc:
        _raise_web_contract_error(
            "invalid_bosses_reference",
            f"Failed to read bosses dataset ({exc}).",
            dataset="bosses",
            path=bosses_path,
        )

    metadata: dict[tuple[str, str], dict[str, Any]] = {}
    for row in frame.to_dict(orient="records"):
        game_version = str(row.get("game_version") or "").strip().lower()
        boss_name = str(row.get("boss_name_canonical") or row.get("boss_name") or "").strip()
        if not game_version or not boss_name:
            continue
        metadata[(game_version, _norm_name(boss_name))] = {
            "boss_id": row.get("boss_id"),
            "boss_name_canonical": boss_name,
            "boss_role": str(row.get("boss_role") or "").strip().lower() or None,
            "battle_type": str(row.get("battle_type") or "").strip().lower() or None,
            "location_name": str(row.get("location_name") or "").strip() or None,
            "progression_order": _safe_int(row.get("progression_order")),
            "progression_depth": _safe_float(row.get("progression_depth")),
            "is_branching": _safe_bool(row.get("is_branching")),
            "branch_group": str(row.get("branch_group") or "").strip() or None,
            "branch_condition": str(row.get("branch_condition") or "").strip() or None,
            "starter_dependency_type": str(row.get("starter_dependency_type") or "").strip() or None,
            "has_team_variants": _safe_bool(row.get("has_team_variants")),
            "starter_type": str(row.get("starter_type") or "").strip().lower() or None,
            "starter_condition": str(row.get("starter_condition") or "").strip().lower() or None,
            "is_optional": _safe_bool(row.get("is_optional")),
            "is_postgame": _safe_bool(row.get("is_postgame")),
            "boss_order": _safe_int(row.get("boss_order")),
            "gym_index": _safe_int(row.get("gym_index")),
            "is_simulatable": _safe_bool(row.get("is_simulatable")),
        }
    return metadata


def _load_move_reference(silver_dir: Path, silver_manifest: dict[str, Any]) -> dict[str, dict[str, Any]]:
    move_reference_path = _dataset_path_from_manifest(silver_dir, silver_manifest, "move_reference")
    try:
        frame = read_parquet(move_reference_path)
    except Exception as exc:
        _raise_web_contract_error(
            "invalid_move_reference",
            f"Failed to read move_reference dataset ({exc}).",
            dataset="move_reference",
            path=move_reference_path,
        )

    reference: dict[str, dict[str, Any]] = {}
    for row in frame.to_dict(orient="records"):
        move_name = str(row.get("move_name") or "").strip().lower()
        if not move_name:
            continue
        reference[move_name] = {
            "move_name": move_name,
            "type": str(row.get("type") or "").strip().lower() or None,
            "damage_class": str(row.get("damage_class") or "").strip().lower() or None,
            "power": _safe_int(row.get("power")),
            "accuracy": _safe_int(row.get("accuracy")),
            "pp": _safe_int(row.get("pp")),
            "effective_power": _safe_int(row.get("effective_power")),
            "power_handling": str(row.get("power_handling") or "").strip().lower() or None,
            "is_status_move": _safe_bool(row.get("is_status_move")),
            "is_damage_move": _safe_bool(row.get("is_damage_move")),
        }
    return reference


def _boss_special_tags(boss_meta: dict[str, Any]) -> list[str]:
    tags: list[str] = []
    boss_role = str(boss_meta.get("boss_role") or "")
    if boss_role == "elite_four":
        tags.append("Elite Four")
    elif boss_role == "champion":
        tags.append("Champion")

    battle_type = str(boss_meta.get("battle_type") or "")
    if battle_type == "double":
        tags.append("Double Battle")

    if boss_meta.get("is_branching"):
        condition = str(boss_meta.get("branch_condition") or "").replace("_", " ").strip()
        if condition:
            tags.append(f"Branching: {condition}")
        else:
            tags.append("Branching")

    if boss_meta.get("has_team_variants"):
        tags.append("Team Variants")

    starter_dependency = str(boss_meta.get("starter_dependency_type") or "")
    if starter_dependency == "team_variant":
        tags.append("Starter Variant Boss")
    elif starter_dependency:
        tags.append(f"Starter Rule: {starter_dependency.replace('_', ' ')}")

    if boss_meta.get("is_postgame"):
        tags.append("Postgame")
    if boss_meta.get("is_optional"):
        tags.append("Optional")
    return tags


def _load_boss_team_payloads(
    silver_dir: Path,
    silver_manifest: dict[str, Any],
    pokemon_reference: dict[str, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    boss_teams_path = _dataset_path_from_manifest(silver_dir, silver_manifest, "boss_teams")
    try:
        frame = read_parquet(boss_teams_path)
    except Exception as exc:
        _raise_web_contract_error(
            "invalid_boss_teams_reference",
            f"Failed to read boss_teams dataset ({exc}).",
            dataset="boss_teams",
            path=boss_teams_path,
        )

    payloads: dict[str, dict[str, Any]] = {}
    grouped = frame.sort_values(["boss_team_id", "pokemon_slot"]).groupby("boss_team_id", dropna=False)
    for boss_team_id, group in grouped:
        team_id = str(boss_team_id or "").strip()
        if not team_id:
            continue
        rows = group.to_dict(orient="records")
        first = rows[0]
        pokemon: list[dict[str, Any]] = []
        for row in rows:
            mon = _with_sprite_fields({"name": str(row.get("pokemon_species") or "")}, pokemon_reference)
            mon["slot"] = _safe_int(row.get("pokemon_slot"))
            mon["level"] = _safe_int(row.get("level"))
            mon["moves"] = [
                str(row.get(f"move_{idx}") or "").strip().lower()
                for idx in range(1, 5)
                if str(row.get(f"move_{idx}") or "").strip()
            ]
            mon["item"] = str(row.get("item") or "").strip().lower() or None
            mon["ability"] = str(row.get("ability") or "").strip().lower() or None
            pokemon.append(mon)

        payloads[team_id] = {
            "team_id": team_id,
            "boss_id": first.get("boss_id"),
            "boss_name": first.get("boss_name"),
            "boss_role": first.get("boss_role"),
            "battle_type": first.get("battle_type"),
            "progression_order": _safe_int(first.get("progression_order")),
            "progression_depth": first.get("progression_depth"),
            "branch_condition": first.get("branch_condition"),
            "branch_group": first.get("branch_group"),
            "is_optional": _safe_bool(first.get("is_optional")),
            "is_postgame": _safe_bool(first.get("is_postgame")),
            "team_variant": first.get("team_variant"),
            "starter_type": first.get("starter_type"),
            "variant_dimension": first.get("variant_dimension"),
            "gym_or_stage": first.get("gym_or_stage"),
            "source_dataset": first.get("source_dataset"),
            "harmonization_status": first.get("harmonization_status"),
            "pokemon": pokemon,
        }
    return payloads


def _load_boss_team_variants(
    silver_dir: Path,
    silver_manifest: dict[str, Any],
    pokemon_reference: dict[str, dict[str, Any]],
) -> dict[tuple[str, str, str], list[dict[str, Any]]]:
    boss_teams_path = _dataset_path_from_manifest(silver_dir, silver_manifest, "boss_teams")
    frame = read_parquet(boss_teams_path)
    for column in ("team_variant", "variant_dimension", "starter_type", "harmonization_status", "gym_or_stage", "ability", "item"):
        if column not in frame.columns:
            frame[column] = None
    variants: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    grouped = frame.sort_values(["game_version", "boss_name", "boss_role", "team_variant", "pokemon_slot"]).groupby(
        ["game_version", "boss_name", "boss_role", "team_variant"],
        dropna=False,
    )
    for (game_version, boss_name, boss_role, _team_variant), group in grouped:
        rows = group.to_dict(orient="records")
        first = rows[0]
        payload = {
            "team_id": str(first.get("boss_team_id") or "").strip() or None,
            "boss_name": first.get("boss_name"),
            "boss_role": first.get("boss_role"),
            "battle_type": first.get("battle_type"),
            "team_variant": first.get("team_variant"),
            "starter_type": str(first.get("starter_type") or "").strip().lower() or None,
            "variant_dimension": first.get("variant_dimension"),
            "gym_or_stage": first.get("gym_or_stage"),
            "harmonization_status": first.get("harmonization_status"),
            "pokemon": [],
        }
        for row in rows:
            mon = _with_sprite_fields({"name": str(row.get("pokemon_species") or "")}, pokemon_reference)
            mon["slot"] = _safe_int(row.get("pokemon_slot"))
            mon["level"] = _safe_int(row.get("level"))
            mon["moves"] = [
                str(row.get(f"move_{idx}") or "").strip().lower()
                for idx in range(1, 5)
                if str(row.get(f"move_{idx}") or "").strip()
            ]
            mon["item"] = str(row.get("item") or "").strip().lower() or None
            mon["ability"] = str(row.get("ability") or "").strip().lower() or None
            payload["pokemon"].append(mon)

        key = (str(game_version).strip().lower(), _norm_name(str(boss_name)), str(boss_role).strip().lower())
        variants.setdefault(key, []).append(payload)
    return variants


def _encounter_boss_key(value: Any) -> str:
    raw = str(value or "").strip().lower()
    if not raw:
        return ""
    if raw.startswith("boss:"):
        raw = raw[5:]
    parts = [part for part in raw.split(":") if part]
    if len(parts) >= 3:
        return ":".join(parts[:-1])
    return raw


def _load_encounter_maps(
    silver_dir: Path,
    silver_manifest: dict[str, Any],
) -> tuple[dict[tuple[str, str], dict[str, Any]], dict[tuple[str, str], dict[str, Any]]]:
    encounters_path = _dataset_path_from_manifest(silver_dir, silver_manifest, "encounters")
    if encounters_path.suffix == ".jsonl":
        frame = read_jsonl(encounters_path)
    else:
        frame = read_parquet(encounters_path)
    if frame.empty:
        return {}, {}

    encounter_maps: dict[tuple[str, str], dict[str, Any]] = {}
    encounter_maps_by_name: dict[tuple[str, str], dict[str, Any]] = {}
    grouped = frame.groupby(["game", "boss_id"], dropna=False)
    for (game_version, boss_id), group in grouped:
        version = str(game_version or "").strip().lower()
        boss_key = _encounter_boss_key(boss_id)
        if not version or not boss_key:
            continue
        location_to_species: dict[str, set[str]] = {}
        location_to_encounters: dict[str, list[dict[str, Any]]] = {}
        for row in group.to_dict(orient="records"):
            location = str(row.get("location") or "").strip().lower()
            species = str(row.get("pokemon") or "").strip().lower()
            if not location or not species:
                continue
            location_to_species.setdefault(location, set()).add(species)
            methods = row.get("methods")
            if isinstance(methods, np.ndarray):
                methods = methods.tolist()
            if not isinstance(methods, list):
                methods = []
            location_to_encounters.setdefault(location, []).append(
                {
                    "species": species,
                    "encounter_chance_max": _safe_int(row.get("encounter_chance_max")),
                    "capture_rate": _safe_int(row.get("capture_rate")),
                    "level_min": _safe_int(row.get("level_min")),
                    "level_max": _safe_int(row.get("level_max")),
                    "encounter_methods": [str(method).strip().lower() for method in methods if str(method).strip()],
                }
            )
        location_map = {location: sorted(species_set) for location, species_set in sorted(location_to_species.items())}
        encounter_maps[(version, boss_key)] = {
            "reachable_location_pokemon": location_map,
            "reachable_location_encounters": dict(sorted(location_to_encounters.items())),
            "catchable_locations_by_pokemon": _invert_location_species_map(location_map),
            "location_count": len(location_map),
            "reachable_pokemon_count": len(_invert_location_species_map(location_map)),
        }
        boss_name_key = _norm_name(boss_key.split(":", 1)[1] if ":" in boss_key else boss_key)
        if boss_name_key and (version, boss_name_key) not in encounter_maps_by_name:
            encounter_maps_by_name[(version, boss_name_key)] = encounter_maps[(version, boss_key)]
    return encounter_maps, encounter_maps_by_name


def _team_combo_key(team_payload: dict[str, Any]) -> str:
    members: list[str] = []
    for member in team_payload.get("pokemon", []):
        if not isinstance(member, dict):
            continue
        name = _norm_name(str(member.get("name") or ""))
        moves = sorted(
            _norm_name(str(move))
            for move in (member.get("moves") or [])
            if str(move).strip()
        )
        if not name:
            continue
        members.append(f"{name}|{','.join(moves)}")
    if not members:
        return f"team_id:{_normalized_team_id(str(team_payload.get('team_id') or ''))}"
    return "combo:" + ";".join(sorted(members))


def _payload_score_key(team_payload: dict[str, Any]) -> tuple[float, float, float, str]:
    rank = team_payload.get("rank_in_boss_version")
    rank_val = float(rank) if isinstance(rank, (int, float)) else float("inf")
    win_rate = float(team_payload.get("mc_win_rate") or 0.0)
    wins = float(team_payload.get("wins") or 0.0)
    team_id = str(team_payload.get("team_id") or "")
    return (rank_val, -win_rate, -wins, team_id)


def _dedupe_team_payloads(team_payloads: list[dict[str, Any]]) -> list[dict[str, Any]]:
    best_by_combo: dict[str, dict[str, Any]] = {}
    for payload in team_payloads:
        combo_key = _team_combo_key(payload)
        current = best_by_combo.get(combo_key)
        if current is None or _payload_score_key(payload) < _payload_score_key(current):
            best_by_combo[combo_key] = payload
    return sorted(best_by_combo.values(), key=_payload_score_key)


def _family_key_for_species(species_name: str) -> str:
    species = _norm_name(species_name)
    return species


def _team_species_set(team_payload: dict[str, Any]) -> set[str]:
    species: set[str] = set()
    for member in team_payload.get("pokemon", []):
        if not isinstance(member, dict):
            continue
        name = str(member.get("name") or "").strip().lower()
        if name:
            species.add(name)
    return species


def _team_family_set(team_payload: dict[str, Any]) -> set[str]:
    return {_family_key_for_species(species) for species in _team_species_set(team_payload)}


def _build_realism_lookup(location_to_encounters: dict[str, list[dict[str, Any]]]) -> dict[str, tuple[float, float]]:
    by_species: dict[str, tuple[float, float]] = {}
    for entries in location_to_encounters.values():
        if not isinstance(entries, list):
            continue
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            species = str(entry.get("species") or "").strip().lower()
            if not species:
                continue
            chance = float(_safe_int(entry.get("encounter_chance_max")) or 0) / 100.0
            capture = float(_safe_int(entry.get("capture_rate")) or 0) / 255.0
            prev = by_species.get(species)
            if prev is None:
                by_species[species] = (chance, capture)
            else:
                by_species[species] = (max(prev[0], chance), max(prev[1], capture))
    return by_species


def _team_realism_score(team_payload: dict[str, Any], realism_lookup: dict[str, tuple[float, float]]) -> float:
    species = _team_species_set(team_payload)
    if not species:
        return 0.0
    values: list[float] = []
    for member_species in species:
        chance, capture = realism_lookup.get(member_species, (0.0, 0.0))
        values.append((0.6 * chance) + (0.4 * capture))
    return float(sum(values) / max(1, len(values)))


def _select_diverse_team_payloads(
    team_payloads: list[dict[str, Any]],
    *,
    limit: int,
    progression_depth: float | None = None,
    realism_lookup: dict[str, tuple[float, float]] | None = None,
) -> list[dict[str, Any]]:
    if limit <= 0 or not team_payloads:
        return []
    deduped = _dedupe_team_payloads(team_payloads)
    if len(deduped) <= limit:
        return deduped

    clamped_depth = max(0.0, min(1.0, float(progression_depth if progression_depth is not None else 0.5)))
    early_weight = 1.0 - clamped_depth
    realism_lookup = realism_lookup or {}
    selected: list[dict[str, Any]] = []
    used_species: set[str] = set()
    used_families: set[str] = set()
    remaining = list(deduped)

    while remaining and len(selected) < limit:
        best_idx = 0
        best_score = float("-inf")
        for idx, payload in enumerate(remaining):
            win_rate = float(payload.get("mc_win_rate") or 0.0)
            wins = float(payload.get("wins") or 0.0)
            species = _team_species_set(payload)
            families = _team_family_set(payload)
            species_overlap = (len(species & used_species) / max(1, len(species))) if species else 0.0
            family_overlap = (len(families & used_families) / max(1, len(families))) if families else 0.0
            novelty_bonus = 1.0 - (0.55 * species_overlap + 0.45 * family_overlap)
            realism = _team_realism_score(payload, realism_lookup)
            score = (
                (1.15 - 0.15 * early_weight) * win_rate
                + 0.02 * min(wins, 500.0) / 500.0
                + (0.18 + 0.22 * early_weight) * novelty_bonus
                + (0.05 + 0.30 * early_weight) * realism
            )
            if score > best_score:
                best_score = score
                best_idx = idx
        pick = remaining.pop(best_idx)
        selected.append(pick)
        used_species.update(_team_species_set(pick))
        used_families.update(_team_family_set(pick))
    return selected


def _invert_location_species_map(location_map: Any) -> dict[str, list[str]]:
    if not isinstance(location_map, dict):
        return {}
    by_species: dict[str, set[str]] = {}
    for location, species_list in location_map.items():
        if not isinstance(species_list, list):
            continue
        for species in species_list:
            species_norm = str(species).strip().lower()
            if not species_norm:
                continue
            by_species.setdefault(species_norm, set()).add(str(location).strip().lower())
    return {species: sorted(locations) for species, locations in sorted(by_species.items())}


def _build_encounter_summary(location_to_encounters: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    if not isinstance(location_to_encounters, dict):
        return {
            "species": [],
            "locations": [],
            "methods": [],
            "species_count": 0,
            "location_count": 0,
        }

    by_species: dict[str, dict[str, Any]] = {}
    location_cards: list[dict[str, Any]] = []
    all_methods: set[str] = set()

    for location, entries in sorted(location_to_encounters.items()):
        if not isinstance(entries, list):
            continue
        location_species: set[str] = set()
        location_methods: set[str] = set()
        level_min: int | None = None
        level_max: int | None = None

        for entry in entries:
            if not isinstance(entry, dict):
                continue
            species = str(entry.get("species") or "").strip().lower()
            if not species:
                continue
            location_species.add(species)

            entry_level_min = _safe_int(entry.get("level_min"))
            entry_level_max = _safe_int(entry.get("level_max"))
            if entry_level_min is not None:
                level_min = entry_level_min if level_min is None else min(level_min, entry_level_min)
            if entry_level_max is not None:
                level_max = entry_level_max if level_max is None else max(level_max, entry_level_max)

            methods_raw = entry.get("encounter_methods")
            methods = [
                str(method).strip().lower()
                for method in methods_raw
                if str(method).strip()
            ] if isinstance(methods_raw, list) else []
            location_methods.update(methods)
            all_methods.update(methods)

            species_entry = by_species.setdefault(
                species,
                {
                    "species": species,
                    "locations": set(),
                    "methods": set(),
                    "encounter_chance_max": None,
                    "capture_rate": None,
                    "level_min": None,
                    "level_max": None,
                },
            )
            cast(set[str], species_entry["locations"]).add(str(location).strip().lower())
            cast(set[str], species_entry["methods"]).update(methods)

            chance_max = _safe_int(entry.get("encounter_chance_max"))
            capture_rate = _safe_int(entry.get("capture_rate"))
            if chance_max is not None:
                current_chance = cast(int | None, species_entry["encounter_chance_max"])
                species_entry["encounter_chance_max"] = chance_max if current_chance is None else max(current_chance, chance_max)
            if capture_rate is not None and species_entry["capture_rate"] is None:
                species_entry["capture_rate"] = capture_rate
            if entry_level_min is not None:
                current_min = cast(int | None, species_entry["level_min"])
                species_entry["level_min"] = entry_level_min if current_min is None else min(current_min, entry_level_min)
            if entry_level_max is not None:
                current_max = cast(int | None, species_entry["level_max"])
                species_entry["level_max"] = entry_level_max if current_max is None else max(current_max, entry_level_max)

        if location_species:
            location_cards.append(
                {
                    "location": str(location).strip().lower(),
                    "species_count": len(location_species),
                    "encounter_count": len([entry for entry in entries if isinstance(entry, dict)]),
                    "species": sorted(location_species),
                    "methods": sorted(location_methods),
                    "level_min": level_min,
                    "level_max": level_max,
                }
            )

    species_cards = [
        {
            "species": species,
            "locations": sorted(cast(set[str], payload["locations"])),
            "location_count": len(cast(set[str], payload["locations"])),
            "methods": sorted(cast(set[str], payload["methods"])),
            "encounter_chance_max": payload["encounter_chance_max"],
            "capture_rate": payload["capture_rate"],
            "level_min": payload["level_min"],
            "level_max": payload["level_max"],
        }
        for species, payload in sorted(by_species.items())
    ]

    return {
        "species": species_cards,
        "locations": location_cards,
        "methods": sorted(all_methods),
        "species_count": len(species_cards),
        "location_count": len(location_cards),
    }


def _to_common_starter_ranking_row(row: dict[str, Any]) -> dict[str, Any]:
    """Normalize starter-ranking rows to the generic ranking schema used by payload builders."""
    return {
        "player_team_id": row.get("player_team_id"),
        "mc_win_rate": row.get("avg_mc_win_rate", row.get("mc_win_rate")),
        "wins": row.get("avg_wins", row.get("wins")),
        "losses": row.get("avg_losses", row.get("losses")),
        "n_trials": row.get("avg_n_trials", row.get("n_trials", row.get("scenario_rows"))),
        "rank_in_boss_version": row.get("rank_in_boss_starter", row.get("rank_in_boss_version")),
        "player_avg_level": row.get("player_avg_level"),
        "boss_avg_level": row.get("boss_avg_level"),
    }


def _coerce_location_pokemon_map(value: Any) -> dict[str, list[str]]:
    if not isinstance(value, dict):
        return {}
    out: dict[str, list[str]] = {}
    for location, species_list in value.items():
        if not isinstance(species_list, list):
            continue
        cleaned = [str(species).strip().lower() for species in species_list if str(species).strip()]
        if cleaned:
            out[str(location).strip().lower()] = cleaned
    return out


def _coerce_location_encounters_map(value: Any) -> dict[str, list[dict[str, Any]]]:
    if not isinstance(value, dict):
        return {}
    out: dict[str, list[dict[str, Any]]] = {}
    for location, entries in value.items():
        if not isinstance(entries, list):
            continue
        cleaned = [entry for entry in entries if isinstance(entry, dict)]
        if cleaned:
            out[str(location).strip().lower()] = cleaned
    return out


def _load_silver_manifest(silver_dir: Path) -> dict[str, Any]:
    manifest_path = silver_dir / "manifest.json"
    try:
        return load_manifest(manifest_path)
    except FileNotFoundError:
        _raise_web_contract_error(
            "missing_manifest",
            "Run Silver first to generate manifest.json.",
            path=manifest_path,
        )
    except Exception as exc:
        _raise_web_contract_error(
            "invalid_manifest_json",
            f"manifest.json is unreadable ({exc}).",
            path=manifest_path,
        )


def _dataset_path_from_manifest(silver_dir: Path, silver_manifest: dict[str, Any], dataset_key: str) -> Path:
    try:
        resolved = resolve_manifest_dataset_path(
            base_dir=silver_dir,
            manifest=silver_manifest,
            dataset_key=dataset_key,
            strict_sharded=False,
        )
    except ValueError:
        _raise_web_contract_error(
            "missing_manifest_datasets",
            "manifest.json requires a top-level datasets object.",
            dataset=dataset_key,
        )
    except ManifestResolutionError as exc:
        if exc.code == "missing_dataset_entry":
            _raise_web_contract_error(
                exc.code,
                f"Add datasets.{dataset_key} to silver/manifest.json.",
                dataset=dataset_key,
            )
        if exc.code == "missing_dataset_file_path":
            _raise_web_contract_error(
                exc.code,
                f"Set datasets.{dataset_key}.file in silver/manifest.json.",
                dataset=dataset_key,
            )
        _raise_web_contract_error(
            exc.code,
            "Regenerate Silver outputs so all required files exist.",
            dataset=dataset_key,
            path=exc.path,
        )
    if not isinstance(resolved, Path):
        _raise_web_contract_error(
            "invalid_dataset_shape",
            f"Expected datasets.{dataset_key}.file for walkthrough payload inputs.",
            dataset=dataset_key,
        )
    return resolved


def build_walkthrough_best_teams_payload(
    silver_dir: Path = SILVER_DIR,
    gold_dir: Path = GOLD_DIR,
) -> Path | None:
    best_by_boss_file = gold_dir / "best_team_by_boss_version.parquet"
    rankings_file = gold_dir / "team_rankings_by_boss_version.parquet"
    rankings_starter_file = gold_dir / "team_rankings_by_boss_version_starter.parquet"
    sequence_rankings_file = gold_dir / "team_rankings_e4_champion_sequence_by_version_starter.parquet"
    if not best_by_boss_file.exists():
        return None

    silver_manifest = _load_silver_manifest(silver_dir)

    best_df = read_parquet(best_by_boss_file)
    teams_df = pd.DataFrame(load_reconstructed_teams_from_silver(silver_dir=silver_dir))
    rankings_df = (
        read_parquet(rankings_file)
        if rankings_file.exists()
        else None
    )
    rankings_starter_df = (
        read_parquet(rankings_starter_file)
        if rankings_starter_file.exists()
        else None
    )
    sequence_rankings_df = (
        read_parquet(sequence_rankings_file)
        if sequence_rankings_file.exists()
        else None
    )

    if best_df.empty or teams_df.empty:
        return None

    team_by_id: dict[str, dict[str, Any]] = {}
    team_by_id_normalized: dict[str, dict[str, Any]] = {}
    merged_team_rows = {
        str(row.get("team_id")): row
        for row in teams_df.to_dict(orient="records")
        if isinstance(row.get("team_id"), str)
    }

    for row in merged_team_rows.values():
        team_id = row.get("team_id")
        if isinstance(team_id, str):
            team_by_id[team_id] = row
            team_by_id_normalized[_normalized_team_id(team_id)] = row

    best_by_key: dict[tuple[str, str], dict[str, Any]] = {}
    for row in best_df.to_dict(orient="records"):
        version = row.get("game_version")
        boss_name = row.get("boss_name")
        if isinstance(version, str) and isinstance(boss_name, str):
            best_by_key[(version, _norm_name(boss_name))] = row

    rankings_by_key: dict[tuple[str, str], list[dict[str, Any]]] = {}
    if rankings_df is not None and not rankings_df.empty:
        ranking_rows = rankings_df.sort_values(
            ["game_version", "boss_name", "rank_in_boss_version", "mc_win_rate"],
            ascending=[True, True, True, False],
        ).to_dict(orient="records")
        for row in ranking_rows:
            version = row.get("game_version")
            boss_name = row.get("boss_name")
            if not isinstance(version, str) or not isinstance(boss_name, str):
                continue
            key = (version, _norm_name(boss_name))
            rankings_by_key.setdefault(key, []).append(row)

    starter_rankings_by_key: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    if rankings_starter_df is not None and not rankings_starter_df.empty:
        starter_version_col = "effective_game_version" if "effective_game_version" in rankings_starter_df.columns else "game_version"
        starter_boss_col = "effective_boss_name" if "effective_boss_name" in rankings_starter_df.columns else "boss_name"
        starter_rank_col = "rank_in_boss_starter" if "rank_in_boss_starter" in rankings_starter_df.columns else "rank_in_boss_version"
        starter_rows = rankings_starter_df.sort_values(
            [starter_version_col, starter_boss_col, "starter_base", starter_rank_col],
            ascending=[True, True, True, True],
        ).to_dict(orient="records")
        for row in starter_rows:
            version = row.get(starter_version_col)
            boss_name = row.get(starter_boss_col)
            starter_base = row.get("starter_base")
            if not isinstance(version, str) or not isinstance(boss_name, str) or not isinstance(starter_base, str):
                continue
            key = (version, _norm_name(boss_name), starter_base)
            starter_rankings_by_key.setdefault(key, []).append(row)

    def _team_payload_from_row(ranking_row: dict[str, Any]) -> dict[str, Any] | None:
        team_id = ranking_row.get("player_team_id")
        return _team_payload_for_id(team_id=team_id, ranking_row=ranking_row, include_reason=True)

    def _team_payload_for_id(
        team_id: Any,
        ranking_row: dict[str, Any] | None = None,
        include_reason: bool = False,
    ) -> dict[str, Any] | None:
        team_details: dict[str, Any] | None = None
        if isinstance(team_id, str):
            for candidate_id in _candidate_team_ids(team_id):
                team_details = team_by_id.get(candidate_id)
                if isinstance(team_details, dict):
                    break
                team_details = team_by_id_normalized.get(_normalized_team_id(candidate_id))
                if isinstance(team_details, dict):
                    break
        if not isinstance(team_details, dict):
            return None

        enriched_pokemon = _enrich_team_pokemon(team_details, pokemon_reference)
        payload = {
            "team_id": team_id,
            "mc_win_rate": ranking_row.get("mc_win_rate") if ranking_row else None,
            "wins": ranking_row.get("wins") if ranking_row else None,
            "losses": ranking_row.get("losses") if ranking_row else None,
            "n_trials": ranking_row.get("n_trials") if ranking_row else None,
            "avg_level": team_details.get("avg_level"),
            "pokemon": enriched_pokemon,
            "rank_in_boss_version": ranking_row.get("rank_in_boss_version") if ranking_row else None,
            "team_role": team_details.get("team_role"),
            "origin": team_details.get("origin"),
            "battle_type": team_details.get("battle_type"),
            "boss_name": team_details.get("boss_name"),
            "gym_index": team_details.get("gym_index"),
            "starter_base": team_details.get("starter_base"),
            "starter_evolved_species": team_details.get("starter_evolved_species"),
            "starter_type": team_details.get("starter_type"),
            "starter_condition": team_details.get("starter_condition"),
            "team_variant": team_details.get("team_variant"),
            "variant_dimension": team_details.get("variant_dimension"),
            "progression_depth": team_details.get("progression_depth"),
            "boss_ace_level": team_details.get("boss_ace_level"),
            "boss_avg_level": team_details.get("boss_avg_level"),
            "level_cap_offset": team_details.get("level_cap_offset"),
            "available_species_count": team_details.get("available_species_count"),
            "max_species_count": team_details.get("max_species_count"),
        }

        if include_reason and ranking_row is not None:
            win_rate = ranking_row.get("mc_win_rate")
            wins = ranking_row.get("wins")
            trials = ranking_row.get("n_trials")
            player_avg_level = ranking_row.get("player_avg_level")
            boss_avg_level = ranking_row.get("boss_avg_level")
            reason_parts: list[str] = []
            if isinstance(win_rate, (int, float)):
                reason_parts.append(f"High simulated win rate ({float(win_rate) * 100:.1f}%)")
            if isinstance(wins, (int, float)) and isinstance(trials, (int, float)) and int(trials) > 0:
                reason_parts.append(f"wins {int(wins)}/{int(trials)} trials")
            if isinstance(player_avg_level, (int, float)) and isinstance(boss_avg_level, (int, float)):
                level_delta = float(player_avg_level) - float(boss_avg_level)
                reason_parts.append(f"avg level delta {level_delta:+.1f}")
            payload["win_reason"] = "; ".join(reason_parts) if reason_parts else "Selected from top simulation ranking"

        return payload

    def _boss_team_payload_from_row(ranking_row: dict[str, Any]) -> dict[str, Any] | None:
        boss_team_id = ranking_row.get("boss_team_id")
        if isinstance(boss_team_id, str):
            payload = boss_team_payloads.get(boss_team_id)
            if isinstance(payload, dict):
                return payload
        return _team_payload_for_id(team_id=boss_team_id, ranking_row=None, include_reason=False)

    starter_choices_by_version = {
        row["game_key"]: row.get("starter_choices", [])
        for row in get_games_config()
    }

    pokemon_reference = _load_pokemon_reference(silver_dir, silver_manifest)
    move_reference = _load_move_reference(silver_dir, silver_manifest)
    boss_metadata = _load_boss_metadata(silver_dir, silver_manifest)
    boss_team_payloads = _load_boss_team_payloads(silver_dir, silver_manifest, pokemon_reference)
    boss_team_variants = _load_boss_team_variants(silver_dir, silver_manifest, pokemon_reference)
    encounter_maps, encounter_maps_by_name = _load_encounter_maps(silver_dir, silver_manifest)

    bosses_path = _dataset_path_from_manifest(silver_dir, silver_manifest, "bosses")
    bosses_df = read_parquet(bosses_path)
    if bosses_df.empty:
        return None
    if "is_simulatable" in bosses_df.columns:
        bosses_df = bosses_df[bosses_df["is_simulatable"].fillna(False).astype(bool)].copy()
    if bosses_df.empty:
        return None

    walkthroughs: dict[str, list[dict[str, Any]]] = {}
    rows_by_version: dict[str, list[dict[str, Any]]] = {}
    for boss_row in bosses_df.to_dict(orient="records"):
        version = str(boss_row.get("game_version") or "").strip().lower()
        boss_name = str(boss_row.get("boss_name_canonical") or boss_row.get("boss_name") or "").strip()
        if not version or not boss_name:
            continue

        boss_name_key = _norm_name(boss_name)
        boss_meta = boss_metadata.get((version, boss_name_key), {})
        encounter_payload = encounter_maps.get(
            (version, _encounter_boss_key(boss_row.get("boss_id"))),
            encounter_maps_by_name.get((version, boss_name_key), {}),
        )
        best = best_by_key.get((version, boss_name_key))
        recommended_team = _team_payload_from_row(best) if best is not None else None
        boss_team = _boss_team_payload_from_row(best) if best is not None else None

        top_rankings = rankings_by_key.get((version, boss_name_key), [])
        all_ranked_teams: list[dict[str, Any]] = []
        for ranking_row in top_rankings:
            payload = _team_payload_from_row(ranking_row)
            if payload is not None:
                all_ranked_teams.append(payload)
        all_ranked_teams = _dedupe_team_payloads(all_ranked_teams)
        if all_ranked_teams:
            recommended_team = all_ranked_teams[0]

        variant_key = (version, boss_name_key, str(boss_meta.get("boss_role") or "").strip().lower())
        variants = boss_team_variants.get(variant_key, [])
        if boss_team is None and variants:
            boss_team = variants[0]

        reachable_location_pokemon = _coerce_location_pokemon_map(encounter_payload.get("reachable_location_pokemon"))
        reachable_location_encounters = _coerce_location_encounters_map(encounter_payload.get("reachable_location_encounters"))
        progression_depth = _safe_float(boss_meta.get("progression_depth"))
        realism_lookup = _build_realism_lookup(reachable_location_encounters)
        diverse_top_teams = _select_diverse_team_payloads(
            all_ranked_teams,
            limit=5,
            progression_depth=progression_depth,
            realism_lookup=realism_lookup,
        )
        if diverse_top_teams:
            recommended_team = diverse_top_teams[0]

        top_teams_by_starter: dict[str, list[dict[str, Any]]] = {}
        for starter in starter_choices_by_version.get(version, []):
            starter_rank_rows = starter_rankings_by_key.get((version, boss_name_key, starter), [])
            starter_teams: list[dict[str, Any]] = []
            if starter_rank_rows:
                for starter_row in starter_rank_rows:
                    payload = _team_payload_from_row(_to_common_starter_ranking_row(starter_row))
                    if payload is not None:
                        starter_teams.append(payload)
            else:
                starter_family_norm = {_norm_name(member) for member in get_starter_family_members(starter)}
                starter_teams = [
                    team_payload
                    for team_payload in all_ranked_teams
                    if any(
                        _norm_name(str(member.get("name", ""))) in starter_family_norm
                        for member in team_payload.get("pokemon", [])
                        if isinstance(member, dict)
                    )
                ]
            top_teams_by_starter[starter] = _select_diverse_team_payloads(
                starter_teams,
                limit=5,
                progression_depth=progression_depth,
                realism_lookup=realism_lookup,
            )

        row = {
            "boss_key": str(boss_row.get("boss_id") or f"{version}:{boss_name_key}"),
            "boss_id": _encounter_boss_key(boss_row.get("boss_id")) or str(boss_row.get("boss_id") or ""),
            "boss_slug": _species_slug(boss_name),
            "boss_order": boss_meta.get("boss_order") or boss_meta.get("progression_order") or boss_row.get("boss_order") or boss_row.get("progression_order"),
            "part": 0,
            "boss_name": boss_name,
            "heading": boss_meta.get("location_name") or boss_row.get("location_name"),
            "location_count": encounter_payload.get("location_count", len(reachable_location_pokemon)),
            "reachable_locations": sorted(reachable_location_pokemon.keys()),
            "reachable_pokemon_count": encounter_payload.get("reachable_pokemon_count"),
            "reachable_location_pokemon": reachable_location_pokemon,
            "reachable_location_encounters": reachable_location_encounters,
            "catchable_locations_by_pokemon": encounter_payload.get("catchable_locations_by_pokemon", {}),
            "encounter_summary": _build_encounter_summary(reachable_location_encounters),
            "boss_metadata": {
                **boss_meta,
                "special_tags": _boss_special_tags(boss_meta),
            },
            "boss_team": boss_team,
            "boss_team_variants": variants,
            "recommended_team": recommended_team,
            "top_teams": diverse_top_teams if diverse_top_teams else all_ranked_teams[:5],
            "top_teams_by_starter": top_teams_by_starter,
        }
        top_teams_for_metrics = row["top_teams"] if isinstance(row.get("top_teams"), list) else []
        unique_species = {species for team in top_teams_for_metrics for species in _team_species_set(team)}
        unique_families = {family for team in top_teams_for_metrics for family in _team_family_set(team)}
        row["team_diversity_metrics"] = {
            "top_team_count": len(top_teams_for_metrics),
            "unique_species_count": len(unique_species),
            "unique_family_count": len(unique_families),
        }
        if not row["reachable_pokemon_count"]:
            row["reachable_pokemon_count"] = len(_invert_location_species_map(reachable_location_pokemon))
        if not row["catchable_locations_by_pokemon"]:
            row["catchable_locations_by_pokemon"] = _invert_location_species_map(reachable_location_pokemon)
        rows_by_version.setdefault(version, []).append(row)

    for version, rows in rows_by_version.items():
        walkthroughs[version] = sorted(
            rows,
            key=lambda item: (
                item.get("boss_order") or 0,
                str((item.get("boss_metadata") or {}).get("boss_role") or ""),
                str(item.get("boss_name") or ""),
                str(item.get("boss_key") or ""),
            ),
        )

    starter_family_members_by_version = {
        version: {
            starter: get_starter_family_members(starter)
            for starter in starters
        }
        for version, starters in starter_choices_by_version.items()
    }

    elite_four_champion_sequence_by_version: dict[str, dict[str, Any]] = {}
    if sequence_rankings_df is not None and not sequence_rankings_df.empty:
        seq_rows = sequence_rankings_df.sort_values(
            ["effective_game_version", "starter_base", "rank_in_sequence", "sequence_score"],
            ascending=[True, True, True, False],
        ).to_dict(orient="records")
        by_version_starter: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for row in seq_rows:
            version = str(row.get("effective_game_version") or "").strip().lower()
            starter = str(row.get("starter_base") or "").strip().lower()
            if not version or not starter:
                continue
            by_version_starter.setdefault((version, starter), []).append(row)

        for (version, starter), rows in by_version_starter.items():
            sequence_payloads: list[dict[str, Any]] = []
            for seq_row in rows:
                pseudo_row = {
                    "player_team_id": seq_row.get("player_team_id"),
                    "mc_win_rate": seq_row.get("sequence_win_rate"),
                    "wins": seq_row.get("sequence_wins"),
                    "losses": None,
                    "n_trials": seq_row.get("sequence_n_trials"),
                    "rank_in_boss_version": seq_row.get("rank_in_sequence"),
                    "player_avg_level": None,
                    "boss_avg_level": None,
                }
                payload = _team_payload_from_row(pseudo_row)
                if payload is None:
                    continue
                payload["sequence_win_rate"] = seq_row.get("sequence_win_rate")
                payload["sequence_score"] = seq_row.get("sequence_score")
                payload["bosses_covered"] = seq_row.get("bosses_covered")
                payload["sequence_completion_prob"] = seq_row.get("sequence_completion_prob", seq_row.get("sequence_win_rate"))
                payload["sequence_expected_wins"] = seq_row.get("sequence_expected_wins")
                payload["sequence_expected_wins_pct"] = seq_row.get("sequence_expected_wins_pct")
                payload["strict_clear_rate"] = seq_row.get("strict_clear_rate")
                payload["degraded_ratio"] = seq_row.get("degraded_ratio")
                payload["rank_in_sequence"] = seq_row.get("rank_in_sequence")
                sequence_payloads.append(payload)

            starter_entry = elite_four_champion_sequence_by_version.setdefault(version, {"top_teams_overall": [], "by_starter": {}})
            starter_entry["by_starter"][starter] = _select_diverse_team_payloads(
                sequence_payloads,
                limit=5,
                progression_depth=0.95,
                realism_lookup={},
            )

        for version, payload in elite_four_champion_sequence_by_version.items():
            flattened: list[dict[str, Any]] = []
            for starter_teams in payload.get("by_starter", {}).values():
                flattened.extend(starter_teams)
            payload["top_teams_overall"] = _select_diverse_team_payloads(
                flattened,
                limit=10,
                progression_depth=0.95,
                realism_lookup={},
            )

    output = {
        "versions": sorted(walkthroughs.keys()),
        "starter_choices_by_version": starter_choices_by_version,
        "starter_family_members_by_version": starter_family_members_by_version,
        "move_reference": move_reference,
        "walkthroughs": walkthroughs,
        "elite_four_champion_sequence_by_version": elite_four_champion_sequence_by_version,
    }

    output_path = gold_dir / "walkthrough_best_teams.json"
    write_json(output_path, _json_safe(output))
    return output_path
