"""Build a web-friendly payload: best team per walkthrough boss and version."""
from __future__ import annotations

import logging
import re
from functools import lru_cache
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pokebase as pb

from src.pipeline.common.io import read_json, read_jsonl, read_parquet, write_json
from src.pipeline.gold.inputs.team_tables import load_reconstructed_teams_from_silver
from src.pipeline.silver.config.game_config import get_games_config, get_starter_family_members
from src.pipeline.settings import SILVER_DIR, GOLD_DIR, get_silver_subdirs


logger = logging.getLogger(__name__)


def _norm_name(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.lower())


def _species_slug(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", str(value).strip().lower()).strip("-")
    # Keep canonical PokeAPI naming for punctuation-heavy species names.
    if slug == "mr-mime":
        return "mr-mime"
    return slug


def _extract_pokeid_from_url(url: str) -> int | None:
    match = re.search(r"/pokemon/(\d+)/?", str(url))
    if not match:
        return None
    try:
        return int(match.group(1))
    except Exception:
        return None


def _variant_candidates(species_slug: str) -> list[str]:
    variants_map = {
        "meowstic": ["meowstic-male", "meowstic-female"],
        "gourgeist": ["gourgeist-average", "gourgeist-small", "gourgeist-large", "gourgeist-super"],
        "pumpkaboo": ["pumpkaboo-average", "pumpkaboo-small", "pumpkaboo-large", "pumpkaboo-super"],
        "jellicent": ["jellicent-male", "jellicent-female"],
        "indeedee": ["indeedee-male", "indeedee-female"],
        "basculin": ["basculin-red-striped", "basculin-blue-striped", "basculin-white-striped"],
    }
    candidates = [species_slug]
    candidates.extend(variants_map.get(species_slug, []))

    # Generic fallbacks for common form naming in PokeAPI.
    generic_suffixes = ["male", "female", "average", "normal"]
    candidates.extend([f"{species_slug}-{suffix}" for suffix in generic_suffixes])

    deduped: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        if candidate in seen:
            continue
        deduped.append(candidate)
        seen.add(candidate)
    return deduped


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    if isinstance(value, np.ndarray):
        return [_json_safe(v) for v in value.tolist()]
    if isinstance(value, (np.integer, np.floating)):
        return value.item()
    if isinstance(value, np.bool_):
        return bool(value)
    return value


@lru_cache(maxsize=2048)
def _resolve_sprite(species_name: str) -> tuple[str | None, str, int | None, str]:
    normalized = _species_slug(species_name)
    requested_url = f"https://pokeapi.co/api/v2/pokemon/{normalized}/"
    tried_variants: list[str] = []
    for candidate_slug in _variant_candidates(normalized):
        tried_variants.append(candidate_slug)
        candidate_url = f"https://pokeapi.co/api/v2/pokemon/{candidate_slug}/"
        try:
            pokemon = pb.pokemon(candidate_slug)
            sprites = getattr(pokemon, "sprites", None)
            sprite_url = getattr(sprites, "front_default", None) if sprites is not None else None
            front_female = getattr(sprites, "front_female", None) if sprites is not None else None
            other = getattr(sprites, "other", None) if sprites is not None else None
            official_artwork = getattr(other, "official_artwork", None) if other is not None else None
            official_front_default = (
                getattr(official_artwork, "front_default", None)
                if official_artwork is not None
                else None
            )
            resource_url = str(getattr(pokemon, "url", candidate_url) or candidate_url)
            pokeid = getattr(pokemon, "id", None)
            pokeid = int(pokeid) if isinstance(pokeid, int) else _extract_pokeid_from_url(resource_url)
            pokeid_sprite_url = (
                f"https://raw.githubusercontent.com/PokeAPI/sprites/master/sprites/pokemon/{pokeid}.png"
                if pokeid is not None
                else None
            )
            resolved_sprite_url = str(pokeid_sprite_url or sprite_url) if (pokeid_sprite_url or sprite_url) else None
            sprite_candidates = [
                str(value)
                for value in (pokeid_sprite_url, sprite_url, front_female, official_front_default)
                if value
            ]
            if resolved_sprite_url is None:
                continue
            logger.info(
                "[gold][web] sprite resolved species=%s species_slug=%s resolved_variant=%s pokeid=%s pokemon_url=%s selected_sprite_url=%s sprite_candidates=%s",
                species_name,
                normalized,
                candidate_slug,
                pokeid,
                resource_url,
                resolved_sprite_url,
                sprite_candidates,
            )
            return resolved_sprite_url, resource_url, pokeid, candidate_slug
        except Exception:
            continue

    logger.warning(
        "[gold][web] sprite missing species=%s species_slug=%s pokeid=%s pokemon_url=%s selected_sprite_url=%s sprite_candidates=%s tried_variants=%s",
        species_name,
        normalized,
        None,
        requested_url,
        None,
        [],
        tried_variants,
    )
    return None, requested_url, None, normalized


def _with_sprite_fields(pokemon_entry: Any) -> dict[str, Any]:
    if isinstance(pokemon_entry, dict):
        enriched = dict(pokemon_entry)
    else:
        enriched = {"name": str(pokemon_entry)}

    name = enriched.get("name")
    if not isinstance(name, str) or not name.strip():
        enriched["sprite_url"] = None
        enriched["sprite_source_url"] = None
        return enriched

    sprite_url, source_url, pokeid, species_slug = _resolve_sprite(name)
    enriched["sprite_url"] = sprite_url
    enriched["sprite_source_url"] = source_url
    enriched["pokeid"] = pokeid
    enriched["species_slug"] = species_slug
    return enriched


def _enrich_team_pokemon(team_details: dict[str, Any]) -> list[dict[str, Any]]:
    raw_pokemon = team_details.get("details", [])
    if isinstance(raw_pokemon, list) and raw_pokemon:
        return [_with_sprite_fields(entry) for entry in raw_pokemon]

    # Fallback for rows that only contain species names.
    names_only = team_details.get("pokemon", [])
    if isinstance(names_only, list) and names_only:
        return [_with_sprite_fields({"name": str(name)}) for name in names_only]
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


def _load_silver_manifest(silver_dir: Path) -> dict[str, Any] | None:
    manifest_path = silver_dir / "manifest.json"
    if not manifest_path.exists():
        return None
    try:
        payload = read_json(manifest_path)
    except Exception:
        logger.warning("[gold][web] failed to read silver manifest path=%s", manifest_path, exc_info=True)
        return None
    return payload if isinstance(payload, dict) else None


def _dataset_path_from_manifest(silver_dir: Path, silver_manifest: dict[str, Any] | None, dataset_key: str) -> Path | None:
    if not isinstance(silver_manifest, dict):
        return None
    datasets = silver_manifest.get("datasets")
    if not isinstance(datasets, dict):
        return None
    dataset = datasets.get(dataset_key)
    if not isinstance(dataset, dict):
        return None
    rel_path = dataset.get("file")
    if not isinstance(rel_path, str) or not rel_path.strip():
        return None
    path = silver_dir / rel_path
    return path if path.exists() else None


def _snapshot_files_from_manifest(silver_dir: Path, silver_manifest: dict[str, Any] | None) -> list[Path]:
    if not isinstance(silver_manifest, dict):
        return []
    datasets = silver_manifest.get("datasets")
    if not isinstance(datasets, dict):
        return []
    boss_records = datasets.get("boss_records")
    if not isinstance(boss_records, dict):
        return []
    files = boss_records.get("files")
    if not isinstance(files, list):
        return []

    resolved: list[Path] = []
    for rel_path in files:
        if not isinstance(rel_path, str) or not rel_path.strip():
            continue
        path = silver_dir / rel_path
        if path.exists():
            resolved.append(path)
    return sorted(set(resolved))


def _fallback_snapshot_maps_by_boss(snapshot_available_path: Path) -> tuple[dict[tuple[str, str], dict[str, list[str]]], dict[tuple[str, str], dict[str, list[dict[str, Any]]]]]:
    if not snapshot_available_path.exists():
        return {}, {}

    try:
        snapshot_df = read_parquet(snapshot_available_path)
    except Exception:
        logger.warning("[gold][web] failed to load snapshot_available_pokemon parquet for catch fallback", exc_info=True)
        return {}, {}

    if snapshot_df.empty:
        return {}, {}

    pokemon_by_boss: dict[tuple[str, str], dict[str, list[str]]] = {}
    encounters_by_boss: dict[tuple[str, str], dict[str, list[dict[str, Any]]]] = {}

    for row in snapshot_df.to_dict(orient="records"):
        version = str(row.get("game_version") or "").strip().lower()
        boss_id = str(row.get("boss_id") or "").strip().lower()
        species = str(row.get("pokemon_species") or "").strip().lower()
        location_id = str(row.get("first_available_location_id") or "").strip().lower()
        if not version or not boss_id or not species or not location_id:
            continue

        _, _, location_slug = location_id.partition(":")
        location_slug = location_slug or location_id
        key = (version, boss_id)

        loc_species = pokemon_by_boss.setdefault(key, {}).setdefault(location_slug, [])
        if species not in loc_species:
            loc_species.append(species)

        encounter_rows = encounters_by_boss.setdefault(key, {}).setdefault(location_slug, [])
        encounter_rows.append(
            {
                "species": species,
                "level_min": row.get("min_level"),
                "level_max": row.get("max_level"),
                "encounter_chance_max": None,
                "capture_rate": None,
                "encounter_methods": [row.get("encounter_method")] if row.get("encounter_method") else [],
            }
        )

    return pokemon_by_boss, encounters_by_boss


def build_walkthrough_best_teams_payload(
    silver_dir: Path = SILVER_DIR,
    gold_dir: Path = GOLD_DIR,
) -> Path | None:
    silver_subdirs = get_silver_subdirs(silver_dir)
    snapshots_dir = silver_subdirs["snapshots"]

    best_by_boss_file = gold_dir / "best_team_by_boss_version.parquet"
    rankings_file = gold_dir / "team_rankings_by_boss_version.parquet"
    rankings_starter_file = gold_dir / "team_rankings_by_boss_version_starter.parquet"
    if not best_by_boss_file.exists():
        return None

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

    def _dedupe_team_payloads(team_payloads: list[dict[str, Any]]) -> list[dict[str, Any]]:
        deduped: list[dict[str, Any]] = []
        seen: set[str] = set()
        for payload in team_payloads:
            team_id = payload.get("team_id")
            if isinstance(team_id, str):
                key = _normalized_team_id(team_id)
            else:
                names = [
                    _norm_name(str(member.get("name", "")))
                    for member in payload.get("pokemon", [])
                    if isinstance(member, dict)
                ]
                key = "pokemon:" + ",".join(sorted(name for name in names if name))
            if key in seen:
                continue
            seen.add(key)
            deduped.append(payload)
        return deduped

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

        enriched_pokemon = _enrich_team_pokemon(team_details)
        payload = {
            "team_id": team_id,
            "mc_win_rate": ranking_row.get("mc_win_rate") if ranking_row else None,
            "wins": ranking_row.get("wins") if ranking_row else None,
            "losses": ranking_row.get("losses") if ranking_row else None,
            "n_trials": ranking_row.get("n_trials") if ranking_row else None,
            "avg_level": team_details.get("avg_level"),
            "pokemon": enriched_pokemon,
            "rank_in_boss_version": ranking_row.get("rank_in_boss_version") if ranking_row else None,
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
        return _team_payload_for_id(team_id=ranking_row.get("boss_team_id"), ranking_row=None, include_reason=False)

    starter_choices_by_version = {
        row["game_key"]: row.get("starter_choices", [])
        for row in get_games_config()
    }

    silver_manifest = _load_silver_manifest(silver_dir)
    snapshot_paths = _snapshot_files_from_manifest(silver_dir, silver_manifest)
    if not snapshot_paths:
        snapshot_paths = sorted(snapshots_dir.glob("*_boss_snapshots.jsonl"))

    snapshot_available_path = (
        _dataset_path_from_manifest(silver_dir, silver_manifest, "snapshot_available_pokemon")
        or silver_subdirs["references"] / "snapshot_available_pokemon.parquet"
    )
    fallback_location_map_by_boss, fallback_encounters_by_boss = _fallback_snapshot_maps_by_boss(snapshot_available_path)

    walkthroughs: dict[str, list[dict[str, Any]]] = {}

    for snapshot_path in snapshot_paths:
        version = snapshot_path.stem.replace("_boss_snapshots", "")
        snapshot_df = read_jsonl(snapshot_path)
        if snapshot_df.empty:
            continue

        rows_by_key: dict[str, dict[str, Any]] = {}
        for snap in snapshot_df.sort_values(["boss_order", "part"]).to_dict(orient="records"):
            boss_name = snap.get("boss_name")
            if not isinstance(boss_name, str):
                continue

            boss_order = snap.get("boss_order")
            boss_key = f"{version}:{boss_order}:{_norm_name(boss_name)}"

            best = best_by_key.get((version, _norm_name(boss_name)))
            recommended_team = None
            if best is not None:
                recommended_team = _team_payload_from_row(best)
            boss_team = _boss_team_payload_from_row(best) if best is not None else None

            top_rankings = rankings_by_key.get((version, _norm_name(boss_name)), [])
            all_ranked_teams: list[dict[str, Any]] = []
            for ranking_row in top_rankings:
                payload = _team_payload_from_row(ranking_row)
                if payload is not None:
                    all_ranked_teams.append(payload)

            all_ranked_teams = _dedupe_team_payloads(all_ranked_teams)

            top_teams_by_starter: dict[str, list[dict[str, Any]]] = {}
            for starter in starter_choices_by_version.get(version, []):
                starter_rank_rows = starter_rankings_by_key.get((version, _norm_name(boss_name), starter), [])
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
                top_teams_by_starter[starter] = _dedupe_team_payloads(starter_teams)[:5]

            row = {
                "boss_key": boss_key,
                "boss_id": snap.get("boss_id"),
                "boss_slug": snap.get("boss_slug"),
                "boss_order": boss_order,
                "part": snap.get("part"),
                "boss_name": boss_name,
                "location_count": snap.get("reachable_location_count"),
                "reachable_location_pokemon": _coerce_location_pokemon_map(snap.get("reachable_location_pokemon")),
                "reachable_location_encounters": _coerce_location_encounters_map(snap.get("reachable_location_encounters")),
                "boss_team": boss_team,
                "recommended_team": recommended_team,
                "top_teams": all_ranked_teams[:5],
                "top_teams_by_starter": top_teams_by_starter,
            }

            if isinstance(row.get("boss_id"), str):
                fallback_key = (version, str(row["boss_id"]).strip().lower())
                if not row["reachable_location_pokemon"]:
                    row["reachable_location_pokemon"] = fallback_location_map_by_boss.get(fallback_key, {})
                if not row["reachable_location_encounters"]:
                    row["reachable_location_encounters"] = fallback_encounters_by_boss.get(fallback_key, {})

            existing = rows_by_key.get(boss_key)
            if existing is None or (row.get("part") or 0) < (existing.get("part") or 0):
                rows_by_key[boss_key] = row

        walkthroughs[version] = sorted(
            rows_by_key.values(),
            key=lambda item: (
                item.get("boss_order") or 0,
                item.get("part") or 0,
                str(item.get("boss_name") or ""),
            ),
        )

    starter_family_members_by_version = {
        version: {
            starter: get_starter_family_members(starter)
            for starter in starters
        }
        for version, starters in starter_choices_by_version.items()
    }

    output = {
        "versions": sorted(walkthroughs.keys()),
        "starter_choices_by_version": starter_choices_by_version,
        "starter_family_members_by_version": starter_family_members_by_version,
        "walkthroughs": walkthroughs,
    }

    output_path = gold_dir / "walkthrough_best_teams.json"
    write_json(output_path, _json_safe(output))
    return output_path


