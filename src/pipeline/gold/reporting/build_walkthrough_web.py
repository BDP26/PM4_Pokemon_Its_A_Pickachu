"""Build a web-friendly payload: best team per walkthrough boss and version."""
from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any, NoReturn, cast

import numpy as np
import pandas as pd

from src.pipeline.common.io import read_json, read_jsonl, read_parquet, write_json
from src.pipeline.gold.inputs.team_tables import load_reconstructed_teams_from_silver
from src.pipeline.silver.config.game_config import get_games_config, get_starter_family_members
from src.pipeline.settings import SILVER_DIR, GOLD_DIR


logger = logging.getLogger(__name__)


class GoldWebContractError(ValueError):
    """Raised when Gold walkthrough inputs violate Silver manifest contract."""


def _raise_web_contract_error(code: str, message: str, *, dataset: str | None = None, path: Path | None = None) -> NoReturn:
    parts: list[str] = [f"[gold.contract.web] {code}"]
    if dataset:
        parts.append(f"dataset={dataset}")
    if path is not None:
        parts.append(f"path={path}")
    parts.append(f"action=\"{message}\"")
    raise GoldWebContractError(" ".join(parts))


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


def _safe_int(value: Any) -> int | None:
    try:
        if value is None or (isinstance(value, float) and np.isnan(value)):
            return None
        return int(value)
    except Exception:
        return None


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


def _build_sprite_fields_from_url(species_name: str, pokemon_url: str | None) -> tuple[str | None, str | None, int | None, str]:
    species_slug = _species_slug(species_name)
    source_url = str(pokemon_url or f"https://pokeapi.co/api/v2/pokemon/{species_slug}/")
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
    if not manifest_path.exists():
        _raise_web_contract_error(
            "missing_manifest",
            "Run Silver first to generate manifest.json.",
            path=manifest_path,
        )
    try:
        payload = read_json(manifest_path)
    except Exception as exc:
        _raise_web_contract_error(
            "invalid_manifest_json",
            f"manifest.json is unreadable ({exc}).",
            path=manifest_path,
        )
    if not isinstance(payload, dict):
        _raise_web_contract_error(
            "invalid_manifest_shape",
            "manifest.json must be a JSON object.",
            path=manifest_path,
        )
    return cast(dict[str, Any], payload)


def _dataset_path_from_manifest(silver_dir: Path, silver_manifest: dict[str, Any], dataset_key: str) -> Path:
    datasets = silver_manifest.get("datasets")
    if not isinstance(datasets, dict):
        _raise_web_contract_error(
            "missing_manifest_datasets",
            "manifest.json requires a top-level datasets object.",
            dataset=dataset_key,
        )
    dataset = datasets.get(dataset_key)
    if not isinstance(dataset, dict):
        _raise_web_contract_error(
            "missing_dataset_entry",
            f"Add datasets.{dataset_key} to silver/manifest.json.",
            dataset=dataset_key,
        )
    rel_path = dataset.get("file")
    if not isinstance(rel_path, str) or not rel_path.strip():
        _raise_web_contract_error(
            "missing_dataset_file_path",
            f"Set datasets.{dataset_key}.file in silver/manifest.json.",
            dataset=dataset_key,
        )
    path = silver_dir / cast(str, rel_path)
    # Silver may publish partitioned parquet datasets as directories.
    if not path.exists():
        _raise_web_contract_error(
            "missing_dataset_file",
            "Regenerate Silver outputs so all required files exist.",
            dataset=dataset_key,
            path=path,
        )
    return path


def _snapshot_files_from_manifest(silver_dir: Path, silver_manifest: dict[str, Any]) -> list[Path]:
    datasets = silver_manifest.get("datasets")
    if not isinstance(datasets, dict):
        _raise_web_contract_error(
            "missing_manifest_datasets",
            "manifest.json requires a top-level datasets object.",
            dataset="boss_records",
        )
    boss_records = datasets.get("boss_records")
    if not isinstance(boss_records, dict):
        _raise_web_contract_error(
            "missing_dataset_entry",
            "Add datasets.boss_records with files[] in silver/manifest.json.",
            dataset="boss_records",
        )
    files = boss_records.get("files")
    if not isinstance(files, list) or not files:
        _raise_web_contract_error(
            "missing_snapshot_files",
            "Populate datasets.boss_records.files with snapshot JSONL inputs.",
            dataset="boss_records",
        )

    resolved: list[Path] = []
    files_list = cast(list[Any], files)
    for index, rel_path in enumerate(files_list):
        if not isinstance(rel_path, str) or not rel_path.strip():
            _raise_web_contract_error(
                "invalid_snapshot_entry",
                f"datasets.boss_records.files[{index}] must be a non-empty string path.",
                dataset="boss_records",
            )
        path = silver_dir / rel_path
        if not path.exists() or not path.is_file():
            _raise_web_contract_error(
                "missing_snapshot_file",
                "Regenerate Silver snapshots and refresh manifest entries.",
                dataset="boss_records",
                path=path,
            )
        resolved.append(path)
    return sorted(set(resolved))


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
    pokemon_reference = _load_pokemon_reference(silver_dir, silver_manifest)
    snapshot_paths = _snapshot_files_from_manifest(silver_dir, silver_manifest)

    encounters_path = _dataset_path_from_manifest(silver_dir, silver_manifest, "encounters")

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
            if all_ranked_teams:
                recommended_team = all_ranked_teams[0]

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
                "catchable_locations_by_pokemon": {},
                "boss_team": boss_team,
                "recommended_team": recommended_team,
                "top_teams": all_ranked_teams[:5],
                "top_teams_by_starter": top_teams_by_starter,
            }

            if isinstance(row.get("boss_id"), str):
                fallback_key = (version, str(row["boss_id"]).strip().lower())
            location_map = row.get("reachable_location_pokemon")
            row["catchable_locations_by_pokemon"] = _invert_location_species_map(location_map if isinstance(location_map, dict) else {})

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
                    "wins": None,
                    "losses": None,
                    "n_trials": None,
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
                payload["degraded_ratio"] = seq_row.get("degraded_ratio")
                payload["rank_in_sequence"] = seq_row.get("rank_in_sequence")
                sequence_payloads.append(payload)

            starter_entry = elite_four_champion_sequence_by_version.setdefault(version, {"top_teams_overall": [], "by_starter": {}})
            deduped = _dedupe_team_payloads(sequence_payloads)
            starter_entry["by_starter"][starter] = deduped[:5]

        for version, payload in elite_four_champion_sequence_by_version.items():
            flattened: list[dict[str, Any]] = []
            for starter_teams in payload.get("by_starter", {}).values():
                flattened.extend(starter_teams)
            payload["top_teams_overall"] = _dedupe_team_payloads(flattened)[:10]

    output = {
        "versions": sorted(walkthroughs.keys()),
        "starter_choices_by_version": starter_choices_by_version,
        "starter_family_members_by_version": starter_family_members_by_version,
        "walkthroughs": walkthroughs,
        "elite_four_champion_sequence_by_version": elite_four_champion_sequence_by_version,
    }

    output_path = gold_dir / "walkthrough_best_teams.json"
    write_json(output_path, _json_safe(output))
    return output_path

