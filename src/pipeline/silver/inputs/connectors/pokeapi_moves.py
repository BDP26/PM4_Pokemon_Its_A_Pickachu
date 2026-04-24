"""Parquet-first helpers for team member move information.

Design:
- normal runtime is strict parquet-only
- first-run bootstrap is explicit via bootstrap_move_reference_cache(...)
- no hidden fallback during normal team generation/simulation
"""

import logging
from pathlib import Path
from typing import Any

import pokebase as pb

from src.pipeline.common.io import write_parquet
from src.pipeline.silver.config.team_config import (
    FORM_LOOKUP_FALLBACKS,
    GAME_TO_VERSION_GROUP,
    GENERIC_FORM_SUFFIXES,
)
from src.pipeline.silver.inputs.reference_context import load_move_reference_tables, normalize_move_name, normalize_species_slug
from src.pipeline.settings import SILVER_DIR


logger = logging.getLogger(__name__)

_MOVE_PROFILE_CACHE: dict[str, dict[str, Any]] = {}
_LEARNABLE_BY_GAME_SPECIES: dict[tuple[str, str], dict[str, int]] = {}
_LEARNABLE_CACHE: dict[tuple[str, int, str], tuple[str, ...]] = {}
_CACHE_SILVER_DIR: str | None = None


def _normalize_learned_level(raw_level: Any) -> int:
    try:
        level = int(raw_level or 0)
    except (TypeError, ValueError):
        level = 0
    return level if level > 0 else 1


def _species_lookup_candidates(species_slug: str) -> list[str]:
    candidates = [species_slug]
    candidates.extend(FORM_LOOKUP_FALLBACKS.get(species_slug, ()))
    for suffix in GENERIC_FORM_SUFFIXES:
        candidates.append(f"{species_slug}-{suffix}")

    seen: set[str] = set()
    ordered: list[str] = []
    for candidate in candidates:
        if candidate and candidate not in seen:
            seen.add(candidate)
            ordered.append(candidate)
    return ordered


def _clear_loaded_caches() -> None:
    global _CACHE_SILVER_DIR
    _MOVE_PROFILE_CACHE.clear()
    _LEARNABLE_BY_GAME_SPECIES.clear()
    _LEARNABLE_CACHE.clear()
    _CACHE_SILVER_DIR = None


def _ensure_parquet_cache_loaded(silver_dir: Path = SILVER_DIR) -> None:
    """Strict parquet-only runtime cache load."""
    global _CACHE_SILVER_DIR

    cache_key = str(Path(silver_dir).resolve())
    if _CACHE_SILVER_DIR == cache_key:
        return

    _MOVE_PROFILE_CACHE.clear()
    _LEARNABLE_BY_GAME_SPECIES.clear()
    _LEARNABLE_CACHE.clear()

    references_dir = silver_dir / "references"
    move_reference_path = references_dir / "move_reference.parquet"
    learnable_path = references_dir / "learnable_moves.parquet"

    if not move_reference_path.exists():
        raise FileNotFoundError(
            f"Missing move reference parquet: {move_reference_path}. "
            "Run bootstrap_move_reference_cache(...) before team generation."
        )
    if not learnable_path.exists():
        raise FileNotFoundError(
            f"Missing learnable move parquet: {learnable_path}. "
            "Run bootstrap_move_reference_cache(...) before team generation."
        )

    move_profiles, grouped = load_move_reference_tables(silver_dir=silver_dir)

    _MOVE_PROFILE_CACHE.update(move_profiles)
    _LEARNABLE_BY_GAME_SPECIES.update(grouped)
    _CACHE_SILVER_DIR = cache_key


def _api_move_profile(move_name: str) -> dict[str, Any]:
    normalized = normalize_move_name(move_name)
    try:
        move = pb.move(normalized)
        return {
            "move_name": normalized,
            "power": int(getattr(move, "power", 0) or 0),
            "damage_class": str(getattr(getattr(move, "damage_class", None), "name", "status") or "status"),
            "type": str(getattr(getattr(move, "type", None), "name", "") or "") or None,
            "accuracy": getattr(move, "accuracy", None),
            "pp": getattr(move, "pp", None),
        }
    except Exception:
        return {
            "move_name": normalized,
            "power": 0,
            "damage_class": "status",
            "type": None,
            "accuracy": None,
            "pp": None,
        }


def _api_learnable_move_levels_for_species(species: str, game_version: str) -> dict[str, int]:
    species_slug = normalize_species_slug(species)
    version_group = GAME_TO_VERSION_GROUP.get(game_version, game_version)

    resolved_poke = None
    for candidate in _species_lookup_candidates(species_slug):
        try:
            resolved_poke = pb.pokemon(candidate)
            break
        except Exception:
            continue

    if resolved_poke is None:
        return {}

    discovered: dict[str, int] = {}
    for move_slot in getattr(resolved_poke, "moves", []):
        move_name = normalize_move_name(getattr(getattr(move_slot, "move", None), "name", "") or "")
        if not move_name:
            continue
        for detail in getattr(move_slot, "version_group_details", []):
            detail_group = str(getattr(getattr(detail, "version_group", None), "name", "") or "").strip().lower()
            if detail_group != version_group:
                continue
            learn_method = str(getattr(getattr(detail, "move_learn_method", None), "name", "") or "").strip().lower()
            if learn_method != "level-up":
                continue
            learned_at = _normalize_learned_level(getattr(detail, "level_learned_at", None))
            discovered[move_name] = min(discovered.get(move_name, learned_at), learned_at)

    return discovered


def bootstrap_move_reference_cache(
    entries: list[tuple[str, int, str, list[str]]],
    silver_dir: Path = SILVER_DIR,
) -> dict[str, int]:
    """Explicit first-run materialization from API -> parquet.

    This is the only place allowed to call external move/species APIs.
    """
    target_pairs: set[tuple[str, str]] = set()
    required_moves: set[str] = set()

    for species, _, game_version, provided_moves in entries:
        game_version_norm = str(game_version).strip().lower()
        species_slug = normalize_species_slug(species)
        if not game_version_norm or not species_slug:
            continue
        target_pairs.add((game_version_norm, species_slug))
        for move_name in provided_moves:
            normalized = normalize_move_name(move_name)
            if normalized:
                required_moves.add(normalized)

    logger.info(
        "[silver/moves] bootstrap fetching species_count=%s required_move_hints=%s",
        len(target_pairs),
        len(required_moves),
    )

    learnable_rows: list[dict[str, Any]] = []
    all_referenced_moves: set[str] = set(required_moves)

    for game_version, species_slug in sorted(target_pairs):
        move_levels = _api_learnable_move_levels_for_species(species_slug, game_version)
        logger.info(
            "[silver/moves] bootstrap species game_version=%s species=%s move_count=%s",
            game_version,
            species_slug,
            len(move_levels),
        )
        for move_name, learned_level in sorted(move_levels.items()):
            normalized_move = normalize_move_name(move_name)
            if not normalized_move:
                continue
            all_referenced_moves.add(normalized_move)
            learnable_rows.append(
                {
                    "game_version": game_version,
                    "pokemon_species": species_slug,
                    "move_name": normalized_move,
                    "learned_level": _normalize_learned_level(learned_level),
                    "learn_method": "level-up",
                }
            )

    move_rows: list[dict[str, Any]] = []
    for move_name in sorted(all_referenced_moves):
        move_rows.append(_api_move_profile(move_name))

    references_dir = silver_dir / "references"
    references_dir.mkdir(parents=True, exist_ok=True)

    if learnable_rows:
        write_parquet(
            references_dir / "learnable_moves.parquet",
            learnable_rows,
            partition_cols=["game_version", "pokemon_species"],
        )
    if move_rows:
        write_parquet(references_dir / "move_reference.parquet", move_rows)

    logger.info(
        "[silver/moves] bootstrap parquet write summary learnable_rows=%s move_rows=%s learnable_games=%s learnable_species=%s",
        len(learnable_rows),
        len(move_rows),
        len({str(row.get('game_version') or '').strip().lower() for row in learnable_rows if row.get('game_version')}),
        len({str(row.get('pokemon_species') or '').strip().lower() for row in learnable_rows if row.get('pokemon_species')}),
    )

    _clear_loaded_caches()

    return {
        "entry_count": len(entries),
        "target_pairs": len(target_pairs),
        "learnable_rows": len(learnable_rows),
        "move_rows": len(move_rows),
    }


def _move_profile(move_name: str, silver_dir: Path = SILVER_DIR) -> dict[str, Any]:
    _ensure_parquet_cache_loaded(silver_dir=silver_dir)
    normalized = normalize_move_name(move_name)
    payload = _MOVE_PROFILE_CACHE.get(normalized)
    if payload is None:
        return {
            "move_name": normalized,
            "power": 0,
            "damage_class": "status",
            "type": None,
            "accuracy": None,
            "pp": None,
        }
    return payload


def _is_known_move(move_name: str, silver_dir: Path = SILVER_DIR) -> bool:
    _ensure_parquet_cache_loaded(silver_dir=silver_dir)
    return normalize_move_name(move_name) in _MOVE_PROFILE_CACHE


def _learnable_move_levels_for_species(species: str, game_version: str, silver_dir: Path = SILVER_DIR) -> dict[str, int]:
    _ensure_parquet_cache_loaded(silver_dir=silver_dir)

    species_slug = normalize_species_slug(species)
    game_version_norm = str(game_version).lower().strip()

    exact = _LEARNABLE_BY_GAME_SPECIES.get((game_version_norm, species_slug))
    if exact is not None:
        return dict(exact)

    version_group = GAME_TO_VERSION_GROUP.get(game_version_norm, game_version_norm)
    for (known_game, known_species), known_levels in _LEARNABLE_BY_GAME_SPECIES.items():
        if known_species != species_slug:
            continue
        known_group = GAME_TO_VERSION_GROUP.get(known_game, known_game)
        if known_group == version_group:
            _LEARNABLE_BY_GAME_SPECIES[(game_version_norm, species_slug)] = dict(known_levels)
            return dict(known_levels)

    _LEARNABLE_BY_GAME_SPECIES[(game_version_norm, species_slug)] = {}
    return {}


def _learnable_moves_for_species(species: str, level: int, game_version: str, silver_dir: Path = SILVER_DIR) -> tuple[str, ...]:
    _ensure_parquet_cache_loaded(silver_dir=silver_dir)

    species_slug = normalize_species_slug(species)
    game_version_norm = str(game_version).lower().strip()
    cache_key = (species_slug, int(level), game_version_norm)

    cached = _LEARNABLE_CACHE.get(cache_key)
    if cached is not None:
        return cached

    level_cap = int(level)
    move_levels = _learnable_move_levels_for_species(species_slug, game_version_norm, silver_dir=silver_dir)
    filtered = tuple(sorted(move for move, learned_level in move_levels.items() if int(learned_level) <= level_cap))
    _LEARNABLE_CACHE[cache_key] = filtered
    return filtered


def prefetch_species_move_data(entries: list[tuple[str, int, str, list[str]]], silver_dir: Path = SILVER_DIR) -> None:
    _ensure_parquet_cache_loaded(silver_dir=silver_dir)
    for species, level, game_version, provided_moves in entries:
        _learnable_moves_for_species(species, level, game_version, silver_dir=silver_dir)
        for move_name in provided_moves:
            _move_profile(str(move_name), silver_dir=silver_dir)


def persist_move_reference_cache(
    entries: list[tuple[str, int, str, list[str]]],
    silver_dir: Path = SILVER_DIR,
) -> dict[str, int]:
    """Persist from already-materialized parquet caches only."""
    _ensure_parquet_cache_loaded(silver_dir=silver_dir)

    target_pairs: set[tuple[str, str]] = set()
    required_moves: set[str] = set()

    for species, _, game_version, provided_moves in entries:
        game_version_norm = str(game_version).strip().lower()
        species_slug = normalize_species_slug(species)
        if not game_version_norm or not species_slug:
            continue
        target_pairs.add((game_version_norm, species_slug))
        _learnable_move_levels_for_species(species_slug, game_version_norm, silver_dir=silver_dir)
        for move_name in provided_moves:
            normalized = normalize_move_name(move_name)
            if normalized:
                required_moves.add(normalized)

    logger.info(
        "[silver/moves] persist loading species_count=%s required_move_hints=%s",
        len(target_pairs),
        len(required_moves),
    )

    learnable_rows: list[dict[str, Any]] = []
    all_referenced_moves: set[str] = set(required_moves)

    for game_version, species_slug in sorted(target_pairs):
        move_levels = _LEARNABLE_BY_GAME_SPECIES.get((game_version, species_slug), {})
        logger.info(
            "[silver/moves] persist species game_version=%s species=%s move_count=%s",
            game_version,
            species_slug,
            len(move_levels),
        )
        for move_name, learned_level in sorted(move_levels.items()):
            normalized_move = normalize_move_name(move_name)
            if not normalized_move:
                continue
            all_referenced_moves.add(normalized_move)
            learnable_rows.append(
                {
                    "game_version": game_version,
                    "pokemon_species": species_slug,
                    "move_name": normalized_move,
                    "learned_level": _normalize_learned_level(learned_level),
                    "learn_method": "level-up",
                }
            )

    move_rows: list[dict[str, Any]] = []
    for move_name in sorted(all_referenced_moves):
        profile = _move_profile(move_name, silver_dir=silver_dir)
        move_rows.append(
            {
                "move_name": move_name,
                "power": int(profile.get("power") or 0),
                "damage_class": str(profile.get("damage_class") or "status"),
                "type": profile.get("type"),
                "accuracy": profile.get("accuracy"),
                "pp": profile.get("pp"),
            }
        )

    references_dir = silver_dir / "references"
    references_dir.mkdir(parents=True, exist_ok=True)

    if learnable_rows:
        write_parquet(
            references_dir / "learnable_moves.parquet",
            learnable_rows,
            partition_cols=["game_version", "pokemon_species"],
        )
    if move_rows:
        write_parquet(references_dir / "move_reference.parquet", move_rows)

    logger.info(
        "[silver/moves] persist parquet write summary learnable_rows=%s move_rows=%s learnable_games=%s learnable_species=%s",
        len(learnable_rows),
        len(move_rows),
        len({str(row.get('game_version') or '').strip().lower() for row in learnable_rows if row.get('game_version')}),
        len({str(row.get('pokemon_species') or '').strip().lower() for row in learnable_rows if row.get('pokemon_species')}),
    )

    _clear_loaded_caches()

    return {
        "entry_count": len(entries),
        "target_pairs": len(target_pairs),
        "learnable_rows": len(learnable_rows),
        "move_rows": len(move_rows),
    }


def _damaging_moves_for_species(species: str, level: int, game_version: str, silver_dir: Path = SILVER_DIR) -> tuple[str, ...]:
    learnable_moves = _learnable_moves_for_species(species, level, game_version, silver_dir=silver_dir)
    if not learnable_moves:
        return ()

    filtered: list[str] = []
    for move_name in learnable_moves:
        profile = _move_profile(move_name, silver_dir=silver_dir)
        power = int(profile.get("power") or 0)
        damage_class = str(profile.get("damage_class") or "status")
        if power <= 0:
            continue
        if damage_class not in {"physical", "special"}:
            continue
        filtered.append(move_name)
    return tuple(filtered)


def _build_member_moves(
    name: str,
    level: int,
    moves: list[str],
    game_version: str,
    silver_dir: Path = SILVER_DIR,
) -> dict[str, Any] | None:
    cleaned_moves = [normalize_move_name(move) for move in moves if str(move).strip()]
    learnable_moves = list(_learnable_moves_for_species(name, level, game_version, silver_dir=silver_dir))
    learnable_move_levels = _learnable_move_levels_for_species(name, game_version, silver_dir=silver_dir)

    move_details: dict[str, Any] = {}
    for move_name in learnable_moves:
        if not _is_known_move(move_name, silver_dir=silver_dir):
            continue
        move_details[move_name] = _move_profile(move_name, silver_dir=silver_dir)

    for move_name in cleaned_moves:
        if move_name in move_details or not _is_known_move(move_name, silver_dir=silver_dir):
            continue
        move_details[move_name] = _move_profile(move_name, silver_dir=silver_dir)

    return {
        "species": str(name).strip().lower(),
        "level": int(level),
        "game_version": game_version,
        "provided_moves": cleaned_moves,
        "learnable_moves": learnable_moves,
        "learnable_move_levels": dict(sorted(learnable_move_levels.items())),
        "move_details": move_details,
    }


def _build_member_detail(
    name: str,
    level: int,
    moves: list[str],
    game_version: str,
    origin: str,
    silver_dir: Path = SILVER_DIR,
) -> dict[str, Any] | None:
    cleaned_moves = [normalize_move_name(move) for move in moves if str(move).strip()]

    if origin == "kaggle":
        return {
            "name": str(name).strip().lower(),
            "level": int(level),
            "moves": cleaned_moves[:4],
            "origin": origin,
        }

    learnable_moves = list(_damaging_moves_for_species(name, level, game_version, silver_dir=silver_dir))
    if not learnable_moves:
        logger.info(
            "[silver/teams] skip member without learnable moves species=%s level=%s version=%s origin=%s",
            name,
            level,
            game_version,
            origin,
        )
        return None

    valid_moves: list[str] = []
    seen_moves: set[str] = set()

    for move in cleaned_moves:
        if move in learnable_moves and move not in seen_moves:
            valid_moves.append(move)
            seen_moves.add(move)

    for move in learnable_moves:
        if move in seen_moves:
            continue
        valid_moves.append(move)
        seen_moves.add(move)

    return {
        "name": str(name).strip().lower(),
        "level": int(level),
        "moves": valid_moves,
        "origin": origin,
    }
