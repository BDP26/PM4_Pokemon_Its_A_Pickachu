"""Parquet-first helpers for team member move information.

Reads move + learnable references from Silver parquet and serves lookups from
in-memory caches. API fallback is enabled only for the bootstrap run when no
parquet references are available yet.
"""

import logging
from pathlib import Path
from typing import Any

import pokebase as pb

from src.pipeline.common.io import read_parquet, write_parquet
from src.pipeline.silver.config.team_config import (
    FORM_LOOKUP_FALLBACKS,
    GAME_TO_VERSION_GROUP,
    GENERIC_FORM_SUFFIXES,
    SPECIES_SLUG_ALIASES,
)
from src.pipeline.settings import SILVER_DIR


logger = logging.getLogger(__name__)

_MOVE_PROFILE_CACHE: dict[str, tuple[int, str]] = {}
_LEARNABLE_BY_GAME_SPECIES: dict[tuple[str, str], dict[str, int]] = {}
_LEARNABLE_CACHE: dict[tuple[str, int, str], tuple[str, ...]] = {}
_CACHE_SILVER_DIR: str | None = None
_BOOTSTRAP_MOVE_FALLBACK_ENABLED = False
_BOOTSTRAP_LEARNABLE_FALLBACK_ENABLED = False


def _normalize_learned_level(raw_level: Any) -> int:
    """Normalize API/parquet learned level; treat null/0 as level 1."""
    try:
        level = int(raw_level or 0)
    except (TypeError, ValueError):
        level = 0
    return level if level > 0 else 1


def _normalize_species_slug(species: str) -> str:
    normalized = str(species).strip().lower().replace(".", " ").replace("_", " ")
    normalized = " ".join(normalized.split())
    if normalized in SPECIES_SLUG_ALIASES:
        return SPECIES_SLUG_ALIASES[normalized]
    return normalized.replace("'", "").replace(" ", "-")


def _normalize_move_name(move: Any) -> str:
    normalized = str(move).strip().lower().replace(".", " ").replace("_", " ")
    normalized = " ".join(normalized.split())
    normalized = normalized.replace("'", "")
    return normalized.replace(" ", "-")


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


def _ensure_parquet_cache_loaded(silver_dir: Path = SILVER_DIR) -> None:
    """Load caches from parquet; isolate caches by silver_dir path."""
    global _CACHE_SILVER_DIR, _BOOTSTRAP_MOVE_FALLBACK_ENABLED, _BOOTSTRAP_LEARNABLE_FALLBACK_ENABLED

    cache_key = str(Path(silver_dir).resolve())
    if _CACHE_SILVER_DIR == cache_key:
        return

    _MOVE_PROFILE_CACHE.clear()
    _LEARNABLE_BY_GAME_SPECIES.clear()
    _LEARNABLE_CACHE.clear()

    references_dir = silver_dir / "references"
    move_reference_path = references_dir / "move_reference.parquet"
    learnable_candidates = [
        references_dir / "pokemon_learnable_moves.parquet",
        references_dir / "learnable_moves.parquet",
    ]

    if move_reference_path.exists():
        try:
            move_df = read_parquet(move_reference_path)
            for row in move_df.to_dict(orient="records"):
                move_name = _normalize_move_name(row.get("move_name"))
                if not move_name:
                    continue
                _MOVE_PROFILE_CACHE[move_name] = (
                    int(row.get("power") or 0),
                    str(row.get("damage_class") or "status"),
                )
        except Exception:
            logger.debug("[silver/teams] failed to load move_reference parquet", exc_info=True)

    learn_df = None
    for learnable_path in learnable_candidates:
        if not learnable_path.exists():
            continue
        try:
            learn_df = read_parquet(learnable_path)
            break
        except Exception:
            logger.debug("[silver/teams] failed to load learnable parquet path=%s", learnable_path, exc_info=True)

    if learn_df is not None:
        grouped: dict[tuple[str, str], dict[str, int]] = {}
        for row in learn_df.to_dict(orient="records"):
            game_version = str(row.get("game_version") or "").strip().lower()
            species = _normalize_species_slug(row.get("pokemon_species") or "")
            move_name = _normalize_move_name(row.get("move_name"))
            if not game_version or not species or not move_name:
                continue
            learned_level = _normalize_learned_level(row.get("learned_level"))
            slot = grouped.setdefault((game_version, species), {})
            slot[move_name] = min(slot.get(move_name, learned_level), learned_level)

        _LEARNABLE_BY_GAME_SPECIES.update(grouped)

    # Fallbacks are independent: learnables may be missing while move profiles exist.
    _BOOTSTRAP_MOVE_FALLBACK_ENABLED = not _MOVE_PROFILE_CACHE
    _BOOTSTRAP_LEARNABLE_FALLBACK_ENABLED = not _LEARNABLE_BY_GAME_SPECIES
    _CACHE_SILVER_DIR = cache_key


def _move_profile(move_name: str) -> tuple[int, str]:
    """Return move profile from parquet cache; optional bootstrap API fallback."""
    _ensure_parquet_cache_loaded()
    normalized = _normalize_move_name(move_name)
    cached = _MOVE_PROFILE_CACHE.get(normalized)
    if cached is not None:
        return cached

    if not _BOOTSTRAP_MOVE_FALLBACK_ENABLED:
        return (0, "status")

    try:
        move = pb.move(normalized)
        profile = (
            int(getattr(move, "power", 0) or 0),
            str(getattr(getattr(move, "damage_class", None), "name", "status") or "status"),
        )
        _MOVE_PROFILE_CACHE[normalized] = profile
        return profile
    except Exception:
        return (0, "status")


def _is_known_move(move_name: str) -> bool:
    _ensure_parquet_cache_loaded()
    return _normalize_move_name(move_name) in _MOVE_PROFILE_CACHE


def _learnable_moves_for_species(species: str, level: int, game_version: str) -> tuple[str, ...]:
    """Return level-filtered learnable moves from parquet, with bootstrap fallback."""
    _ensure_parquet_cache_loaded()

    species_slug = _normalize_species_slug(species)
    game_version_norm = str(game_version).lower().strip()
    cache_key = (species_slug, int(level), game_version_norm)
    cached = _LEARNABLE_CACHE.get(cache_key)
    if cached is not None:
        return cached

    level_cap = int(level)
    move_levels = _learnable_move_levels_for_species(species_slug, game_version_norm)
    if move_levels:
        filtered = tuple(sorted(move for move, learned_level in move_levels.items() if learned_level <= level_cap))
        _LEARNABLE_CACHE[cache_key] = filtered
        return filtered

    result: tuple[str, ...] = ()
    _LEARNABLE_CACHE[cache_key] = result
    return result


def _learnable_move_levels_for_species(species: str, game_version: str) -> dict[str, int]:
    """Return all level-up learnable moves with learned levels for one game version."""
    _ensure_parquet_cache_loaded()

    species_slug = _normalize_species_slug(species)
    game_version_norm = str(game_version).lower().strip()
    cached_levels = _LEARNABLE_BY_GAME_SPECIES.get((game_version_norm, species_slug))
    version_group = GAME_TO_VERSION_GROUP.get(game_version_norm, game_version_norm)

    # Always short-circuit when this exact species+game was already resolved.
    if cached_levels is not None:
        return dict(cached_levels)

    # Reuse existing species data from a sibling game in the same version group.
    for (known_game, known_species), known_levels in _LEARNABLE_BY_GAME_SPECIES.items():
        if known_species != species_slug:
            continue
        known_group = GAME_TO_VERSION_GROUP.get(known_game, known_game)
        if known_group != version_group:
            continue
        _LEARNABLE_BY_GAME_SPECIES[(game_version_norm, species_slug)] = dict(known_levels)
        return dict(known_levels)

    resolved_poke = None
    for candidate in _species_lookup_candidates(species_slug):
        try:
            resolved_poke = pb.pokemon(candidate)
            break
        except Exception:
            continue

    if resolved_poke is None:
        if cached_levels is not None:
            return dict(cached_levels)
        _LEARNABLE_BY_GAME_SPECIES[(game_version_norm, species_slug)] = {}
        return {}

    discovered: dict[str, int] = {}
    for move_slot in getattr(resolved_poke, "moves", []):
        move_name = _normalize_move_name(getattr(getattr(move_slot, "move", None), "name", "") or "")
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

    if cached_levels:
        for move_name, learned_level in cached_levels.items():
            learned_level_norm = _normalize_learned_level(learned_level)
            discovered[move_name] = min(discovered.get(move_name, learned_level_norm), learned_level_norm)

    _LEARNABLE_BY_GAME_SPECIES[(game_version_norm, species_slug)] = discovered
    return dict(discovered)


def prefetch_species_move_data(entries: list[tuple[str, int, str, list[str]]]) -> None:
    """Warm caches and touch requested keys for predictable runtime."""
    _ensure_parquet_cache_loaded()
    for species, level, game_version, provided_moves in entries:
        _learnable_moves_for_species(species, level, game_version)
        for move_name in provided_moves:
            _move_profile(str(move_name))


def persist_move_reference_cache(
    entries: list[tuple[str, int, str, list[str]]],
    silver_dir: Path = SILVER_DIR,
) -> dict[str, int]:
    """Materialize move-reference parquet files for requested game/species entries."""
    _ensure_parquet_cache_loaded(silver_dir=silver_dir)

    target_pairs: set[tuple[str, str]] = set()
    required_moves: set[str] = set()

    for species, _, game_version, provided_moves in entries:
        game_version_norm = str(game_version).strip().lower()
        species_slug = _normalize_species_slug(species)
        if not game_version_norm or not species_slug:
            continue
        target_pairs.add((game_version_norm, species_slug))
        _learnable_move_levels_for_species(species_slug, game_version_norm)
        for move_name in provided_moves:
            normalized = _normalize_move_name(move_name)
            if normalized:
                required_moves.add(normalized)

    learnable_rows: list[dict[str, Any]] = []
    all_referenced_moves: set[str] = set(required_moves)

    for game_version, species_slug in sorted(target_pairs):
        move_levels = _LEARNABLE_BY_GAME_SPECIES.get((game_version, species_slug), {})
        if not move_levels:
            continue
        for move_name, learned_level in sorted(move_levels.items()):
            normalized_move = _normalize_move_name(move_name)
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
        power, damage_class = _move_profile(move_name)
        move_rows.append(
            {
                "move_name": move_name,
                "power": int(power),
                "damage_class": str(damage_class or "status"),
                "type": None,
                "accuracy": None,
                "pp": None,
            }
        )

    references_dir = silver_dir / "references"
    references_dir.mkdir(parents=True, exist_ok=True)
    if learnable_rows:
        write_parquet(references_dir / "learnable_moves.parquet", learnable_rows, partition_cols=["game_version"])
        write_parquet(
            references_dir / "pokemon_learnable_moves.parquet",
            learnable_rows,
            partition_cols=["game_version", "pokemon_species"],
        )
    if move_rows:
        write_parquet(references_dir / "move_reference.parquet", move_rows)

    # Ensure subsequent calls re-read from freshly materialized parquet files.
    global _CACHE_SILVER_DIR
    _CACHE_SILVER_DIR = None

    return {
        "entry_count": len(entries),
        "target_pairs": len(target_pairs),
        "learnable_rows": len(learnable_rows),
        "move_rows": len(move_rows),
    }


def _damaging_moves_for_species(species: str, level: int, game_version: str) -> tuple[str, ...]:
    learnable_moves = _learnable_moves_for_species(species, level, game_version)
    if not learnable_moves:
        return ()

    filtered: list[str] = []
    for move_name in learnable_moves:
        power, damage_class = _move_profile(move_name)
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
) -> dict[str, Any] | None:
    """Build detailed move info from parquet-backed references."""
    cleaned_moves = [_normalize_move_name(move) for move in moves if str(move).strip()]
    learnable_moves = list(_learnable_moves_for_species(name, level, game_version))
    learnable_move_levels = _learnable_move_levels_for_species(name, game_version)

    move_details: dict[str, Any] = {}
    for move_name in learnable_moves:
        if not _is_known_move(move_name):
            continue
        power, damage_class = _move_profile(move_name)
        move_details[move_name] = {"power": power, "damage_class": damage_class}

    # Keep provided moves in detail table only when they exist in parquet reference.
    for move_name in cleaned_moves:
        if move_name in move_details or not _is_known_move(move_name):
            continue
        power, damage_class = _move_profile(move_name)
        move_details[move_name] = {"power": power, "damage_class": damage_class}

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
) -> dict[str, Any] | None:
    """Build lean team member entry with up to 4 moves."""
    cleaned_moves = [_normalize_move_name(move) for move in moves if str(move).strip()]

    # Kaggle boss members keep provided moves as-is.
    if origin == "kaggle":
        return {
            "name": str(name).strip().lower(),
            "level": int(level),
            "moves": cleaned_moves[:4],
            "origin": origin,
        }

    learnable_moves = list(_damaging_moves_for_species(name, level, game_version))
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

    # keep valid cleaned moves (no duplicates)
    for move in cleaned_moves:
        if move in learnable_moves and move not in seen_moves:
            valid_moves.append(move)
            seen_moves.add(move)

    # then add remaining learnable moves (no limit anymore)
    for move in learnable_moves:
        if move in seen_moves:
            continue
        valid_moves.append(move)
        seen_moves.add(move)

    return {
        "name": str(name).strip().lower(),
        "level": int(level),
        "moves": valid_moves,  # no longer capped at 4
        "origin": origin,
    }
