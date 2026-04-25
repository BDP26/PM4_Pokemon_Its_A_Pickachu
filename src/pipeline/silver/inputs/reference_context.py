"""Offline reference context for deterministic team generation.

This module is intentionally connector-free:
- reads only persisted Silver parquet references
- exposes pure helpers for boss/player team builders
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.pipeline.common.io import read_parquet
from src.pipeline.silver.config.team_config import GAME_TO_VERSION_GROUP, SPECIES_SLUG_ALIASES
from src.pipeline.silver.move_power import normalize_move_power_name, resolve_effective_power
from src.pipeline.settings import SILVER_DIR


def normalize_key(value: Any) -> str:
    normalized = str(value).strip().lower().replace(".", " ").replace("_", " ")
    normalized = " ".join(normalized.split())
    normalized = normalized.replace("'", "")
    normalized = normalized.replace(" ", "-")
    while "--" in normalized:
        normalized = normalized.replace("--", "-")
    return normalized.strip("-")


def normalize_species_slug(species: Any) -> str:
    normalized = str(species).strip().lower().replace(".", " ").replace("_", " ")
    normalized = " ".join(normalized.split())
    if normalized in SPECIES_SLUG_ALIASES:
        return SPECIES_SLUG_ALIASES[normalized]
    return normalize_key(normalized)


def normalize_move_name(move: Any) -> str:
    return normalize_move_power_name(move)


@dataclass(frozen=True)
class MoveReferenceContext:
    move_profiles: dict[str, dict[str, Any]]
    learnable_by_game_species: dict[tuple[str, str], dict[str, int]]

    def _version_group(self, game_version: str) -> str:
        normalized = str(game_version).strip().lower()
        return GAME_TO_VERSION_GROUP.get(normalized, normalized)

    def learnable_levels(self, species: str, game_version: str) -> dict[str, int]:
        species_slug = normalize_species_slug(species)
        game_norm = str(game_version).strip().lower()
        exact = self.learnable_by_game_species.get((game_norm, species_slug))
        if exact is not None:
            return dict(exact)

        version_group = self._version_group(game_norm)
        for (known_game, known_species), known_levels in self.learnable_by_game_species.items():
            if known_species != species_slug:
                continue
            if self._version_group(known_game) == version_group:
                return dict(known_levels)
        return {}

    def learnable_moves(self, species: str, level: int, game_version: str) -> list[str]:
        level_cap = max(1, int(level))
        levels = self.learnable_levels(species, game_version)
        return sorted(move for move, learned_level in levels.items() if int(learned_level) <= level_cap)

    def damaging_moves(self, species: str, level: int, game_version: str) -> list[str]:
        candidates = self.learnable_moves(species, level, game_version)
        kept: list[str] = []
        for move in candidates:
            profile = self.move_profiles.get(move, {})
            effective_power = float(profile.get("effective_power") or 0.0)
            damage_class = str(profile.get("damage_class") or "").strip().lower()
            if effective_power > 0 and damage_class in {"physical", "special"}:
                kept.append(move)
        return kept

    def build_member_detail(
        self,
        *,
        name: str,
        level: int,
        moves: list[str],
        game_version: str,
        origin: str,
    ) -> dict[str, Any] | None:
        cleaned_moves = [normalize_move_name(move) for move in moves if str(move).strip()]

        if origin == "kaggle":
            return {
                "name": normalize_species_slug(name),
                "level": int(level),
                "moves": cleaned_moves[:4],
                "origin": origin,
            }

        learnable_moves = self.damaging_moves(name, int(level), game_version)
        if not learnable_moves:
            return None

        valid_moves: list[str] = []
        seen_moves: set[str] = set()
        for move in cleaned_moves:
            if move in learnable_moves and move not in seen_moves:
                valid_moves.append(move)
                seen_moves.add(move)
        for move in learnable_moves:
            if move not in seen_moves:
                valid_moves.append(move)
                seen_moves.add(move)

        return {
            "name": normalize_species_slug(name),
            "level": int(level),
            "moves": valid_moves,
            "origin": origin,
        }

    def build_member_moves(
        self,
        *,
        name: str,
        level: int,
        moves: list[str],
        game_version: str,
    ) -> dict[str, Any]:
        cleaned_moves = [normalize_move_name(move) for move in moves if str(move).strip()]
        learnable_moves = self.learnable_moves(name, int(level), game_version)
        learnable_move_levels = self.learnable_levels(name, game_version)

        missing_provided_moves = sorted(
            move_name
            for move_name in cleaned_moves
            if move_name and move_name not in self.move_profiles
        )
        if missing_provided_moves:
            raise ValueError(
                "Kaggle boss move reference validation failed: "
                f"species={normalize_species_slug(name)} "
                f"game_version={str(game_version).strip().lower()} "
                f"missing_moves={missing_provided_moves}"
            )

        move_details: dict[str, Any] = {}

        for move_name in learnable_moves:
            if move_name in self.move_profiles:
                move_details[move_name] = dict(self.move_profiles[move_name])

        for move_name in cleaned_moves:
            if move_name not in move_details:
                move_details[move_name] = dict(self.move_profiles[move_name])

        return {
            "species": normalize_species_slug(name),
            "level": int(level),
            "game_version": str(game_version).strip().lower(),
            "provided_moves": cleaned_moves,
            "learnable_moves": learnable_moves,
            "learnable_move_levels": dict(sorted(learnable_move_levels.items())),
            "move_details": move_details,
        }


def load_move_reference_tables(
    silver_dir: Path = SILVER_DIR,
) -> tuple[dict[str, dict[str, Any]], dict[tuple[str, str], dict[str, int]]]:
    references_dir = silver_dir / "references"
    move_reference_path = references_dir / "move_reference.parquet"
    learnable_path = references_dir / "learnable_moves.parquet"

    if not move_reference_path.exists():
        raise FileNotFoundError(f"Missing move reference parquet: {move_reference_path}")
    if not learnable_path.exists():
        raise FileNotFoundError(f"Missing learnable moves parquet: {learnable_path}")

    move_profiles: dict[str, dict[str, Any]] = {}
    for row in read_parquet(move_reference_path).to_dict(orient="records"):
        move_name = normalize_move_name(row.get("move_name"))
        if not move_name:
            continue
        raw_power = row.get("power")
        if isinstance(raw_power, float) and raw_power != raw_power:
            raw_power = None
        effective_power, power_handling = resolve_effective_power(
            move_name=move_name,
            power=raw_power,
            damage_class=row.get("damage_class"),
        )
        stored_effective_power = row.get("effective_power", effective_power)
        if isinstance(stored_effective_power, float) and stored_effective_power != stored_effective_power:
            stored_effective_power = effective_power
        stored_power_handling = row.get("power_handling", power_handling)
        if not isinstance(stored_power_handling, str) or not stored_power_handling.strip():
            stored_power_handling = power_handling
        move_profiles[move_name] = {
            "move_name": move_name,
            "power": raw_power,
            "raw_power": raw_power,
            "damage_class": str(row.get("damage_class") or "status"),
            "type": row.get("type"),
            "accuracy": row.get("accuracy"),
            "pp": row.get("pp"),
            "effective_power": stored_effective_power,
            "power_handling": stored_power_handling,
            "is_status_move": row.get("is_status_move", str(row.get("damage_class") or "").strip().lower() == "status"),
            "is_damage_move": row.get("is_damage_move", effective_power > 0),
            "is_null_power": row.get("is_null_power", raw_power is None),
        }

    learnable_by_game_species: dict[tuple[str, str], dict[str, int]] = {}
    for row in read_parquet(learnable_path).to_dict(orient="records"):
        game_version = str(row.get("game_version") or "").strip().lower()
        species = normalize_species_slug(row.get("pokemon_species") or "")
        move_name = normalize_move_name(row.get("move_name"))
        if not game_version or not species or not move_name:
            continue
        try:
            learned_level = max(1, int(row.get("learned_level") or 1))
        except (TypeError, ValueError):
            learned_level = 1
        slot = learnable_by_game_species.setdefault((game_version, species), {})
        slot[move_name] = min(slot.get(move_name, learned_level), learned_level)

    return move_profiles, learnable_by_game_species


def load_reference_context(silver_dir: Path = SILVER_DIR) -> MoveReferenceContext:
    move_profiles, learnable_by_game_species = load_move_reference_tables(silver_dir=silver_dir)
    return MoveReferenceContext(
        move_profiles=move_profiles,
        learnable_by_game_species=learnable_by_game_species,
    )
