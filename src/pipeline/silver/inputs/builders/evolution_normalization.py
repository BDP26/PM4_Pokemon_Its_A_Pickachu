from __future__ import annotations

import logging
from typing import Any

from src.pipeline.silver.inputs.connectors.pokeapi_evolution import build_species_evolution_rules
from src.pipeline.silver.transforms.keys import normalize_key_part

logger = logging.getLogger(__name__)


_SPECIAL_TRIGGERS = {
    "trade",
    "use-item",
    "item",
    "friendship",
    "time",
    "move",
    "gender",
    "known-move",
    "held-item",
}

_NATIONAL_DEX_GENERATION_CUTOFFS: tuple[tuple[int, int], ...] = (
    (151, 1),
    (251, 2),
    (386, 3),
    (493, 4),
    (649, 5),
    (721, 6),
)


def _normalize_trigger(trigger: Any) -> str:
    value = str(trigger or "level-up").strip().lower().replace("_", "-")
    return value or "level-up"


def _parse_level(value: Any) -> int | None:
    if value is None:
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _parse_species_id(value: Any) -> int | None:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _introduced_generation_from_species_id(species_id: Any) -> int | None:
    parsed_id = _parse_species_id(species_id)
    if parsed_id is None:
        return None
    for max_species_id, generation in _NATIONAL_DEX_GENERATION_CUTOFFS:
        if parsed_id <= max_species_id:
            return generation
    return _NATIONAL_DEX_GENERATION_CUTOFFS[-1][1] + 1


def _rules_look_like_pokeapi_species_rules(rules: dict[str, Any]) -> bool:
    if not rules:
        return False
    sample_value = next(iter(rules.values()))
    return isinstance(sample_value, dict) and "evolution_stage" in sample_value


def build_level_up_evolution_index_from_species_rules(
    species_rules: dict[str, dict[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    """Convert flattened PokeAPI species rules into adjacency transitions."""
    by_base_stage: dict[tuple[str, int], list[dict[str, Any]]] = {}
    for species_name, info in species_rules.items():
        species = normalize_key_part(species_name)
        if not species or not isinstance(info, dict):
            continue
        base_species = normalize_key_part(info.get("base_species") or species)
        stage = int(info.get("evolution_stage") or 1)
        by_base_stage.setdefault((base_species, stage), []).append(
            {
                "species": species,
                "min_level_from_previous": _parse_level(info.get("min_level_from_previous")),
                "special_evolution_conditions": list(info.get("special_evolution_conditions") or []),
                "introduced_generation": _introduced_generation_from_species_id(
                    info.get("species_id") or info.get("pokeapi_id")
                )
                or _parse_level(info.get("introduced_generation")),
            }
        )

    transitions: dict[str, list[dict[str, Any]]] = {}
    for species_name, info in species_rules.items():
        current_species = normalize_key_part(species_name)
        if not current_species or not isinstance(info, dict):
            continue
        base_species = normalize_key_part(info.get("base_species") or current_species)
        stage = int(info.get("evolution_stage") or 1)
        next_species_rows = by_base_stage.get((base_species, stage + 1), [])
        if not next_species_rows:
            continue
        for candidate in next_species_rows:
            special_conditions = candidate.get("special_evolution_conditions") or []
            if isinstance(special_conditions, list) and special_conditions:
                for condition in special_conditions:
                    if not isinstance(condition, dict):
                        continue
                    trigger_name = str(condition.get("trigger") or "").strip().lower().replace("_", "-")
                    transitions.setdefault(current_species, []).append(
                        {
                            "to_species": candidate.get("species"),
                            "trigger": trigger_name or "special",
                            "min_level": _parse_level(condition.get("min_level")) or candidate.get("min_level_from_previous"),
                            "required_item": normalize_key_part(condition.get("item")),
                            "introduced_generation": candidate.get("introduced_generation"),
                        }
                    )
                continue
            transitions.setdefault(current_species, []).append(
                {
                    "to_species": candidate.get("species"),
                    "trigger": "level-up",
                    "min_level": candidate.get("min_level_from_previous"),
                    "required_item": None,
                    "introduced_generation": candidate.get("introduced_generation"),
                }
            )
    return transitions


def build_level_up_evolution_index_from_chain(chain: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    """Build transition rules from a raw PokeAPI evolution chain payload."""
    species_rules = build_species_evolution_rules(chain)
    return build_level_up_evolution_index_from_species_rules(species_rules)


def normalize_species_for_level(
    species: str,
    *,
    member_level: int,
    evolution_rules: dict[str, list[dict[str, Any]]] | None,
    target_generation: int | None = None,
    allow_trade_evolutions: bool = False,
    allow_item_evolutions: bool = False,
    item_evolution_default_level: int = 1,
) -> tuple[str, list[dict[str, Any]]]:
    """Normalize a species to the highest legal level-up evolution at the given level."""
    current = normalize_key_part(species)
    if not current:
        return "", []

    rules_raw = evolution_rules or {}
    rules: dict[str, list[dict[str, Any]]]
    if _rules_look_like_pokeapi_species_rules(rules_raw):
        rules = build_level_up_evolution_index_from_species_rules(rules_raw)  # type: ignore[arg-type]
    else:
        rules = rules_raw  # type: ignore[assignment]
    applied: list[dict[str, Any]] = []
    level_cap = max(1, int(member_level or 1))

    for _ in range(8):
        options = rules.get(current, [])
        if not isinstance(options, list) or not options:
            break

        eligible: list[tuple[int, str, dict[str, Any]]] = []
        for option in options:
            if not isinstance(option, dict):
                continue
            target = normalize_key_part(option.get("to_species") or option.get("evolves_to") or option.get("species"))
            if not target:
                continue
            introduced_generation = _introduced_generation_from_species_id(
                option.get("species_id") or option.get("pokeapi_id")
            ) or _parse_level(option.get("introduced_generation"))
            if target_generation is not None and introduced_generation is not None and introduced_generation > target_generation:
                continue
            trigger = _normalize_trigger(option.get("trigger") or option.get("evolution_trigger"))
            min_level = _parse_level(option.get("min_level") or option.get("min_level_from_previous") or option.get("level"))
            if trigger == "level-up":
                if min_level is None or level_cap < min_level:
                    continue
            elif trigger == "trade" and allow_trade_evolutions:
                min_level = min_level or 1
            elif trigger in {"use-item", "item"}:
                required_item = normalize_key_part(option.get("required_item") or option.get("item"))
                if not (allow_item_evolutions and required_item):
                    continue
                fallback_level = _parse_level(item_evolution_default_level) or 1
                # Item evolutions have no fixed min_level in PokeAPI; use the current team level cap.
                min_level = level_cap or fallback_level
            elif trigger in _SPECIAL_TRIGGERS:
                continue
            else:
                continue
            eligible.append((min_level or 0, target, option))

        if not eligible:
            break

        eligible.sort(key=lambda row: (row[0], row[1]))
        min_level, target, option = eligible[-1]
        if target == current:
            break
        applied.append({
            "from_species": current,
            "to_species": target,
            "trigger": _normalize_trigger(option.get("trigger") or option.get("evolution_trigger")),
            "min_level": min_level,
        })
        current = target

    return current, applied


def normalize_candidate_pool_for_level(
    candidates: list[tuple[str, int, int, int]],
    *,
    member_level: int,
    evolution_rules: dict[str, list[dict[str, Any]]] | None,
    legal_species: set[str] | None,
    target_generation: int | None = None,
    allow_trade_evolutions: bool = False,
    allow_item_evolutions: bool = False,
    item_evolution_default_level: int = 1,
) -> tuple[list[tuple[str, int, int, int]], dict[str, int]]:
    """Apply level-up normalization and deduplicate the candidate pool."""
    normalized_rows: dict[str, tuple[str, int, int, int]] = {}
    transformed = 0
    removed_invalid = 0

    for species, chance, lvl_max, capture in candidates:
        # Use the same level capping as legal_species_pool_for_level: a pokemon
        # caught at level_max cannot have evolved past that level when caught.
        effective_level = max(1, min(int(member_level or 1), int(lvl_max or member_level or 1)))
        normalized_species, applied = normalize_species_for_level(
            species,
            member_level=effective_level,
            evolution_rules=evolution_rules,
            target_generation=target_generation,
            allow_trade_evolutions=allow_trade_evolutions,
            allow_item_evolutions=allow_item_evolutions,
            item_evolution_default_level=item_evolution_default_level,
        )
        if applied:
            transformed += 1
        if not normalized_species:
            continue
        if legal_species is not None and normalized_species not in legal_species:
            removed_invalid += 1
            continue

        existing = normalized_rows.get(normalized_species)
        if existing is None:
            normalized_rows[normalized_species] = (normalized_species, int(chance or 0), int(lvl_max or 0), int(capture or 0))
            continue
        normalized_rows[normalized_species] = (
            normalized_species,
            max(existing[1], int(chance or 0)),
            max(existing[2], int(lvl_max or 0)),
            max(existing[3], int(capture or 0)),
        )

    normalized = sorted(normalized_rows.values(), key=lambda item: (-item[1], -item[2], -item[3], item[0]))
    diagnostics = {
        "input": len(candidates),
        "transformed": transformed,
        "removed_after_validation": removed_invalid,
        "deduped": max(0, len(candidates) - len(normalized)),
        "output": len(normalized),
    }
    return normalized, diagnostics


def legal_species_pool_for_level(
    candidates: list[tuple[str, int, int, int]],
    *,
    member_level: int,
    evolution_rules: dict[str, list[dict[str, Any]]] | None,
    target_generation: int | None = None,
    allow_trade_evolutions: bool = False,
    allow_item_evolutions: bool = False,
    item_evolution_default_level: int = 1,
) -> set[str]:
    """Return the species set that remains legal after forced evolutions at the given level."""
    legal_species: set[str] = set()
    for species, _, lvl_max, _ in candidates:
        # Effective level is capped by encounter level_max: a pokemon caught at level 20
        # cannot have evolved past level 20 even if the player_level_cap is higher.
        effective_level = max(1, min(int(member_level or 1), int(lvl_max or member_level or 1)))
        normalized_species, _ = normalize_species_for_level(
            species,
            member_level=effective_level,
            evolution_rules=evolution_rules,
            target_generation=target_generation,
            allow_trade_evolutions=allow_trade_evolutions,
            allow_item_evolutions=allow_item_evolutions,
            item_evolution_default_level=item_evolution_default_level,
        )
        if normalized_species:
            legal_species.add(normalized_species)
    return legal_species


def validate_candidate_pool(
    candidates: list[tuple[str, int, int, int]],
    *,
    legal_species: set[str],
    game_version: str,
) -> None:
    invalid = sorted({species for species, _, _, _ in candidates if species not in legal_species})
    if invalid:
        raise ValueError(
            f"Invalid normalized candidates for game={game_version}: "
            f"missing_from_legal_encounters={invalid[:20]}"
        )


def validate_generated_team(
    team_species: list[str],
    *,
    legal_species: set[str],
    game_version: str,
    boss_name: str,
) -> None:
    invalid = sorted({normalize_key_part(species) for species in team_species if normalize_key_part(species) not in legal_species})
    if invalid:
        raise ValueError(
            f"Invalid generated team for game={game_version} boss={boss_name}: "
            f"illegal_species={invalid}"
        )
