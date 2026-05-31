"""Compact offline player-team generation for Silver.

Silver persists logical team/member/move-option references only.
Concrete move-set expansion is deferred to Gold/simulation.
"""

from __future__ import annotations

import logging
import random
import time
from itertools import combinations, islice
from typing import Any

import pandas as pd

from src.pipeline.silver.config.game_config import (
    get_starter_choices,
    get_starter_type,
    get_starters_by_type,
    resolve_starter_species_for_level,
)
from src.pipeline.silver.config.team_config import (
    ALLOW_ITEM_EVOLUTIONS,
    DEFAULT_CATCH_POOL_SIZE,
    DEFAULT_MEMBER_LEVEL,
    DEFAULT_MEMBER_MOVESET_COMBO_LIMIT,
    DEFAULT_MEMBER_MOVE_OPTION_LIMIT,
    DEFAULT_SOURCE_TEAM_COMBO_LIMIT,
    DEFAULT_SOURCE_TEAM_POOL_SIZE,
    DEFAULT_TEAM_MEMBER_LIMIT,
    DEFAULT_TEAM_TYPE_WEIGHT_CAP,
    ITEM_EVOLUTION_DEFAULT_LEVEL,
    MOVESET_WIDTH,
)
from src.pipeline.silver.inputs.builders.evolution_normalization import (
    legal_species_pool_for_level,
    normalize_candidate_pool_for_level,
    normalize_species_for_level,
    validate_candidate_pool,
    validate_generated_team,
)
from src.pipeline.silver.inputs.reference_context import MoveReferenceContext
from src.pipeline.silver.inputs.species_classification import is_restricted_encounter_species
from src.pipeline.common.type_chart import build_type_chart
from src.pipeline.silver.transforms.keys import (
    make_moveset_combo_id,
    make_pokemon_instance_id,
    make_team_id,
    normalize_key_part,
    stable_digest,
)
from src.pipeline.silver.transforms.progression_depth import (
    ProgressionDepthContext,
)


logger = logging.getLogger(__name__)
MAX_SOURCE_TEAM_SIZE = 5
EARLY_GAME_LEVEL_OFFSET = 6
LATE_GAME_LEVEL_OFFSET = 1
INVALID_NULLABLE_KEY_TOKENS = {"", "nan", "none", "null", "<na>", "na"}
GAME_VERSION_TO_GENERATION = {
    "red": 1,
    "blue": 1,
    "gold": 2,
    "silver": 2,
    "ruby": 3,
    "sapphire": 3,
    "diamond": 4,
    "pearl": 4,
    "black": 5,
    "white": 5,
    "black-white": 5,
    "x": 6,
    "y": 6,
}
_EXCLUDED_ENCOUNTER_METHODS = {"only one", "gift", "npc trade"}
_TYPE_CHART = build_type_chart()
_TYPE_WEIGHT_CAP_EPSILON = 1e-9


def _normalize_nullable_key_part(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    normalized = normalize_key_part(value)
    if normalized in INVALID_NULLABLE_KEY_TOKENS:
        return None
    return normalized or None


def _normalize_boss_id(value: Any) -> str:
    return str(value or "").strip().lower()


def _boss_slug_from_id(value: Any) -> str:
    normalized = _normalize_boss_id(value)
    if not normalized:
        return ""
    return normalize_key_part(normalized.split(":")[-1])


def _family_root_for_species(species: str) -> str:
    normalized = normalize_key_part(species)
    if not normalized:
        return ""
    return normalized


def _resolve_starters_for_condition(game_version: str, starter_condition: str | None) -> list[str]:
    condition = _normalize_nullable_key_part(starter_condition)
    if condition is None:
        return get_starter_choices(game_version)
    starters_by_type = get_starters_by_type(game_version, condition)
    if starters_by_type:
        return starters_by_type
    return [condition]


def _stable_species_signature(species_names: list[str]) -> str:
    return "|".join(sorted(normalize_key_part(name) for name in species_names if normalize_key_part(name)))


def _clamp_progression_depth(value: Any) -> float:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        numeric = 0.0
    return max(0.0, min(1.0, numeric))


def _safe_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _progression_level_offset(progression_depth: float) -> int:
    depth = _clamp_progression_depth(progression_depth)
    span = EARLY_GAME_LEVEL_OFFSET - LATE_GAME_LEVEL_OFFSET
    return max(LATE_GAME_LEVEL_OFFSET, int(round(LATE_GAME_LEVEL_OFFSET + ((1.0 - depth) * span))))


def _level_cap_from_progression(*, boss_ace_level: int, progression_depth: float) -> int:
    ace_level = max(1, int(boss_ace_level or DEFAULT_MEMBER_LEVEL))
    offset = _progression_level_offset(progression_depth)
    return max(1, ace_level - offset)


def _effective_member_level(*, level_cap: int, encounter_level_max: int) -> int:
    capped_level_cap = max(1, int(level_cap or DEFAULT_MEMBER_LEVEL))
    capped_encounter = max(1, int(encounter_level_max or DEFAULT_MEMBER_LEVEL))
    return min(capped_level_cap, capped_encounter)


def _generation_for_game_version(game_version: str) -> int | None:
    normalized = normalize_key_part(game_version)
    return GAME_VERSION_TO_GENERATION.get(normalized)


def _normalize_encounter_methods(value: Any) -> set[str]:
    if isinstance(value, (set, tuple)):
        source = list(value)
    elif hasattr(value, "tolist") and not isinstance(value, (str, bytes, dict)):
        source = list(value.tolist())
    elif isinstance(value, list):
        source = value
    else:
        return set()
    return {
        str(method).strip().lower()
        for method in source
        if str(method).strip()
    }


def _is_excluded_encounter_method(method: str) -> bool:
    normalized = str(method or "").strip().lower()
    if not normalized:
        return False
    compact = normalized.replace("-", " ").replace("_", " ")
    compact = " ".join(part for part in compact.split() if part)
    if compact in _EXCLUDED_ENCOUNTER_METHODS:
        return True
    return any(token in compact for token in ("only one", "npc trade", "gift"))


def _has_excluded_encounter_method(methods: Any) -> bool:
    if isinstance(methods, set):
        method_iter = methods
    elif isinstance(methods, list):
        method_iter = methods
    else:
        return False
    return any(_is_excluded_encounter_method(str(method)) for method in method_iter)


def _target_team_fill_size(
    *,
    progression_depth: float | None,
    catch_pool_size: int,
) -> int:
    """Scale generated team size with progression: early small, late larger."""
    max_size = max(1, min(catch_pool_size, MAX_SOURCE_TEAM_SIZE))
    if max_size <= 2:
        return max_size
    depth = _clamp_progression_depth(0.5 if progression_depth is None else progression_depth)
    # 2 slots in very early game, gradually up to max_size near endgame.
    scaled = 2 + int(round(depth * float(max_size - 2)))
    return max(1, min(max_size, scaled))


def _normalize_trigger_name(trigger: Any) -> str:
    return str(trigger or "").strip().lower().replace("_", "-")


def _parse_positive_int(value: Any) -> int | None:
    try:
        parsed = int(str(value).strip())
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _normalize_adjacency_evolution_rules(
    evolution_rules: dict[str, Any] | None,
) -> dict[str, list[dict[str, Any]]]:
    rules_raw = evolution_rules or {}
    if not isinstance(rules_raw, dict):
        return {}
    if rules_raw:
        sample = next(iter(rules_raw.values()))
        if isinstance(sample, dict) and "evolution_stage" in sample:
            return normalize_candidate_pool_for_level.__globals__["build_level_up_evolution_index_from_species_rules"](rules_raw)  # type: ignore[index]
    normalized: dict[str, list[dict[str, Any]]] = {}
    for species, options in rules_raw.items():
        species_key = normalize_key_part(species)
        if not species_key or not isinstance(options, list):
            continue
        clean_options = [option for option in options if isinstance(option, dict)]
        if clean_options:
            normalized[species_key] = clean_options
    return normalized


def _propagate_obtainable_candidates(
    raw_candidates: list[tuple[str, int, int, int]],
    *,
    evolution_rules: dict[str, Any] | None,
    allow_trade_evolutions: bool,
    allow_item_evolutions: bool,
    item_evolution_default_level: int,
) -> tuple[list[tuple[str, int, int, int]], dict[str, dict[str, Any]], set[str]]:
    direct_rows: dict[str, tuple[str, int, int, int]] = {}
    for species, chance, lvl_max, capture in raw_candidates:
        key = normalize_key_part(species)
        if not key:
            continue
        existing = direct_rows.get(key)
        row = (key, int(chance or 0), int(lvl_max or 0), int(capture or 0))
        if existing is None:
            direct_rows[key] = row
            continue
        direct_rows[key] = (key, max(existing[1], row[1]), max(existing[2], row[2]), max(existing[3], row[3]))

    rules = _normalize_adjacency_evolution_rules(evolution_rules)
    obtainable_rows = dict(direct_rows)
    obtainable_meta: dict[str, dict[str, Any]] = {
        species: {"directly_catchable": True, "obtain_method": "direct", "evolves_from": None, "required_item": None, "min_level": None}
        for species in direct_rows
    }
    queue = list(direct_rows.keys())

    while queue:
        current = queue.pop(0)
        source_row = obtainable_rows.get(current)
        if source_row is None:
            continue
        for option in rules.get(current, []):
            target = normalize_key_part(option.get("to_species") or option.get("evolves_to") or option.get("species"))
            if not target:
                continue
            trigger = _normalize_trigger_name(option.get("trigger") or option.get("evolution_trigger"))
            raw_min_level = option.get("min_level") or option.get("min_level_from_previous") or option.get("level")
            try:
                min_level = int(str(raw_min_level).strip()) if raw_min_level is not None else None
            except (TypeError, ValueError):
                min_level = None
            required_item = normalize_key_part(option.get("required_item") or option.get("item"))
            supported = False
            method = "evolution"
            assigned_min_level = min_level
            if trigger == "level-up":
                supported = min_level is not None
                method = "level_evolution"
            elif trigger == "trade":
                supported = allow_trade_evolutions
                method = "trade_evolution"
                assigned_min_level = min_level or 1
            elif trigger in {"use-item", "item"}:
                supported = allow_item_evolutions and bool(required_item)
                method = "item_evolution"
                assigned_min_level = max(1, int(source_row[2] or 0), int(item_evolution_default_level or 1))
            if not supported:
                continue
            if target in obtainable_rows:
                continue
            obtainable_rows[target] = (target, source_row[1], source_row[2], source_row[3])
            obtainable_meta[target] = {
                "directly_catchable": False,
                "obtain_method": method,
                "evolves_from": current,
                "required_item": required_item if method == "item_evolution" else None,
                "min_level": assigned_min_level,
            }
            queue.append(target)

    return (
        sorted(obtainable_rows.values(), key=lambda item: (item[0])),
        obtainable_meta,
        set(direct_rows.keys()),
    )


def _filter_candidates_by_obtainable_level_cap(
    raw_candidates: list[tuple[str, int, int, int]],
    *,
    direct_level_min_by_species: dict[str, int],
    level_cap: int,
    evolution_rules: dict[str, Any] | None,
    allow_trade_evolutions: bool,
    allow_item_evolutions: bool,
    item_evolution_default_level: int,
) -> list[tuple[str, int, int, int]]:
    _, obtainable_meta, _ = _propagate_obtainable_candidates(
        raw_candidates,
        evolution_rules=evolution_rules,
        allow_trade_evolutions=allow_trade_evolutions,
        allow_item_evolutions=allow_item_evolutions,
        item_evolution_default_level=item_evolution_default_level,
    )

    min_obtainable_cache: dict[str, int] = {}

    def _min_obtainable_level(species_name: str) -> int:
        species_key = normalize_key_part(species_name)
        if not species_key:
            return 1
        if species_key in min_obtainable_cache:
            return min_obtainable_cache[species_key]

        info = obtainable_meta.get(species_key, {})
        direct_min = _parse_positive_int(direct_level_min_by_species.get(species_key)) or 1
        if bool(info.get("directly_catchable", True)):
            min_obtainable_cache[species_key] = direct_min
            return direct_min

        parent = normalize_key_part(info.get("evolves_from"))
        parent_min = _min_obtainable_level(parent) if parent else direct_min
        evolution_min = _parse_positive_int(info.get("min_level")) or 1
        value = max(parent_min, evolution_min)
        min_obtainable_cache[species_key] = value
        return value

    cap = max(1, int(level_cap or 1))
    return [row for row in raw_candidates if _min_obtainable_level(row[0]) <= cap]



def _dedupe_species_by_family(candidates: list[tuple[str, int, int, int]]) -> list[tuple[str, int, int, int]]:
    seen_roots: set[str] = set()
    result: list[tuple[str, int, int, int]] = []
    for species, chance_max, level_max, capture_rate in candidates:
        root = _family_root_for_species(species)
        if root in seen_roots:
            continue
        seen_roots.add(root)
        result.append((species, chance_max, level_max, capture_rate))
    return result


def _normalize_type_name(value: Any) -> str:
    return str(value or "").strip().lower()


def _species_type_weights(
    species: str,
    *,
    pokemon_types_by_species: dict[str, tuple[str | None, str | None]],
) -> dict[str, float]:
    type_1, type_2 = pokemon_types_by_species.get(normalize_key_part(species), (None, None))
    t1 = _normalize_type_name(type_1)
    t2 = _normalize_type_name(type_2)
    if t1 and t2:
        return {t1: 0.5, t2: 0.5}
    if t1:
        return {t1: 1.0}
    if t2:
        return {t2: 1.0}
    return {}


def _primary_type_of_species(
    species: str,
    *,
    pokemon_types_by_species: dict[str, tuple[str | None, str | None]],
) -> str:
    type_1, _ = pokemon_types_by_species.get(normalize_key_part(species), (None, None))
    return _normalize_type_name(type_1)


def _species_team_cap_type_weights(
    species: str,
    *,
    pokemon_types_by_species: dict[str, tuple[str | None, str | None]],
) -> dict[str, float]:
    type_1, type_2 = pokemon_types_by_species.get(normalize_key_part(species), (None, None))
    t1 = _normalize_type_name(type_1)
    t2 = _normalize_type_name(type_2)
    if t1 and t2:
        if t1 == t2:
            return {t1: 1.5}
        return {t1: 0.75, t2: 0.75}
    if t1:
        return {t1: 1.0}
    if t2:
        return {t2: 1.0}
    return {}


def _type_multiplier(attacking_type: str, defending_type: str) -> float:
    attacking = str(attacking_type or "").strip().title()
    defending = str(defending_type or "").strip().title()
    return float(_TYPE_CHART.get(attacking, {}).get(defending, 1.0))


def _team_fractional_distribution(
    species_list: list[str],
    *,
    pokemon_types_by_species: dict[str, tuple[str | None, str | None]],
) -> dict[str, float]:
    counts: dict[str, float] = {}
    for species in species_list:
        for type_name, weight in _species_type_weights(species, pokemon_types_by_species=pokemon_types_by_species).items():
            counts[type_name] = counts.get(type_name, 0.0) + float(weight)
    total = sum(counts.values())
    if total <= 0:
        return {}
    return {type_name: value / total for type_name, value in counts.items()}


def _distribution_distance_l1(left: dict[str, float], right: dict[str, float]) -> float:
    keys = set(left) | set(right)
    return sum(abs(float(left.get(key, 0.0)) - float(right.get(key, 0.0))) for key in keys)


def _boss_counter_score(
    species_list: list[str],
    *,
    pokemon_types_by_species: dict[str, tuple[str | None, str | None]],
    boss_type_profile: dict[str, float],
) -> float:
    if not boss_type_profile:
        return 1.0
    per_member: list[float] = []
    for species in species_list:
        member_weights = _species_type_weights(species, pokemon_types_by_species=pokemon_types_by_species)
        if not member_weights:
            continue
        best_vs_boss = 1.0
        for attacker_type in member_weights:
            weighted = sum(_type_multiplier(attacker_type, defender_type) * float(weight) for defender_type, weight in boss_type_profile.items())
            best_vs_boss = max(best_vs_boss, weighted)
        per_member.append(best_vs_boss)
    if not per_member:
        return 1.0
    return sum(per_member) / float(len(per_member))


def _team_type_weight_cap(progression_depth: float | None) -> float:
    _ = progression_depth
    return max(0.0, float(DEFAULT_TEAM_TYPE_WEIGHT_CAP))


def _team_type_weight_totals(
    species_list: list[str],
    *,
    pokemon_types_by_species: dict[str, tuple[str | None, str | None]],
    ) -> dict[str, float]:
    counts: dict[str, float] = {}
    for species in species_list:
        for type_name, weight in _species_team_cap_type_weights(
            species,
            pokemon_types_by_species=pokemon_types_by_species,
        ).items():
            counts[type_name] = counts.get(type_name, 0.0) + float(weight)
    return counts


def _respects_team_type_weight_cap(
    species_list: list[str],
    *,
    pokemon_types_by_species: dict[str, tuple[str | None, str | None]],
    cap: float,
) -> bool:
    if cap <= 0:
        return True
    for weight in _team_type_weight_totals(
        species_list,
        pokemon_types_by_species=pokemon_types_by_species,
    ).values():
        if weight - float(cap) > _TYPE_WEIGHT_CAP_EPSILON:
            return False
    return True


def _rank_candidate_pool(
    candidates: list[tuple[str, int, int, int]],
    *,
    boss_level: int,
    pool_size: int,
    progression_depth: float | None = None,
) -> tuple[list[tuple[str, int, int, int]], dict[str, int]]:
    if not candidates:
        return [], {"input": 0, "output": 0, "pruned": 0, "family_pruned": 0}

    family_deduped = _dedupe_species_by_family(candidates)
    family_pruned = max(0, len(candidates) - len(family_deduped))
    scored: list[tuple[float, tuple[str, int, int, int]]] = []

    clamped_depth = _clamp_progression_depth(0.5 if progression_depth is None else progression_depth)
    early_focus = 1.0 - clamped_depth
    for species, chance_max, level_max, capture_rate in family_deduped:
        level_gap = abs(int(level_max or 0) - int(boss_level or DEFAULT_MEMBER_LEVEL))
        level_realism = max(0.0, 1.0 - min(level_gap, 25) / 25.0)
        chance_signal = min(max(float(chance_max), 0.0), 100.0) / 100.0
        capture_signal = min(max(float(capture_rate), 0.0), 255.0) / 255.0
        rarity_penalty = (1.0 - chance_signal) * 0.6 + (1.0 - capture_signal) * 0.4
        score = (
            (0.20 + 0.25 * early_focus) * chance_signal
            + (0.15 + 0.20 * early_focus) * capture_signal
            + (0.65 - 0.35 * early_focus) * level_realism
            - (0.35 * early_focus) * rarity_penalty
        )
        scored.append((score, (species, chance_max, level_max, capture_rate)))

    scored.sort(key=lambda item: (-item[0], -item[1][1], -item[1][2], -item[1][3], item[1][0]))
    ranked = [row for _, row in scored]
    constrained = ranked[: max(1, pool_size)]

    diagnostics = {
        "input": len(candidates),
        "output": len(constrained),
        "pruned": max(0, len(ranked) - len(constrained)),
        "family_pruned": family_pruned,
    }
    return constrained, diagnostics


def _filter_candidates_with_damaging_moves(
    candidates: list[tuple[str, int, int, int]],
    *,
    level_cap: int,
    game_version: str,
    reference_context: MoveReferenceContext | None,
) -> tuple[list[tuple[str, int, int, int]], dict[str, int]]:
    if reference_context is None:
        return candidates, {"removed_no_damaging_moves": 0}

    filtered: list[tuple[str, int, int, int]] = []
    removed = 0
    for species, chance_max, level_max, capture_rate in candidates:
        effective_level = _effective_member_level(level_cap=level_cap, encounter_level_max=level_max)
        if reference_context.damaging_moves(species, effective_level, game_version):
            filtered.append((species, chance_max, level_max, capture_rate))
            continue
        removed += 1

    return filtered, {"removed_no_damaging_moves": removed}




def _base_team_diversity_score(species_combo: tuple[tuple[str, int, int, int], ...]) -> tuple[int, int, int, str]:
    chance_sum = sum(item[1] for item in species_combo)
    level_sum = sum(item[2] for item in species_combo)
    capture_sum = sum(item[3] for item in species_combo)
    signature = _stable_species_signature([item[0] for item in species_combo])
    return (chance_sum, level_sum, capture_sum, signature)


def _primary_type_signature(
    species_list: list[str],
    *,
    pokemon_types_by_species: dict[str, tuple[str | None, str | None]],
) -> str:
    primary_types = sorted(
        {
            primary_type
            for primary_type in (
                _primary_type_of_species(species, pokemon_types_by_species=pokemon_types_by_species)
                for species in species_list
            )
            if primary_type
        }
    )
    return "|".join(primary_types)


def _generate_diverse_species_combos(
    candidates: list[tuple[str, int, int, int]],
    team_fill_size: int,
    combo_limit: int,
    *,
    progression_depth: float | None,
    pokemon_types_by_species: dict[str, tuple[str | None, str | None]],
    game_type_target_distribution: dict[str, float],
    boss_type_profile: dict[str, float],
) -> list[list[str]]:
    if team_fill_size <= 0:
        return [[]]

    unique_family_candidates = _dedupe_species_by_family(candidates)
    if len(unique_family_candidates) < team_fill_size:
        unique_family_candidates = candidates
    if len(unique_family_candidates) < team_fill_size:
        return []

    def _species_types(species_name: str) -> set[str]:
        return {
            type_name
            for type_name in _species_team_cap_type_weights(
                species_name,
                pokemon_types_by_species=pokemon_types_by_species,
            ).keys()
            if type_name
        }

    def _target_type_count_for_team(*, size: int, depth: float | None) -> int:
        # Early game: fewer anchor types; late game: broader type spread.
        clamped = _clamp_progression_depth(0.5 if depth is None else depth)
        desired = 2 + int(round(clamped * 2.0))
        return max(2, min(size, desired))

    def _weighted_distinct_types(
        *,
        type_weights: dict[str, float],
        n_types: int,
        rng: random.Random,
    ) -> list[str]:
        available = {k: float(v) for k, v in type_weights.items() if float(v) > 0.0}
        selected: list[str] = []
        for _ in range(max(0, n_types)):
            if not available:
                break
            items = list(available.items())
            total = sum(weight for _, weight in items)
            if total <= 0:
                break
            pick = rng.random() * total
            cursor = 0.0
            chosen = items[-1][0]
            for type_name, weight in items:
                cursor += float(weight)
                if cursor >= pick:
                    chosen = type_name
                    break
            selected.append(chosen)
            available.pop(chosen, None)
        return selected

    def _candidate_orders() -> list[list[tuple[str, int, int, int]]]:
        if not unique_family_candidates:
            return []
        orders: list[list[tuple[str, int, int, int]]] = [list(unique_family_candidates)]
        n_types = _target_type_count_for_team(size=team_fill_size, depth=progression_depth)
        order_seed = stable_digest(
            "|".join(f"{species}:{chance}:{level}:{capture}" for species, chance, level, capture in unique_family_candidates)
            + f"|fill={team_fill_size}|limit={combo_limit}|depth={_clamp_progression_depth(0.5 if progression_depth is None else progression_depth):.4f}"
        )
        rng = random.Random(int(order_seed[:12], 16))

        # Build type weights from encounter distribution, fallback to uniform over observed candidate types.
        observed_type_counts: dict[str, float] = {}
        for species, *_ in unique_family_candidates:
            for type_name in _species_types(species):
                observed_type_counts[type_name] = observed_type_counts.get(type_name, 0.0) + 1.0
        weighted_types = {
            type_name: float(game_type_target_distribution.get(type_name, 0.0))
            for type_name in observed_type_counts
        }
        if not any(weighted_types.values()):
            weighted_types = observed_type_counts

        for _ in range(4):
            selected_types = _weighted_distinct_types(type_weights=weighted_types, n_types=n_types, rng=rng)
            if not selected_types:
                continue
            selected_set = set(selected_types)
            prioritized = sorted(
                unique_family_candidates,
                key=lambda row: (
                    -len(_species_types(row[0]) & selected_set),
                    -max((weighted_types.get(t, 0.0) for t in _species_types(row[0]) & selected_set), default=0.0),
                    -row[1],
                    -row[2],
                    -row[3],
                    row[0],
                ),
            )
            orders.append(prioritized)
        return orders

    scored: list[tuple[tuple[float, int, float, int, int, int, str], list[str]]] = []
    cap = _team_type_weight_cap(progression_depth)
    search_budget_per_order = max(combo_limit * 20, combo_limit)
    for ordered_candidates in _candidate_orders():
        raw_combos = combinations(ordered_candidates, team_fill_size)
        for combo in islice(raw_combos, search_budget_per_order):
            species_list = [item[0] for item in combo]
            if not _respects_team_type_weight_cap(
                species_list,
                pokemon_types_by_species=pokemon_types_by_species,
                cap=cap,
            ):
                continue
            type_distribution = _team_fractional_distribution(
                species_list,
                pokemon_types_by_species=pokemon_types_by_species,
            )
            realism_distance = _distribution_distance_l1(type_distribution, game_type_target_distribution)
            counter_score = _boss_counter_score(
                species_list,
                pokemon_types_by_species=pokemon_types_by_species,
                boss_type_profile=boss_type_profile,
            )
            unique_primary_types = len(
                {
                    _primary_type_of_species(species, pokemon_types_by_species=pokemon_types_by_species)
                    for species in species_list
                    if _primary_type_of_species(species, pokemon_types_by_species=pokemon_types_by_species)
                }
            )
            chance_sum, level_sum, capture_sum, signature = _base_team_diversity_score(combo)
            scored.append(((-realism_distance, unique_primary_types, counter_score, chance_sum, level_sum, capture_sum, signature), species_list))

    scored.sort(key=lambda item: (-item[0][0], -item[0][1], -item[0][2], -item[0][3], -item[0][4], -item[0][5], item[0][6]))
    seen: set[str] = set()
    seen_primary_type_signatures: set[str] = set()
    deferred: list[list[str]] = []
    out: list[list[str]] = []
    for _, species_list in scored:
        signature = _stable_species_signature(species_list)
        if signature in seen:
            continue
        seen.add(signature)
        primary_signature = _primary_type_signature(
            species_list,
            pokemon_types_by_species=pokemon_types_by_species,
        )
        if primary_signature and primary_signature not in seen_primary_type_signatures:
            seen_primary_type_signatures.add(primary_signature)
            out.append(species_list)
        else:
            deferred.append(species_list)
        if len(out) >= combo_limit:
            break
    if len(out) < combo_limit:
        for species_list in deferred:
            out.append(species_list)
            if len(out) >= combo_limit:
                break
    if out:
        return out

    # Enumeration frontier can miss feasible teams on certain ranked-order prefixes.
    # Run a constructive type-weighted search before upstream fallback.
    constructed: list[list[str]] = []
    seen_constructed: set[str] = set()
    clamped_depth = _clamp_progression_depth(0.5 if progression_depth is None else progression_depth)
    n_types = max(2, min(team_fill_size, 2 + int(round(clamped_depth * 2.0))))
    observed_type_counts: dict[str, float] = {}
    for species, *_ in unique_family_candidates:
        for type_name in _species_team_cap_type_weights(
            species,
            pokemon_types_by_species=pokemon_types_by_species,
        ).keys():
            if type_name:
                observed_type_counts[type_name] = observed_type_counts.get(type_name, 0.0) + 1.0
    weighted_types = {
        type_name: float(game_type_target_distribution.get(type_name, 0.0))
        for type_name in observed_type_counts
    }
    if not any(weighted_types.values()):
        weighted_types = observed_type_counts
    rng_seed = stable_digest(
        "|".join(f"{species}:{chance}:{level}:{capture}" for species, chance, level, capture in unique_family_candidates)
        + f"|constructive|fill={team_fill_size}|limit={combo_limit}"
    )
    rng = random.Random(int(rng_seed[:12], 16))
    trials = max(combo_limit * 50, 500)
    cap = _team_type_weight_cap(progression_depth)
    for _ in range(trials):
        selected_types = _weighted_distinct_types(type_weights=weighted_types, n_types=n_types, rng=rng)
        if not selected_types:
            continue
        selected_type_set = set(selected_types)
        bucket = [
            row for row in unique_family_candidates
            if _species_team_cap_type_weights(row[0], pokemon_types_by_species=pokemon_types_by_species).keys() & selected_type_set
        ]
        if len(bucket) < team_fill_size:
            continue
        rng.shuffle(bucket)
        team: list[str] = []
        type_totals: dict[str, float] = {}
        for species, *_ in bucket:
            if species in team:
                continue
            weights = _species_team_cap_type_weights(species, pokemon_types_by_species=pokemon_types_by_species)
            if any((type_totals.get(t, 0.0) + w) - float(cap) > _TYPE_WEIGHT_CAP_EPSILON for t, w in weights.items()):
                continue
            team.append(species)
            for t, w in weights.items():
                type_totals[t] = type_totals.get(t, 0.0) + float(w)
            if len(team) >= team_fill_size:
                break
        if len(team) < team_fill_size:
            continue
        signature = _stable_species_signature(team)
        if signature in seen_constructed:
            continue
        seen_constructed.add(signature)
        constructed.append(team)
        if len(constructed) >= combo_limit:
            break
    return constructed


def _fallback_team_with_type_weight_cap(
    candidates: list[tuple[str, int, int, int]],
    *,
    team_fill_size: int,
    pokemon_types_by_species: dict[str, tuple[str | None, str | None]],
    cap: float,
) -> list[str]:
    selected: list[str] = []
    type_weights: dict[str, float] = {}
    for species, _, _, _ in candidates:
        weights = _species_team_cap_type_weights(species, pokemon_types_by_species=pokemon_types_by_species)
        if any(
            (type_weights.get(type_name, 0.0) + weight) - float(cap) > _TYPE_WEIGHT_CAP_EPSILON
            for type_name, weight in weights.items()
        ):
            continue
        selected.append(species)
        for type_name, weight in weights.items():
            type_weights[type_name] = type_weights.get(type_name, 0.0) + float(weight)
        if len(selected) >= team_fill_size:
            break
    if len(selected) >= team_fill_size:
        return selected
    # Strict mode: do not backfill with cap-violating species.
    return selected




def build_progression_source_teams_from_encounters(
    encounters_df: pd.DataFrame,
    bosses_df: pd.DataFrame,
    boss_teams: list[dict[str, Any]] | None = None,
    progression_depth_context: ProgressionDepthContext | None = None,
    catch_pool_size: int = DEFAULT_CATCH_POOL_SIZE,
    evolution_rules_by_game: dict[str, dict[str, list[dict[str, Any]]]] | None = None,
    reference_context: MoveReferenceContext | None = None,
    allow_trade_evolutions: bool = False,
    allow_item_evolutions: bool = ALLOW_ITEM_EVOLUTIONS,
    item_evolution_default_level: int = ITEM_EVOLUTION_DEFAULT_LEVEL,
    pokemon_types_by_species: dict[str, tuple[str | None, str | None]] | None = None,
    boss_type_profile_by_key: dict[tuple[str, str], dict[str, float]] | None = None,
) -> list[dict[str, Any]]:
    """Build player source teams using persisted Silver references only."""
    required_encounter_columns = {
        "boss_id",
        "location",
        "pokemon",
        "level_min",
        "level_max",
        "encounter_chance_max",
        "capture_rate",
        "game",
    }
    missing_encounter_columns = sorted(required_encounter_columns - set(encounters_df.columns))
    if missing_encounter_columns:
        raise ValueError(f"encounters.parquet missing required columns: {missing_encounter_columns}")

    required_boss_columns = {"boss_id", "game_version", "boss_name_canonical", "boss_order"}
    missing_boss_columns = sorted(required_boss_columns - set(bosses_df.columns))
    if missing_boss_columns:
        raise ValueError(f"bosses.parquet missing required columns: {missing_boss_columns}")

    encounters = encounters_df.rename(
        columns={
            "game": "game_version",
            "pokemon": "pokemon_species",
        }
    ).copy()
    encounters["game_version"] = encounters["game_version"].map(normalize_key_part)
    encounters["boss_id"] = encounters["boss_id"].map(_normalize_boss_id)
    encounters["boss_slug"] = encounters["boss_id"].map(_boss_slug_from_id)
    encounters["location"] = encounters["location"].map(normalize_key_part)
    encounters["pokemon_species"] = encounters["pokemon_species"].map(normalize_key_part)
    encounters["level_min"] = pd.to_numeric(encounters["level_min"], errors="coerce").fillna(0).astype(int)
    encounters["level_max"] = pd.to_numeric(encounters["level_max"], errors="coerce").fillna(0).astype(int)
    encounters["encounter_chance_max"] = pd.to_numeric(encounters["encounter_chance_max"], errors="coerce").fillna(0).astype(int)
    encounters["capture_rate"] = pd.to_numeric(encounters["capture_rate"], errors="coerce").fillna(0).astype(int)
    encounters["methods"] = encounters["methods"].apply(_normalize_encounter_methods) if "methods" in encounters.columns else [set() for _ in range(len(encounters))]
    encounters = encounters[
        (encounters["game_version"] != "")
        & (encounters["boss_id"] != "")
        & (encounters["pokemon_species"] != "")
    ]
    encounters = encounters[
        ~encounters["methods"].apply(_has_excluded_encounter_method)
    ]

    bosses = bosses_df.copy()
    bosses["game_version"] = bosses["game_version"].map(normalize_key_part)
    bosses["boss_id"] = bosses["boss_id"].map(_normalize_boss_id)
    bosses["boss_name"] = bosses["boss_name_canonical"].map(normalize_key_part)
    bosses["boss_slug"] = bosses["boss_name"].map(normalize_key_part)
    bosses["boss_order"] = pd.to_numeric(bosses["boss_order"], errors="coerce").fillna(0).astype(int)
    if "gym_index" in bosses.columns:
        bosses["gym_index"] = pd.to_numeric(bosses["gym_index"], errors="coerce").fillna(bosses["boss_order"]).astype(int)
    else:
        bosses["gym_index"] = bosses["boss_order"]
    bosses["starter_condition"] = (
        bosses["starter_condition"].map(_normalize_nullable_key_part) if "starter_condition" in bosses.columns else None
    )
    bosses = bosses[(bosses["game_version"] != "") & (bosses["boss_id"] != "")]

    logger.info(
        "[silver/teams] encounters rows loaded=%s bosses loaded=%s",
        len(encounters),
        len(bosses),
    )
    per_game_rows = encounters.groupby("game_version", dropna=False, observed=False).size().to_dict()
    logger.info("[silver/teams] per-game encounter rows=%s", per_game_rows)

    boss_id_by_game_slug = {
        (str(row.game_version), str(row.boss_slug)): str(row.boss_id)
        for row in bosses[["game_version", "boss_slug", "boss_id"]].drop_duplicates().itertuples(index=False)
        if str(row.game_version) and str(row.boss_slug) and str(row.boss_id)
    }
    encounters["boss_id"] = [
        boss_id_by_game_slug.get((str(game_version), str(boss_slug)), str(boss_id))
        for game_version, boss_slug, boss_id in encounters[["game_version", "boss_slug", "boss_id"]].itertuples(index=False, name=None)
    ]

    legal_pairs = {
        (str(row.game_version), str(row.boss_id))
        for row in encounters[["game_version", "boss_id"]].drop_duplicates().itertuples(index=False)
    }
    if progression_depth_context is None:
        raise ValueError("progression_depth_context is required")
    evolution_rules_by_game = evolution_rules_by_game or {}
    max_team_fill_size = max(1, min(catch_pool_size, MAX_SOURCE_TEAM_SIZE))
    candidate_pool_size = max(max_team_fill_size, DEFAULT_SOURCE_TEAM_POOL_SIZE)
    sources: list[dict[str, Any]] = []
    pokemon_types_by_species = pokemon_types_by_species or {}
    boss_type_profile_by_key = boss_type_profile_by_key or {}

    game_type_target_distribution: dict[str, dict[str, float]] = {}
    for game_version, game_rows in encounters.groupby("game_version", observed=False):
        counts: dict[str, float] = {}
        total = 0.0
        grouped_species = (
            game_rows.groupby("pokemon_species", as_index=False)
            .agg(encounter_chance_max=("encounter_chance_max", "max"))
            .itertuples(index=False)
        )
        for row in grouped_species:
            species = str(row.pokemon_species)
            chance_weight = max(1.0, float(row.encounter_chance_max or 0.0))
            species_weights = _species_type_weights(species, pokemon_types_by_species=pokemon_types_by_species)
            for type_name, type_weight in species_weights.items():
                weighted = chance_weight * float(type_weight)
                counts[type_name] = counts.get(type_name, 0.0) + weighted
                total += weighted
        if total > 0:
            game_type_target_distribution[str(game_version)] = {
                type_name: value / total
                for type_name, value in counts.items()
            }

    dropped_missing_boss = 0
    processed_bosses = 0
    boss_rows_sorted = list(bosses.sort_values(["game_version", "boss_order", "boss_id"]).itertuples(index=False))
    for boss in boss_rows_sorted:
        processed_bosses += 1
        game_version = str(boss.game_version)
        boss_id = str(boss.boss_id)
        boss_name = normalize_key_part(getattr(boss, "boss_name", None) or boss_id)
        starter_condition = _normalize_nullable_key_part(getattr(boss, "starter_condition", None))
        gym_index = int(getattr(boss, "gym_index", getattr(boss, "boss_order", 0)) or 0)
        part = f"order-{gym_index}"

        if (game_version, boss_id) not in legal_pairs:
            dropped_missing_boss += 1
            logger.warning(
                "[silver/teams] skipping boss without encounter pool game=%s boss_id=%s boss_name=%s",
                game_version,
                boss_id,
                boss_name,
            )
            continue

        boss_rows = encounters[(encounters["game_version"] == game_version) & (encounters["boss_id"] == boss_id)]
        if boss_rows.empty:
            continue
        progression = progression_depth_context.require(
            game_version=game_version,
            boss_id=boss_id,
            boss_name=boss_name,
        )
        player_level_cap = _level_cap_from_progression(
            boss_ace_level=progression.boss_ace_level,
            progression_depth=progression.progression_depth,
        )
        team_fill_size = _target_team_fill_size(
            progression_depth=progression.progression_depth,
            catch_pool_size=max_team_fill_size,
        )

        grouped = (
            boss_rows.groupby("pokemon_species", as_index=False)
            .agg(
                level_max=("level_max", "max"),
                level_min=("level_min", "min"),
                encounter_chance_max=("encounter_chance_max", "max"),
                capture_rate=("capture_rate", "max"),
            )
            .sort_values(["pokemon_species"])
        )
        raw_candidates = [
            (
                str(row.pokemon_species),
                int(row.encounter_chance_max),
                int(row.level_max),
                int(row.capture_rate),
            )
            for row in grouped.itertuples(index=False)
            if not is_restricted_encounter_species(str(row.pokemon_species))
        ]
        direct_level_min_by_species = {
            str(row.pokemon_species): int(row.level_min)
            for row in grouped.itertuples(index=False)
            if not is_restricted_encounter_species(str(row.pokemon_species))
        }
        evolution_rules = evolution_rules_by_game.get(game_version, {})
        raw_candidates = _filter_candidates_by_obtainable_level_cap(
            raw_candidates,
            direct_level_min_by_species=direct_level_min_by_species,
            level_cap=player_level_cap,
            evolution_rules=evolution_rules,
            allow_trade_evolutions=allow_trade_evolutions,
            allow_item_evolutions=allow_item_evolutions,
            item_evolution_default_level=item_evolution_default_level,
        )
        raw_species = {species for species, _, _, _ in raw_candidates}
        logger.debug(
            "[silver/teams] per-boss raw candidate count game=%s boss_id=%s boss_name=%s count=%s",
            game_version,
            boss_id,
            boss_name,
            len(raw_candidates),
        )

        evolution_rules = evolution_rules_by_game.get(game_version, {})
        target_generation = _generation_for_game_version(game_version)
        legal_species = legal_species_pool_for_level(
            raw_candidates,
            member_level=player_level_cap,
            evolution_rules=evolution_rules,
            target_generation=target_generation,
            allow_trade_evolutions=allow_trade_evolutions,
            allow_item_evolutions=allow_item_evolutions,
            item_evolution_default_level=item_evolution_default_level,
        )
        normalized_candidates, normalization_diag = normalize_candidate_pool_for_level(
            raw_candidates,
            member_level=player_level_cap,
            evolution_rules=evolution_rules,
            legal_species=legal_species if legal_species else None,
            target_generation=target_generation,
            allow_trade_evolutions=allow_trade_evolutions,
            allow_item_evolutions=allow_item_evolutions,
            item_evolution_default_level=item_evolution_default_level,
        )
        validate_candidate_pool(normalized_candidates, legal_species=legal_species or raw_species, game_version=game_version)

        normalized_candidates, move_diag = _filter_candidates_with_damaging_moves(
            normalized_candidates,
            level_cap=player_level_cap,
            game_version=game_version,
            reference_context=reference_context,
        )

        candidate_pool, rank_diag = _rank_candidate_pool(
            normalized_candidates,
            boss_level=player_level_cap,
            pool_size=candidate_pool_size,
            progression_depth=progression.progression_depth,
        )
        logger.debug(
            "[silver/teams] per-boss final candidate count game=%s boss_id=%s boss_name=%s raw=%s final=%s evolved=%s removed=%s no_damage_removed=%s pruned=%s progression_depth=%.4f level_cap=%s offset=%s ace_level=%s",
            game_version,
            boss_id,
            boss_name,
            len(raw_candidates),
            len(candidate_pool),
            normalization_diag.get("transformed", 0),
            normalization_diag.get("removed_after_validation", 0),
            move_diag.get("removed_no_damaging_moves", 0),
            rank_diag.get("pruned", 0),
            progression.progression_depth,
            player_level_cap,
            _progression_level_offset(progression.progression_depth),
            progression.boss_ace_level,
        )

        species_combos = _generate_diverse_species_combos(
            candidates=candidate_pool,
            team_fill_size=team_fill_size,
            combo_limit=DEFAULT_SOURCE_TEAM_COMBO_LIMIT,
            progression_depth=progression.progression_depth,
            pokemon_types_by_species=pokemon_types_by_species,
            game_type_target_distribution=game_type_target_distribution.get(game_version, {}),
            boss_type_profile=boss_type_profile_by_key.get((game_version, boss_id), {}),
        )
        if not species_combos:
            fallback = _fallback_team_with_type_weight_cap(
                candidate_pool,
                team_fill_size=team_fill_size,
                pokemon_types_by_species=pokemon_types_by_species,
                cap=_team_type_weight_cap(progression.progression_depth),
            )
            if fallback:
                species_combos = [fallback]
                if len(fallback) < team_fill_size:
                    logger.warning(
                        "[silver/teams] strict type-cap fallback produced reduced team game=%s boss_id=%s boss_name=%s "
                        "target_size=%s fallback_size=%s",
                        game_version,
                        boss_id,
                        boss_name,
                        team_fill_size,
                        len(fallback),
                    )
            else:
                logger.warning(
                    "[silver/teams] strict type-cap fallback could not form full team game=%s boss_id=%s boss_name=%s "
                    "target_size=%s fallback_size=%s",
                    game_version,
                    boss_id,
                    boss_name,
                    team_fill_size,
                    len(fallback),
                )
        level_by_species = {
            species: _effective_member_level(level_cap=player_level_cap, encounter_level_max=level_max)
            for species, _, level_max, _ in candidate_pool
        }

        for combo_index, selected_species in enumerate(species_combos, start=1):
            invalid_reasons: list[dict[str, str]] = []
            for pokemon_species in selected_species:
                if pokemon_species not in (legal_species or raw_species):
                    invalid_reasons.append(
                        {
                            "game_version": game_version,
                            "boss_id": boss_id,
                            "boss_name": boss_name,
                            "pokemon_species": pokemon_species,
                            "reason": "species_not_in_encounters_for_boss",
                        }
                    )
            if invalid_reasons:
                raise ValueError(f"Invalid player candidates detected: {invalid_reasons[:20]}")

            validate_generated_team(
                selected_species,
                legal_species=legal_species or raw_species,
                game_version=game_version,
                boss_name=boss_name,
            )
            type_weights = _team_type_weight_totals(
                selected_species,
                pokemon_types_by_species=pokemon_types_by_species,
            )
            type_cap = _team_type_weight_cap(progression.progression_depth)
            violating = {t: w for t, w in type_weights.items() if w - float(type_cap) > _TYPE_WEIGHT_CAP_EPSILON}
            if violating:
                raise ValueError(
                    "Generated team violates weighted type cap "
                    f"game_version={game_version} boss_id={boss_id} boss_name={boss_name} "
                    f"cap={type_cap} offending={violating} species={selected_species}"
                )
            source_team_id = make_team_id(
                "progression",
                game_version,
                boss_name,
                variant=f"{boss_id}:{part}:{combo_index}:{_stable_species_signature(selected_species)}",
            )
            sources.append(
                {
                    "team_id": source_team_id,
                    "game_version": game_version,
                    "team_role": "player",
                    "origin": "generated",
                    "is_player_candidate": True,
                    "boss_id": boss_id,
                    "boss_name": boss_name,
                    "gym": str(boss_name).strip() or None,
                    "gym_index": gym_index,
                    "starter_condition": starter_condition,
                    "starter_type": starter_condition,
                    "avg_level": player_level_cap,
                    "player_max_level": progression.boss_ace_level,
                    "pokemon": selected_species,
                    "levels": [level_by_species.get(species, player_level_cap) for species in selected_species],
                    "progression_pool_id": f"pool:{game_version}:{boss_id}",
                    "part": part,
                    "boss_index": progression.boss_index,
                    "max_boss_index": progression.max_boss_index,
                    "available_species_count": progression.available_species_count,
                    "max_species_count": progression.max_species_count,
                    "progression_depth": progression.progression_depth,
                    "boss_ace_level": progression.boss_ace_level,
                    "boss_avg_level": progression.boss_avg_level,
                    "level_cap_offset": _progression_level_offset(progression.progression_depth),
                }
            )
    logger.info(
        "[silver/teams] built progression source teams from encounters source_teams=%s processed_bosses=%s dropped_missing_boss=%s",
        len(sources),
        processed_bosses,
        dropped_missing_boss,
    )
    return sources


def build_player_team_compact_tables(
    progression_source_teams: list[dict[str, Any]],
    reference_context: MoveReferenceContext,
    *,
    evolution_rules_by_game: dict[str, dict[str, list[dict[str, Any]]]] | None = None,
    allow_trade_evolutions: bool = False,
    allow_item_evolutions: bool = ALLOW_ITEM_EVOLUTIONS,
    item_evolution_default_level: int = ITEM_EVOLUTION_DEFAULT_LEVEL,
) -> dict[str, list[dict[str, Any]]]:
    started_at = time.perf_counter()
    source_teams: list[dict[str, Any]] = []
    source_team_members: list[dict[str, Any]] = []
    member_move_options: list[dict[str, Any]] = []
    member_moveset_combos: list[dict[str, Any]] = []
    pokemon_moveset_options: list[dict[str, Any]] = []
    sampling_plan: list[dict[str, Any]] = []

    moveset_context_seen: set[tuple[str, str, int, str]] = set()
    moveset_choice_seen: set[tuple[str, str, int, str, str]] = set()
    estimated_avoided_variants = 0

    evolution_rules_by_game = evolution_rules_by_game or {}
    for progression_team in progression_source_teams:
        team_role = normalize_key_part(progression_team.get("team_role"))
        origin = normalize_key_part(progression_team.get("origin"))
        is_player_candidate = bool(progression_team.get("is_player_candidate", True))
        boss_name = normalize_key_part(progression_team.get("boss_name") or progression_team.get("gym"))
        boss_id = str(progression_team.get("boss_id") or "").strip().lower()
        starter_condition = _normalize_nullable_key_part(progression_team.get("starter_condition"))
        if team_role == "boss" or origin == "kaggle" or not is_player_candidate:
            continue
        game_version = normalize_key_part(progression_team.get("game_version"))
        target_generation = _generation_for_game_version(game_version)
        evolution_rules = evolution_rules_by_game.get(game_version, {})
        avg_level = int(progression_team.get("avg_level") or DEFAULT_MEMBER_LEVEL)
        starters = _resolve_starters_for_condition(game_version, starter_condition)
        if not game_version or not starters:
            continue

        for starter_base in starters:
            starter_species = resolve_starter_species_for_level(starter_base, avg_level)
            starter_type = get_starter_type(starter_base)
            source_team_id = make_team_id(
                "player-source",
                game_version,
                boss_name,
                source_team_id=str(progression_team.get("team_id") or ""),
                variant=f"starter:{starter_base}",
            )
            logical_species = [starter_species] + list(progression_team.get("pokemon") or [])
            levels = [avg_level] + [int(level) for level in list(progression_team.get("levels") or [])]
            logical_species = logical_species[:DEFAULT_TEAM_MEMBER_LIMIT]
            levels = levels[: len(logical_species)]

            source_teams.append(
                {
                    "source_team_id": source_team_id,
                    "game_version": game_version,
                    "team_role": "player",
                    "origin": "generated",
                    "is_player_candidate": True,
                    "boss_id": boss_id,
                    "boss_name": boss_name,
                    "gym_index": progression_team.get("gym_index"),
                    "starter_condition": starter_condition,
                    "starter_type": starter_type,
                    "starter_base": normalize_key_part(starter_base),
                    "starter_evolved_species": normalize_key_part(starter_species),
                    "progression_source_team_id": progression_team.get("team_id"),
                    "progression_pool_id": progression_team.get("progression_pool_id"),
                    "avg_level": avg_level,
                    "player_max_level": progression_team.get("player_max_level", avg_level),
                    "member_count": len(logical_species),
                    "boss_index": progression_team.get("boss_index"),
                    "max_boss_index": progression_team.get("max_boss_index"),
                    "available_species_count": progression_team.get("available_species_count"),
                    "max_species_count": progression_team.get("max_species_count"),
                    "progression_depth": progression_team.get("progression_depth"),
                    "boss_ace_level": progression_team.get("boss_ace_level"),
                    "boss_avg_level": progression_team.get("boss_avg_level"),
                    "level_cap_offset": progression_team.get("level_cap_offset"),
                }
            )

            member_combo_counts: list[int] = []
            for slot, species in enumerate(logical_species, start=1):
                species_norm = normalize_key_part(species)
                level = int(levels[slot - 1] if slot - 1 < len(levels) else avg_level)
                if species_norm:
                    species_norm, _ = normalize_species_for_level(
                        species_norm,
                        member_level=level,
                        evolution_rules=evolution_rules,
                        target_generation=target_generation,
                        allow_trade_evolutions=allow_trade_evolutions,
                        allow_item_evolutions=allow_item_evolutions,
                        item_evolution_default_level=item_evolution_default_level,
                    )
                member_id = make_pokemon_instance_id(source_team_id, slot, species_norm)
                source_team_members.append(
                    {
                        "team_member_id": member_id,
                        "source_team_id": source_team_id,
                        "game_version": game_version,
                        "team_role": "player",
                        "origin": "generated",
                        "boss_id": boss_id,
                        "boss_name": boss_name,
                        "gym_index": progression_team.get("gym_index"),
                        "starter_condition": starter_condition,
                        "starter_type": starter_type,
                        "slot": slot,
                        "pokemon_species": species_norm,
                        "level": level,
                        "progression_pool_id": progression_team.get("progression_pool_id"),
                        "is_starter": slot == 1,
                    }
                )

                learnable = reference_context.damaging_moves(species_norm, level, game_version)
                ranked_moves = sorted(learnable)[:DEFAULT_MEMBER_MOVE_OPTION_LIMIT]
                member_combos = _build_member_moveset_combos(
                    ranked_moves,
                    combo_limit=DEFAULT_MEMBER_MOVESET_COMBO_LIMIT,
                )
                member_combo_counts.append(len(member_combos))

                context_key = (game_version, species_norm, level, "damaging-level-up-v1")
                if context_key not in moveset_context_seen:
                    moveset_context_seen.add(context_key)
                    pokemon_moveset_options.append(
                        {
                            "moveset_context_id": f"ctx:{stable_digest(*context_key, length=20)}",
                            "game_version": game_version,
                            "pokemon_species": species_norm,
                            "level": level,
                            "move_policy": "damaging-level-up-v1",
                            "candidate_move_count": len(ranked_moves),
                        }
                    )

                for rank, move_name in enumerate(ranked_moves, start=1):
                    score = float(max(0, DEFAULT_MEMBER_MOVE_OPTION_LIMIT - rank + 1))
                    member_move_options.append(
                        {
                            "team_member_id": member_id,
                            "source_team_id": source_team_id,
                            "game_version": game_version,
                            "slot": slot,
                            "pokemon_species": species_norm,
                            "level": level,
                            "move_name": move_name,
                            "option_rank": rank,
                            "option_score": score,
                            "moveset_context_id": f"ctx:{stable_digest(*context_key, length=20)}",
                        }
                    )
                    global_key = (game_version, species_norm, level, "damaging-level-up-v1", move_name)
                    if global_key in moveset_choice_seen:
                        continue
                    moveset_choice_seen.add(global_key)
                    pokemon_moveset_options.append(
                        {
                            "moveset_context_id": f"ctx:{stable_digest(*context_key, length=20)}",
                            "game_version": game_version,
                            "pokemon_species": species_norm,
                            "level": level,
                            "move_policy": "damaging-level-up-v1",
                            "move_name": move_name,
                            "option_rank": rank,
                            "option_score": score,
                        }
                    )

                for combo_rank, combo_moves in enumerate(member_combos, start=1):
                    normalized_moves = sorted({normalize_key_part(move) for move in combo_moves if normalize_key_part(move)})
                    combo_row: dict[str, Any] = {
                        "moveset_combo_id": make_moveset_combo_id(member_id, normalized_moves),
                        "team_id": source_team_id,
                        "pokemon_instance_id": member_id,
                        "slot_index": slot,
                        "game_version": game_version,
                        "pokemon_name": species_norm,
                        "level": level,
                        "moves": normalized_moves,
                        "move_count": len(normalized_moves),
                        "combo_rank": combo_rank,
                        "combo_score": float(max(1, len(member_combos) - combo_rank + 1)),
                        "source": "damaging-level-up-v1",
                    }
                    for idx in range(MOVESET_WIDTH):
                        combo_row[f"move_{idx + 1}"] = normalized_moves[idx] if idx < len(normalized_moves) else None
                    member_moveset_combos.append(combo_row)

            team_combo_space = estimate_team_moveset_space(member_combo_counts)
            estimated_avoided_variants += max(0, team_combo_space - 1)
            sampling_plan.append(
                {
                    "source_team_id": source_team_id,
                    "sampling_seed": stable_digest(source_team_id, "simulation"),
                    "move_policy": "top_rank_then_seeded_shuffle",
                    "max_moves_per_member": MOVESET_WIDTH,
                    "estimated_combo_space": team_combo_space,
                }
            )

    logger.info(
        "[silver/teams] built player teams source_teams=%s candidate_teams=%s members=%s moveset_combos=%s member_move_options=%s pokemon_moveset_options=%s progression_sources=%s avoided_full_cartesian_estimate=%s elapsed_s=%.2f",
        len(source_teams),
        len(source_teams),
        len(source_team_members),
        len(member_moveset_combos),
        len(member_move_options),
        len(pokemon_moveset_options),
        len(progression_source_teams),
        estimated_avoided_variants,
        time.perf_counter() - started_at,
    )

    return {
        "source_teams": source_teams,
        "source_team_members": source_team_members,
        "member_moveset_combos": member_moveset_combos,
        "member_move_options": member_move_options,
        "pokemon_moveset_options": pokemon_moveset_options,
        "simulation_sampling_plan": sampling_plan,
    }


def estimate_team_moveset_space(member_combo_counts: list[int]) -> int:
    space = 1
    for count in member_combo_counts:
        normalized = max(1, int(count or 0))
        space *= normalized
    return space


def _build_member_moveset_combos(moves: list[str], *, combo_limit: int) -> list[list[str]]:
    unique_moves = sorted({normalize_key_part(move) for move in moves if normalize_key_part(move)})
    if not unique_moves:
        return [[]]
    if len(unique_moves) <= MOVESET_WIDTH:
        return [unique_moves]
    combos = [list(combo) for combo in combinations(unique_moves, MOVESET_WIDTH)]
    return combos[: max(1, int(combo_limit or 1))]


def build_player_teams_from_progression_context(
    progression_source_teams: list[dict[str, Any]],
    reference_context: MoveReferenceContext | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    """Build compact candidate teams and per-member moveset combos (no full-team Cartesian expansion)."""
    if reference_context is None:
        raise ValueError("reference_context is required for offline team generation")

    compact = build_player_team_compact_tables(progression_source_teams, reference_context)
    candidate_teams = [
        {
            "team_id": row.get("source_team_id"),
            "source_team_id": row.get("progression_source_team_id"),
            "game_version": row.get("game_version"),
            "boss_id": row.get("boss_id"),
            "gym": row.get("boss_name"),
            "boss_name": row.get("boss_name"),
            "gym_index": row.get("gym_index"),
            "starter_condition": row.get("starter_condition"),
            "starter_type": row.get("starter_type"),
            "avg_level": row.get("avg_level"),
            "player_max_level": row.get("player_max_level"),
            "starter_base": row.get("starter_base"),
            "starter_evolved_species": row.get("starter_evolved_species"),
            "team_role": "player",
            "is_player_candidate": True,
            "boss_index": row.get("boss_index"),
            "max_boss_index": row.get("max_boss_index"),
            "available_species_count": row.get("available_species_count"),
            "max_species_count": row.get("max_species_count"),
            "progression_depth": row.get("progression_depth"),
            "boss_ace_level": row.get("boss_ace_level"),
            "boss_avg_level": row.get("boss_avg_level"),
            "level_cap_offset": row.get("level_cap_offset"),
        }
        for row in compact["source_teams"]
    ]
    candidate_team_members = compact["source_team_members"]
    member_moveset_combos = compact["member_moveset_combos"]
    return candidate_teams, candidate_team_members, member_moveset_combos
