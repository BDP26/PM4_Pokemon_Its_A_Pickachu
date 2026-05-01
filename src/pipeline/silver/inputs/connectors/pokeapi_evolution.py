"""PokeAPI evolution-chain helpers for team-validity and UI metadata."""

from __future__ import annotations

from collections import deque
from functools import lru_cache
import logging
from typing import Any
import pokebase as pb


def pokebase_get_data(endpoint: str, resource_name_or_id: str | int | None):
    loader = getattr(pb, str(endpoint).strip().lower().replace("-", "_"), None)
    if not callable(loader):
        raise ValueError(f"Unsupported pokebase endpoint: {endpoint}")
    return loader(resource_name_or_id)


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

logger = logging.getLogger(__name__)


@lru_cache(maxsize=1024)
def _get_resource(endpoint: str, resource_name_or_id: str | int | None) -> dict[str, Any]:
    if not endpoint:
        return {}
    try:
        payload = pokebase_get_data(endpoint, resource_name_or_id)
    except Exception:
        logger.warning(
            "[silver/evolution] resource fetch failed endpoint=%s resource=%s",
            endpoint,
            resource_name_or_id,
        )
        return {}
    normalized = _normalize_pokebase_payload(payload)
    return normalized if isinstance(normalized, dict) else {}


def get_evolution_chain_for_species(species_name: str) -> dict[str, Any]:
    """Load raw evolution-chain payload for a species from PokeAPI."""
    species = str(species_name).strip().lower()
    if not species:
        return {}

    species_payload = _get_resource("pokemon-species", species)
    evo_chain = species_payload.get("evolution_chain") if isinstance(species_payload, dict) else None
    evo_url = str(evo_chain.get("url") or "") if isinstance(evo_chain, dict) else ""
    evo_id: str | int | None = None
    if evo_url:
        parts = [part for part in evo_url.rstrip("/").split("/") if part]
        if parts:
            last = parts[-1]
            evo_id = int(last) if last.isdigit() else last
    if evo_id is None:
        return {}
    return _get_resource("evolution-chain", evo_id)


def _detail_has_special_condition(detail: dict[str, Any]) -> bool:
    for key, value in detail.items():
        if key in {"trigger", "min_level"}:
            continue
        if value is None:
            continue
        if isinstance(value, str) and not value:
            continue
        if isinstance(value, list) and not value:
            continue
        return True
    return False


def _normalize_evolution_detail(detail: dict[str, Any]) -> dict[str, Any]:
    trigger = (detail.get("trigger") or {}).get("name") if isinstance(detail.get("trigger"), dict) else None
    normalized = {
        "trigger": trigger,
        "min_level": detail.get("min_level") if isinstance(detail.get("min_level"), int) else None,
        "item": (detail.get("item") or {}).get("name") if isinstance(detail.get("item"), dict) else None,
        "held_item": (detail.get("held_item") or {}).get("name") if isinstance(detail.get("held_item"), dict) else None,
        "known_move": (detail.get("known_move") or {}).get("name") if isinstance(detail.get("known_move"), dict) else None,
        "known_move_type": (detail.get("known_move_type") or {}).get("name")
        if isinstance(detail.get("known_move_type"), dict)
        else None,
        "location": (detail.get("location") or {}).get("name") if isinstance(detail.get("location"), dict) else None,
        "min_happiness": detail.get("min_happiness") if isinstance(detail.get("min_happiness"), int) else None,
        "min_beauty": detail.get("min_beauty") if isinstance(detail.get("min_beauty"), int) else None,
        "min_affection": detail.get("min_affection") if isinstance(detail.get("min_affection"), int) else None,
        "needs_overworld_rain": bool(detail.get("needs_overworld_rain")) if detail.get("needs_overworld_rain") is not None else None,
        "party_species": (detail.get("party_species") or {}).get("name") if isinstance(detail.get("party_species"), dict) else None,
        "party_type": (detail.get("party_type") or {}).get("name") if isinstance(detail.get("party_type"), dict) else None,
        "relative_physical_stats": detail.get("relative_physical_stats")
        if isinstance(detail.get("relative_physical_stats"), int)
        else None,
        "time_of_day": detail.get("time_of_day") if isinstance(detail.get("time_of_day"), str) else None,
        "trade_species": (detail.get("trade_species") or {}).get("name") if isinstance(detail.get("trade_species"), dict) else None,
        "turn_upside_down": bool(detail.get("turn_upside_down")) if detail.get("turn_upside_down") is not None else None,
    }
    return {key: value for key, value in normalized.items() if value is not None and value != ""}


def flatten_evolution_chain(chain: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Flatten PokeAPI chain into per-species stage, level validity, and metadata."""
    root = chain.get("chain") if isinstance(chain, dict) else None
    if not isinstance(root, dict):
        return {}

    species_rules: dict[str, dict[str, Any]] = {}

    root_name_raw = (root.get("species") or {}).get("name") if isinstance(root.get("species"), dict) else None
    if not isinstance(root_name_raw, str) or not root_name_raw:
        return {}
    root_name = root_name_raw.strip().lower()

    queue: deque[tuple[dict[str, Any], str, int, int | None, int | None]] = deque([(root, root_name, 1, None, None)])

    while queue:
        node, base_species, stage, cumulative_level_req, min_level_from_previous = queue.popleft()
        species_obj = node.get("species") if isinstance(node.get("species"), dict) else {}
        name_raw = species_obj.get("name")
        if not isinstance(name_raw, str) or not name_raw:
            continue
        species_name = name_raw.strip().lower()

        existing = species_rules.get(species_name)
        if existing is None:
            species_rules[species_name] = {
                "species_name": species_name,
                "base_species": base_species,
                "evolution_stage": stage,
                "min_valid_level": cumulative_level_req,
                "min_level_from_previous": min_level_from_previous,
                "special_evolution_conditions": [],
            }
        else:
            if existing.get("min_valid_level") is None or (
                cumulative_level_req is not None and cumulative_level_req < int(existing["min_valid_level"])
            ):
                existing["min_valid_level"] = cumulative_level_req
            existing["evolution_stage"] = min(int(existing.get("evolution_stage") or stage), stage)

        for child in node.get("evolves_to", []) or []:
            if not isinstance(child, dict):
                continue

            details = child.get("evolution_details") if isinstance(child.get("evolution_details"), list) else []
            level_thresholds: list[int] = []
            special_conditions: list[dict[str, Any]] = []

            for detail in details:
                if not isinstance(detail, dict):
                    continue
                trigger = (detail.get("trigger") or {}).get("name") if isinstance(detail.get("trigger"), dict) else None
                min_level = detail.get("min_level") if isinstance(detail.get("min_level"), int) else None
                if trigger == "level-up" and min_level is not None:
                    level_thresholds.append(min_level)
                if trigger != "level-up" or _detail_has_special_condition(detail) or min_level is None:
                    normalized = _normalize_evolution_detail(detail)
                    if normalized:
                        special_conditions.append(normalized)

            edge_min_level = min(level_thresholds) if level_thresholds else None
            child_cumulative = cumulative_level_req
            if edge_min_level is not None:
                child_cumulative = max(cumulative_level_req or edge_min_level, edge_min_level)

            child_name_raw = (child.get("species") or {}).get("name") if isinstance(child.get("species"), dict) else None
            child_name = str(child_name_raw).strip().lower() if isinstance(child_name_raw, str) else ""
            if child_name:
                entry = species_rules.setdefault(
                    child_name,
                    {
                        "species_name": child_name,
                        "base_species": base_species,
                        "evolution_stage": stage + 1,
                        "min_valid_level": child_cumulative,
                        "min_level_from_previous": edge_min_level,
                        "special_evolution_conditions": [],
                    },
                )
                if entry.get("min_valid_level") is None or (
                    child_cumulative is not None and child_cumulative < int(entry["min_valid_level"])
                ):
                    entry["min_valid_level"] = child_cumulative
                if entry.get("min_level_from_previous") is None and edge_min_level is not None:
                    entry["min_level_from_previous"] = edge_min_level
                for condition in special_conditions:
                    if condition not in entry["special_evolution_conditions"]:
                        entry["special_evolution_conditions"].append(condition)

            queue.append((child, base_species, stage + 1, child_cumulative, edge_min_level))

    return species_rules


def build_species_evolution_rules(chain: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Build normalized species evolution rules from a raw chain payload."""
    return flatten_evolution_chain(chain)


def is_species_valid_for_team(species_name: str, level: int, rules: dict[str, dict[str, Any]]) -> bool:
    """Team-building validity based only on cumulative level-up requirements."""
    species = str(species_name).strip().lower()
    info = rules.get(species)
    if not info:
        return True
    min_valid = info.get("min_valid_level")
    return min_valid is None or int(level) >= int(min_valid)


def get_species_evolution_rules(species_name: str) -> dict[str, dict[str, Any]]:
    """Convenience helper: fetch and flatten evolution chain for a species."""
    chain = get_evolution_chain_for_species(species_name)
    return build_species_evolution_rules(chain)
