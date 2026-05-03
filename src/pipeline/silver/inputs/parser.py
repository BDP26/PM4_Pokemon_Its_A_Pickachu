from typing import Optional

from bs4 import BeautifulSoup

from src.pipeline.silver.config.boss_config import BOSS_ALIASES, CHAMPION_BY_GAME, ELITE_FOUR_BY_GAME
from src.pipeline.silver.inputs.location_mapper import LocationMapper


def normalize_text(value: str) -> str:
    value = value.replace("[ edit source ]", " ")
    value = value.replace("[edit source]", " ")
    value = value.replace("\xa0", " ")
    return " ".join(value.split()).strip()


def normalize_heading(value: str) -> str:
    text = normalize_text(value).lower()
    text = text.replace("’", "'")
    return f" {text} "


def _is_stage_or_location_alias(alias: str) -> bool:
    alias_l = alias.lower()
    tokens = (
        "gym",
        "city",
        "town",
        "route",
        "island",
        "plateau",
        "hall",
        "league",
        "cave",
        "forest",
        "gate",
        "road",
    )
    return any(token in alias_l for token in tokens)


def _is_location_only_alias(alias: str) -> bool:
    alias_l = alias.lower()
    location_tokens = (
        "city",
        "town",
        "route",
        "island",
        "cave",
        "forest",
        "gate",
        "road",
    )
    battle_tokens = (
        "gym",
        "elite four",
        "champion",
        "league",
        "hall",
        "battle",
        "vs",
        "leader",
    )
    has_location = any(token in alias_l for token in location_tokens)
    has_battle = any(token in alias_l for token in battle_tokens)
    return has_location and not has_battle


def _looks_like_boss_fight_heading(heading_text: str, alias: str, canonical_boss: str) -> bool:
    heading_plain = normalize_text(heading_text).lower()
    alias_plain = normalize_text(alias).lower()
    boss_plain = normalize_text(canonical_boss).lower()
    if heading_plain == boss_plain:
        return True
    if heading_plain == alias_plain:
        return True
    cues = (
        "gym",
        "elite four",
        "champion",
        "battle",
        "vs",
        "leader",
    )
    return any(cue in heading_plain for cue in cues)


def _is_location_only_heading_alias_for_boss(game_key: str, heading_text: str, canonical_boss: str) -> bool:
    aliases = BOSS_ALIASES.get(game_key, {}).get(canonical_boss, [])
    heading_norm = normalize_heading(heading_text)
    for alias in aliases:
        if normalize_heading(alias) == heading_norm and _is_location_only_alias(alias):
            return True
    return False


def match_bosses(
    game_key: str,
    heading_text: str,
    expected_bosses: list[str],
) -> list[str]:
    heading = normalize_heading(heading_text)
    aliases_for_game = BOSS_ALIASES.get(game_key, {})
    if not aliases_for_game:
        return []

    alias_to_bosses: dict[str, set[str]] = {}
    direct_matches: set[str] = set()
    for canonical_boss, aliases in aliases_for_game.items():
        for alias in aliases:
            alias_norm = normalize_heading(alias)
            alias_to_bosses.setdefault(alias_norm, set()).add(canonical_boss)
            if alias_norm not in heading:
                continue
            if not _looks_like_boss_fight_heading(heading_text, alias, canonical_boss):
                continue
            direct_matches.add(canonical_boss)

    if not direct_matches:
        return []

    expanded_matches = set(direct_matches)
    for canonical_boss in list(direct_matches):
        aliases = aliases_for_game.get(canonical_boss, [])
        for alias in aliases:
            if not _is_stage_or_location_alias(alias):
                continue
            siblings = alias_to_bosses.get(normalize_heading(alias), set())
            if len(siblings) > 1:
                expanded_matches.update(siblings)

    ordered = [boss for boss in expected_bosses if boss in expanded_matches]
    return ordered


def apply_endgame_fallbacks(
    game_key: str,
    expected_bosses: list[str],
    seen_bosses: set[str],
    boss_encounters: list[dict],
    fallback_contexts: list[dict],
) -> None:
    if not fallback_contexts:
        return

    elite_four = ELITE_FOUR_BY_GAME.get(game_key, [])
    champion = CHAMPION_BY_GAME.get(game_key)

    # Use the latest relevant fallback context
    context = fallback_contexts[-1]
    reachable = context["reachable_locations"]
    part = context["part"]
    heading = context["heading"]

    # Fill remaining Elite Four in canonical order
    for boss in elite_four:
        if boss not in seen_bosses and boss in expected_bosses:
            boss_encounters.append(
                {
                    "game": game_key,
                    "boss_name": boss,
                    "heading": heading,
                    "part": part,
                    "reachable_locations": reachable,
                    "location_count": len(reachable),
                }
            )
            seen_bosses.add(boss)

    # Fill champion if still missing
    if champion and champion in expected_bosses and champion not in seen_bosses:
        boss_encounters.append(
            {
                "game": game_key,
                "boss_name": champion,
                "heading": heading,
                "part": part,
                "reachable_locations": reachable,
                "location_count": len(reachable),
            }
        )
        seen_bosses.add(champion)


def extract_game_data(game_payload: dict, mapper: LocationMapper) -> list[dict]:
    game_key = game_payload["game_key"]
    cumulative_slugs: list[str] = []
    boss_encounters: list[dict] = []
    boss_encounter_by_name: dict[str, dict] = {}
    seen_bosses: set[str] = set()

    expected_bosses = game_payload.get("bosses", [])
    heading_debug: list[str] = []
    fallback_contexts: list[dict] = []

    for part in game_payload.get("parts", []):
        soup = BeautifulSoup(part.get("html", ""), "lxml")
        content = soup.find("div", class_="mw-parser-output")
        if content is None:
            continue

        for element in content.find_all(["h2", "h3", "a"]):
            if element.name == "a" and element.get("title"):
                title = normalize_text(str(element["title"]))
                if mapper.is_location_title(title):
                    slug = mapper.resolve(title, game_payload["route_prefix"])
                    if slug:
                        cumulative_slugs.append(slug)
                continue

            if element.name not in {"h2", "h3"}:
                continue

            heading_text = normalize_text(str(element.get_text()))
            heading_debug.append(heading_text)
            heading_norm = normalize_heading(heading_text)

            # Remember generic endgame sections for later filling.
            if any(
                token in heading_norm
                for token in [
                    " elite four ",
                    " the elite four ",
                    " pokemon league ",
                    " pokémon league ",
                    " indigo plateau ",
                    " great hall ",
                    " champion ",
                ]
            ):
                fallback_contexts.append(
                    {
                        "heading": heading_text,
                        "part": part["part"],
                        "reachable_locations": list(dict.fromkeys(cumulative_slugs)),
                    }
                )

            matched_bosses = match_bosses(game_key, heading_text, expected_bosses)
            if not matched_bosses:
                continue

            reachable = list(dict.fromkeys(cumulative_slugs))
            for canonical_boss in matched_bosses:
                is_location_only_alias_match = _is_location_only_heading_alias_for_boss(game_key, heading_text, canonical_boss)
                candidate = {
                    "game": game_key,
                    "boss_name": canonical_boss,
                    "heading": heading_text,
                    "part": part["part"],
                    "reachable_locations": reachable,
                    "location_count": len(reachable),
                    "_location_only_alias_match": is_location_only_alias_match,
                }
                existing = boss_encounter_by_name.get(canonical_boss)
                # Keep the strongest context first; location-only aliases should not replace stronger battle-aligned matches.
                if existing is None:
                    boss_encounter_by_name[canonical_boss] = candidate
                else:
                    existing_location_only = bool(existing.get("_location_only_alias_match", False))
                    if is_location_only_alias_match and not existing_location_only:
                        pass
                    elif not is_location_only_alias_match and existing_location_only:
                        boss_encounter_by_name[canonical_boss] = candidate
                    elif int(candidate["location_count"]) >= int(existing.get("location_count", 0) or 0):
                        boss_encounter_by_name[canonical_boss] = candidate
                seen_bosses.add(canonical_boss)

    apply_endgame_fallbacks(
        game_key=game_key,
        expected_bosses=expected_bosses,
        seen_bosses=seen_bosses,
        boss_encounters=boss_encounters,
        fallback_contexts=fallback_contexts,
    )
    for entry in boss_encounters:
        canonical = entry.get("boss_name")
        if isinstance(canonical, str):
            existing = boss_encounter_by_name.get(canonical)
            if existing is None or int(entry.get("location_count", 0) or 0) >= int(existing.get("location_count", 0) or 0):
                boss_encounter_by_name[canonical] = entry

    missing_bosses = [boss for boss in expected_bosses if boss not in seen_bosses]
    if missing_bosses:
        print(f"[silver] warning {game_key}: missing bosses -> {missing_bosses}")
        print(f"[silver] headings seen for {game_key}:")
        for heading in heading_debug:
            print(f"  - {heading}")

    # Keep canonical order from game_config.
    ordered_records = [boss_encounter_by_name[boss] for boss in expected_bosses if boss in boss_encounter_by_name]
    for record in ordered_records:
        record.pop("_location_only_alias_match", None)
    return ordered_records


def enforce_parser_coverage(
    *,
    game_key: str,
    records: list[dict],
    expected_bosses: list[str],
    min_coverage: float,
) -> dict[str, float | int | str]:
    expected = len(expected_bosses)
    extracted = len(records)
    coverage = (extracted / expected) if expected else 1.0
    report: dict[str, float | int | str] = {
        "game_key": game_key,
        "expected_bosses": expected,
        "extracted_bosses": extracted,
        "coverage": coverage,
        "min_coverage": min_coverage,
    }
    if coverage < min_coverage:
        raise ValueError(
            f"[silver] parser coverage gate failed for {game_key}: {extracted}/{expected} ({coverage:.2%}) "
            f"below threshold {min_coverage:.2%}"
        )
    return report
