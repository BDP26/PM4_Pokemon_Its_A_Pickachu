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


def match_boss(game_key: str, heading_text: str, seen_bosses: set[str]) -> Optional[str]:
    heading = normalize_heading(heading_text)
    aliases_for_game = BOSS_ALIASES.get(game_key, {})

    for canonical_boss, aliases in aliases_for_game.items():
        if canonical_boss in seen_bosses:
            continue

        for alias in aliases:
            alias_norm = normalize_heading(alias)
            if alias_norm in heading:
                return canonical_boss

    return None


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

            canonical_boss = match_boss(game_key, heading_text, seen_bosses)
            if canonical_boss is None:
                continue

            reachable = list(dict.fromkeys(cumulative_slugs))
            boss_encounters.append(
                {
                    "game": game_key,
                    "boss_name": canonical_boss,
                    "heading": heading_text,
                    "part": part["part"],
                    "reachable_locations": reachable,
                    "location_count": len(reachable),
                }
            )
            seen_bosses.add(canonical_boss)

    apply_endgame_fallbacks(
        game_key=game_key,
        expected_bosses=expected_bosses,
        seen_bosses=seen_bosses,
        boss_encounters=boss_encounters,
        fallback_contexts=fallback_contexts,
    )

    missing_bosses = [boss for boss in expected_bosses if boss not in seen_bosses]
    if missing_bosses:
        print(f"[silver] warning {game_key}: missing bosses -> {missing_bosses}")
        print(f"[silver] headings seen for {game_key}:")
        for heading in heading_debug:
            print(f"  - {heading}")

    # Keep canonical order from game_config.
    boss_by_name = {record["boss_name"]: record for record in boss_encounters}
    return [boss_by_name[boss] for boss in expected_bosses if boss in boss_by_name]


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

