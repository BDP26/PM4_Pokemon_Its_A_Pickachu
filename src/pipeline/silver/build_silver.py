import json
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Optional

from bs4 import BeautifulSoup
from tqdm import tqdm

from src.pipeline.common.http import build_session
from src.pipeline.common.io import read_json, read_jsonl, write_json
from src.pipeline.silver.boss_config import BOSS_ALIASES, CHAMPION_BY_GAME, ELITE_FOUR_BY_GAME
from src.pipeline.settings import BRONZE_DIR, POKEAPI, SILVER_DIR, ensure_medallion_dirs
from src.pipeline.silver.location_mapper import LocationMapper

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


def summarize_unmapped_locations(misses: list[dict]) -> dict:
    by_reason = Counter()
    by_tried_slug = Counter()
    by_raw_title = Counter()
    examples_by_reason = defaultdict(list)

    for miss in misses:
        reason = miss.get("reason", "unknown")
        raw_title = miss.get("raw_title", "")
        tried_slug = miss.get("tried_slug") or ""

        by_reason[reason] += 1
        if tried_slug:
            by_tried_slug[tried_slug] += 1
        if raw_title:
            by_raw_title[raw_title] += 1

        if len(examples_by_reason[reason]) < 10:
            examples_by_reason[reason].append(miss)

    return {
        "total_unmapped_events": len(misses),
        "by_reason": dict(by_reason.most_common()),
        "top_tried_slugs": dict(by_tried_slug.most_common(50)),
        "top_raw_titles": dict(by_raw_title.most_common(50)),
        "examples_by_reason": dict(examples_by_reason),
    }


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
                title = normalize_text(element["title"])
                if mapper.is_location_title(title):
                    slug = mapper.resolve(title, game_payload["route_prefix"])
                    if slug:
                        cumulative_slugs.append(slug)
                continue

            if element.name not in {"h2", "h3"}:
                continue

            heading_text = normalize_text(element.get_text(" ", strip=True))
            heading_debug.append(heading_text)
            heading_norm = normalize_heading(heading_text)

            # remember generic endgame sections for later filling
            if any(token in heading_norm for token in [
                " elite four ",
                " the elite four ",
                " pokemon league ",
                " pokémon league ",
                " indigo plateau ",
                " great hall ",
                " champion ",
            ]):
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

    # Fill missing endgame bosses from generic league/champion headings
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

    # keep canonical order from game_config
    boss_by_name = {record["boss_name"]: record for record in boss_encounters}
    ordered_records = [boss_by_name[boss] for boss in expected_bosses if boss in boss_by_name]

    return ordered_records


def get_location_areas(location_slugs: list[str], throttle_seconds: float = 0.1) -> dict[str, list[str]]:
    session = build_session()
    area_map: dict[str, list[str]] = {}

    unique_locations = sorted(set(location_slugs))
    for slug in tqdm(unique_locations, desc="[silver] mapping location areas"):
        try:
            response = session.get(f"{POKEAPI}/location/{slug}", timeout=10)
            if response.status_code == 200:
                payload = response.json()
                area_map[slug] = [entry["name"] for entry in payload.get("areas", [])]
            else:
                area_map[slug] = []
        except Exception:
            area_map[slug] = []

        time.sleep(throttle_seconds)

    return area_map


def build_silver_from_bronze(bronze_dir: Path = BRONZE_DIR, silver_dir: Path = SILVER_DIR) -> None:
    ensure_medallion_dirs()
    silver_dir.mkdir(parents=True, exist_ok=True)

    location_index_path = bronze_dir / "pokeapi" / "location_index.json"
    bulbapedia_dir = bronze_dir / "bulbapedia"

    if not location_index_path.exists() or not bulbapedia_dir.exists():
        raise FileNotFoundError(
            "Bronze inputs are missing. Run the bronze step first: python -m pipeline.run_pipeline --layer bronze"
        )

    location_index = read_json(location_index_path)
    mapper = LocationMapper(location_index)

    all_records: list[dict] = []
    all_slugs: list[str] = []

    for game_file in sorted(bulbapedia_dir.glob("*.json")):
        game_payload = read_json(game_file)
        records = extract_game_data(game_payload, mapper)
        game_key = game_payload["game_key"]

        if not records:
            print(f"[silver] skipped {game_file.name}: no boss records extracted")
            continue

        output_path = silver_dir / f"{game_key}_data.jsonl"
        with output_path.open("w", encoding="utf-8") as handle:
            for record in records:
                handle.write(json.dumps(record, ensure_ascii=False) + "\n")

        all_records.extend(records)
        for record in records:
            all_slugs.extend(record["reachable_locations"])

        print(f"[silver] wrote {output_path.name} with {len(records)} records")

    write_json(silver_dir / "unmapped_locations_detailed.json", mapper.misses)

    unmapped_summary = summarize_unmapped_locations(mapper.misses)
    write_json(silver_dir / "unmapped_locations_summary.json", unmapped_summary)

    compact_unmapped = [
        {
            "raw_title": miss["raw_title"],
            "tried_slug": miss["tried_slug"],
            "reason": miss["reason"],
        }
        for miss in mapper.misses
    ]
    write_json(silver_dir / "unmapped_locations.json", compact_unmapped)

    area_map = get_location_areas(all_slugs)
    write_json(silver_dir / "location_to_area_map.json", area_map)

    print(f"[silver] unmapped location events: {len(mapper.misses)}")
    print(f"[silver] done: {len(all_records)} boss snapshots across {len(set(r['game'] for r in all_records))} games")


def build_silver_from_existing_files(silver_dir: Path = SILVER_DIR) -> None:
    all_slugs: list[str] = []
    game_files = sorted(silver_dir.glob("*_data.jsonl"))
    if not game_files:
        raise FileNotFoundError(f"No *_data.jsonl files found in {silver_dir}")

    for game_file in game_files:
        dataframe = read_jsonl(game_file)
        for locations in dataframe["reachable_locations"]:
            all_slugs.extend(locations)

    area_map = get_location_areas(all_slugs)
    write_json(silver_dir / "location_to_area_map.json", area_map)
    print(f"[silver] refreshed location_to_area_map.json from {len(game_files)} game files")


if __name__ == "__main__":
    build_silver_from_bronze()