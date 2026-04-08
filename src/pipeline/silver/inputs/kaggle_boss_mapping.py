import csv
import re
from collections import defaultdict
from pathlib import Path

from src.pipeline.silver.config.boss_config import BOSS_ALIASES, boss_id, boss_slug, dataset_boss_candidates, dataset_game_name


def normalize_join_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.lower())


def is_probable_match(candidate: str, value: str) -> bool:
    candidate_key = normalize_join_key(candidate)
    value_key = normalize_join_key(value)
    if not candidate_key or not value_key:
        return False
    if candidate_key == value_key:
        return True
    # Avoid broad substring matches for very short names like "N".
    if len(candidate_key) < 3 or len(value_key) < 3:
        return False
    return candidate_key in value_key or value_key in candidate_key


def load_kaggle_rows_by_game(bronze_dir: Path) -> dict[str, list[dict[str, str]]]:
    kaggle_csv_path = bronze_dir / "kagglehub" / "gym_leaders_elite_four.csv"
    rows_by_game: dict[str, list[dict[str, str]]] = defaultdict(list)

    if not kaggle_csv_path.exists():
        return {}

    with kaggle_csv_path.open("r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle, delimiter=";")
        for row in reader:
            game = (row.get("Game") or "").strip()
            gym = (row.get("Gym") or "").strip()
            leader = (row.get("Gym leader") or "").strip()
            if not game or not leader:
                continue
            rows_by_game[game].append({"gym": gym, "leader": leader})

    return dict(rows_by_game)


def harmonized_kaggle_candidates(
    game_key: str,
    canonical_boss: str,
    kaggle_rows_by_game: dict[str, list[dict[str, str]]],
) -> list[str]:
    base_candidates = dataset_boss_candidates(game_key, canonical_boss)
    aliases = BOSS_ALIASES.get(game_key, {}).get(canonical_boss, [])
    game_name = dataset_game_name(game_key)
    game_rows = kaggle_rows_by_game.get(game_name, [])

    alias_candidates = [canonical_boss, *base_candidates, *aliases]
    matched: list[str] = []
    seen: set[str] = set()

    for row in game_rows:
        leader = row["leader"]
        gym = row["gym"]
        matches_leader = any(is_probable_match(alias, leader) for alias in alias_candidates)
        matches_gym = any(
            is_probable_match(alias, gym)
            and "elite four" not in alias.lower()
            and "champion" not in alias.lower()
            for alias in alias_candidates
        )
        if matches_leader or matches_gym:
            if leader not in seen:
                seen.add(leader)
                matched.append(leader)

    return matched or base_candidates


def build_harmonized_candidates_by_boss(
    game_key: str,
    expected_bosses: list[str],
    kaggle_rows_by_game: dict[str, list[dict[str, str]]],
) -> dict[str, list[str]]:
    return {
        boss: harmonized_kaggle_candidates(game_key, boss, kaggle_rows_by_game)
        for boss in expected_bosses
    }


def enrich_boss_records(
    records: list[dict],
    expected_bosses: list[str],
    harmonized_candidates_by_boss: dict[str, list[str]],
) -> list[dict]:
    if not records:
        return records

    order_lookup = {boss: idx + 1 for idx, boss in enumerate(expected_bosses)}
    for record in records:
        game_key = record["game"]
        canonical_boss = record["boss_name"]
        record["version"] = game_key
        record["version_name"] = dataset_game_name(game_key)
        record["boss_id"] = boss_id(game_key, canonical_boss)
        record["boss_slug"] = boss_slug(canonical_boss)
        record["boss_name_canonical"] = canonical_boss
        record["boss_name_source"] = record.get("heading", "")
        record["boss_order"] = order_lookup.get(canonical_boss)
        record["dataset_game"] = dataset_game_name(game_key)
        record["dataset_boss_candidates"] = harmonized_candidates_by_boss.get(
            canonical_boss,
            dataset_boss_candidates(game_key, canonical_boss),
        )

    return records


def build_boss_mapping_payload(
    game_key: str,
    expected_bosses: list[str],
    harmonized_candidates_by_boss: dict[str, list[str]],
) -> dict:
    return {
        "version": game_key,
        "version_name": dataset_game_name(game_key),
        "boss_mapping": [
            {
                "boss_order": idx + 1,
                "boss_id": boss_id(game_key, boss),
                "boss_name_canonical": boss,
                "boss_slug": boss_slug(boss),
                "dataset_game": dataset_game_name(game_key),
                "dataset_boss_candidates": harmonized_candidates_by_boss.get(
                    boss,
                    dataset_boss_candidates(game_key, boss),
                ),
            }
            for idx, boss in enumerate(expected_bosses)
        ],
    }


