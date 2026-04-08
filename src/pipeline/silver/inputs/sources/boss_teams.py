"""Boss-team extraction from Kaggle CSV source."""

import csv
import logging
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Collection

from src.pipeline.silver.config.team_config import CSV_PROGRESS_LOG_INTERVAL, DEFAULT_MEMBER_LEVEL, KAGGLE_CSV_DELIMITER
from src.pipeline.silver.inputs.connectors.pokeapi_moves import (
    _build_member_detail,
    _build_member_moves,
    prefetch_species_move_data,
)
from src.pipeline.silver.transforms.keys import make_pokemon_instance_id, make_team_id


logger = logging.getLogger(__name__)


def extract_boss_teams_from_kaggle_source(
    bronze_dir: Path,
    allowed_versions: Collection[str] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    started_at = time.perf_counter()
    kaggle_file = bronze_dir / "kagglehub" / "gym_leaders_elite_four.csv"
    logger.info("[silver/teams] start extract file=%s", kaggle_file)

    if not kaggle_file.exists():
        logger.warning("[silver/teams] file not found; skipping file=%s", kaggle_file)
        return [], {}

    allowed_versions_set = {version.lower() for version in allowed_versions} if allowed_versions else None
    skipped_versions: dict[str, int] = defaultdict(int)
    total_rows = 0
    kept_rows = 0

    teams_by_leader: dict[str, dict[str, Any]] = defaultdict(lambda: {"members": [], "game": None, "gym": None, "gym_leader": None})
    move_storage: dict[str, Any] = {}
    selected_rows: list[dict[str, Any]] = []

    try:
        with open(kaggle_file, "r", encoding="utf-8") as handle:
            reader = csv.DictReader(handle, delimiter=KAGGLE_CSV_DELIMITER)
            for row in reader:
                total_rows += 1
                game = str(row.get("Game") or "").strip().lower()
                if allowed_versions_set is not None and game not in allowed_versions_set:
                    skipped_versions[game] += 1
                    continue
                kept_rows += 1

                selected_rows.append(dict(row))

        prefetch_entries: list[tuple[str, int, str, list[str]]] = []
        processed_rows = 0
        for row in selected_rows:
            processed_rows += 1
            game = str(row.get("Game") or "").strip().lower()
            pokemon = str(row.get("Pokemon") or "").strip().lower()
            level = int(row["Level"]) if row.get("Level") else DEFAULT_MEMBER_LEVEL
            moves = [
                move.strip().lower().replace(" ", "-")
                for move in [row.get("Move 1", ""), row.get("Move 2", ""), row.get("Move 3", ""), row.get("Move 4", "")]
                if move and move.strip()
            ]
            prefetch_entries.append((pokemon, level, game, moves[:4]))

        prefetch_species_move_data(prefetch_entries)

        for row in selected_rows:
            game = str(row.get("Game") or "").strip().lower()
            gym_leader = str(row.get("Gym leader") or "").strip().lower()
            pokemon = str(row.get("Pokemon") or "").strip().lower()
            level = int(row["Level"]) if row.get("Level") else DEFAULT_MEMBER_LEVEL
            moves = [
                move.strip().lower().replace(" ", "-")
                for move in [row.get("Move 1", ""), row.get("Move 2", ""), row.get("Move 3", ""), row.get("Move 4", "")]
                if move and move.strip()
            ]

            key = f"{game}:{gym_leader}"
            teams_by_leader[key]["game"] = game
            teams_by_leader[key]["gym"] = row.get("Gym")
            teams_by_leader[key]["gym_leader"] = gym_leader
            teams_by_leader[key]["team_id"] = make_team_id("boss", game, gym_leader)

            member_detail = _build_member_detail(
                name=pokemon,
                level=level,
                moves=moves[:4],
                game_version=game,
                origin="kaggle",
            )
            if member_detail is None:
                continue

            slot_index = len(teams_by_leader[key]["members"]) + 1
            instance_id = make_pokemon_instance_id(teams_by_leader[key]["team_id"], slot_index, pokemon)
            member_detail["pokemon_instance_id"] = instance_id
            teams_by_leader[key]["members"].append(member_detail)

            move_details = _build_member_moves(name=pokemon, level=level, moves=moves[:4], game_version=game)
            if move_details is not None:
                move_details["pokemon_instance_id"] = instance_id
                move_details["team_id"] = make_team_id("boss", game, gym_leader)
                move_details["slot_index"] = slot_index
                move_storage[instance_id] = move_details

            if processed_rows % CSV_PROGRESS_LOG_INTERVAL == 0:
                logger.info(
                    "[silver/teams] progress csv rows=%s kept=%s leaders=%s",
                    processed_rows,
                    kept_rows,
                    len(teams_by_leader),
                )
    except Exception as exc:
        logger.exception("[silver/teams] error while reading csv: %s", exc)
        return [], {}

    teams: list[dict[str, Any]] = []
    for (_, data) in sorted(teams_by_leader.items()):
        game = str(data.get("game") or "unknown").strip().lower()
        members = data.get("members", [])
        if not isinstance(members, list) or not members:
            continue

        gym_leader = str(data.get("gym_leader") or "unknown").strip()
        team_id = str(data.get("team_id") or make_team_id("boss", game, gym_leader))
        pokemon = [member["name"] for member in members]
        levels = [int(member["level"]) for member in members]
        moves = [list(member.get("moves", [])) for member in members]
        pokemon_instance_ids = [str(member.get("pokemon_instance_id") or "") for member in members]

        teams.append(
            {
                "team_id": team_id,
                "boss_name": gym_leader.title() if gym_leader else None,
                "gym": data.get("gym"),
                "game_version": game,
                "team_role": "boss",
                "is_player_candidate": False,
                "pokemon": pokemon,
                "levels": levels,
                "moves": moves,
                "pokemon_instance_ids": pokemon_instance_ids,
                "avg_level": sum(levels) // len(levels) if levels else DEFAULT_MEMBER_LEVEL,
            }
        )

    if skipped_versions:
        skipped_preview = ", ".join(f"{version}={count}" for version, count in sorted(skipped_versions.items()))
        logger.info("[silver/teams] skipped non-config versions: %s", skipped_preview)

    logger.info(
        "[silver/teams] done boss extract rows_total=%s rows_kept=%s leaders=%s boss_teams=%s move_records=%s elapsed_s=%.2f",
        total_rows,
        kept_rows,
        len(teams_by_leader),
        len(teams),
        len(move_storage),
        time.perf_counter() - started_at,
    )
    return teams, move_storage




