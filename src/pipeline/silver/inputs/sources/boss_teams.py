"""Boss-team extraction from Kaggle CSV source."""

import csv
import logging
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Collection

from src.pipeline.silver.config.team_config import CSV_PROGRESS_LOG_INTERVAL, DEFAULT_MEMBER_LEVEL, KAGGLE_CSV_DELIMITER
from src.pipeline.silver.inputs.reference_context import MoveReferenceContext
from src.pipeline.silver.transforms.keys import make_pokemon_instance_id, make_team_id


logger = logging.getLogger(__name__)


def _read_kaggle_boss_csv(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle, delimiter=KAGGLE_CSV_DELIMITER)
        for row in reader:
            rows.append(dict(row))
    return rows


def _filter_allowed_versions(
    rows: list[dict[str, Any]],
    allowed_versions: Collection[str] | None,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    allowed_versions_set = {version.lower() for version in allowed_versions} if allowed_versions else None
    skipped_versions: dict[str, int] = defaultdict(int)
    filtered: list[dict[str, Any]] = []

    for row in rows:
        game = str(row.get("Game") or "").strip().lower()
        if allowed_versions_set is not None and game not in allowed_versions_set:
            skipped_versions[game] += 1
            continue
        filtered.append(row)

    return filtered, dict(skipped_versions)


def _normalize_kaggle_row(row: dict[str, Any]) -> dict[str, Any]:
    level_raw = str(row.get("Level") or "").strip()
    try:
        level = int(level_raw) if level_raw else DEFAULT_MEMBER_LEVEL
    except (TypeError, ValueError):
        level = DEFAULT_MEMBER_LEVEL

    moves = [
        move.strip().lower().replace(" ", "-")
        for move in [row.get("Move 1", ""), row.get("Move 2", ""), row.get("Move 3", ""), row.get("Move 4", "")]
        if move and str(move).strip()
    ]

    return {
        "game": str(row.get("Game") or "").strip().lower(),
        "gym_leader": str(row.get("Gym leader") or "").strip().lower(),
        "gym": row.get("Gym"),
        "pokemon": str(row.get("Pokemon") or "").strip().lower(),
        "level": level,
        "moves": moves,
    }


def _group_rows_by_team(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = defaultdict(
        lambda: {"members": [], "rows": [], "game": None, "gym": None, "gym_leader": None, "team_id": None}
    )
    for row in rows:
        game = str(row.get("game") or "").strip().lower()
        gym_leader = str(row.get("gym_leader") or "").strip().lower()
        key = f"{game}:{gym_leader}"
        grouped[key]["game"] = game
        grouped[key]["gym"] = row.get("gym")
        grouped[key]["gym_leader"] = gym_leader
        grouped[key]["team_id"] = make_team_id("boss", game, gym_leader)
        grouped[key]["rows"].append(row)
    return grouped


def _build_team_members_and_moves(
    grouped_rows: dict[str, dict[str, Any]],
    reference_context: MoveReferenceContext,
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    move_storage: dict[str, Any] = {}
    processed_rows = 0
    kept_rows = sum(len(data.get("rows", [])) for data in grouped_rows.values())
    stage_started_at = time.perf_counter()
    logger.info(
        "[silver/teams] stage=member_move_build start rows=%s leaders=%s",
        kept_rows,
        len(grouped_rows),
    )

    for data in grouped_rows.values():
        for row in data.get("rows", []):
            processed_rows += 1
            game = str(row.get("game") or "").strip().lower()
            gym_leader = str(data.get("gym_leader") or "").strip().lower()
            pokemon = str(row.get("pokemon") or "").strip().lower()
            level = int(row.get("level") or DEFAULT_MEMBER_LEVEL)
            moves = list(row.get("moves") or [])[:4]

            member_detail = reference_context.build_member_detail(
                name=pokemon,
                level=level,
                moves=moves,
                game_version=game,
                origin="kaggle",
            )
            if member_detail is None:
                continue

            slot_index = len(data["members"]) + 1
            team_id = str(data.get("team_id") or make_team_id("boss", game, gym_leader))
            instance_id = make_pokemon_instance_id(team_id, slot_index, pokemon)
            member_detail["pokemon_instance_id"] = instance_id
            data["members"].append(member_detail)

            move_details = reference_context.build_member_moves(name=pokemon, level=level, moves=moves, game_version=game)
            if move_details is not None:
                move_details["pokemon_instance_id"] = instance_id
                move_details["team_id"] = team_id
                move_details["slot_index"] = slot_index
                move_storage[instance_id] = move_details

            if processed_rows % CSV_PROGRESS_LOG_INTERVAL == 0:
                completion_pct = (processed_rows / kept_rows * 100.0) if kept_rows else 100.0
                logger.info(
                    "[silver/teams] progress stage=member_move_build rows=%s/%s pct=%.1f leaders=%s move_records=%s",
                    processed_rows,
                    kept_rows,
                    completion_pct,
                    len(grouped_rows),
                    len(move_storage),
                )

    logger.info(
        "[silver/teams] stage=member_move_build done rows=%s leaders=%s move_records=%s elapsed_s=%.2f",
        processed_rows,
        len(grouped_rows),
        len(move_storage),
        time.perf_counter() - stage_started_at,
    )

    return grouped_rows, move_storage


def _assemble_team_records(grouped_data: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    teams: list[dict[str, Any]] = []
    for _, data in sorted(grouped_data.items()):
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
    return teams


def extract_boss_teams_from_kaggle_source(
    bronze_dir: Path,
    allowed_versions: Collection[str] | None = None,
    reference_context: MoveReferenceContext | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if reference_context is None:
        raise ValueError("reference_context is required for offline boss team creation")
    started_at = time.perf_counter()
    kaggle_file = bronze_dir / "kagglehub" / "gym_leaders_elite_four.csv"
    logger.info("[silver/teams] start extract file=%s", kaggle_file)

    if not kaggle_file.exists():
        logger.warning("[silver/teams] file not found; skipping file=%s", kaggle_file)
        return [], {}

    teams_by_leader: dict[str, dict[str, Any]] = {}
    move_storage: dict[str, Any] = {}
    skipped_versions: dict[str, int] = {}
    total_rows = 0
    kept_rows = 0

    try:
        stage_started_at = time.perf_counter()
        logger.info("[silver/teams] stage=read_csv start")
        raw_rows = _read_kaggle_boss_csv(kaggle_file)
        total_rows = len(raw_rows)
        logger.info(
            "[silver/teams] stage=read_csv done rows_total=%s elapsed_s=%.2f",
            total_rows,
            time.perf_counter() - stage_started_at,
        )

        stage_started_at = time.perf_counter()
        logger.info("[silver/teams] stage=filter_versions start allowed_versions=%s", sorted(allowed_versions) if allowed_versions else "ALL")
        selected_rows, skipped_versions = _filter_allowed_versions(raw_rows, allowed_versions)
        kept_rows = len(selected_rows)
        logger.info(
            "[silver/teams] stage=filter_versions done rows_kept=%s rows_skipped=%s elapsed_s=%.2f",
            kept_rows,
            total_rows - kept_rows,
            time.perf_counter() - stage_started_at,
        )

        stage_started_at = time.perf_counter()
        logger.info("[silver/teams] stage=normalize_rows start")
        normalized_rows = [_normalize_kaggle_row(row) for row in selected_rows]
        logger.info(
            "[silver/teams] stage=normalize_rows done rows=%s elapsed_s=%.2f",
            len(normalized_rows),
            time.perf_counter() - stage_started_at,
        )

        stage_started_at = time.perf_counter()
        logger.info("[silver/teams] stage=group_rows start")
        grouped_rows = _group_rows_by_team(normalized_rows)
        logger.info(
            "[silver/teams] stage=group_rows done leaders=%s elapsed_s=%.2f",
            len(grouped_rows),
            time.perf_counter() - stage_started_at,
        )

        teams_by_leader, move_storage = _build_team_members_and_moves(grouped_rows, reference_context)
    except Exception as exc:
        logger.exception("[silver/teams] error while reading csv: %s", exc)
        return [], {}

    stage_started_at = time.perf_counter()
    logger.info("[silver/teams] stage=assemble_team_records start")
    teams = _assemble_team_records(teams_by_leader)
    logger.info(
        "[silver/teams] stage=assemble_team_records done boss_teams=%s elapsed_s=%.2f",
        len(teams),
        time.perf_counter() - stage_started_at,
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

