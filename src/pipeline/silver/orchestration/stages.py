"""Composable Silver orchestration stages with typed inputs/outputs."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.pipeline.common.io import read_json
from src.pipeline.silver.config.team_config import PARSER_MIN_BOSS_COVERAGE
from src.pipeline.silver.inputs.kaggle_boss_mapping import (
    build_boss_mapping_payload,
    build_harmonized_candidates_by_boss,
    enrich_boss_records,
)
from src.pipeline.silver.inputs.location_mapper import LocationMapper
from src.pipeline.silver.inputs.parser import enforce_parser_coverage, extract_game_data


@dataclass(frozen=True)
class ParseStageOutput:
    all_records: list[dict[str, Any]]
    all_slugs: list[str]
    records_with_game_keys: list[tuple[str, list[dict[str, Any]]]]
    boss_mapping_by_version: dict[str, dict[str, Any]]


def run_parse_stage(
    *,
    game_files: list[Path],
    mapper: LocationMapper,
    kaggle_rows_by_game: dict[str, list[dict[str, Any]]],
) -> ParseStageOutput:
    all_records: list[dict[str, Any]] = []
    all_slugs: list[str] = []
    boss_mapping_by_version: dict[str, dict[str, Any]] = {}
    records_with_game_keys: list[tuple[str, list[dict[str, Any]]]] = []

    for game_file in game_files:
        game_payload = read_json(game_file)
        records = extract_game_data(game_payload, mapper)
        game_key = game_payload["game_key"]
        expected_bosses = game_payload.get("bosses", [])
        enforce_parser_coverage(
            game_key=game_key,
            records=records,
            expected_bosses=expected_bosses,
            min_coverage=PARSER_MIN_BOSS_COVERAGE,
        )

        harmonized_candidates_by_boss = build_harmonized_candidates_by_boss(
            game_key=game_key,
            expected_bosses=expected_bosses,
            kaggle_rows_by_game=kaggle_rows_by_game,
        )
        records = enrich_boss_records(records, expected_bosses, harmonized_candidates_by_boss)
        boss_mapping_by_version[game_key] = build_boss_mapping_payload(
            game_key,
            expected_bosses,
            harmonized_candidates_by_boss,
        )

        if not records:
            continue

        all_records.extend(records)
        records_with_game_keys.append((game_key, records))
        for record in records:
            all_slugs.extend(record.get("reachable_locations", []))

    return ParseStageOutput(
        all_records=all_records,
        all_slugs=all_slugs,
        records_with_game_keys=records_with_game_keys,
        boss_mapping_by_version=boss_mapping_by_version,
    )

