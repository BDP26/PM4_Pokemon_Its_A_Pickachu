from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
from bs4 import BeautifulSoup

from src.pipeline.silver.config.team_config import GAME_TO_VERSION_GROUP
from src.pipeline.silver.transforms.keys import normalize_key_part

_BOSS_WEIGHT = 0.6
_SPECIES_WEIGHT = 0.4


def _normalized_series(frame: pd.DataFrame, column: str) -> pd.Series:
    return frame[column].map(normalize_key_part) if column in frame.columns else pd.Series(dtype="string")


def _lower_string_series(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(dtype="string")
    return frame[column].astype(str).map(lambda value: value.strip().lower())


def _boss_name_tokens(value: str) -> set[str]:
    return {token for token in re.split(r"[^a-z0-9]+", normalize_key_part(value)) if token}


def _boss_slug_from_id(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    if not normalized:
        return ""
    return normalize_key_part(normalized.split(":")[-1])


def _require_columns(frame: pd.DataFrame, required: set[str], name: str) -> None:
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"{name} missing required columns: {missing}")


def _coerce_depth(value: Any) -> float:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return 0.0
    return max(0.0, min(1.0, numeric))


def _effective_progression_game_version(value: Any) -> str:
    normalized = normalize_key_part(value)
    return GAME_TO_VERSION_GROUP.get(normalized, normalized)


@dataclass(frozen=True)
class ProgressionDepthEntry:
    game_version: str
    boss_id: str
    boss_name: str
    boss_index: int
    max_boss_index: int
    available_species_count: int
    max_species_count: int
    progression_depth: float
    boss_ace_level: int
    boss_avg_level: int
    starter_condition: str | None = None


@dataclass(frozen=True)
class ProgressionDepthContext:
    by_boss_id: dict[tuple[str, str], ProgressionDepthEntry]
    by_boss_name: dict[tuple[str, str], ProgressionDepthEntry]

    def require(self, *, game_version: str, boss_id: str, boss_name: str) -> ProgressionDepthEntry:
        key_by_id = (normalize_key_part(game_version), normalize_key_part(boss_id))
        if key_by_id in self.by_boss_id:
            return self.by_boss_id[key_by_id]

        key_by_name = (normalize_key_part(game_version), normalize_key_part(boss_name))
        if key_by_name in self.by_boss_name:
            return self.by_boss_name[key_by_name]

        game_key = normalize_key_part(game_version)
        requested_tokens = _boss_name_tokens(boss_name)
        if requested_tokens:
            candidates: list[ProgressionDepthEntry] = []
            for (candidate_game, _), entry in self.by_boss_name.items():
                if candidate_game != game_key:
                    continue
                candidate_tokens = _boss_name_tokens(entry.boss_name)
                if not candidate_tokens:
                    continue
                if candidate_tokens.issubset(requested_tokens) or requested_tokens.issubset(candidate_tokens):
                    candidates.append(entry)
            if len(candidates) == 1:
                return candidates[0]

        raise ValueError(
            "Missing progression depth context "
            f"for game_version={normalize_key_part(game_version)} boss_id={normalize_key_part(boss_id)} boss_name={normalize_key_part(boss_name)}"
        )


def build_boss_level_table(boss_team_members_df: pd.DataFrame) -> pd.DataFrame:
    _require_columns(boss_team_members_df, {"game_version", "boss_name", "level"}, "boss_team_members")
    if boss_team_members_df.empty:
        raise ValueError("boss_team_members is empty; cannot build boss level table")

    frame = boss_team_members_df.copy()
    frame["game_version"] = _normalized_series(frame, "game_version")
    frame["boss_name"] = _normalized_series(frame, "boss_name")
    frame["level"] = pd.to_numeric(frame["level"], errors="coerce").fillna(0).astype(int)
    frame = frame[(frame["game_version"] != "") & (frame["boss_name"] != "") & (frame["level"] > 0)]
    if frame.empty:
        raise ValueError("boss_team_members has no valid boss level rows")

    grouped = (
        frame.groupby(["game_version", "boss_name"], observed=True)
        .agg(
            boss_ace_level=("level", "max"),
            boss_avg_level=("level", "mean"),
        )
        .reset_index()
        .sort_values(["game_version", "boss_name"])
    )
    grouped["boss_avg_level"] = grouped["boss_avg_level"].round().astype(int)
    return grouped.reset_index(drop=True)


def build_boss_level_table_with_mapping(
    boss_team_members_df: pd.DataFrame,
    bosses_df: pd.DataFrame,
    bronze_dir: Path | None = None,
) -> pd.DataFrame:
    grouped = build_boss_level_table(boss_team_members_df)
    _require_columns(bosses_df, {"game_version", "boss_name_canonical", "boss_name_kaggle"}, "bosses")

    bosses = bosses_df.copy()
    bosses["game_version"] = _normalized_series(bosses, "game_version")
    bosses["boss_name"] = _normalized_series(bosses, "boss_name_canonical")
    bosses["boss_name_kaggle"] = _normalized_series(bosses, "boss_name_kaggle")
    bosses = bosses[(bosses["game_version"] != "") & (bosses["boss_name"] != "")]

    mapped = grouped.merge(
        bosses[["game_version", "boss_name", "boss_name_kaggle"]],
        left_on=["game_version", "boss_name"],
        right_on=["game_version", "boss_name_kaggle"],
        how="left",
        suffixes=("", "_canonical"),
    )
    mapped["boss_name"] = mapped["boss_name_canonical"].fillna(mapped["boss_name"])
    mapped = mapped.drop(columns=["boss_name_kaggle", "boss_name_canonical"], errors="ignore")
    mapped = mapped.sort_values(["game_version", "boss_name"]).reset_index(drop=True)

    required = bosses[["game_version", "boss_name"]].drop_duplicates().reset_index(drop=True)
    available = mapped[["game_version", "boss_name"]].drop_duplicates().reset_index(drop=True)
    missing = required.merge(available, on=["game_version", "boss_name"], how="left", indicator=True)
    missing = missing[missing["_merge"] == "left_only"][["game_version", "boss_name"]].reset_index(drop=True)

    if not missing.empty and bronze_dir is not None:
        bronze_levels = _build_bronze_boss_level_fallback(
            bronze_dir=bronze_dir,
            target_bosses=missing,
        )
        if not bronze_levels.empty:
            mapped = pd.concat([mapped, bronze_levels], ignore_index=True)
            mapped = (
                mapped.sort_values(["game_version", "boss_name", "boss_ace_level", "boss_avg_level"])
                .drop_duplicates(subset=["game_version", "boss_name"], keep="last")
                .reset_index(drop=True)
            )

    return mapped


def _build_bronze_boss_level_fallback(
    *,
    bronze_dir: Path,
    target_bosses: pd.DataFrame,
) -> pd.DataFrame:
    _require_columns(target_bosses, {"game_version", "boss_name"}, "target_bosses")
    targets = target_bosses.copy()
    targets["game_version"] = _normalized_series(targets, "game_version")
    targets["boss_name"] = _normalized_series(targets, "boss_name")
    targets = targets[(targets["game_version"] != "") & (targets["boss_name"] != "")]
    if targets.empty:
        return pd.DataFrame(columns=["game_version", "boss_name", "boss_ace_level", "boss_avg_level"])

    target_keys = {(row.game_version, row.boss_name) for row in targets.itertuples(index=False)}
    best_rows: dict[tuple[str, str], dict[str, int | str]] = {}

    for game_version in sorted(targets["game_version"].drop_duplicates().tolist()):
        bronze_path = bronze_dir / "bulbapedia" / f"{game_version}.json"
        if not bronze_path.exists():
            continue
        payload = json.loads(bronze_path.read_text(encoding="utf-8"))
        for part in payload.get("parts", []):
            soup = BeautifulSoup(str(part.get("html") or ""), "lxml")
            for party_box in soup.select("div.partycontainer"):
                name_node = party_box.select_one("div.partyname")
                if name_node is None:
                    continue
                boss_name = normalize_key_part(name_node.get_text(" ", strip=True))
                key = (game_version, boss_name)
                if key not in target_keys:
                    continue

                levels = _extract_party_levels(party_box)
                if not levels:
                    continue

                ace_level = max(levels)
                avg_level = int(round(sum(levels) / len(levels)))
                score = (ace_level, avg_level, len(levels))
                current = best_rows.get(key)
                current_score = None
                if current is not None:
                    current_score = (
                        int(current["boss_ace_level"]),
                        int(current["boss_avg_level"]),
                        int(current["member_count"]),
                    )
                if current_score is None or score > current_score:
                    best_rows[key] = {
                        "game_version": game_version,
                        "boss_name": boss_name,
                        "boss_ace_level": ace_level,
                        "boss_avg_level": avg_level,
                        "member_count": len(levels),
                    }

    if not best_rows:
        return pd.DataFrame(columns=["game_version", "boss_name", "boss_ace_level", "boss_avg_level"])

    return (
        pd.DataFrame(best_rows.values())[["game_version", "boss_name", "boss_ace_level", "boss_avg_level"]]
        .sort_values(["game_version", "boss_name"])
        .reset_index(drop=True)
    )


def _extract_party_levels(party_box: BeautifulSoup) -> list[int]:
    levels: list[int] = []
    for level_node in party_box.select("span.PKMNlevel"):
        match = re.search(r"Lv\.\s*(\d+)", level_node.get_text(" ", strip=True))
        if match is None:
            continue
        levels.append(int(match.group(1)))
    return levels


def build_progression_depth_context(
    progression_depth_df: pd.DataFrame,
    boss_level_df: pd.DataFrame,
) -> ProgressionDepthContext:
    _require_columns(
        progression_depth_df,
        {
            "game_version",
            "boss_id",
            "boss_name",
            "boss_index",
            "max_boss_index",
            "available_species_count",
            "max_species_count",
            "progression_depth",
        },
        "progression_depth",
    )
    _require_columns(boss_level_df, {"game_version", "boss_name", "boss_ace_level", "boss_avg_level"}, "boss_level")

    depth_frame = progression_depth_df.copy()
    depth_frame["game_version"] = _normalized_series(depth_frame, "game_version")
    depth_frame["boss_id"] = _normalized_series(depth_frame, "boss_id")
    depth_frame["boss_name"] = _normalized_series(depth_frame, "boss_name")
    if "starter_condition" in depth_frame.columns:
        depth_frame["starter_condition"] = _normalized_series(depth_frame, "starter_condition")
    else:
        depth_frame["starter_condition"] = None
    depth_frame["progression_depth"] = depth_frame["progression_depth"].map(_coerce_depth)

    level_frame = boss_level_df.copy()
    level_frame["game_version"] = _normalized_series(level_frame, "game_version")
    level_frame["boss_name"] = _normalized_series(level_frame, "boss_name")
    level_frame["boss_ace_level"] = pd.to_numeric(level_frame["boss_ace_level"], errors="coerce").fillna(0).astype(int)
    level_frame["boss_avg_level"] = pd.to_numeric(level_frame["boss_avg_level"], errors="coerce").fillna(0).astype(int)

    merged = depth_frame.merge(level_frame, on=["game_version", "boss_name"], how="left")
    merged = merged.sort_values(["game_version", "boss_index", "boss_id"]).reset_index(drop=True)
    invalid_levels = (
        merged["boss_ace_level"].isna()
        | merged["boss_avg_level"].isna()
        | (pd.to_numeric(merged["boss_ace_level"], errors="coerce").fillna(0) <= 0)
        | (pd.to_numeric(merged["boss_avg_level"], errors="coerce").fillna(0) <= 0)
    )
    if invalid_levels.any():
        missing_rows = merged[invalid_levels][["game_version", "boss_id", "boss_name"]]
        sample = ",".join(
            f"{row.game_version}:{row.boss_id}:{row.boss_name}"
            for row in missing_rows.head(10).itertuples(index=False)
        )
        raise ValueError(f"Missing boss level context for progression depth rows sample=[{sample}]")
    merged["boss_ace_level"] = merged["boss_ace_level"].astype(int)
    merged["boss_avg_level"] = merged["boss_avg_level"].astype(int)

    by_boss_id: dict[tuple[str, str], ProgressionDepthEntry] = {}
    by_boss_name: dict[tuple[str, str], ProgressionDepthEntry] = {}
    for row in merged.to_dict(orient="records"):
        entry = ProgressionDepthEntry(
            game_version=str(row.get("game_version") or "").strip().lower(),
            boss_id=str(row.get("boss_id") or "").strip().lower(),
            boss_name=str(row.get("boss_name") or "").strip().lower(),
            boss_index=int(row.get("boss_index") or 0),
            max_boss_index=int(row.get("max_boss_index") or 0),
            available_species_count=int(row.get("available_species_count") or 0),
            max_species_count=int(row.get("max_species_count") or 0),
            progression_depth=float(row.get("progression_depth") or 0.0),
            boss_ace_level=int(row.get("boss_ace_level") or 0),
            boss_avg_level=int(row.get("boss_avg_level") or 0),
            starter_condition=str(row.get("starter_condition") or "").strip().lower() or None,
        )
        by_boss_id[(entry.game_version, entry.boss_id)] = entry
        by_boss_name[(entry.game_version, entry.boss_name)] = entry

    return ProgressionDepthContext(by_boss_id=by_boss_id, by_boss_name=by_boss_name)


def build_progression_depth_table(
    bosses_df: pd.DataFrame,
    encounters_df: pd.DataFrame,
) -> pd.DataFrame:
    _require_columns(bosses_df, {"game_version", "boss_id", "boss_name_canonical", "boss_order"}, "bosses")
    _require_columns(encounters_df, {"game", "boss_id", "pokemon"}, "encounters")

    if bosses_df.empty:
        raise ValueError("bosses is empty; cannot build progression depth")
    if encounters_df.empty:
        raise ValueError("encounters is empty; cannot build progression depth")

    bosses = bosses_df.copy()
    bosses["game_version"] = _normalized_series(bosses, "game_version")
    bosses["progression_game_version"] = bosses["game_version"].map(_effective_progression_game_version)
    bosses["boss_id"] = _lower_string_series(bosses, "boss_id")
    bosses["boss_name"] = _normalized_series(bosses, "boss_name_canonical")
    bosses["boss_slug"] = bosses["boss_name"].map(normalize_key_part)
    bosses["boss_order"] = pd.to_numeric(bosses["boss_order"], errors="coerce").fillna(0).astype(int)
    if "gym_index" in bosses.columns:
        bosses["gym_index"] = pd.to_numeric(bosses["gym_index"], errors="coerce").fillna(bosses["boss_order"]).astype(int)
    else:
        bosses["gym_index"] = bosses["boss_order"]
    if "starter_condition" in bosses.columns:
        bosses["starter_condition"] = _normalized_series(bosses, "starter_condition")
    else:
        bosses["starter_condition"] = None
    bosses = bosses[(bosses["game_version"] != "") & (bosses["boss_id"] != "") & (bosses["boss_name"] != "")]
    if bosses.empty:
        raise ValueError("bosses has no valid progression rows")

    encounters = encounters_df.copy()
    encounters["game_version"] = _normalized_series(encounters, "game")
    encounters["progression_game_version"] = encounters["game_version"].map(_effective_progression_game_version)
    encounters["boss_id"] = _lower_string_series(encounters, "boss_id")
    encounters["boss_slug"] = encounters["boss_id"].map(_boss_slug_from_id)
    encounters["pokemon"] = _normalized_series(encounters, "pokemon")
    encounters = encounters[
        (encounters["game_version"] != "")
        & (encounters["boss_id"] != "")
        & (encounters["pokemon"] != "")
    ]
    if encounters.empty:
        raise ValueError("encounters has no valid species rows for progression depth")

    output_rows: list[dict[str, Any]] = []

    for game_version, bosses_game in bosses.groupby("game_version", sort=True, observed=False):
        bosses_sorted = bosses_game.sort_values(["gym_index", "boss_order", "boss_id"]).reset_index(drop=True)
        gym_indices = sorted(int(value) for value in bosses_sorted["gym_index"].drop_duplicates().tolist())
        expected_indices = list(range(gym_indices[0], gym_indices[0] + len(gym_indices)))
        if gym_indices != expected_indices:
            raise ValueError(
                f"Boss gym_index values must be contiguous for game_version={game_version}: "
                f"observed={gym_indices} expected={expected_indices}"
            )
        if bosses_sorted["boss_id"].duplicated().any():
            duplicates = bosses_sorted[bosses_sorted["boss_id"].duplicated(keep=False)]["boss_id"].tolist()
            raise ValueError(f"Duplicate boss_id values for game_version={game_version}: {duplicates}")

        progression_game_version = str(bosses_sorted["progression_game_version"].iloc[0] or "")
        encounters_game = encounters[encounters["progression_game_version"] == progression_game_version]
        if encounters_game.empty:
            raise ValueError(f"Encounter pool is empty for game_version={game_version}")

        relevant_boss_ids = {str(boss_id) for boss_id in bosses_sorted["boss_id"].tolist()}
        boss_id_by_slug = {
            str(row.boss_slug): str(row.boss_id)
            for row in bosses_sorted[["boss_id", "boss_slug"]].drop_duplicates().itertuples(index=False)
            if str(row.boss_slug)
        }
        species_by_boss: dict[str, set[str]] = {}
        for row in encounters_game[["boss_id", "boss_slug", "pokemon"]].drop_duplicates().itertuples(index=False):
            boss_id = str(row.boss_id)
            if boss_id not in relevant_boss_ids:
                boss_id = boss_id_by_slug.get(str(row.boss_slug), boss_id)
            if boss_id not in relevant_boss_ids:
                continue
            species_by_boss.setdefault(boss_id, set()).add(str(row.pokemon))

        max_boss_index = max(gym_indices)
        max_species_count = len({species for species_set in species_by_boss.values() for species in species_set})
        if max_species_count <= 0:
            raise ValueError(f"Encounter species pool is empty for game_version={game_version}")

        seen_species: set[str] = set()
        previous_depth = -1.0
        for gym_index, bosses_at_gym in bosses_sorted.groupby("gym_index", sort=True, observed=False):
            gym_species: set[str] = set()
            for boss in bosses_at_gym.itertuples(index=False):
                boss_id = str(boss.boss_id)
                boss_species = species_by_boss.get(boss_id, set())
                if not boss_species:
                    raise ValueError(f"Missing encounter pool for game_version={game_version} boss_id={boss_id}")
                gym_species.update(boss_species)

            prior_species_count = len(seen_species)
            seen_species.update(gym_species)
            available_species_count = len(seen_species)
            if available_species_count < prior_species_count:
                raise ValueError(
                    "Encounter species counts are inconsistent with cumulative boss accumulation "
                    f"for game_version={game_version} gym_index={gym_index} prior={prior_species_count} current={available_species_count}"
                )

            progression_depth = (_BOSS_WEIGHT * (int(gym_index) / max_boss_index)) + (
                _SPECIES_WEIGHT * (available_species_count / max_species_count)
            )
            if not 0.0 <= progression_depth <= 1.0:
                raise ValueError(
                    f"progression_depth out of bounds for game_version={game_version} gym_index={gym_index}: {progression_depth}"
                )
            if progression_depth <= previous_depth:
                raise ValueError(
                    "progression_depth must be strictly increasing with gym_index "
                    f"for game_version={game_version}: prev={previous_depth} current={progression_depth} gym_index={gym_index}"
                )
            previous_depth = progression_depth

            for boss in bosses_at_gym.itertuples(index=False):
                output_rows.append(
                    {
                        "game_version": game_version,
                        "boss_id": str(boss.boss_id),
                        "boss_name": str(boss.boss_name),
                        "boss_index": int(gym_index),
                        "max_boss_index": max_boss_index,
                        "available_species_count": available_species_count,
                        "max_species_count": max_species_count,
                        "progression_depth": progression_depth,
                        "starter_condition": str(getattr(boss, "starter_condition", "") or "").strip().lower() or None,
                    }
                )

        if len(seen_species) != max_species_count:
            raise ValueError(
                f"Final cumulative species count mismatch for game_version={game_version}: "
                f"seen={len(seen_species)} expected={max_species_count}"
            )

    return pd.DataFrame(output_rows).sort_values(["game_version", "boss_index", "boss_id"]).reset_index(drop=True)
