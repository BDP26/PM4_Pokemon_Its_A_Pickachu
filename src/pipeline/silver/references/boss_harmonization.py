from __future__ import annotations

from dataclasses import asdict, dataclass
from difflib import get_close_matches
import logging
from pathlib import Path
from typing import Any

import pandas as pd

from src.pipeline.common.io import write_json, write_parquet
from src.pipeline.silver.config.game_config import normalize_starter_type
from src.pipeline.silver.transforms.keys import normalize_key_part, stable_digest

logger = logging.getLogger(__name__)

KAGGLE_SOURCE_DATASET = "kaggle_gym_leaders_elite_four"
PROGRESSION_SOURCE_DATASET = "bulbapedia_story_progression"
HARMONIZED_SOURCE_PRIORITY = "kaggle_teams_bulbapedia_progression"

ALLOWED_GAME_NAME_MAP: dict[str, str] = {
    "Red": "red",
    "Blue": "blue",
    "Gold": "gold",
    "Silver": "silver",
    "Ruby": "ruby",
    "Sapphire": "sapphire",
    "Diamond": "diamond",
    "Pearl": "pearl",
    "Black": "black",
    "White": "white",
    "X": "x",
    "Y": "y",
}

ALLOWED_GAME_VERSIONS = tuple(ALLOWED_GAME_NAME_MAP.values())

KAGGLE_SPECIAL_CASES: dict[tuple[str, str], str] = {
    ("red", "champion-blue-bulbasaur"): "blue",
    ("red", "champion-blue-squirtle"): "blue",
    ("red", "champion-blue-charmander"): "blue",
    ("blue", "champion-blue-bulbasaur"): "blue",
    ("blue", "champion-blue-squirtle"): "blue",
    ("blue", "champion-blue-charmander"): "blue",
    ("gold", "champion-lance"): "lance",
    ("silver", "champion-lance"): "lance",
    ("ruby", "champion-steven"): "steven",
    ("sapphire", "champion-steven"): "steven",
    ("ruby", "tate-liza"): "tate-and-liza",
    ("sapphire", "tate-liza"): "tate-and-liza",
    ("diamond", "champion-cynthia"): "cynthia",
    ("pearl", "champion-cynthia"): "cynthia",
    ("black", "champion-alder"): "alder",
    ("white", "champion-alder"): "alder",
    ("x", "champion-diantha"): "diantha",
    ("y", "champion-diantha"): "diantha",
}

BLUE_VARIANT_BY_SPECIES: dict[str, str] = {
    "charizard": "charizard_variant",
    "blastoise": "blastoise_variant",
    "venusaur": "venusaur_variant",
}

STARTER_TYPE_BY_BLUE_VARIANT: dict[str, str] = {
    "charizard_variant": "grass",
    "blastoise_variant": "fire",
    "venusaur_variant": "water",
}


@dataclass(frozen=True)
class ProgressionBossSpec:
    boss_name: str
    boss_role: str
    progression_order: int
    location_name: str | None = None
    battle_type: str = "single"
    source_boss_name_bulbapedia: str | None = None
    is_branching: bool = False
    branch_group: str | None = None
    branch_condition: str | None = None
    is_optional: bool = False
    is_postgame: bool = False
    starter_dependency_type: str = "none"
    has_team_variants: bool = False
    starter_type: str | None = None


@dataclass(frozen=True)
class BossHarmonizationResult:
    kaggle_rows: pd.DataFrame
    bosses: pd.DataFrame
    boss_teams: pd.DataFrame
    progression_edges: pd.DataFrame
    diagnostics: dict[str, Any]


def _progression_specs() -> dict[str, tuple[ProgressionBossSpec, ...]]:
    return {
        "red": (
            ProgressionBossSpec("Brock", "gym", 1, "Pewter City"),
            ProgressionBossSpec("Misty", "gym", 2, "Cerulean City"),
            ProgressionBossSpec("Lt. Surge", "gym", 3, "Vermilion City"),
            ProgressionBossSpec("Erika", "gym", 4, "Celadon City"),
            ProgressionBossSpec("Koga", "gym", 5, "Fuchsia City"),
            ProgressionBossSpec("Sabrina", "gym", 6, "Saffron City"),
            ProgressionBossSpec("Blaine", "gym", 7, "Cinnabar Island"),
            ProgressionBossSpec("Giovanni", "gym", 8, "Viridian City"),
            ProgressionBossSpec("Lorelei", "elite_four", 9, "Indigo Plateau"),
            ProgressionBossSpec("Bruno", "elite_four", 9, "Indigo Plateau"),
            ProgressionBossSpec("Agatha", "elite_four", 9, "Indigo Plateau"),
            ProgressionBossSpec("Lance", "elite_four", 9, "Indigo Plateau"),
            ProgressionBossSpec("Blue", "champion", 10, "Indigo Plateau", starter_dependency_type="team_variant", has_team_variants=True),
        ),
        "blue": (
            ProgressionBossSpec("Brock", "gym", 1, "Pewter City"),
            ProgressionBossSpec("Misty", "gym", 2, "Cerulean City"),
            ProgressionBossSpec("Lt. Surge", "gym", 3, "Vermilion City"),
            ProgressionBossSpec("Erika", "gym", 4, "Celadon City"),
            ProgressionBossSpec("Koga", "gym", 5, "Fuchsia City"),
            ProgressionBossSpec("Sabrina", "gym", 6, "Saffron City"),
            ProgressionBossSpec("Blaine", "gym", 7, "Cinnabar Island"),
            ProgressionBossSpec("Giovanni", "gym", 8, "Viridian City"),
            ProgressionBossSpec("Lorelei", "elite_four", 9, "Indigo Plateau"),
            ProgressionBossSpec("Bruno", "elite_four", 9, "Indigo Plateau"),
            ProgressionBossSpec("Agatha", "elite_four", 9, "Indigo Plateau"),
            ProgressionBossSpec("Lance", "elite_four", 9, "Indigo Plateau"),
            ProgressionBossSpec("Blue", "champion", 10, "Indigo Plateau", starter_dependency_type="team_variant", has_team_variants=True),
        ),
        "gold": (
            ProgressionBossSpec("Falkner", "gym", 1, "Violet City"),
            ProgressionBossSpec("Bugsy", "gym", 2, "Azalea Town"),
            ProgressionBossSpec("Whitney", "gym", 3, "Goldenrod City"),
            ProgressionBossSpec("Morty", "gym", 4, "Ecruteak City"),
            ProgressionBossSpec("Chuck", "gym", 5, "Cianwood City"),
            ProgressionBossSpec("Jasmine", "gym", 6, "Olivine City"),
            ProgressionBossSpec("Pryce", "gym", 7, "Mahogany Town"),
            ProgressionBossSpec("Clair", "gym", 8, "Blackthorn City"),
            ProgressionBossSpec("Will", "elite_four", 9, "Indigo Plateau"),
            ProgressionBossSpec("Koga", "elite_four", 9, "Indigo Plateau"),
            ProgressionBossSpec("Bruno", "elite_four", 9, "Indigo Plateau"),
            ProgressionBossSpec("Karen", "elite_four", 9, "Indigo Plateau"),
            ProgressionBossSpec("Lance", "champion", 10, "Indigo Plateau"),
            ProgressionBossSpec("Lt. Surge", "gym", 11, "Vermilion City", is_optional=True, is_postgame=True),
            ProgressionBossSpec("Sabrina", "gym", 12, "Saffron City", is_optional=True, is_postgame=True),
            ProgressionBossSpec("Misty", "gym", 13, "Cerulean City", is_optional=True, is_postgame=True),
            ProgressionBossSpec("Erika", "gym", 14, "Celadon City", is_optional=True, is_postgame=True),
            ProgressionBossSpec("Janine", "gym", 15, "Fuchsia City", is_optional=True, is_postgame=True),
            ProgressionBossSpec("Brock", "gym", 16, "Pewter City", is_optional=True, is_postgame=True),
            ProgressionBossSpec("Blaine", "gym", 17, "Seafoam Islands", is_optional=True, is_postgame=True),
            ProgressionBossSpec("Blue", "gym", 18, "Viridian City", is_optional=True, is_postgame=True),
        ),
        "silver": (
            ProgressionBossSpec("Falkner", "gym", 1, "Violet City"),
            ProgressionBossSpec("Bugsy", "gym", 2, "Azalea Town"),
            ProgressionBossSpec("Whitney", "gym", 3, "Goldenrod City"),
            ProgressionBossSpec("Morty", "gym", 4, "Ecruteak City"),
            ProgressionBossSpec("Chuck", "gym", 5, "Cianwood City"),
            ProgressionBossSpec("Jasmine", "gym", 6, "Olivine City"),
            ProgressionBossSpec("Pryce", "gym", 7, "Mahogany Town"),
            ProgressionBossSpec("Clair", "gym", 8, "Blackthorn City"),
            ProgressionBossSpec("Will", "elite_four", 9, "Indigo Plateau"),
            ProgressionBossSpec("Koga", "elite_four", 9, "Indigo Plateau"),
            ProgressionBossSpec("Bruno", "elite_four", 9, "Indigo Plateau"),
            ProgressionBossSpec("Karen", "elite_four", 9, "Indigo Plateau"),
            ProgressionBossSpec("Lance", "champion", 10, "Indigo Plateau"),
            ProgressionBossSpec("Lt. Surge", "gym", 11, "Vermilion City", is_optional=True, is_postgame=True),
            ProgressionBossSpec("Sabrina", "gym", 12, "Saffron City", is_optional=True, is_postgame=True),
            ProgressionBossSpec("Misty", "gym", 13, "Cerulean City", is_optional=True, is_postgame=True),
            ProgressionBossSpec("Erika", "gym", 14, "Celadon City", is_optional=True, is_postgame=True),
            ProgressionBossSpec("Janine", "gym", 15, "Fuchsia City", is_optional=True, is_postgame=True),
            ProgressionBossSpec("Brock", "gym", 16, "Pewter City", is_optional=True, is_postgame=True),
            ProgressionBossSpec("Blaine", "gym", 17, "Seafoam Islands", is_optional=True, is_postgame=True),
            ProgressionBossSpec("Blue", "gym", 18, "Viridian City", is_optional=True, is_postgame=True),
        ),
        "ruby": (
            ProgressionBossSpec("Roxanne", "gym", 1, "Rustboro City"),
            ProgressionBossSpec("Brawly", "gym", 2, "Dewford Town"),
            ProgressionBossSpec("Wattson", "gym", 3, "Mauville City"),
            ProgressionBossSpec("Flannery", "gym", 4, "Lavaridge Town"),
            ProgressionBossSpec("Norman", "gym", 5, "Petalburg City"),
            ProgressionBossSpec("Winona", "gym", 6, "Fortree City"),
            ProgressionBossSpec("Tate and Liza", "gym", 7, "Mossdeep City", battle_type="double"),
            ProgressionBossSpec("Wallace", "gym", 8, "Sootopolis City"),
            ProgressionBossSpec("Sidney", "elite_four", 9, "Ever Grande City"),
            ProgressionBossSpec("Phoebe", "elite_four", 9, "Ever Grande City"),
            ProgressionBossSpec("Glacia", "elite_four", 9, "Ever Grande City"),
            ProgressionBossSpec("Drake", "elite_four", 9, "Ever Grande City"),
            ProgressionBossSpec("Steven", "champion", 10, "Ever Grande City"),
        ),
        "sapphire": (
            ProgressionBossSpec("Roxanne", "gym", 1, "Rustboro City"),
            ProgressionBossSpec("Brawly", "gym", 2, "Dewford Town"),
            ProgressionBossSpec("Wattson", "gym", 3, "Mauville City"),
            ProgressionBossSpec("Flannery", "gym", 4, "Lavaridge Town"),
            ProgressionBossSpec("Norman", "gym", 5, "Petalburg City"),
            ProgressionBossSpec("Winona", "gym", 6, "Fortree City"),
            ProgressionBossSpec("Tate and Liza", "gym", 7, "Mossdeep City", battle_type="double"),
            ProgressionBossSpec("Wallace", "gym", 8, "Sootopolis City"),
            ProgressionBossSpec("Sidney", "elite_four", 9, "Ever Grande City"),
            ProgressionBossSpec("Phoebe", "elite_four", 9, "Ever Grande City"),
            ProgressionBossSpec("Glacia", "elite_four", 9, "Ever Grande City"),
            ProgressionBossSpec("Drake", "elite_four", 9, "Ever Grande City"),
            ProgressionBossSpec("Steven", "champion", 10, "Ever Grande City"),
        ),
        "diamond": (
            ProgressionBossSpec("Roark", "gym", 1, "Oreburgh City"),
            ProgressionBossSpec("Gardenia", "gym", 2, "Eterna City"),
            ProgressionBossSpec("Maylene", "gym", 3, "Veilstone City"),
            ProgressionBossSpec("Crasher Wake", "gym", 4, "Pastoria City"),
            ProgressionBossSpec("Fantina", "gym", 5, "Hearthome City"),
            ProgressionBossSpec("Byron", "gym", 6, "Canalave City"),
            ProgressionBossSpec("Candice", "gym", 7, "Snowpoint City"),
            ProgressionBossSpec("Volkner", "gym", 8, "Sunyshore City"),
            ProgressionBossSpec("Aaron", "elite_four", 9, "Pokemon League"),
            ProgressionBossSpec("Bertha", "elite_four", 9, "Pokemon League"),
            ProgressionBossSpec("Flint", "elite_four", 9, "Pokemon League"),
            ProgressionBossSpec("Lucian", "elite_four", 9, "Pokemon League"),
            ProgressionBossSpec("Cynthia", "champion", 10, "Pokemon League"),
        ),
        "pearl": (
            ProgressionBossSpec("Roark", "gym", 1, "Oreburgh City"),
            ProgressionBossSpec("Gardenia", "gym", 2, "Eterna City"),
            ProgressionBossSpec("Maylene", "gym", 3, "Veilstone City"),
            ProgressionBossSpec("Crasher Wake", "gym", 4, "Pastoria City"),
            ProgressionBossSpec("Fantina", "gym", 5, "Hearthome City"),
            ProgressionBossSpec("Byron", "gym", 6, "Canalave City"),
            ProgressionBossSpec("Candice", "gym", 7, "Snowpoint City"),
            ProgressionBossSpec("Volkner", "gym", 8, "Sunyshore City"),
            ProgressionBossSpec("Aaron", "elite_four", 9, "Pokemon League"),
            ProgressionBossSpec("Bertha", "elite_four", 9, "Pokemon League"),
            ProgressionBossSpec("Flint", "elite_four", 9, "Pokemon League"),
            ProgressionBossSpec("Lucian", "elite_four", 9, "Pokemon League"),
            ProgressionBossSpec("Cynthia", "champion", 10, "Pokemon League"),
        ),
        "black": (
            ProgressionBossSpec("Chili", "gym", 1, "Striaton City", is_branching=True, branch_group="striaton_gym", branch_condition="starter_type", starter_dependency_type="branching", starter_type="grass"),
            ProgressionBossSpec("Cilan", "gym", 1, "Striaton City", is_branching=True, branch_group="striaton_gym", branch_condition="starter_type", starter_dependency_type="branching", starter_type="water"),
            ProgressionBossSpec("Cress", "gym", 1, "Striaton City", is_branching=True, branch_group="striaton_gym", branch_condition="starter_type", starter_dependency_type="branching", starter_type="fire"),
            ProgressionBossSpec("Lenora", "gym", 2, "Nacrene City"),
            ProgressionBossSpec("Burgh", "gym", 3, "Castelia City"),
            ProgressionBossSpec("Elesa", "gym", 4, "Nimbasa City"),
            ProgressionBossSpec("Clay", "gym", 5, "Driftveil City"),
            ProgressionBossSpec("Skyla", "gym", 6, "Mistralton City"),
            ProgressionBossSpec("Brycen", "gym", 7, "Icirrus City"),
            ProgressionBossSpec("Drayden", "gym", 8, "Opelucid City"),
            ProgressionBossSpec("Shauntal", "elite_four", 9, "Pokemon League", is_branching=True, branch_group="elite_four_unova"),
            ProgressionBossSpec("Grimsley", "elite_four", 9, "Pokemon League", is_branching=True, branch_group="elite_four_unova"),
            ProgressionBossSpec("Caitlin", "elite_four", 9, "Pokemon League", is_branching=True, branch_group="elite_four_unova"),
            ProgressionBossSpec("Marshal", "elite_four", 9, "Pokemon League", is_branching=True, branch_group="elite_four_unova"),
            ProgressionBossSpec("Alder", "champion", 10, "Pokemon League"),
        ),
        "white": (
            ProgressionBossSpec("Chili", "gym", 1, "Striaton City", is_branching=True, branch_group="striaton_gym", branch_condition="starter_type", starter_dependency_type="branching", starter_type="grass"),
            ProgressionBossSpec("Cilan", "gym", 1, "Striaton City", is_branching=True, branch_group="striaton_gym", branch_condition="starter_type", starter_dependency_type="branching", starter_type="water"),
            ProgressionBossSpec("Cress", "gym", 1, "Striaton City", is_branching=True, branch_group="striaton_gym", branch_condition="starter_type", starter_dependency_type="branching", starter_type="fire"),
            ProgressionBossSpec("Lenora", "gym", 2, "Nacrene City"),
            ProgressionBossSpec("Burgh", "gym", 3, "Castelia City"),
            ProgressionBossSpec("Elesa", "gym", 4, "Nimbasa City"),
            ProgressionBossSpec("Clay", "gym", 5, "Driftveil City"),
            ProgressionBossSpec("Skyla", "gym", 6, "Mistralton City"),
            ProgressionBossSpec("Brycen", "gym", 7, "Icirrus City"),
            ProgressionBossSpec("Iris", "gym", 8, "Opelucid City"),
            ProgressionBossSpec("Shauntal", "elite_four", 9, "Pokemon League", is_branching=True, branch_group="elite_four_unova"),
            ProgressionBossSpec("Grimsley", "elite_four", 9, "Pokemon League", is_branching=True, branch_group="elite_four_unova"),
            ProgressionBossSpec("Caitlin", "elite_four", 9, "Pokemon League", is_branching=True, branch_group="elite_four_unova"),
            ProgressionBossSpec("Marshal", "elite_four", 9, "Pokemon League", is_branching=True, branch_group="elite_four_unova"),
            ProgressionBossSpec("Alder", "champion", 10, "Pokemon League"),
        ),
        "x": (
            ProgressionBossSpec("Viola", "gym", 1, "Santalune City"),
            ProgressionBossSpec("Grant", "gym", 2, "Cyllage City"),
            ProgressionBossSpec("Korrina", "gym", 3, "Shalour City"),
            ProgressionBossSpec("Ramos", "gym", 4, "Coumarine City"),
            ProgressionBossSpec("Clemont", "gym", 5, "Lumiose City"),
            ProgressionBossSpec("Valerie", "gym", 6, "Laverre City"),
            ProgressionBossSpec("Olympia", "gym", 7, "Anistar City"),
            ProgressionBossSpec("Wulfric", "gym", 8, "Snowbelle City"),
            ProgressionBossSpec("Malva", "elite_four", 9, "Pokemon League", is_branching=True, branch_group="elite_four_kalos"),
            ProgressionBossSpec("Siebold", "elite_four", 9, "Pokemon League", is_branching=True, branch_group="elite_four_kalos"),
            ProgressionBossSpec("Wikstrom", "elite_four", 9, "Pokemon League", is_branching=True, branch_group="elite_four_kalos"),
            ProgressionBossSpec("Drasna", "elite_four", 9, "Pokemon League", is_branching=True, branch_group="elite_four_kalos"),
            ProgressionBossSpec("Diantha", "champion", 10, "Pokemon League"),
        ),
        "y": (
            ProgressionBossSpec("Viola", "gym", 1, "Santalune City"),
            ProgressionBossSpec("Grant", "gym", 2, "Cyllage City"),
            ProgressionBossSpec("Korrina", "gym", 3, "Shalour City"),
            ProgressionBossSpec("Ramos", "gym", 4, "Coumarine City"),
            ProgressionBossSpec("Clemont", "gym", 5, "Lumiose City"),
            ProgressionBossSpec("Valerie", "gym", 6, "Laverre City"),
            ProgressionBossSpec("Olympia", "gym", 7, "Anistar City"),
            ProgressionBossSpec("Wulfric", "gym", 8, "Snowbelle City"),
            ProgressionBossSpec("Malva", "elite_four", 9, "Pokemon League", is_branching=True, branch_group="elite_four_kalos"),
            ProgressionBossSpec("Siebold", "elite_four", 9, "Pokemon League", is_branching=True, branch_group="elite_four_kalos"),
            ProgressionBossSpec("Wikstrom", "elite_four", 9, "Pokemon League", is_branching=True, branch_group="elite_four_kalos"),
            ProgressionBossSpec("Drasna", "elite_four", 9, "Pokemon League", is_branching=True, branch_group="elite_four_kalos"),
            ProgressionBossSpec("Diantha", "champion", 10, "Pokemon League"),
        ),
    }


def _coerce_nullable_text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    text = str(value).strip()
    return text or None


def _normalized_candidate(value: Any) -> str:
    return normalize_key_part(_coerce_nullable_text(value) or "")


def _normalized_starter_type(value: Any) -> str | None:
    return normalize_starter_type(_coerce_nullable_text(value))


def _boss_identifier(
    *,
    game_version: str,
    boss_name: str,
    progression_order: int | None,
    branch_group: str | None,
    branch_condition: str | None,
    location_name: str | None,
) -> str:
    boss_norm = normalize_key_part(boss_name)
    digest = stable_digest(
        game_version,
        boss_norm,
        progression_order or "",
        branch_group or "",
        branch_condition or "",
        location_name or "",
        length=12,
    )
    suffix = boss_norm or "unknown"
    return f"boss:{game_version}:{suffix}:{digest}"


def make_canonical_boss_id(
    *,
    game_version: str,
    boss_name: str,
    progression_order: int | None,
    branch_group: str | None = None,
    branch_condition: str | None = None,
    location_name: str | None = None,
) -> str:
    return _boss_identifier(
        game_version=game_version,
        boss_name=boss_name,
        progression_order=progression_order,
        branch_group=branch_group,
        branch_condition=branch_condition,
        location_name=location_name,
    )


def _boss_team_identifier(*, boss_id: str, kaggle_boss_key: str, team_variant: str) -> str:
    digest = stable_digest(boss_id, kaggle_boss_key, team_variant, length=12)
    return f"boss-team:{digest}"


def _normalize_kaggle_rows(raw_df: pd.DataFrame) -> pd.DataFrame:
    normalized = raw_df.copy()
    normalized.columns = [normalize_key_part(column).replace("-", "_") for column in normalized.columns]
    normalized = normalized.rename(
        columns={
            "generation": "generation",
            "game": "game_raw",
            "gym": "gym_or_stage",
            "gym_leader": "boss_name_raw",
            "pokemon": "pokemon_species",
            "level": "level",
            "move_1": "move_1",
            "move_2": "move_2",
            "move_3": "move_3",
            "move_4": "move_4",
        }
    )
    normalized["source_dataset"] = KAGGLE_SOURCE_DATASET
    normalized["source_record_id"] = [f"kaggle:{index}" for index in range(1, len(normalized) + 1)]
    normalized["generation"] = pd.to_numeric(normalized["generation"], errors="coerce").astype("Int64")
    normalized["game_raw"] = normalized["game_raw"].map(_coerce_nullable_text)
    normalized["game_version"] = normalized["game_raw"].map(lambda value: ALLOWED_GAME_NAME_MAP.get(value or ""))
    normalized["gym_or_stage"] = normalized["gym_or_stage"].map(_coerce_nullable_text)
    normalized["boss_name"] = normalized["boss_name_raw"].map(_coerce_nullable_text)
    normalized["pokemon_species"] = normalized["pokemon_species"].map(
        lambda value: normalize_key_part(_coerce_nullable_text(value) or "") or None
    )
    normalized["level"] = pd.to_numeric(normalized["level"], errors="coerce").astype("Int64")
    for column in ("move_1", "move_2", "move_3", "move_4"):
        normalized[column] = normalized[column].map(_coerce_nullable_text).map(
            lambda value: normalize_key_part(value) if value else None
        )
    normalized["normalized_boss_name"] = normalized["boss_name"].map(_normalized_candidate)
    normalized["kaggle_boss_key"] = normalized.apply(
        lambda row: ":".join(
            [
                str(row.get("game_version") or "unknown"),
                str(row.get("normalized_boss_name") or "unknown"),
                normalize_key_part(row.get("gym_or_stage") or "unknown"),
            ]
        ),
        axis=1,
    )
    return normalized[
        [
            "generation",
            "game_version",
            "gym_or_stage",
            "boss_name",
            "pokemon_species",
            "level",
            "move_1",
            "move_2",
            "move_3",
            "move_4",
            "source_dataset",
            "source_record_id",
            "game_raw",
            "boss_name_raw",
            "normalized_boss_name",
            "kaggle_boss_key",
        ]
    ].copy()


def load_kaggle_boss_rows(csv_path: Path) -> tuple[pd.DataFrame, dict[str, Any]]:
    logger.info("[silver/bosses] loading kaggle boss csv ...")
    raw_df = pd.read_csv(csv_path, sep=";")
    raw_rows = len(raw_df)
    logger.info("[silver/bosses] raw_rows=%s", raw_rows)

    normalized = _normalize_kaggle_rows(raw_df)
    generation_filtered = normalized[normalized["generation"].fillna(0).astype(int) <= 6].copy()
    logger.info("[silver/bosses] filtered_generation_rows=%s", len(generation_filtered))

    base_version_filtered = generation_filtered[generation_filtered["game_version"].isin(ALLOWED_GAME_VERSIONS)].copy()
    logger.info("[silver/bosses] filtered_base_version_rows=%s", len(base_version_filtered))

    excluded_games = sorted(
        {
            str(game).strip()
            for game in normalized["game_raw"].dropna().tolist()
            if str(game).strip() not in ALLOWED_GAME_NAME_MAP
        }
    )
    logger.info("[silver/bosses] excluded_games=%s", excluded_games)

    diagnostics = {
        "total_kaggle_rows_raw": raw_rows,
        "total_kaggle_rows_after_generation_filter": int(len(generation_filtered)),
        "total_kaggle_rows_after_base_version_filter": int(len(base_version_filtered)),
        "excluded_games": excluded_games,
        "included_games": list(ALLOWED_GAME_VERSIONS),
    }
    return base_version_filtered.reset_index(drop=True), diagnostics


def _infer_kaggle_role(gym_or_stage: str | None, boss_name: str | None) -> str:
    stage_norm = normalize_key_part(gym_or_stage or "")
    boss_norm = normalize_key_part(boss_name or "")
    if stage_norm == "elite-four":
        if boss_norm.startswith("champion-"):
            return "champion"
        return "elite_four"
    return "gym"


def _progression_rows() -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for game_version, specs in _progression_specs().items():
        unique_orders = sorted({int(spec.progression_order) for spec in specs})
        max_rank = max(1, len(unique_orders))
        depth_by_order = {
            order: (index + 1) / max_rank
            for index, order in enumerate(unique_orders)
        }
        for spec in specs:
            normalized_boss_name = normalize_key_part(spec.boss_name)
            source_boss_name_bulbapedia = spec.source_boss_name_bulbapedia or spec.boss_name
            progression_key = (
                f"{game_version}:{spec.progression_order}:{normalized_boss_name}:"
                f"{normalize_key_part(spec.branch_condition or 'main')}"
            )
            boss_id = _boss_identifier(
                game_version=game_version,
                boss_name=spec.boss_name,
                progression_order=spec.progression_order,
                branch_group=spec.branch_group,
                branch_condition=spec.branch_condition,
                location_name=spec.location_name,
            )
            rows.append(
                {
                    "boss_id": boss_id,
                    "game_version": game_version,
                    "generation": _generation_for_game_version(game_version),
                    "boss_name": spec.boss_name,
                    "normalized_boss_name": normalized_boss_name,
                    "boss_role": spec.boss_role,
                    "battle_type": spec.battle_type,
                    "source_boss_name_kaggle": None,
                    "source_boss_name_bulbapedia": source_boss_name_bulbapedia,
                    "kaggle_boss_key": None,
                    "bulbapedia_progression_key": progression_key,
                    "location_name": spec.location_name,
                    "progression_order": int(spec.progression_order),
                    "progression_depth": float(depth_by_order[int(spec.progression_order)]),
                    "is_branching": bool(spec.is_branching),
                    "branch_group": spec.branch_group,
                    "branch_condition": spec.branch_condition,
                    "starter_dependency_type": spec.starter_dependency_type,
                    "has_team_variants": bool(spec.has_team_variants),
                    "starter_type": _normalized_starter_type(spec.starter_type),
                    "is_optional": bool(spec.is_optional),
                    "is_postgame": bool(spec.is_postgame),
                    "source_priority": HARMONIZED_SOURCE_PRIORITY,
                    "harmonization_status": "progression_only",
                    "harmonization_notes": "Bulbapedia progression node has not been mapped to a Kaggle team yet.",
                    "boss_name_canonical": spec.boss_name,
                    "boss_name_kaggle": None,
                    "boss_name_aliases": [spec.boss_name],
                    "boss_order": int(spec.progression_order),
                    "gym_index": int(spec.progression_order),
                    "starter_condition": _normalized_starter_type(spec.starter_type),
                    "is_simulatable": False,
                }
            )
    return pd.DataFrame(rows).sort_values(["game_version", "progression_order", "boss_name"]).reset_index(drop=True)


def _generation_for_game_version(game_version: str) -> int:
    return {
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
        "x": 6,
        "y": 6,
    }[game_version]


def _alias_lookup(progression_df: pd.DataFrame) -> dict[tuple[str, str], str]:
    alias_lookup: dict[tuple[str, str], str] = {}
    for row in progression_df.to_dict(orient="records"):
        game_version = str(row["game_version"])
        canonical = str(row["normalized_boss_name"])
        alias_lookup[(game_version, canonical)] = canonical
        source_name = normalize_key_part(row.get("source_boss_name_bulbapedia") or "")
        if source_name:
            alias_lookup[(game_version, source_name)] = canonical
    return alias_lookup


def _group_kaggle_teams(kaggle_rows: pd.DataFrame) -> list[dict[str, Any]]:
    grouped: list[dict[str, Any]] = []
    sort_frame = kaggle_rows.sort_values(["game_version", "gym_or_stage", "boss_name", "source_record_id"]).copy()
    for (game_version, boss_name, gym_or_stage), group in sort_frame.groupby(
        ["game_version", "boss_name", "gym_or_stage"],
        dropna=False,
        sort=True,
    ):
        group_rows = group.to_dict(orient="records")
        normalized_boss_name = normalize_key_part(boss_name or "")
        kaggle_role = _infer_kaggle_role(gym_or_stage, boss_name)
        kaggle_boss_key = str(group_rows[0]["kaggle_boss_key"])
        grouped.append(
            {
                "game_version": str(game_version),
                "boss_name_raw": boss_name,
                "normalized_boss_name": normalized_boss_name,
                "gym_or_stage": gym_or_stage,
                "kaggle_boss_role": kaggle_role,
                "kaggle_boss_key": kaggle_boss_key,
                "rows": group_rows,
            }
        )
    return grouped


def _build_progression_indexes(
    progression_df: pd.DataFrame,
) -> tuple[
    dict[tuple[str, str, str], list[dict[str, Any]]],
    dict[tuple[str, str], list[dict[str, Any]]],
]:
    by_role: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    by_name: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in progression_df.to_dict(orient="records"):
        role_key = (
            str(row["game_version"]),
            str(row["normalized_boss_name"]),
            str(row["boss_role"]),
        )
        name_key = (
            str(row["game_version"]),
            str(row["normalized_boss_name"]),
        )
        by_role.setdefault(role_key, []).append(row)
        by_name.setdefault(name_key, []).append(row)
    return by_role, by_name


def _single_candidate(candidates: list[dict[str, Any]]) -> dict[str, Any] | None:
    if len(candidates) != 1:
        return None
    return candidates[0]


def _diagnostic_suggestions(
    *,
    game_version: str,
    normalized_name: str,
    progression_df: pd.DataFrame,
) -> list[str]:
    candidates = progression_df[progression_df["game_version"].eq(game_version)]["normalized_boss_name"].tolist()
    return list(get_close_matches(normalized_name, candidates, n=3, cutoff=0.6))


def _fallback_manual_review_boss(team_group: dict[str, Any]) -> dict[str, Any]:
    game_version = str(team_group["game_version"])
    boss_name = str(team_group["boss_name_raw"] or "unknown")
    location_name = _coerce_nullable_text(team_group.get("gym_or_stage"))
    boss_id = _boss_identifier(
        game_version=game_version,
        boss_name=boss_name,
        progression_order=None,
        branch_group=None,
        branch_condition=None,
        location_name=location_name,
    )
    normalized_boss_name = normalize_key_part(boss_name)
    return {
        "boss_id": boss_id,
        "game_version": game_version,
        "generation": _generation_for_game_version(game_version),
        "boss_name": boss_name,
        "normalized_boss_name": normalized_boss_name,
        "boss_role": str(team_group.get("kaggle_boss_role") or "gym"),
        "battle_type": "single",
        "source_boss_name_kaggle": boss_name,
        "source_boss_name_bulbapedia": None,
        "kaggle_boss_key": str(team_group["kaggle_boss_key"]),
        "bulbapedia_progression_key": None,
        "location_name": location_name,
        "progression_order": None,
        "progression_depth": None,
        "is_branching": False,
        "branch_group": None,
        "branch_condition": None,
        "starter_dependency_type": "none",
        "has_team_variants": False,
        "starter_type": None,
        "is_optional": False,
        "is_postgame": False,
        "source_priority": HARMONIZED_SOURCE_PRIORITY,
        "harmonization_status": "manual_review",
        "harmonization_notes": "Kaggle team could not be mapped to a Bulbapedia progression node.",
        "boss_name_canonical": boss_name,
        "boss_name_kaggle": boss_name,
        "boss_name_aliases": [boss_name],
        "boss_order": None,
        "gym_index": None,
        "starter_condition": None,
        "is_simulatable": True,
    }


def _blue_team_variant_metadata(group: dict[str, Any]) -> dict[str, str | None]:
    team_species = {
        str(row.get("pokemon_species") or "").strip().lower()
        for row in list(group.get("rows") or [])
        if str(row.get("pokemon_species") or "").strip()
    }
    variant_species = sorted(team_species & set(BLUE_VARIANT_BY_SPECIES))
    if len(variant_species) != 1:
        raise ValueError(
            "Blue champion team variant detection failed: "
            f"game_version={group.get('game_version')} kaggle_boss_key={group.get('kaggle_boss_key')} "
            f"matched_species={variant_species}"
        )
    team_variant = BLUE_VARIANT_BY_SPECIES[variant_species[0]]
    return {
        "team_variant": team_variant,
        "starter_type": STARTER_TYPE_BY_BLUE_VARIANT[team_variant],
        "variant_dimension": "starter_type",
    }


def _assign_team_variant_metadata(mapped_groups: list[dict[str, Any]]) -> dict[str, dict[str, str | None]]:
    groups_by_boss_id: dict[str, list[dict[str, Any]]] = {}
    for group in mapped_groups:
        boss_id = str(group["boss_row"]["boss_id"])
        groups_by_boss_id.setdefault(boss_id, []).append(group)

    metadata_by_group: dict[str, dict[str, str | None]] = {}
    for boss_id, boss_groups in groups_by_boss_id.items():
        boss_row = dict(boss_groups[0]["boss_row"])
        starter_dependency_type = str(boss_row.get("starter_dependency_type") or "none")
        game_version = str(boss_row.get("game_version") or "")
        boss_name = str(boss_row.get("boss_name") or "")
        boss_role = str(boss_row.get("boss_role") or "")

        if starter_dependency_type == "team_variant":
            if (game_version, boss_name, boss_role) not in {("red", "Blue", "champion"), ("blue", "Blue", "champion")}:
                raise ValueError(
                    "Unsupported starter-dependent team variant boss detected: "
                    f"boss_id={boss_id} game_version={game_version} boss_name={boss_name} boss_role={boss_role}"
                )
            if len(boss_groups) != 3:
                raise ValueError(
                    "Blue champion must have exactly three Kaggle team variants: "
                    f"game_version={game_version} boss_id={boss_id} variant_count={len(boss_groups)}"
                )
            seen_variants: set[str] = set()
            for group in boss_groups:
                metadata = _blue_team_variant_metadata(group)
                team_variant = str(metadata["team_variant"])
                if team_variant in seen_variants:
                    raise ValueError(
                        "Duplicate Blue champion team variant detected: "
                        f"game_version={game_version} team_variant={team_variant}"
                    )
                seen_variants.add(team_variant)
                metadata_by_group[str(group["kaggle_boss_key"])] = metadata
            continue

        if len(boss_groups) > 1:
            raise ValueError(
                "Multiple Kaggle teams mapped to a non-variant canonical boss: "
                f"boss_id={boss_id} game_version={game_version} boss_name={boss_name} group_count={len(boss_groups)}"
            )

        group = boss_groups[0]
        metadata_by_group[str(group["kaggle_boss_key"])] = {
            "team_variant": "default",
            "starter_type": _normalized_starter_type(boss_row.get("starter_type")),
            "variant_dimension": None,
        }
    return metadata_by_group


def harmonize_boss_references(
    *,
    kaggle_rows: pd.DataFrame,
) -> BossHarmonizationResult:
    logger.info("[silver/bosses] loading progression data ...")
    progression_df = _progression_rows()
    progression_count = len(progression_df)

    by_role, by_name = _build_progression_indexes(progression_df)
    alias_lookup = _alias_lookup(progression_df)

    logger.info("[silver/bosses] harmonizing bosses ...")
    unmatched_kaggle: list[dict[str, Any]] = []
    matched_groups: list[dict[str, Any]] = []
    manual_review_rows: list[dict[str, Any]] = []
    boss_rows_by_id = {str(row["boss_id"]): dict(row) for row in progression_df.to_dict(orient="records")}

    for team_group in _group_kaggle_teams(kaggle_rows):
        game_version = str(team_group["game_version"])
        normalized_boss_name = str(team_group["normalized_boss_name"])
        kaggle_role = str(team_group["kaggle_boss_role"])

        matched_row: dict[str, Any] | None = _single_candidate(by_role.get((game_version, normalized_boss_name, kaggle_role), []))
        match_type = "exact_name_role"
        if matched_row is None:
            matched_row = _single_candidate(by_name.get((game_version, normalized_boss_name), []))
            match_type = "exact_name"
        if matched_row is None:
            alias_target = alias_lookup.get((game_version, normalized_boss_name))
            if alias_target:
                matched_row = _single_candidate(by_name.get((game_version, alias_target), []))
                match_type = "alias_map"
        if matched_row is None:
            alias_target = KAGGLE_SPECIAL_CASES.get((game_version, normalized_boss_name))
            if alias_target:
                matched_row = _single_candidate(by_name.get((game_version, alias_target), []))
                match_type = "special_case"

        if matched_row is None:
            suggestions = _diagnostic_suggestions(
                game_version=game_version,
                normalized_name=normalized_boss_name,
                progression_df=progression_df,
            )
            unmatched_kaggle.append(
                {
                    "game_version": game_version,
                    "kaggle_boss_key": team_group["kaggle_boss_key"],
                    "boss_name": team_group["boss_name_raw"],
                    "gym_or_stage": team_group["gym_or_stage"],
                    "boss_role": kaggle_role,
                    "suggestions": suggestions,
                }
            )
            review_row = _fallback_manual_review_boss(team_group)
            boss_rows_by_id[str(review_row["boss_id"])] = review_row
            manual_review_rows.append(review_row)
            matched_groups.append(
                {
                    **team_group,
                    "boss_row": review_row,
                    "match_type": "manual_review",
                }
            )
            continue

        boss_id = str(matched_row["boss_id"])
        canonical_row = dict(boss_rows_by_id[boss_id])
        canonical_row["source_boss_name_kaggle"] = canonical_row.get("source_boss_name_kaggle") or team_group["boss_name_raw"]
        canonical_row["boss_name_kaggle"] = canonical_row.get("boss_name_kaggle") or team_group["boss_name_raw"]
        canonical_row["kaggle_boss_key"] = canonical_row.get("kaggle_boss_key") or team_group["kaggle_boss_key"]
        canonical_row["harmonization_status"] = match_type
        canonical_row["harmonization_notes"] = f"Matched Kaggle boss team by {match_type}."
        canonical_row["is_simulatable"] = True
        aliases = {
            *list(canonical_row.get("boss_name_aliases") or []),
            canonical_row["boss_name"],
            team_group["boss_name_raw"],
        }
        canonical_row["boss_name_aliases"] = sorted(alias for alias in aliases if alias)
        boss_rows_by_id[boss_id] = canonical_row
        matched_groups.append(
            {
                **team_group,
                "boss_row": canonical_row,
                "match_type": match_type,
            }
        )

    variant_metadata_by_group = _assign_team_variant_metadata(matched_groups)
    boss_team_rows: list[dict[str, Any]] = []
    for team_group in matched_groups:
        boss_row = dict(team_group["boss_row"])
        kaggle_boss_key = str(team_group["kaggle_boss_key"])
        variant_metadata = dict(variant_metadata_by_group[kaggle_boss_key])
        team_variant = str(variant_metadata["team_variant"])
        starter_type = _normalized_starter_type(variant_metadata.get("starter_type"))
        variant_dimension = _coerce_nullable_text(variant_metadata.get("variant_dimension"))
        boss_team_id = _boss_team_identifier(
            boss_id=str(boss_row["boss_id"]),
            kaggle_boss_key=kaggle_boss_key,
            team_variant=team_variant,
        )
        ordered_rows = sorted(team_group["rows"], key=lambda row: str(row["source_record_id"]))
        for pokemon_slot, row in enumerate(ordered_rows, start=1):
            boss_team_rows.append(
                {
                    "boss_team_id": boss_team_id,
                    "boss_id": str(boss_row["boss_id"]),
                    "game_version": str(row["game_version"]),
                    "generation": int(row["generation"]),
                    "boss_name": str(boss_row["boss_name"]),
                    "boss_role": str(boss_row["boss_role"]),
                    "battle_type": str(boss_row["battle_type"]),
                    "progression_order": boss_row.get("progression_order"),
                    "progression_depth": boss_row.get("progression_depth"),
                    "branch_condition": boss_row.get("branch_condition"),
                    "branch_group": boss_row.get("branch_group"),
                    "is_optional": boss_row.get("is_optional"),
                    "is_postgame": boss_row.get("is_postgame"),
                    "team_variant": team_variant,
                    "starter_type": starter_type,
                    "variant_dimension": variant_dimension,
                    "pokemon_slot": pokemon_slot,
                    "pokemon_species": row["pokemon_species"],
                    "level": int(row["level"]) if not pd.isna(row["level"]) else None,
                    "move_1": row["move_1"],
                    "move_2": row["move_2"],
                    "move_3": row["move_3"],
                    "move_4": row["move_4"],
                    "item": None,
                    "ability": None,
                    "source_dataset": str(row["source_dataset"]),
                    "source_record_id": str(row["source_record_id"]),
                    "harmonization_status": str(team_group["match_type"]),
                    "gym_or_stage": row["gym_or_stage"],
                    "source_boss_name_kaggle": row["boss_name"],
                }
            )

    bosses_df = pd.DataFrame(list(boss_rows_by_id.values())).sort_values(
        ["game_version", "progression_order", "boss_name", "boss_id"],
        na_position="last",
    ).reset_index(drop=True)
    boss_teams_df = pd.DataFrame(boss_team_rows).sort_values(
        ["game_version", "boss_id", "boss_team_id", "pokemon_slot", "source_record_id"]
    ).reset_index(drop=True)
    _validate_starter_dependency_rules(bosses_df, boss_teams_df)
    progression_edges_df = _build_progression_edges(bosses_df)

    duplicate_boss_ids = bosses_df[bosses_df["boss_id"].duplicated(keep=False)]["boss_id"].drop_duplicates().tolist()
    duplicate_boss_team_rows = []
    if not boss_teams_df.empty:
        duplicates = boss_teams_df[boss_teams_df.duplicated(subset=["boss_team_id", "pokemon_slot"], keep=False)]
        duplicate_boss_team_rows = duplicates[
            ["boss_team_id", "pokemon_slot", "pokemon_species", "level"]
        ].to_dict(orient="records")

    progression_only = bosses_df[
        bosses_df["bulbapedia_progression_key"].notna()
        & ~bosses_df["boss_id"].isin(boss_teams_df["boss_id"].dropna().tolist())
    ].copy()
    unmatched_progression = progression_only[
        ["game_version", "boss_name", "boss_role", "progression_order", "branch_condition", "is_postgame"]
    ].to_dict(orient="records")

    diagnostics = {
        "total_bulbapedia_progression_nodes": progression_count,
        "matched_count": len(matched_groups) - len(unmatched_kaggle),
        "unmatched_kaggle_bosses": unmatched_kaggle,
        "unmatched_bulbapedia_nodes": unmatched_progression,
        "duplicate_boss_ids": duplicate_boss_ids,
        "duplicate_boss_team_rows": duplicate_boss_team_rows,
        "progression_cycles": _find_progression_cycles(progression_edges_df),
        "missing_progression_order": bosses_df[bosses_df["progression_order"].isna()]["boss_id"].tolist(),
        "missing_boss_teams_for_progression_bosses": progression_only["boss_id"].tolist(),
        "kaggle_teams_without_progression_mapping": [row["kaggle_boss_key"] for row in unmatched_kaggle],
        "special_cases_detected": _special_cases_detected(bosses_df, boss_teams_df),
    }
    logger.info(
        "[silver/bosses] matched=%s unmatched_kaggle=%s unmatched_progression=%s",
        diagnostics["matched_count"],
        len(unmatched_kaggle),
        len(unmatched_progression),
    )
    return BossHarmonizationResult(
        kaggle_rows=kaggle_rows,
        bosses=bosses_df,
        boss_teams=boss_teams_df,
        progression_edges=progression_edges_df,
        diagnostics=diagnostics,
    )


def _build_progression_edges(bosses_df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    progression_rows = bosses_df[bosses_df["progression_order"].notna() & bosses_df["bulbapedia_progression_key"].notna()].copy()
    progression_rows["progression_order"] = progression_rows["progression_order"].astype(int)
    for game_version, group in progression_rows.groupby("game_version", sort=True):
        ordered_orders = sorted(group["progression_order"].drop_duplicates().tolist())
        for index in range(1, len(ordered_orders)):
            previous_order = ordered_orders[index - 1]
            current_order = ordered_orders[index]
            previous_rows = group[group["progression_order"].eq(previous_order)].to_dict(orient="records")
            current_rows = group[group["progression_order"].eq(current_order)].to_dict(orient="records")
            edge_type = _edge_type(previous_rows, current_rows)
            notes = _edge_notes(previous_rows, current_rows)
            for previous_row in previous_rows:
                for current_row in current_rows:
                    rows.append(
                        {
                            "game_version": game_version,
                            "from_boss_id": previous_row["boss_id"],
                            "to_boss_id": current_row["boss_id"],
                            "edge_type": edge_type,
                            "progression_order_from": previous_order,
                            "progression_order_to": current_order,
                            "source_dataset": PROGRESSION_SOURCE_DATASET,
                            "notes": notes,
                        }
                    )
    return pd.DataFrame(rows).sort_values(
        ["game_version", "progression_order_from", "progression_order_to", "from_boss_id", "to_boss_id"]
    ).reset_index(drop=True)


def _edge_type(previous_rows: list[dict[str, Any]], current_rows: list[dict[str, Any]]) -> str:
    if len(current_rows) > 1:
        return "branch"
    if len(previous_rows) > 1:
        current_role = {str(row.get("boss_role") or "") for row in current_rows}
        if current_role & {"champion", "story_boss"}:
            return "requires_all_previous"
        return "branch_converge"
    return "linear"


def _edge_notes(previous_rows: list[dict[str, Any]], current_rows: list[dict[str, Any]]) -> str | None:
    current_branch_groups = {str(row.get("branch_group") or "") for row in current_rows if row.get("branch_group")}
    previous_branch_groups = {str(row.get("branch_group") or "") for row in previous_rows if row.get("branch_group")}
    if current_branch_groups == {"striaton_gym"}:
        return "Starter-dependent Striaton Gym branch."
    if current_branch_groups in ({"elite_four_unova"}, {"elite_four_kalos"}):
        return "Elite Four order is not strictly linear in this game."
    if previous_branch_groups in ({"elite_four_unova"}, {"elite_four_kalos"}):
        return "All prior Elite Four battles are required before the next story boss."
    return None


def _find_progression_cycles(edges_df: pd.DataFrame) -> list[list[str]]:
    adjacency: dict[str, set[str]] = {}
    for row in edges_df.to_dict(orient="records"):
        adjacency.setdefault(str(row["from_boss_id"]), set()).add(str(row["to_boss_id"]))
        adjacency.setdefault(str(row["to_boss_id"]), set())

    cycles: list[list[str]] = []
    visited: set[str] = set()
    visiting: set[str] = set()

    def visit(node: str, stack: list[str]) -> None:
        if node in visiting:
            cycle_start = stack.index(node) if node in stack else 0
            cycles.append(stack[cycle_start:] + [node])
            return
        if node in visited:
            return
        visiting.add(node)
        for child in sorted(adjacency.get(node, set())):
            visit(child, stack + [node])
        visiting.remove(node)
        visited.add(node)

    for node in sorted(adjacency):
        visit(node, [])
    return cycles


def _validate_starter_dependency_rules(bosses_df: pd.DataFrame, boss_teams_df: pd.DataFrame) -> None:
    non_none = bosses_df[bosses_df["starter_dependency_type"].fillna("none").ne("none")].copy()
    allowed_dependency_rows = {
        ("black", "Chili", "gym", "branching"),
        ("black", "Cilan", "gym", "branching"),
        ("black", "Cress", "gym", "branching"),
        ("white", "Chili", "gym", "branching"),
        ("white", "Cilan", "gym", "branching"),
        ("white", "Cress", "gym", "branching"),
        ("red", "Blue", "champion", "team_variant"),
        ("blue", "Blue", "champion", "team_variant"),
    }
    observed_dependency_rows = {
        (
            str(row.get("game_version") or ""),
            str(row.get("boss_name") or ""),
            str(row.get("boss_role") or ""),
            str(row.get("starter_dependency_type") or "none"),
        )
        for row in non_none.to_dict(orient="records")
    }
    unexpected_dependency_rows = sorted(observed_dependency_rows - allowed_dependency_rows)
    if unexpected_dependency_rows:
        raise ValueError(
            "Unexpected starter-dependent bosses detected outside the approved special cases: "
            f"rows={unexpected_dependency_rows}"
        )

    expected_striaton_types = {"Chili": "grass", "Cilan": "water", "Cress": "fire"}
    for game_version in ("black", "white"):
        striaton_team_keys = boss_teams_df[
            boss_teams_df["game_version"].eq(game_version)
            & boss_teams_df["branch_group"].eq("striaton_gym")
        ][["boss_name", "boss_team_id", "starter_type"]].drop_duplicates()
        if striaton_team_keys.empty:
            continue

        striaton_bosses = bosses_df[
            bosses_df["game_version"].eq(game_version)
            & bosses_df["branch_group"].eq("striaton_gym")
        ].copy()
        if set(striaton_bosses["boss_name"].tolist()) != set(expected_striaton_types) or len(striaton_bosses) != 3:
            raise ValueError(
                "Striaton Gym must have exactly three branching bosses per game: "
                f"game_version={game_version} bosses={sorted(striaton_bosses['boss_name'].tolist())}"
            )
        if (
            striaton_bosses["starter_dependency_type"].fillna("none").ne("branching").any()
            or striaton_bosses["branch_condition"].fillna("").ne("starter_type").any()
            or striaton_bosses["is_branching"].fillna(False).ne(True).any()
        ):
            raise ValueError(f"Invalid Striaton branching metadata for game_version={game_version}")
        if len(striaton_team_keys) != 3:
            raise ValueError(
                "Striaton Gym must resolve to exactly three boss teams per game: "
                f"game_version={game_version} team_count={len(striaton_team_keys)}"
            )
        for boss_name, expected_type in expected_striaton_types.items():
            starter_types = sorted(
                {
                    _normalized_starter_type(value)
                    for value in striaton_team_keys[striaton_team_keys["boss_name"].eq(boss_name)]["starter_type"].tolist()
                }
            )
            if starter_types != [expected_type]:
                raise ValueError(
                    "Striaton boss did not map to exactly one canonical starter type: "
                    f"game_version={game_version} boss_name={boss_name} starter_types={starter_types}"
                )

    expected_blue_variants = {"charizard_variant", "blastoise_variant", "venusaur_variant"}
    final_starter_species = set(BLUE_VARIANT_BY_SPECIES)
    for game_version in ("red", "blue"):
        blue_team_keys = boss_teams_df[
            boss_teams_df["game_version"].eq(game_version)
            & boss_teams_df["boss_name"].eq("Blue")
            & boss_teams_df["boss_role"].eq("champion")
        ][["boss_team_id", "team_variant", "starter_type", "variant_dimension"]].drop_duplicates()
        if blue_team_keys.empty:
            continue

        blue_rows = bosses_df[
            bosses_df["game_version"].eq(game_version)
            & bosses_df["boss_name"].eq("Blue")
            & bosses_df["boss_role"].eq("champion")
        ].copy()
        if len(blue_rows) != 1:
            raise ValueError(
                "Blue champion must remain a single canonical boss row per game: "
                f"game_version={game_version} row_count={len(blue_rows)}"
            )
        blue_row = blue_rows.iloc[0]
        if (
            str(blue_row.get("starter_dependency_type") or "none") != "team_variant"
            or bool(blue_row.get("has_team_variants", False)) is not True
            or bool(blue_row.get("is_branching", False)) is True
        ):
            raise ValueError(f"Invalid Blue champion team-variant metadata for game_version={game_version}")
        if len(blue_team_keys) != 3:
            raise ValueError(
                "Blue champion must have exactly three team variants per game: "
                f"game_version={game_version} team_count={len(blue_team_keys)}"
            )
        if set(blue_team_keys["team_variant"].tolist()) != expected_blue_variants:
            raise ValueError(
                "Blue champion variants did not match the expected starter-dependent teams: "
                f"game_version={game_version} variants={sorted(blue_team_keys['team_variant'].tolist())}"
            )
        if set(blue_team_keys["variant_dimension"].dropna().tolist()) != {"starter_type"}:
            raise ValueError(
                "Blue champion variants must be keyed by starter_type: "
                f"game_version={game_version} dimensions={sorted(blue_team_keys['variant_dimension'].dropna().tolist())}"
            )

        for team_key in blue_team_keys.to_dict(orient="records"):
            team_rows = boss_teams_df[boss_teams_df["boss_team_id"].eq(team_key["boss_team_id"])]
            species = {
                str(value).strip().lower()
                for value in team_rows["pokemon_species"].tolist()
                if str(value).strip()
            }
            matched_species = sorted(species & final_starter_species)
            if len(matched_species) != 1:
                raise ValueError(
                    "Blue team variant must contain exactly one final starter species: "
                    f"game_version={game_version} boss_team_id={team_key['boss_team_id']} matched_species={matched_species}"
                )


def _special_cases_detected(bosses_df: pd.DataFrame, boss_teams_df: pd.DataFrame) -> list[str]:
    detected: list[str] = []
    striaton = bosses_df[bosses_df["branch_group"].eq("striaton_gym")]
    if not striaton.empty:
        detected.append("striaton_branching")
    for branch_group in ("elite_four_unova", "elite_four_kalos"):
        if bosses_df["branch_group"].eq(branch_group).any():
            detected.append(branch_group)
    bw_postgame_champion = bosses_df[
        bosses_df["game_version"].isin(["black", "white"])
        & bosses_df["boss_name"].eq("Alder")
    ]
    if len(bw_postgame_champion) >= 1:
        detected.append("bw_final_story_boss_semantics")
    blue_variants = boss_teams_df[
        boss_teams_df["boss_name"].eq("Blue")
        & boss_teams_df["boss_role"].eq("champion")
        & boss_teams_df["team_variant"].ne("default")
    ]
    if not blue_variants.empty:
        detected.append("blue_team_variants")
    return detected


def build_and_write_boss_references(
    *,
    bronze_dir: Path,
    references_dir: Path,
    diagnostics_dir: Path,
) -> BossHarmonizationResult:
    kaggle_path = bronze_dir / "kagglehub" / "gym_leaders_elite_four.csv"
    kaggle_rows, loader_diagnostics = load_kaggle_boss_rows(kaggle_path)
    result = harmonize_boss_references(kaggle_rows=kaggle_rows)
    diagnostics = {**loader_diagnostics, **result.diagnostics}
    merged = BossHarmonizationResult(
        kaggle_rows=result.kaggle_rows,
        bosses=result.bosses,
        boss_teams=result.boss_teams,
        progression_edges=result.progression_edges,
        diagnostics=diagnostics,
    )

    write_parquet(references_dir / "bosses.parquet", merged.bosses)
    logger.info("[silver/bosses] wrote bosses rows=%s", len(merged.bosses))
    write_parquet(references_dir / "boss_teams.parquet", merged.boss_teams)
    logger.info("[silver/bosses] wrote boss_teams rows=%s", len(merged.boss_teams))
    write_parquet(references_dir / "progression_edges.parquet", merged.progression_edges)
    logger.info("[silver/bosses] wrote progression_edges rows=%s", len(merged.progression_edges))
    write_json(diagnostics_dir / "boss_harmonization_report.json", merged.diagnostics)
    logger.info("[silver/bosses] wrote diagnostics ...")
    return merged


def build_boss_team_payloads(boss_teams_df: pd.DataFrame) -> list[dict[str, Any]]:
    teams: list[dict[str, Any]] = []
    if boss_teams_df.empty:
        return teams

    ordered = boss_teams_df.sort_values(["boss_team_id", "pokemon_slot", "source_record_id"]).copy()
    for boss_team_id, group in ordered.groupby("boss_team_id", sort=True):
        first = group.iloc[0]
        levels = [
            int(level) if not pd.isna(level) else None
            for level in group["level"].tolist()
        ]
        avg_level = int(round(sum(level for level in levels if level is not None) / max(1, len([level for level in levels if level is not None]))))
        teams.append(
            {
                "team_id": str(boss_team_id),
                "boss_team_id": str(boss_team_id),
                "boss_id": str(first["boss_id"]),
                "boss_name": str(first["boss_name"]),
                "team_role": "boss",
                "boss_role": str(first["boss_role"]),
                "battle_type": str(first["battle_type"]),
                "game_version": str(first["game_version"]),
                "gym": first.get("gym_or_stage"),
                "gym_index": first.get("progression_order"),
                "starter_condition": first.get("starter_type"),
                "starter_type": first.get("starter_type"),
                "progression_depth": first.get("progression_depth"),
                "pokemon": group["pokemon_species"].tolist(),
                "levels": [int(level) if level is not None else avg_level for level in levels],
                "moves": [
                    [move for move in [row.move_1, row.move_2, row.move_3, row.move_4] if move]
                    for row in group.itertuples(index=False)
                ],
                "pokemon_instance_ids": [],
                "avg_level": avg_level,
                "team_variant": str(first["team_variant"]),
                "variant_dimension": first.get("variant_dimension"),
                "is_optional": bool(first.get("is_optional", False)),
                "is_postgame": bool(first.get("is_postgame", False)),
            }
        )
    return teams


def build_boss_team_members_reference_rows(boss_teams_df: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if boss_teams_df.empty:
        return rows

    for row in boss_teams_df.sort_values(["boss_team_id", "pokemon_slot"]).to_dict(orient="records"):
        # Use the variant-specific team identifier here. Canonical boss IDs can
        # legitimately map to multiple teams (for example Blue's starter-based
        # champion variants), and collapsing rows back to the canonical boss ID
        # breaks both coverage validation and per-member move counts.
        boss_team_id = str(row.get("boss_team_id") or row.get("boss_id") or "").strip().lower()
        canonical_boss_id = str(row.get("boss_id") or "").strip().lower() or None
        moves = [row.get("move_1"), row.get("move_2"), row.get("move_3"), row.get("move_4")]
        normalized_moves = [move for move in moves if move]
        if not normalized_moves:
            rows.append(
                {
                    "boss_id": boss_team_id,
                    "canonical_boss_id": canonical_boss_id,
                    "game_version": row["game_version"],
                    "boss_role": row["boss_role"],
                    "boss_name": normalize_key_part(row["boss_name"]),
                    "starter_condition": row.get("starter_type"),
                    "starter_type": row.get("starter_type"),
                    "gym_index": row.get("progression_order"),
                    "slot": row["pokemon_slot"],
                    "pokemon_species": row["pokemon_species"],
                    "level": row["level"],
                    "move_name": None,
                    "move_slot": None,
                    "source": "kaggle",
                }
            )
            continue
        for move_slot, move_name in enumerate(normalized_moves, start=1):
            rows.append(
                {
                    "boss_id": boss_team_id,
                    "canonical_boss_id": canonical_boss_id,
                    "game_version": row["game_version"],
                    "boss_role": row["boss_role"],
                    "boss_name": normalize_key_part(row["boss_name"]),
                    "starter_condition": row.get("starter_type"),
                    "starter_type": row.get("starter_type"),
                    "gym_index": row.get("progression_order"),
                    "slot": row["pokemon_slot"],
                    "pokemon_species": row["pokemon_species"],
                    "level": row["level"],
                    "move_name": move_name,
                    "move_slot": move_slot,
                    "source": "kaggle",
                }
            )
    return rows
