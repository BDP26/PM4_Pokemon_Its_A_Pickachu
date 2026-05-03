from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.pipeline.silver.references.boss_harmonization import (
    build_boss_team_members_reference_rows,
    harmonize_boss_references,
    load_kaggle_boss_rows,
    make_canonical_boss_id,
)


def _write_kaggle_csv(path: Path, rows: list[dict[str, object]]) -> None:
    frame = pd.DataFrame(rows)
    frame.to_csv(path, sep=";", index=False)


def _minimal_row(
    *,
    generation: int,
    game: str,
    gym: str,
    leader: str,
    pokemon: str,
    level: int,
    move_1: str | None = None,
    move_2: str | None = None,
    move_3: str | None = None,
    move_4: str | None = None,
) -> dict[str, object]:
    return {
        "Generation": generation,
        "Game": game,
        "Gym": gym,
        "Gym leader": leader,
        "Pokemon": pokemon,
        "Level": level,
        "Move 1": move_1,
        "Move 2": move_2,
        "Move 3": move_3,
        "Move 4": move_4,
    }


def test_load_kaggle_boss_rows_parses_semicolon_and_filters_versions(tmp_path: Path) -> None:
    csv_path = tmp_path / "gym_leaders_elite_four.csv"
    _write_kaggle_csv(
        csv_path,
        [
            _minimal_row(generation=1, game="Red", gym="Pewter City", leader="Brock", pokemon="Geodude", level=12, move_1="Tackle"),
            _minimal_row(generation=1, game="Yellow", gym="Pewter City", leader="Brock", pokemon="Geodude", level=12, move_1="Tackle"),
            _minimal_row(generation=2, game="Crystal", gym="Violet City", leader="Falkner", pokemon="Pidgey", level=7, move_1="Gust"),
            _minimal_row(generation=6, game="X", gym="Shalour City", leader="Korrina", pokemon="Mienfoo", level=29, move_1="Fake Out"),
            _minimal_row(generation=7, game="Sun", gym="Iki Town", leader="Hala", pokemon="Makuhita", level=15, move_1="Arm Thrust"),
        ],
    )

    rows, diagnostics = load_kaggle_boss_rows(csv_path)

    assert rows["game_version"].tolist() == ["red", "x"]
    assert diagnostics["total_kaggle_rows_raw"] == 5
    assert diagnostics["total_kaggle_rows_after_generation_filter"] == 4
    assert diagnostics["total_kaggle_rows_after_base_version_filter"] == 2
    assert diagnostics["excluded_games"] == ["Crystal", "Sun", "Yellow"]


def test_missing_moves_are_preserved_as_null_and_slots_follow_source_order(tmp_path: Path) -> None:
    csv_path = tmp_path / "gym_leaders_elite_four.csv"
    _write_kaggle_csv(
        csv_path,
        [
            _minimal_row(generation=1, game="Red", gym="Pewter City", leader="Brock", pokemon="Geodude", level=12, move_1="Tackle"),
            _minimal_row(generation=1, game="Red", gym="Pewter City", leader="Brock", pokemon="Onix", level=14, move_1="Tackle", move_2="Screech"),
        ],
    )

    kaggle_rows, _ = load_kaggle_boss_rows(csv_path)
    result = harmonize_boss_references(kaggle_rows=kaggle_rows)
    team_rows = result.boss_teams.sort_values("pokemon_slot").reset_index(drop=True)

    assert team_rows["pokemon_slot"].tolist() == [1, 2]
    assert team_rows["pokemon_species"].tolist() == ["geodude", "onix"]
    assert team_rows.loc[0, "move_2"] in (None, "") or pd.isna(team_rows.loc[0, "move_2"])
    assert team_rows.loc[0, "move_3"] is None or pd.isna(team_rows.loc[0, "move_3"])
    assert team_rows.loc[0, "move_4"] is None or pd.isna(team_rows.loc[0, "move_4"])


def test_exact_name_matching_normalizes_casing(tmp_path: Path) -> None:
    csv_path = tmp_path / "gym_leaders_elite_four.csv"
    _write_kaggle_csv(
        csv_path,
        [
            _minimal_row(generation=1, game="Blue", gym="Cerulean City", leader="misty", pokemon="Staryu", level=18, move_1="Water Gun"),
            _minimal_row(generation=1, game="Blue", gym="Cerulean City", leader="misty", pokemon="Starmie", level=21, move_1="BubbleBeam"),
        ],
    )

    kaggle_rows, _ = load_kaggle_boss_rows(csv_path)
    result = harmonize_boss_references(kaggle_rows=kaggle_rows)
    boss = result.bosses[result.bosses["boss_name"].eq("Misty")].iloc[0]

    assert boss["harmonization_status"] == "exact_name_role"
    assert boss["source_boss_name_kaggle"] == "misty"


def test_alias_and_special_case_matching_maps_variant_champion_teams(tmp_path: Path) -> None:
    csv_path = tmp_path / "gym_leaders_elite_four.csv"
    _write_kaggle_csv(
        csv_path,
        [
            _minimal_row(generation=1, game="Red", gym="Elite Four", leader="Champion Blue Bulbasaur", pokemon="Pidgeot", level=61, move_1="Wing Attack"),
            _minimal_row(generation=1, game="Red", gym="Elite Four", leader="Champion Blue Bulbasaur", pokemon="Alakazam", level=59, move_1="Psychic"),
            _minimal_row(generation=1, game="Red", gym="Elite Four", leader="Champion Blue Bulbasaur", pokemon="Rhydon", level=61, move_1="Horn Drill"),
            _minimal_row(generation=1, game="Red", gym="Elite Four", leader="Champion Blue Bulbasaur", pokemon="Gyarados", level=63, move_1="Hydro Pump"),
            _minimal_row(generation=1, game="Red", gym="Elite Four", leader="Champion Blue Bulbasaur", pokemon="Exeggutor", level=61, move_1="Stomp"),
            _minimal_row(generation=1, game="Red", gym="Elite Four", leader="Champion Blue Bulbasaur", pokemon="Charizard", level=65, move_1="Fire Blast"),
            _minimal_row(generation=1, game="Red", gym="Elite Four", leader="Champion Blue charmander", pokemon="Pidgeot", level=61, move_1="Wing Attack"),
            _minimal_row(generation=1, game="Red", gym="Elite Four", leader="Champion Blue charmander", pokemon="Alakazam", level=59, move_1="Psychic"),
            _minimal_row(generation=1, game="Red", gym="Elite Four", leader="Champion Blue charmander", pokemon="Rhydon", level=61, move_1="Horn Drill"),
            _minimal_row(generation=1, game="Red", gym="Elite Four", leader="Champion Blue charmander", pokemon="Exeggutor", level=63, move_1="Stomp"),
            _minimal_row(generation=1, game="Red", gym="Elite Four", leader="Champion Blue charmander", pokemon="Arcanine", level=61, move_1="Ember"),
            _minimal_row(generation=1, game="Red", gym="Elite Four", leader="Champion Blue charmander", pokemon="Blastoise", level=65, move_1="Hydro Pump"),
            _minimal_row(generation=1, game="Red", gym="Elite Four", leader="Champion Blue Squirtle", pokemon="Pidgeot", level=61, move_1="Wing Attack"),
            _minimal_row(generation=1, game="Red", gym="Elite Four", leader="Champion Blue Squirtle", pokemon="Alakazam", level=59, move_1="Psychic"),
            _minimal_row(generation=1, game="Red", gym="Elite Four", leader="Champion Blue Squirtle", pokemon="Rhydon", level=61, move_1="Horn Drill"),
            _minimal_row(generation=1, game="Red", gym="Elite Four", leader="Champion Blue Squirtle", pokemon="Arcanine", level=63, move_1="Ember"),
            _minimal_row(generation=1, game="Red", gym="Elite Four", leader="Champion Blue Squirtle", pokemon="Gyarados", level=61, move_1="Hydro Pump"),
            _minimal_row(generation=1, game="Red", gym="Elite Four", leader="Champion Blue Squirtle", pokemon="Venusaur", level=65, move_1="Solar Beam"),
        ],
    )

    kaggle_rows, _ = load_kaggle_boss_rows(csv_path)
    result = harmonize_boss_references(kaggle_rows=kaggle_rows)
    boss = result.bosses[
        result.bosses["game_version"].eq("red") & result.bosses["boss_name"].eq("Blue")
    ].iloc[0]
    assert boss["harmonization_status"] == "special_case"
    assert boss["starter_dependency_type"] == "team_variant"
    assert bool(boss["has_team_variants"]) is True
    variants = result.boss_teams[["team_variant", "starter_type", "variant_dimension"]].drop_duplicates().sort_values("team_variant")
    assert variants.to_dict(orient="records") == [
        {"team_variant": "blastoise_variant", "starter_type": "fire", "variant_dimension": "starter_type"},
        {"team_variant": "charizard_variant", "starter_type": "grass", "variant_dimension": "starter_type"},
        {"team_variant": "venusaur_variant", "starter_type": "water", "variant_dimension": "starter_type"},
    ]


def test_unresolved_mapping_is_flagged_in_diagnostics(tmp_path: Path) -> None:
    csv_path = tmp_path / "gym_leaders_elite_four.csv"
    _write_kaggle_csv(
        csv_path,
        [
            _minimal_row(generation=1, game="Red", gym="Unknown Stage", leader="MissingNo Boss", pokemon="Rhydon", level=100, move_1="Tackle"),
        ],
    )

    kaggle_rows, _ = load_kaggle_boss_rows(csv_path)
    result = harmonize_boss_references(kaggle_rows=kaggle_rows)

    assert len(result.diagnostics["unmatched_kaggle_bosses"]) == 1
    assert result.diagnostics["kaggle_teams_without_progression_mapping"] == ["red:missingno-boss:unknown-stage"]
    assert result.boss_teams.iloc[0]["harmonization_status"] == "manual_review"
    manual_review = result.bosses[result.bosses["harmonization_status"].eq("manual_review")]
    assert len(manual_review) == 1


def test_black_white_striaton_branching_and_endgame_semantics(tmp_path: Path) -> None:
    csv_path = tmp_path / "gym_leaders_elite_four.csv"
    _write_kaggle_csv(
        csv_path,
        [
            _minimal_row(generation=5, game="Black", gym="Striaton City", leader="Chili", pokemon="Lillipup", level=12, move_1="Tackle"),
            _minimal_row(generation=5, game="Black", gym="Striaton City", leader="Cilan", pokemon="Pansage", level=14, move_1="Vine Whip"),
            _minimal_row(generation=5, game="Black", gym="Striaton City", leader="Cress", pokemon="Panpour", level=14, move_1="Water Gun"),
            _minimal_row(generation=5, game="Black", gym="Elite Four", leader="Champion Alder", pokemon="Accelgor", level=75, move_1="Bug Buzz"),
            _minimal_row(generation=5, game="Black", gym="Elite Four", leader="Champion Alder", pokemon="Volcarona", level=77, move_1="Heat Wave"),
        ],
    )

    kaggle_rows, _ = load_kaggle_boss_rows(csv_path)
    result = harmonize_boss_references(kaggle_rows=kaggle_rows)
    black_rows = result.bosses[result.bosses["game_version"].eq("black")]
    striaton = black_rows[black_rows["branch_group"].eq("striaton_gym")].sort_values("boss_name")
    striaton_teams = result.boss_teams[result.boss_teams["branch_group"].eq("striaton_gym")][
        ["boss_name", "starter_type"]
    ].drop_duplicates().sort_values("boss_name")

    assert striaton["boss_name"].tolist() == ["Chili", "Cilan", "Cress"]
    assert striaton["progression_order"].tolist() == [1, 1, 1]
    assert striaton["branch_condition"].tolist() == ["starter_type", "starter_type", "starter_type"]
    assert striaton["starter_dependency_type"].tolist() == ["branching", "branching", "branching"]
    assert striaton_teams.to_dict(orient="records") == [
        {"boss_name": "Chili", "starter_type": "grass"},
        {"boss_name": "Cilan", "starter_type": "water"},
        {"boss_name": "Cress", "starter_type": "fire"},
    ]

    alder_row = black_rows[black_rows["boss_name"].eq("Alder")].iloc[0]

    assert bool(alder_row["is_postgame"]) is False
    assert alder_row["boss_role"] == "champion"


def test_elite_four_order_independent_edges_are_explicit(tmp_path: Path) -> None:
    csv_path = tmp_path / "gym_leaders_elite_four.csv"
    _write_kaggle_csv(
        csv_path,
        [
            _minimal_row(generation=6, game="X", gym="Elite Four", leader="Malva", pokemon="Pyroar", level=63, move_1="Flamethrower"),
            _minimal_row(generation=6, game="X", gym="Elite Four", leader="Siebold", pokemon="Clawitzer", level=64, move_1="Water Pulse"),
            _minimal_row(generation=6, game="X", gym="Elite Four", leader="Wikstrom", pokemon="Klefki", level=63, move_1="Flash Cannon"),
            _minimal_row(generation=6, game="X", gym="Elite Four", leader="Drasna", pokemon="Dragalge", level=65, move_1="Dragon Pulse"),
            _minimal_row(generation=6, game="X", gym="Elite Four", leader="Champion Diantha", pokemon="Hawlucha", level=64, move_1="Flying Press"),
        ],
    )

    kaggle_rows, _ = load_kaggle_boss_rows(csv_path)
    result = harmonize_boss_references(kaggle_rows=kaggle_rows)
    x_rows = result.bosses[result.bosses["game_version"].eq("x")]
    elite_four = x_rows[x_rows["boss_role"].eq("elite_four")]
    edges = result.progression_edges[result.progression_edges["game_version"].eq("x")]

    assert elite_four["progression_order"].nunique() == 1
    champion_incoming = edges[edges["to_boss_id"].eq(x_rows[x_rows["boss_name"].eq("Diantha")].iloc[0]["boss_id"])]
    assert set(champion_incoming["edge_type"]) == {"requires_all_previous"}


def test_boss_id_generation_is_deterministic_and_distinguishes_repeated_encounters() -> None:
    first = make_canonical_boss_id(game_version="black", boss_name="Alder", progression_order=12, location_name="Pokemon League")
    second = make_canonical_boss_id(game_version="black", boss_name="Alder", progression_order=12, location_name="Pokemon League")
    repeated = make_canonical_boss_id(game_version="black", boss_name="Alder", progression_order=14, location_name="Driftveil City")

    assert first == second
    assert first != repeated


def test_allowed_project_kaggle_rows_are_not_silently_dropped() -> None:
    csv_path = Path("data/bronze/kagglehub/gym_leaders_elite_four.csv")
    kaggle_rows, diagnostics = load_kaggle_boss_rows(csv_path)
    result = harmonize_boss_references(kaggle_rows=kaggle_rows)

    assert diagnostics["total_kaggle_rows_after_base_version_filter"] == 684
    assert len(result.boss_teams) == 684
    assert result.diagnostics["unmatched_kaggle_bosses"] == []
    starter_dependent = result.bosses[result.bosses["starter_dependency_type"].ne("none")][
        ["game_version", "boss_name", "starter_dependency_type"]
    ].sort_values(["game_version", "boss_name"])
    assert starter_dependent.to_dict(orient="records") == [
        {"game_version": "black", "boss_name": "Chili", "starter_dependency_type": "branching"},
        {"game_version": "black", "boss_name": "Cilan", "starter_dependency_type": "branching"},
        {"game_version": "black", "boss_name": "Cress", "starter_dependency_type": "branching"},
        {"game_version": "blue", "boss_name": "Blue", "starter_dependency_type": "team_variant"},
        {"game_version": "red", "boss_name": "Blue", "starter_dependency_type": "team_variant"},
        {"game_version": "white", "boss_name": "Chili", "starter_dependency_type": "branching"},
        {"game_version": "white", "boss_name": "Cilan", "starter_dependency_type": "branching"},
        {"game_version": "white", "boss_name": "Cress", "starter_dependency_type": "branching"},
    ]
    assert sorted(result.boss_teams["game_version"].dropna().unique().tolist()) == [
        "black",
        "blue",
        "diamond",
        "gold",
        "pearl",
        "red",
        "ruby",
        "sapphire",
        "silver",
        "white",
        "x",
        "y",
    ]


def test_boss_team_member_reference_rows_preserve_variant_specific_team_ids() -> None:
    boss_teams_df = pd.DataFrame(
        [
            {
                "boss_id": "boss:red:blue:44d00b565814",
                "boss_team_id": "boss-team:fire",
                "boss_name": "Blue",
                "boss_role": "champion",
                "game_version": "red",
                "pokemon_slot": 1,
                "pokemon_species": "pidgeot",
                "level": 61,
                "move_1": "wing-attack",
                "move_2": "mirror-move",
                "move_3": "sky-attack",
                "move_4": "whirlwind",
                "starter_type": "fire",
                "progression_order": 10,
            },
            {
                "boss_id": "boss:red:blue:44d00b565814",
                "boss_team_id": "boss-team:water",
                "boss_name": "Blue",
                "boss_role": "champion",
                "game_version": "red",
                "pokemon_slot": 1,
                "pokemon_species": "pidgeot",
                "level": 61,
                "move_1": "wing-attack",
                "move_2": "mirror-move",
                "move_3": "sky-attack",
                "move_4": "whirlwind",
                "starter_type": "water",
                "progression_order": 10,
            },
        ]
    )

    rows = build_boss_team_members_reference_rows(boss_teams_df)

    assert {row["boss_id"] for row in rows} == {"boss-team:fire", "boss-team:water"}
    assert {row["canonical_boss_id"] for row in rows} == {"boss:red:blue:44d00b565814"}
