from __future__ import annotations

from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
SILVER_SIM_DIR = ROOT / "data" / "silver" / "simulation"
POKEMON_REF_PATH = ROOT / "data" / "silver" / "references" / "pokemon_data.parquet"


def load_type_reference() -> pd.DataFrame:
    pokemon = pd.read_parquet(POKEMON_REF_PATH)[["pokemon_species", "type_1", "type_2"]]
    pokemon = pokemon.drop_duplicates(subset=["pokemon_species"]).copy()
    pokemon["pokemon_species"] = pokemon["pokemon_species"].astype(str).str.lower().str.strip()
    return pokemon


def build_team_type_summary(pokemon_ref: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    team_rows: list[pd.DataFrame] = []
    member_rows: list[pd.DataFrame] = []

    for path in sorted(SILVER_SIM_DIR.glob("source_teams_*.parquet")):
        game_version = path.stem.replace("source_teams_", "")
        teams = pd.read_parquet(path)
        teams["game_version"] = game_version
        team_rows.append(teams)

    for path in sorted(SILVER_SIM_DIR.glob("source_team_members_*.parquet")):
        game_version = path.stem.replace("source_team_members_", "")
        members = pd.read_parquet(path)
        members["game_version"] = game_version
        members["pokemon_species"] = members["pokemon_species"].astype(str).str.lower().str.strip()
        member_rows.append(members)

    team_rows = [df for df in team_rows if not df.empty]
    member_rows = [df for df in member_rows if not df.empty]
    all_teams = pd.concat(team_rows, ignore_index=True)
    all_members = pd.concat(member_rows, ignore_index=True)

    joined = all_members.merge(pokemon_ref, on="pokemon_species", how="left")

    type_sets = (
        joined.groupby(["game_version", "source_team_id"])
        .apply(
            lambda group: sorted(
                set(group["type_1"].dropna().tolist()) | set(group["type_2"].dropna().tolist())
            ),
            include_groups=False,
        )
        .reset_index(name="team_types")
    )
    type_sets["team_type_count"] = type_sets["team_types"].apply(len)

    summary = all_teams[["game_version", "source_team_id", "team_role", "boss_id", "boss_name"]].drop_duplicates()
    summary = summary.merge(type_sets, on=["game_version", "source_team_id"], how="left")

    return summary, joined


def print_global_summary(summary: pd.DataFrame, joined: pd.DataFrame) -> None:
    total_teams = len(summary)
    mono_teams = int((summary["team_type_count"] == 1).sum())

    missing_species = (
        joined.loc[joined["type_1"].isna(), "pokemon_species"].dropna().drop_duplicates().sort_values()
    )

    print("=== Source Team Typing Diversity (Silver) ===")
    print(f"Total source teams: {total_teams}")
    print(f"Monotype teams (all member typings collapsed): {mono_teams} ({mono_teams / total_teams:.2%})")
    print(f"Species without typing mapping: {len(missing_species)}")
    if not missing_species.empty:
        print("Missing species sample:", ", ".join(missing_species.head(20).tolist()))

    print("\nBy game version:")
    by_game = (
        summary.groupby("game_version")
        .agg(
            teams=("source_team_id", "nunique"),
            monotype_teams=("team_type_count", lambda s: int((s == 1).sum())),
        )
        .reset_index()
    )
    by_game["monotype_pct"] = (by_game["monotype_teams"] / by_game["teams"] * 100).round(1)
    print(by_game.to_string(index=False))


def print_volkner_check(summary: pd.DataFrame, joined: pd.DataFrame) -> None:
    volkner = summary[summary["boss_name"].astype(str).str.lower() == "volkner"].copy()
    print("\n=== Volkner Check (not fully water-based) ===")
    print(f"Volkner source teams found: {len(volkner)}")

    if volkner.empty:
        return

    details = joined.merge(
        volkner[["game_version", "source_team_id"]].drop_duplicates(),
        on=["game_version", "source_team_id"],
        how="inner",
    )

    rows: list[dict[str, object]] = []
    for (game_version, source_team_id), group in details.groupby(["game_version", "source_team_id"]):
        primary_types = group["type_1"].dropna().tolist()
        all_types = sorted(set(group["type_1"].dropna().tolist()) | set(group["type_2"].dropna().tolist()))
        rows.append(
            {
                "game_version": game_version,
                "source_team_id": source_team_id,
                "team_role": str(group["team_role"].iloc[0]),
                "boss_id": str(group["boss_id"].iloc[0]),
                "members": ", ".join(group["pokemon_species"].astype(str).tolist()),
                "all_primary_water": bool(primary_types) and all(t == "water" for t in primary_types),
                "all_types_only_water": all_types == ["water"],
                "all_types": all_types,
            }
        )

    result = pd.DataFrame(rows).sort_values(["game_version", "source_team_id"])
    print(result.to_string(index=False))

    water_only = result[result["all_types_only_water"] | result["all_primary_water"]]
    if water_only.empty:
        print("\nResult: No Volkner source team is fully Water-only.")
    else:
        print("\nResult: At least one Volkner source team is fully Water-only.")


def main() -> None:
    pokemon_ref = load_type_reference()
    summary, joined = build_team_type_summary(pokemon_ref)
    print_global_summary(summary, joined)
    print_volkner_check(summary, joined)


if __name__ == "__main__":
    main()


