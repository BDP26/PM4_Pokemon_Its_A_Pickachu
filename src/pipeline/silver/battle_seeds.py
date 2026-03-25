"""Prepare battle simulation seeds from silver layer data."""
from pathlib import Path

import pandas as pd

from src.pipeline.common.io import read_jsonl, write_jsonl
from src.pipeline.settings import SILVER_DIR


def build_battle_seeds(silver_dir: Path = SILVER_DIR) -> None:
    """
    Create pre-computed battle scenarios for quick simulation lookups.
    
    Seeds map: player_team_id -> boss_team_id -> matchup_probability
    """
    
    # Read teams and matchups
    teams_file = silver_dir / "teams.jsonl"
    matchups_file = silver_dir / "type_matchups.jsonl"
    
    if not teams_file.exists():
        print("[battle_seeds] no teams found, skipping")
        return
    
    teams = read_jsonl(teams_file)
    teams_df = pd.DataFrame(teams)
    
    # Identify boss teams
    boss_teams = teams_df[teams_df["boss_name"].notna()].to_dict(orient="records")
    
    if matchups_file.exists():
        matchups = read_jsonl(matchups_file)
        matchups_df = pd.DataFrame(matchups)
    else:
        matchups_df = pd.DataFrame()
    
    # Create battle scenarios
    scenarios = []
    
    for boss_team in boss_teams:
        boss_id = boss_team.get("team_id")
        boss_name = boss_team.get("boss_name")
        game_version = boss_team.get("game_version")
        boss_level = boss_team.get("avg_level", 20)
        
        # Find matchups for this boss
        if not matchups_df.empty:
            boss_matchups = matchups_df[
                matchups_df["team_id_defender"] == boss_id
            ]
        else:
            boss_matchups = pd.DataFrame()
        
        for _, player_match in boss_matchups.iterrows():
            player_id = player_match.get("team_id_attacker")
            matchup_score = player_match.get("matchup_score", 0.5)
            
            scenarios.append({
                "scenario_id": f"{player_id}_vs_{boss_id}",
                "player_team_id": player_id,
                "boss_team_id": boss_id,
                "boss_name": boss_name,
                "game_version": game_version,
                "boss_level": boss_level,
                "predicted_player_win_chance": round(matchup_score, 3),
                "type_advantage": "positive" if matchup_score > 0.5 else "negative"
            })
    
    write_jsonl(silver_dir / "battle_seeds.jsonl", scenarios)
    
    print(f"[battle_seeds] created {len(scenarios)} battle scenarios")


