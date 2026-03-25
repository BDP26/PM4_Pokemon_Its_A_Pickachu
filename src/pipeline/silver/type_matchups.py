"""Type matchup analysis for battle simulation."""
import json
from pathlib import Path
from typing import Optional

import pokebase as pb
import pandas as pd

from src.pipeline.common.io import read_json, write_json, write_jsonl
from src.pipeline.settings import BRONZE_DIR, SILVER_DIR


def get_pokemon_types(pokemon_id: str) -> list[str]:
    """Get types for a Pokemon."""
    try:
        poke = pb.pokemon(pokemon_id)
        return [t.type.name.title() for t in poke.types]
    except Exception:
        return ["Normal"]


def load_type_chart(bronze_dir: Path = BRONZE_DIR) -> dict[str, dict[str, float]]:
    """Load type effectiveness chart from Bronze layer."""
    type_chart_path = bronze_dir / "type_chart.json"
    
    if type_chart_path.exists():
        return read_json(type_chart_path)
    
    # Fallback: minimal chart
    return _create_minimal_type_chart()


def _create_minimal_type_chart() -> dict[str, dict[str, float]]:
    """Create a minimal type chart if none exists."""
    types = ["Normal", "Fighting", "Flying", "Poison", "Ground", "Rock",
             "Bug", "Ghost", "Steel", "Fire", "Water", "Grass",
             "Electric", "Psychic", "Ice", "Dragon", "Dark", "Fairy"]
    
    return {
        attacking: {defending: 1.0 for defending in types}
        for attacking in types
    }


def calculate_type_advantage(
    attacker_types: list[str],
    defender_types: list[str],
    type_chart: dict[str, dict[str, float]]
) -> float:
    """
    Calculate type advantage multiplier.
    
    Result > 1.0 means advantage, < 1.0 means disadvantage.
    """
    multiplier = 1.0
    
    for atk_type in attacker_types:
        for def_type in defender_types:
            atk_type_norm = atk_type.title()
            def_type_norm = def_type.title()
            
            if atk_type_norm in type_chart and def_type_norm in type_chart[atk_type_norm]:
                multiplier *= type_chart[atk_type_norm][def_type_norm]
    
    return multiplier


def build_type_matchups(
    teams_data: list[dict],
    silver_dir: Path = SILVER_DIR,
    bronze_dir: Path = BRONZE_DIR
) -> None:
    """
    Build type matchup matrix between all teams.
    
    Stores: team_id_attacker, team_id_defender, matchup_score
    """
    type_chart = load_type_chart(bronze_dir)
    
    # Build pokemon type map
    pokemon_types = {}
    
    matchups = []
    
    for attacker in teams_data:
        attacker_id = attacker.get("team_id")
        attacker_pokemon = attacker.get("pokemon", [])
        
        for defender in teams_data:
            defender_id = defender.get("team_id")
            defender_pokemon = defender.get("pokemon", [])
            
            if attacker_id == defender_id:
                continue
            
            # Calculate average type advantage
            advantage_sum = 0
            count = 0
            
            for atk_poke in attacker_pokemon[:6]:
                atk_types = pokemon_types.get(atk_poke)
                if not atk_types:
                    atk_types = get_pokemon_types(atk_poke)
                    pokemon_types[atk_poke] = atk_types
                
                for def_poke in defender_pokemon[:6]:
                    def_types = pokemon_types.get(def_poke)
                    if not def_types:
                        def_types = get_pokemon_types(def_poke)
                        pokemon_types[def_poke] = def_types
                    
                    advantage = calculate_type_advantage(
                        atk_types, def_types, type_chart
                    )
                    advantage_sum += advantage
                    count += 1
            
            avg_advantage = advantage_sum / max(count, 1)
            
            # Normalize to 0-1 scale
            matchup_score = min(1.0, max(0.0, (avg_advantage - 0.5) / 1.5 + 0.5))
            
            matchups.append({
                "team_id_attacker": attacker_id,
                "team_id_defender": defender_id,
                "matchup_score": round(matchup_score, 3),
                "avg_type_advantage": round(avg_advantage, 2),
            })
    
    write_jsonl(silver_dir / "type_matchups.jsonl", matchups)
    
    print(f"[type_matchups] computed {len(matchups)} matchups")

