#!/usr/bin/env python3
"""Quick test to verify all simulation consistency changes are in place."""

import sys
from pathlib import Path

# Check battle_seeds.py filtering
print("[CHECK] battle_seeds.py filtering...")
battle_seeds_path = Path("src/pipeline/gold/simulation/battle_seeds.py")
battle_seeds_content = battle_seeds_path.read_text()
if 'str(player_id).strip() == ""' in battle_seeds_content and 'str(boss_id).strip() == ""' in battle_seeds_content:
    print("✓ battle_seeds.py has empty string filtering")
else:
    print("✗ battle_seeds.py missing empty string filtering")
    sys.exit(1)

# Check monte_carlo_optimizer.py filtering
print("\n[CHECK] monte_carlo_optimizer.py filtering...")
monte_carlo_path = Path("src/pipeline/gold/simulation/monte_carlo_optimizer.py")
monte_carlo_content = monte_carlo_path.read_text()
if "if not player_team_id or not boss_team_id:" in monte_carlo_content:
    print("✓ monte_carlo_optimizer.py has team ID filtering")
else:
    print("✗ monte_carlo_optimizer.py missing team ID filtering")
    sys.exit(1)

if 'str(row.get("team_id_attacker") or "").strip()' in monte_carlo_content:
    print("✓ monte_carlo_optimizer.py has fallback to attacker/defender IDs")
else:
    print("✗ monte_carlo_optimizer.py missing fallback")
    sys.exit(1)

# Check team_battle_simulations.py filtering
print("\n[CHECK] team_battle_simulations.py filtering...")
team_sim_path = Path("src/pipeline/gold/simulation/team_battle_simulations.py")
team_sim_content = team_sim_path.read_text()
if "valid_simulations" in team_sim_content and "attacker_id is not None and defender_id is not None" in team_sim_content:
    print("✓ team_battle_simulations.py has valid_simulations filtering")
else:
    print("✗ team_battle_simulations.py missing valid_simulations filtering")
    sys.exit(1)

# Check diagnostic scripts exist
print("\n[CHECK] Diagnostic scripts...")
diagnose_script = Path("scripts/diagnose_team_ids.py")
if diagnose_script.exists():
    print("✓ scripts/diagnose_team_ids.py exists")
else:
    print("✗ scripts/diagnose_team_ids.py missing")
    sys.exit(1)

consistency_script = Path("scripts/validate_simulation_consistency.py")
if consistency_script.exists():
    print("✓ scripts/validate_simulation_consistency.py exists")
else:
    print("✗ scripts/validate_simulation_consistency.py missing")
    sys.exit(1)

print("\n[SUCCESS] All consistency fixes are in place!")
print("\nNext steps:")
print("1. Delete old simulation data: rm -rf data/gold/simulation data/gold/simulation__tmp__*")
print("2. Re-run pipeline: python -m src.pipeline.run_pipeline gold")
print("3. Validate with: python scripts/validate_simulation_consistency.py")

