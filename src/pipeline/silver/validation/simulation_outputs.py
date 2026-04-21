from __future__ import annotations

from typing import Any


def validate_team_battle_simulations(rows: list[dict[str, Any]], strict: bool = True) -> list[str]:
    errors: list[str] = []

    for index, row in enumerate(rows):
        prefix = f"[simulation_validation] row_index={index}"

        attacker_id = row.get("team_id_attacker")
        defender_id = row.get("team_id_defender")
        if not attacker_id:
            errors.append(f"{prefix} missing team_id_attacker")
        if not defender_id:
            errors.append(f"{prefix} missing team_id_defender")

        predicted = row.get("predicted_player_win_chance")
        if predicted is not None:
            try:
                p = float(predicted)
            except Exception:
                errors.append(f"{prefix} invalid predicted_player_win_chance={predicted!r}")
            else:
                if not (0.0 <= p <= 1.0):
                    errors.append(f"{prefix} predicted_player_win_chance out of bounds={p}")

        n_trials = row.get("n_trials")
        if n_trials is not None:
            try:
                trials = int(n_trials)
            except Exception:
                errors.append(f"{prefix} invalid n_trials={n_trials!r}")
            else:
                if trials < 1:
                    errors.append(f"{prefix} n_trials < 1: {trials}")

        attacker_remaining = row.get("attacker_remaining_pokemon")
        defender_remaining = row.get("defender_remaining_pokemon")
        for field_name, value in [
            ("attacker_remaining_pokemon", attacker_remaining),
            ("defender_remaining_pokemon", defender_remaining),
        ]:
            try:
                numeric = int(value)
            except Exception:
                errors.append(f"{prefix} invalid {field_name}={value!r}")
                continue
            if not (0 <= numeric <= 6):
                errors.append(f"{prefix} {field_name} out of bounds={numeric}")

        attacker_win = row.get("attacker_win")
        if attacker_win is not None:
            attacker_win_bool = bool(attacker_win)
            if attacker_remaining is not None and defender_remaining is not None:
                try:
                    a = int(attacker_remaining)
                    d = int(defender_remaining)
                    if attacker_win_bool and d != 0:
                        errors.append(f"{prefix} attacker_win=True but defender_remaining_pokemon={d}")
                    if not attacker_win_bool and a > 0 and d == 0:
                        errors.append(f"{prefix} attacker_win=False but attacker still has remaining Pokémon and defender has none")
                except Exception:
                    pass

        score = row.get("simulation_score")
        if score is not None:
            try:
                float(score)
            except Exception:
                errors.append(f"{prefix} invalid simulation_score={score!r}")

        warnings = row.get("warnings", [])
        if warnings is not None and not isinstance(warnings, list):
            errors.append(f"{prefix} warnings is not a list")

        duel_summaries = row.get("duel_summaries", [])
        if duel_summaries is not None and not isinstance(duel_summaries, list):
            errors.append(f"{prefix} duel_summaries is not a list")

        degradation_reasons = row.get("degradation_reasons", [])
        if degradation_reasons is not None and not isinstance(degradation_reasons, list):
            errors.append(f"{prefix} degradation_reasons is not a list")

        probability_source = row.get("probability_source")
        if probability_source is not None and not isinstance(probability_source, str):
            errors.append(f"{prefix} probability_source is not a string")

    if strict and errors:
        raise ValueError("Simulation output validation failed:\n" + "\n".join(errors[:200]))
    return errors