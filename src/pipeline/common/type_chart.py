from __future__ import annotations

import json
from pathlib import Path


TYPES = [
    "Normal", "Fighting", "Flying", "Poison", "Ground", "Rock",
    "Bug", "Ghost", "Steel", "Fire", "Water", "Grass",
    "Electric", "Psychic", "Ice", "Dragon", "Dark", "Fairy",
]


def build_type_chart() -> dict[str, dict[str, float]]:
    chart = {
        attacking: {defending: 1.0 for defending in TYPES}
        for attacking in TYPES
    }

    chart["Normal"]["Rock"] = 0.5
    chart["Normal"]["Ghost"] = 0.0
    chart["Normal"]["Steel"] = 0.5

    chart["Fighting"]["Normal"] = 2.0
    chart["Fighting"]["Flying"] = 0.5
    chart["Fighting"]["Poison"] = 0.5
    chart["Fighting"]["Rock"] = 2.0
    chart["Fighting"]["Bug"] = 0.5
    chart["Fighting"]["Ghost"] = 0.0
    chart["Fighting"]["Steel"] = 2.0
    chart["Fighting"]["Psychic"] = 0.5
    chart["Fighting"]["Ice"] = 2.0
    chart["Fighting"]["Dark"] = 2.0
    chart["Fighting"]["Fairy"] = 0.5

    chart["Flying"]["Fighting"] = 2.0
    chart["Flying"]["Rock"] = 0.5
    chart["Flying"]["Bug"] = 2.0
    chart["Flying"]["Steel"] = 0.5
    chart["Flying"]["Grass"] = 2.0
    chart["Flying"]["Electric"] = 0.5

    chart["Poison"]["Poison"] = 0.5
    chart["Poison"]["Ground"] = 0.5
    chart["Poison"]["Rock"] = 0.5
    chart["Poison"]["Ghost"] = 0.5
    chart["Poison"]["Steel"] = 0.0
    chart["Poison"]["Grass"] = 2.0
    chart["Poison"]["Fairy"] = 2.0

    chart["Ground"]["Flying"] = 0.0
    chart["Ground"]["Poison"] = 2.0
    chart["Ground"]["Rock"] = 2.0
    chart["Ground"]["Bug"] = 0.5
    chart["Ground"]["Steel"] = 2.0
    chart["Ground"]["Fire"] = 2.0
    chart["Ground"]["Grass"] = 0.5
    chart["Ground"]["Electric"] = 2.0

    chart["Rock"]["Fighting"] = 0.5
    chart["Rock"]["Flying"] = 2.0
    chart["Rock"]["Ground"] = 0.5
    chart["Rock"]["Bug"] = 2.0
    chart["Rock"]["Steel"] = 0.5
    chart["Rock"]["Fire"] = 2.0
    chart["Rock"]["Ice"] = 2.0

    chart["Bug"]["Fighting"] = 0.5
    chart["Bug"]["Flying"] = 0.5
    chart["Bug"]["Poison"] = 0.5
    chart["Bug"]["Ghost"] = 0.5
    chart["Bug"]["Steel"] = 0.5
    chart["Bug"]["Fire"] = 0.5
    chart["Bug"]["Grass"] = 2.0
    chart["Bug"]["Psychic"] = 2.0
    chart["Bug"]["Dark"] = 2.0
    chart["Bug"]["Fairy"] = 0.5

    chart["Ghost"]["Normal"] = 0.0
    chart["Ghost"]["Ghost"] = 2.0
    chart["Ghost"]["Psychic"] = 2.0
    chart["Ghost"]["Dark"] = 0.5

    chart["Steel"]["Rock"] = 2.0
    chart["Steel"]["Steel"] = 0.5
    chart["Steel"]["Fire"] = 0.5
    chart["Steel"]["Water"] = 0.5
    chart["Steel"]["Electric"] = 0.5
    chart["Steel"]["Ice"] = 2.0
    chart["Steel"]["Fairy"] = 2.0

    chart["Fire"]["Rock"] = 0.5
    chart["Fire"]["Bug"] = 2.0
    chart["Fire"]["Steel"] = 2.0
    chart["Fire"]["Fire"] = 0.5
    chart["Fire"]["Water"] = 0.5
    chart["Fire"]["Grass"] = 2.0
    chart["Fire"]["Ice"] = 2.0
    chart["Fire"]["Dragon"] = 0.5

    chart["Water"]["Ground"] = 2.0
    chart["Water"]["Rock"] = 2.0
    chart["Water"]["Fire"] = 2.0
    chart["Water"]["Water"] = 0.5
    chart["Water"]["Grass"] = 0.5
    chart["Water"]["Dragon"] = 0.5

    chart["Grass"]["Flying"] = 0.5
    chart["Grass"]["Poison"] = 0.5
    chart["Grass"]["Ground"] = 2.0
    chart["Grass"]["Rock"] = 2.0
    chart["Grass"]["Bug"] = 0.5
    chart["Grass"]["Steel"] = 0.5
    chart["Grass"]["Fire"] = 0.5
    chart["Grass"]["Water"] = 2.0
    chart["Grass"]["Grass"] = 0.5
    chart["Grass"]["Dragon"] = 0.5

    chart["Electric"]["Flying"] = 2.0
    chart["Electric"]["Ground"] = 0.0
    chart["Electric"]["Water"] = 2.0
    chart["Electric"]["Grass"] = 0.5
    chart["Electric"]["Electric"] = 0.5
    chart["Electric"]["Dragon"] = 0.5

    chart["Psychic"]["Fighting"] = 2.0
    chart["Psychic"]["Poison"] = 2.0
    chart["Psychic"]["Steel"] = 0.5
    chart["Psychic"]["Psychic"] = 0.5
    chart["Psychic"]["Dark"] = 0.0

    chart["Ice"]["Flying"] = 2.0
    chart["Ice"]["Ground"] = 2.0
    chart["Ice"]["Steel"] = 0.5
    chart["Ice"]["Fire"] = 0.5
    chart["Ice"]["Water"] = 0.5
    chart["Ice"]["Grass"] = 2.0
    chart["Ice"]["Ice"] = 0.5
    chart["Ice"]["Dragon"] = 2.0

    chart["Dragon"]["Steel"] = 0.5
    chart["Dragon"]["Dragon"] = 2.0
    chart["Dragon"]["Fairy"] = 0.0

    chart["Dark"]["Fighting"] = 0.5
    chart["Dark"]["Ghost"] = 2.0
    chart["Dark"]["Psychic"] = 2.0
    chart["Dark"]["Dark"] = 0.5
    chart["Dark"]["Fairy"] = 0.5

    chart["Fairy"]["Fighting"] = 2.0
    chart["Fairy"]["Poison"] = 0.5
    chart["Fairy"]["Steel"] = 0.5
    chart["Fairy"]["Fire"] = 0.5
    chart["Fairy"]["Dragon"] = 2.0
    chart["Fairy"]["Dark"] = 2.0

    return chart


def save_as_json(chart: dict[str, dict[str, float]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as f:
        json.dump(chart, f, indent=2, ensure_ascii=False)

