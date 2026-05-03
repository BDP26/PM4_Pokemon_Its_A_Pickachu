from __future__ import annotations

from src.pipeline.common.type_chart import build_type_chart, save_as_json
from src.pipeline.settings import BRONZE_DIR


def main() -> None:
    chart = build_type_chart()
    json_path = BRONZE_DIR / "type_chart.json"
    save_as_json(chart, json_path)
    print(f"JSON gespeichert unter: {json_path}")


if __name__ == "__main__":
    main()
