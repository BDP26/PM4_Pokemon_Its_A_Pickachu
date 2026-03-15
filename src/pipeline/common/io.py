import json
from pathlib import Path
from typing import Iterable

import pandas as pd


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as file_handle:
        json.dump(payload, file_handle, indent=2)


def read_json(path: Path) -> object:
    with path.open("r", encoding="utf-8") as file_handle:
        return json.load(file_handle)


def write_jsonl(path: Path, records: Iterable[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as file_handle:
        for record in records:
            file_handle.write(json.dumps(record, ensure_ascii=False))
            file_handle.write("\n")


def read_jsonl(path: Path) -> pd.DataFrame:
    return pd.read_json(path, lines=True)

