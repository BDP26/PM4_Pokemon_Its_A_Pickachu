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


def write_parquet(path: Path, records: Iterable[dict] | pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(records, pd.DataFrame):
        dataframe = records
    else:
        dataframe = pd.DataFrame(list(records))
    try:
        dataframe.to_parquet(path, index=False)
    except ImportError as exc:  # pragma: no cover - depends on runtime env
        raise ImportError(
            "Parquet write requires pyarrow or fastparquet. Install dependency: pyarrow"
        ) from exc


def read_jsonl(path: Path) -> pd.DataFrame:
    return pd.read_json(path, lines=True)


def read_parquet(path: Path) -> pd.DataFrame:
    try:
        return pd.read_parquet(path)
    except ImportError as exc:  # pragma: no cover - depends on runtime env
        raise ImportError(
            "Parquet read requires pyarrow or fastparquet. Install dependency: pyarrow"
        ) from exc




