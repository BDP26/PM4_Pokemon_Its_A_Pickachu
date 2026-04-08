import json
import shutil
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


def write_parquet(
    path: Path,
    records: Iterable[dict] | pd.DataFrame,
    partition_cols: list[str] | tuple[str, ...] | None = None,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(records, pd.DataFrame):
        dataframe = records
    else:
        dataframe = pd.DataFrame(list(records))
    try:
        if partition_cols:
            partitions = [column for column in partition_cols if column in dataframe.columns]
            if not partitions:
                # Fallback to plain parquet file when partition columns are absent.
                if path.exists():
                    if path.is_dir():
                        shutil.rmtree(path)
                    else:
                        path.unlink()
                dataframe.to_parquet(path, index=False)
                return

            if path.exists():
                if path.is_dir():
                    shutil.rmtree(path)
                else:
                    path.unlink()
            path.mkdir(parents=True, exist_ok=True)
            grouped = dataframe.groupby(partitions, dropna=False, sort=True)
            for partition_values, frame in grouped:
                values = partition_values if isinstance(partition_values, tuple) else (partition_values,)
                partition_dir = path
                for column, value in zip(partitions, values, strict=False):
                    partition_dir = partition_dir / f"{column}={value}"
                partition_dir.mkdir(parents=True, exist_ok=True)
                # Partition columns are encoded in the directory path and must
                # not be duplicated inside parquet data files.
                frame.drop(columns=partitions, errors="ignore").to_parquet(
                    partition_dir / "part-000.parquet",
                    index=False,
                )
            return

        # Non-partition write may follow an earlier partitioned write target.
        if path.exists() and path.is_dir():
            shutil.rmtree(path)
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




