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


def _to_dataframe(records: Iterable[dict] | pd.DataFrame) -> pd.DataFrame:
    return records if isinstance(records, pd.DataFrame) else pd.DataFrame(list(records))


def _remove_path_if_exists(path: Path) -> None:
    if not path.exists():
        return
    if path.is_dir():
        shutil.rmtree(path)
    else:
        path.unlink()


def _existing_partitions(dataframe: pd.DataFrame, partition_cols: list[str] | tuple[str, ...] | None) -> list[str]:
    if not partition_cols:
        return []
    return [column for column in partition_cols if column in dataframe.columns]


def _write_non_partitioned_parquet(path: Path, dataframe: pd.DataFrame, cleanup_any_path: bool = False) -> None:
    if cleanup_any_path:
        _remove_path_if_exists(path)
    elif path.exists() and path.is_dir():
        shutil.rmtree(path)
    dataframe.to_parquet(path, index=False)


def _write_partitioned_parquet(path: Path, dataframe: pd.DataFrame, partitions: list[str]) -> None:
    _remove_path_if_exists(path)
    path.mkdir(parents=True, exist_ok=True)
    grouped = dataframe.groupby(partitions, dropna=False, sort=True)
    for partition_values, frame in grouped:
        values = partition_values if isinstance(partition_values, tuple) else (partition_values,)
        partition_dir = path
        for column, value in zip(partitions, values, strict=False):
            partition_dir = partition_dir / f"{column}={value}"
        partition_dir.mkdir(parents=True, exist_ok=True)
        frame.drop(columns=partitions, errors="ignore").to_parquet(
            partition_dir / "part-000.parquet",
            index=False,
        )


def write_parquet(
    path: Path,
    records: Iterable[dict] | pd.DataFrame,
    partition_cols: list[str] | tuple[str, ...] | None = None,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    dataframe = _to_dataframe(records)
    try:
        partitions = _existing_partitions(dataframe, partition_cols)
        if partition_cols and not partitions:
            _write_non_partitioned_parquet(path, dataframe, cleanup_any_path=True)
            return

        if partitions:
            _write_partitioned_parquet(path, dataframe, partitions)
            return

        _write_non_partitioned_parquet(path, dataframe)
    except ImportError as exc:
        raise ImportError(
            "Parquet write requires pyarrow or fastparquet. Install dependency: pyarrow"
        ) from exc


def read_jsonl(path: Path) -> pd.DataFrame:
    return pd.read_json(path, lines=True)


def read_parquet(path: Path) -> pd.DataFrame:
    try:
        return pd.read_parquet(path)
    except ImportError as exc:
        raise ImportError(
            "Parquet read requires pyarrow or fastparquet. Install dependency: pyarrow"
        ) from exc


def read_many_parquet(paths: list[Path]) -> pd.DataFrame:
    existing_paths = [path for path in paths if path.exists()]
    if not existing_paths:
        return pd.DataFrame()

    frames = [read_parquet(path) for path in existing_paths]
    if not frames:
        return pd.DataFrame()

    return pd.concat(frames, ignore_index=True)