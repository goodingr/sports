"""Utilities shared across source ingestion modules."""

from __future__ import annotations

import hashlib
import json
import logging
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Generator, Optional

import pandas as pd

from src.data.config import RAW_DATA_DIR
from src.db import loaders


LOGGER = logging.getLogger(__name__)

RAW_SOURCES_DIR = RAW_DATA_DIR / "sources"

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
    "Accept-Encoding": "gzip, deflate",
    "Accept-Language": "en-US,en;q=0.9",
}


@dataclass(slots=True)
class SourceDefinition:
    """Definition of an external data source."""

    key: str
    name: str
    league: Optional[str]
    category: str
    url: Optional[str] = None
    default_frequency: Optional[str] = None
    storage_subdir: Optional[str] = None


class SourceRunHandle:
    """Manages lifecycle of a single source ingestion run."""

    def __init__(
        self,
        definition: SourceDefinition,
        run_record: Dict[str, Any],
        run_dir: Path,
    ) -> None:
        self.definition = definition
        self._run = run_record
        self._run_dir = run_dir
        self._raw_path: Optional[str] = None
        self._message: Optional[str] = None
        self._records: int = 0

    @property
    def storage_dir(self) -> Path:
        return self._run_dir

    def make_path(self, filename: str) -> Path:
        path = self._run_dir / filename
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    def set_message(self, message: str) -> None:
        if self._message:
            self._message = f"{self._message} | {message}"
        else:
            self._message = message

    def set_records(self, records: int) -> None:
        self._records = int(records)

    def add_records(self, records: int) -> None:
        self._records += int(records)

    def set_raw_path(self, raw_path: Path | str) -> None:
        self._raw_path = str(raw_path)

    def record_file(
        self,
        path: Path,
        *,
        season: Optional[int] = None,
        metadata: Optional[Dict[str, Any]] = None,
        records: Optional[int] = None,
        hash_value: Optional[str] = None,
    ) -> None:
        hash_digest = hash_value or compute_file_hash(path)
        loaders.record_source_file(
            self._run["source_id"],
            storage_path=str(path),
            season=season,
            hash_value=hash_digest,
            metadata=metadata,
        )
        if records is not None:
            self.add_records(records)

    def success(self) -> None:
        loaders.finalize_source_run(
            self._run["run_id"],
            status="success",
            message=self._message,
            records_ingested=self._records or None,
            raw_path=self._raw_path or str(self._run_dir),
        )

    def fail(self, exc: BaseException) -> None:
        loaders.finalize_source_run(
            self._run["run_id"],
            status="failed",
            message=f"{exc}",
            records_ingested=self._records or None,
            raw_path=self._raw_path or str(self._run_dir),
        )


def compute_file_hash(path: Path, *, algorithm: str = "sha256", chunk_size: int = 8192) -> str:
    hasher = hashlib.new(algorithm)
    with path.open("rb") as fh:
        while chunk := fh.read(chunk_size):
            hasher.update(chunk)
    return hasher.hexdigest()


def write_dataframe(df: pd.DataFrame, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    suffix = path.suffix.lower()
    if suffix == ".parquet":
        df.to_parquet(path, index=False)
    elif suffix == ".csv":
        df.to_csv(path, index=False)
    else:
        raise ValueError(f"Unsupported dataframe format for {path.suffix}")
    return path


def write_json(data: Any, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(data, str):
        json_obj = json.loads(data)
    else:
        json_obj = data
    path.write_text(json.dumps(json_obj, indent=2, ensure_ascii=False), encoding="utf-8")
    return path


def write_text(text: str, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def default_storage_subdir(definition: SourceDefinition) -> str:
    if definition.storage_subdir:
        return definition.storage_subdir
    if definition.league:
        return f"{definition.league.lower()}/{definition.key}"
    return definition.key


@contextmanager
def source_run(
    definition: SourceDefinition,
    *,
    enabled: bool = True,
) -> Generator[SourceRunHandle, None, None]:
    RAW_SOURCES_DIR.mkdir(parents=True, exist_ok=True)
    subdir = default_storage_subdir(definition)
    run_dir = RAW_SOURCES_DIR / subdir / datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
    run_dir.mkdir(parents=True, exist_ok=True)

    run_record = loaders.start_source_run(
        source_key=definition.key,
        name=definition.name,
        league=definition.league,
        category=definition.category,
        uri=definition.url,
        enabled=enabled,
        default_frequency=definition.default_frequency,
    )

    handle = SourceRunHandle(definition, run_record, run_dir)

    try:
        yield handle
    except Exception as exc:  # noqa: BLE001
        handle.fail(exc)
        raise
    else:
        handle.success()


__all__ = [
    "DEFAULT_HEADERS",
    "RAW_SOURCES_DIR",
    "SourceDefinition",
    "SourceRunHandle",
    "compute_file_hash",
    "source_run",
    "write_dataframe",
    "write_json",
    "write_text",
]

