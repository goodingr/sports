"""SQLite helpers for sports betting warehouse."""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, Optional

from src.data.config import PROJECT_ROOT


DB_PATH = PROJECT_ROOT / "data" / "betting.db"
SCHEMA_PATH = Path(__file__).with_name("schema.sql")


def ensure_parent_directory(db_path: Path = DB_PATH) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)


@contextmanager
def connect(db_path: Optional[Path] = None) -> Iterator[sqlite3.Connection]:
    path = db_path or DB_PATH
    ensure_parent_directory(path)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def initialize(db_path: Optional[Path] = None) -> None:
    path = db_path or DB_PATH
    ensure_parent_directory(path)
    schema_sql = SCHEMA_PATH.read_text(encoding="utf-8")
    with connect(path) as conn:
        conn.executescript(schema_sql)
        try:
            conn.execute("ALTER TABLE models ADD COLUMN league TEXT")
        except sqlite3.OperationalError:
            pass


def vacuum(db_path: Optional[Path] = None) -> None:
    with connect(db_path) as conn:
        conn.execute("VACUUM")


__all__ = ["DB_PATH", "SCHEMA_PATH", "connect", "initialize", "vacuum"]

