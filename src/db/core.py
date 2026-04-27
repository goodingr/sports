"""SQLite helpers for sports betting warehouse."""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
import os
from pathlib import Path
from typing import Iterator, Optional

from src.data.config import PROJECT_ROOT


DB_PATH = PROJECT_ROOT / "data" / "betting.db"
SCHEMA_PATH = Path(__file__).with_name("schema.sql")


def resolve_db_path(db_path: Optional[Path] = None) -> Path:
    if db_path is not None:
        return db_path

    configured = os.getenv("SPORTS_DB_PATH") or os.getenv("DATABASE_PATH") or os.getenv("DB_PATH")
    if configured:
        path = Path(configured).expanduser()
        return path if path.is_absolute() else PROJECT_ROOT / path

    return DB_PATH


def ensure_parent_directory(db_path: Path = DB_PATH) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)


@contextmanager
def connect(db_path: Optional[Path] = None) -> Iterator[sqlite3.Connection]:
    path = resolve_db_path(db_path)
    ensure_parent_directory(path)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def initialize(db_path: Optional[Path] = None) -> None:
    path = resolve_db_path(db_path)
    ensure_parent_directory(path)
    schema_sql = SCHEMA_PATH.read_text(encoding="utf-8")
    with connect(path) as conn:
        conn.executescript(schema_sql)
        _apply_lightweight_migrations(conn)


def _column_names(conn: sqlite3.Connection, table_name: str) -> set[str]:
    return {row[1] for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()}


def _ensure_column(conn: sqlite3.Connection, table_name: str, column_name: str, column_ddl: str) -> None:
    if column_name in _column_names(conn, table_name):
        return
    conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_ddl}")


def _dedupe_current_predictions(conn: sqlite3.Connection) -> None:
    if "predictions" not in {
        row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    }:
        return
    conn.execute(
        """
        DELETE FROM predictions
        WHERE prediction_id NOT IN (
            SELECT prediction_id
            FROM (
                SELECT
                    prediction_id,
                    ROW_NUMBER() OVER (
                        PARTITION BY game_id, model_type
                        ORDER BY predicted_at DESC, prediction_id DESC
                    ) AS rn
                FROM predictions
            )
            WHERE rn = 1
        )
        """
    )


def _apply_lightweight_migrations(conn: sqlite3.Connection) -> None:
    _ensure_column(conn, "models", "league", "TEXT")
    _ensure_column(conn, "predictions", "predicted_total_points", "REAL")
    _dedupe_current_predictions(conn)
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_predictions_current_unique
        ON predictions (game_id, model_type)
        """
    )


def vacuum(db_path: Optional[Path] = None) -> None:
    with connect(db_path) as conn:
        conn.execute("VACUUM")


__all__ = ["DB_PATH", "SCHEMA_PATH", "connect", "initialize", "resolve_db_path", "vacuum"]

