"""SQLite helpers for sports betting warehouse."""

from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, Optional

from src.data.config import PROJECT_ROOT

DB_PATH = PROJECT_ROOT / "data" / "betting.db"
SCHEMA_PATH = Path(__file__).with_name("schema.sql")

# Pragmas applied to every connection opened through `connect`. The worker and
# the API both share the same warehouse, so we want concurrent-safe defaults:
#   - WAL gives concurrent readers while a writer is active.
#   - synchronous=NORMAL is the standard companion for WAL.
#   - busy_timeout lets one connection wait instead of immediately raising
#     `database is locked` when another commit is in flight.
#   - foreign_keys=ON enforces the REFERENCES declared in schema.sql.
_BUSY_TIMEOUT_MS = 5000


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


def _apply_pragmas(conn: sqlite3.Connection) -> None:
    conn.execute(f"PRAGMA busy_timeout = {_BUSY_TIMEOUT_MS}")
    conn.execute("PRAGMA foreign_keys = ON")
    # journal_mode is a per-database setting (persists in the file header).
    # Calling it on every connect is cheap and self-healing if a previous
    # connection rolled the DB back to delete-mode (e.g. an external tool).
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")


@contextmanager
def connect(db_path: Optional[Path] = None) -> Iterator[sqlite3.Connection]:
    path = resolve_db_path(db_path)
    ensure_parent_directory(path)
    conn = sqlite3.connect(path, timeout=_BUSY_TIMEOUT_MS / 1000)
    conn.row_factory = sqlite3.Row
    _apply_pragmas(conn)
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
    _ensure_column(conn, "injury_reports", "player_id", "TEXT")
    _ensure_column(conn, "game_results", "total_close_snapshot_id", "TEXT")
    _ensure_column(conn, "game_results", "total_close_snapshot_time_utc", "TEXT")
    _ensure_column(conn, "game_results", "total_close_book_id", "INTEGER")
    _ensure_column(conn, "game_results", "total_close_book", "TEXT")
    _ensure_column(conn, "game_results", "total_close_source", "TEXT")
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


__all__ = [
    "DB_PATH",
    "SCHEMA_PATH",
    "connect",
    "initialize",
    "resolve_db_path",
    "vacuum",
    "online_backup",
]


def online_backup(destination: Path, db_path: Optional[Path] = None) -> Path:
    """Take an online (hot) backup of the warehouse.

    Uses SQLite's backup API, which copies the database page-by-page while
    other connections continue to read and write. This is the only safe way
    to snapshot a live SQLite file — `cp betting.db backup.db` while the
    worker is mid-commit can produce a torn file that fails integrity checks.
    """
    src_path = resolve_db_path(db_path)
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)

    src = sqlite3.connect(src_path, timeout=_BUSY_TIMEOUT_MS / 1000)
    try:
        _apply_pragmas(src)
        dst = sqlite3.connect(destination)
        try:
            with dst:
                src.backup(dst)
        finally:
            dst.close()
    finally:
        src.close()

    return destination
