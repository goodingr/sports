"""Retain paper settlement revisions and select the evidence available at a cutoff."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone

SETTLEMENT_FIELDS = (
    "trial_id",
    "game_id",
    "settled_at",
    "outcome",
    "profit_units",
    "home_score",
    "away_score",
    "close_snapshot_id",
    "close_fetched_at",
    "close_line",
    "close_price",
    "clv_points",
)


def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {row["name"] for row in conn.execute(f"PRAGMA table_info({table})")}


def _projection(columns: set[str], alias: str = "") -> str:
    prefix = f"{alias}." if alias else ""
    return ", ".join(
        f"{prefix}{field}" if field in columns else f"NULL AS {field}"
        for field in SETTLEMENT_FIELDS
    )


def _aware_timestamp(value: str | datetime) -> datetime:
    timestamp = datetime.fromisoformat(value) if isinstance(value, str) else value
    if timestamp.utcoffset() is None:
        raise ValueError("Paper settlement timestamps must include a time zone")
    return timestamp.astimezone(timezone.utc)


def ensure_settlement_history(conn: sqlite3.Connection) -> None:
    """Create history and retain legacy grades within the caller's transaction."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS paper_settlement_history (
            revision_id INTEGER PRIMARY KEY AUTOINCREMENT,
            trial_id TEXT NOT NULL,
            game_id TEXT NOT NULL,
            settled_at TEXT NOT NULL,
            outcome TEXT NOT NULL CHECK (outcome IN ('win', 'loss', 'push', 'void', 'pending')),
            profit_units REAL NOT NULL,
            home_score INTEGER,
            away_score INTEGER,
            close_snapshot_id TEXT,
            close_fetched_at TEXT,
            close_line REAL,
            close_price REAL,
            clv_points REAL
        )
        """
    )
    current_columns = _columns(conn, "paper_settlements")
    if not current_columns:
        return
    conn.execute(
        f"""
        INSERT INTO paper_settlement_history ({", ".join(SETTLEMENT_FIELDS)})
        SELECT {_projection(current_columns, "current")}
        FROM paper_settlements current
        WHERE NOT EXISTS (
            SELECT 1 FROM paper_settlement_history history
            WHERE history.trial_id = current.trial_id AND history.game_id = current.game_id
        )
        """
    )


def grades_as_of(
    conn: sqlite3.Connection, trial_id: str, cutoff: datetime
) -> dict[str, sqlite3.Row]:
    """Return the latest exact-timestamp revision per game, including pending revisions."""
    cutoff = _aware_timestamp(cutoff)
    history_columns = _columns(conn, "paper_settlement_history")
    revisions = []
    history_games = set()
    if history_columns:
        revisions = conn.execute(
            f"""
            SELECT revision_id, {_projection(history_columns)}
            FROM paper_settlement_history WHERE trial_id = ?
            """,
            (trial_id,),
        ).fetchall()
        history_games = {row["game_id"] for row in revisions}
    current_columns = _columns(conn, "paper_settlements")
    if current_columns:
        revisions.extend(
            row
            for row in conn.execute(
                f"""
                SELECT 0 AS revision_id, {_projection(current_columns)}
                FROM paper_settlements WHERE trial_id = ?
                """,
                (trial_id,),
            )
            if row["game_id"] not in history_games
        )

    latest: dict[str, tuple[tuple[datetime, int], sqlite3.Row]] = {}
    for row in revisions:
        try:
            timestamp = _aware_timestamp(row["settled_at"])
        except (ValueError, TypeError, AttributeError):
            continue
        if timestamp > cutoff:
            continue
        key = (timestamp, row["revision_id"])
        existing = latest.get(row["game_id"])
        if existing is None or key > existing[0]:
            latest[row["game_id"]] = (key, row)
    return {game_id: entry[1] for game_id, entry in latest.items()}
