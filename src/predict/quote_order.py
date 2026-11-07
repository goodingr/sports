"""Exact snapshot ordering for current market execution."""

import sqlite3
from datetime import datetime, timezone


def latest_market_snapshot_id(
    conn: sqlite3.Connection, *, game_id: str, sport_id: int, source: str, market: str
) -> str | None:
    """Find the newest market observation, including incomplete quote pairs."""
    snapshots = conn.execute(
        """
        SELECT DISTINCT s.snapshot_id, s.fetched_at_utc
        FROM odds o JOIN odds_snapshots s USING (snapshot_id)
        WHERE o.game_id = ? AND s.sport_id = ? AND s.source = ? AND o.market = ?
        """,
        (game_id, sport_id, source, market),
    )
    latest = None
    for snapshot_id, fetched_at in snapshots:
        if not isinstance(fetched_at, str):
            continue
        try:
            stamp = datetime.fromisoformat(fetched_at)
            # Historical *_utc rows may lack offsets. This does not make them
            # valid execution evidence, but they still supersede older quotes.
            stamp = (
                stamp.replace(tzinfo=timezone.utc)
                if stamp.utcoffset() is None
                else stamp.astimezone(timezone.utc)
            )
        except (OverflowError, ValueError):
            continue
        candidate = stamp, snapshot_id
        if latest is None or candidate > latest:
            latest = candidate
    return latest[1] if latest is not None else None
