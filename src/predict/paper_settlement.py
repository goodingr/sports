"""Grade captured NBA full-game totals and record their closing quotes."""

from __future__ import annotations

import math
import sqlite3
from datetime import datetime, timedelta, timezone

from src.models.prediction_quality import american_profit, settle_total_side

_CLOSE_WINDOW = timedelta(minutes=90)
_START_TOLERANCE = timedelta(minutes=10)
_MIN_LEAD = timedelta(minutes=15)
_CANCELLED_STATUSES = {"canceled", "cancelled"}


def _utc_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.utcoffset() is None:
        return None
    return parsed.astimezone(timezone.utc)


def _valid_total_line(value: object) -> bool:
    return (
        isinstance(value, (int, float))
        and math.isfinite(value)
        and value > 0
        and (value * 2.0).is_integer()
    )


def _valid_american_odds(value: object) -> bool:
    return isinstance(value, (int, float)) and math.isfinite(value) and abs(value) >= 100


def _close_for_decision(
    conn: sqlite3.Connection,
    decision: sqlite3.Row,
    captured_at: datetime,
    start: datetime,
) -> tuple[str, str, float, float, float] | None:
    window_start = start - _CLOSE_WINDOW
    candidates = conn.execute(
        """
        SELECT o.snapshot_id, s.fetched_at_utc, MIN(o.line) AS line,
               MAX(CASE WHEN LOWER(o.outcome) = 'over' THEN o.price_american END) AS over_price,
               MAX(CASE WHEN LOWER(o.outcome) = 'under' THEN o.price_american END) AS under_price
        FROM odds o
        JOIN odds_snapshots s ON s.snapshot_id = o.snapshot_id
        WHERE o.game_id = ? AND o.book_id = ? AND s.source = ? AND s.sport_id = ?
          AND o.market = 'totals' AND LOWER(o.outcome) IN ('over', 'under')
          AND julianday(s.fetched_at_utc) BETWEEN julianday(?) AND julianday(?)
        GROUP BY o.snapshot_id, s.fetched_at_utc
        HAVING COUNT(*) = 2 AND COUNT(DISTINCT LOWER(o.outcome)) = 2
           AND COUNT(o.line) = 2 AND MIN(o.line) = MAX(o.line)
        """,
        (
            decision["game_id"],
            decision["book_id"],
            decision["odds_source"],
            decision["sport_id"],
            max(captured_at, window_start).isoformat(),
            start.isoformat(),
        ),
    )
    latest = None
    for row in candidates:
        fetched = _utc_timestamp(row["fetched_at_utc"])
        if fetched is None or not (captured_at < fetched < start and fetched >= window_start):
            continue
        if not (
            _valid_total_line(row["line"])
            and _valid_american_odds(row["over_price"])
            and _valid_american_odds(row["under_price"])
        ):
            continue
        candidate = (fetched, row["snapshot_id"], row["line"], row[f"{decision['side']}_price"])
        if latest is None or candidate[:2] > latest[:2]:
            latest = candidate
    if latest is None:
        return None
    fetched, snapshot_id, close_line, close_price = latest
    clv = (
        close_line - decision["line"]
        if decision["side"] == "over"
        else decision["line"] - close_line
    )
    return snapshot_id, fetched.isoformat(), close_line, close_price, clv


def _final_nba_scores(decision: sqlite3.Row) -> tuple[int, int] | None:
    home, away = decision["home_score"], decision["away_score"]
    if not (isinstance(home, int) and isinstance(away, int) and home >= 0 and away >= 0):
        return None
    if home == away:
        return None
    return home, away


def settle_open_trials(conn: sqlite3.Connection, observed_at: datetime) -> int:
    """Append available grades, leaving unresolved events for a later run."""
    if observed_at.utcoffset() is None:
        raise ValueError("Settlement observation time must include a timezone")
    observed = observed_at.astimezone(timezone.utc)
    pending = conn.execute(
        """
        SELECT d.*, g.start_time_utc AS current_start, g.status, g.sport_id,
               r.home_score, r.away_score
        FROM paper_decisions d
        JOIN games g ON g.game_id = d.game_id
        JOIN sports s ON s.sport_id = g.sport_id AND s.league = 'NBA'
        LEFT JOIN game_results r ON r.game_id = d.game_id
        LEFT JOIN paper_settlements p ON p.trial_id = d.trial_id AND p.game_id = d.game_id
        WHERE p.trial_id IS NULL AND LOWER(g.status) IN ('final', 'canceled', 'cancelled')
        """
    ).fetchall()
    inserted = 0
    for decision in pending:
        captured_at = _utc_timestamp(decision["captured_at"])
        start = _utc_timestamp(decision["start_time_utc"])
        if captured_at is None or start is None or not (captured_at < min(start, observed)):
            continue
        status = decision["status"].lower()
        scores: tuple[int | None, int | None] = (None, None)
        outcome, profit = "void", 0.0
        close = None
        if status not in _CANCELLED_STATUSES:
            current_start = _utc_timestamp(decision["current_start"])
            final_scores = _final_nba_scores(decision)
            if (
                current_start is None
                or observed <= max(start, current_start)
                or final_scores is None
            ):
                continue
            scores = final_scores
            earliest_start = min(current_start, start)
            if (
                abs(current_start - start) <= _START_TOLERANCE
                and earliest_start - captured_at >= _MIN_LEAD
            ):
                if not (
                    decision["side"] in {"over", "under"}
                    and _valid_total_line(decision["line"])
                    and _valid_american_odds(decision["price"])
                ):
                    continue
                won = settle_total_side(sum(final_scores), decision["line"], decision["side"])
                outcome = "push" if won is None else ("win" if won else "loss")
                if won is not None:
                    profit = american_profit(decision["price"], won, stake=1.0)
                close = _close_for_decision(conn, decision, captured_at, earliest_start)
        cursor = conn.execute(
            """
            INSERT INTO paper_settlements (
                trial_id, game_id, settled_at, outcome, profit_units, home_score, away_score,
                close_snapshot_id, close_fetched_at, close_line, close_price, clv_points
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (trial_id, game_id) DO NOTHING
            """,
            (
                decision["trial_id"],
                decision["game_id"],
                observed.isoformat(),
                outcome,
                profit,
                *scores,
                *(close if close is not None else (None, None, None, None, None)),
            ),
        )
        inserted += cursor.rowcount
    return inserted
