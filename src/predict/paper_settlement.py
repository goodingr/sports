"""Grade captured NBA full-game totals and record their closing quotes."""

from __future__ import annotations

import json
import math
import sqlite3
from datetime import datetime, timedelta, timezone

from src.models.prediction_quality import american_profit, settle_total_side
from src.predict.paper_settlement_history import (
    SETTLEMENT_FIELDS,
    ensure_settlement_history,
    grades_as_of,
)

_CLOSE_WINDOW = timedelta(minutes=90)
_START_TOLERANCE = timedelta(minutes=10)
_MIN_LEAD = timedelta(minutes=15)
_CANCELLED_STATUSES = {"canceled", "cancelled"}


def _utc_timestamp(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
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


def _decision_rows(conn: sqlite3.Connection, trial_id: str | None = None) -> list[sqlite3.Row]:
    condition = "WHERE d.trial_id = ?" if trial_id is not None else ""
    has_trial_specification = any(
        row["name"] == "specification" for row in conn.execute("PRAGMA table_info(paper_trials)")
    )
    specification = "trial.specification" if has_trial_specification else "NULL"
    trial_join = (
        "LEFT JOIN paper_trials trial ON trial.trial_id = d.trial_id"
        if has_trial_specification
        else ""
    )
    return conn.execute(
        f"""
        SELECT d.*, g.start_time_utc AS current_start, g.status, g.sport_id,
               g.home_team_id AS current_home_team_id, g.away_team_id AS current_away_team_id,
               s.league AS current_league, r.home_score, r.away_score,
               {specification} AS trial_specification
        FROM paper_decisions d
        LEFT JOIN games g ON g.game_id = d.game_id
        LEFT JOIN sports s ON s.sport_id = g.sport_id
        LEFT JOIN game_results r ON r.game_id = d.game_id
        {trial_join}
        {condition}
        """,
        (trial_id,) if trial_id is not None else (),
    ).fetchall()


def _minimum_lead(decision: sqlite3.Row) -> timedelta | None:
    if decision["trial_specification"] is None:
        return _MIN_LEAD
    try:
        minutes = json.loads(decision["trial_specification"]).get("min_minutes_before_start", 15)
        if not isinstance(minutes, (int, float)) or not math.isfinite(minutes) or minutes <= 0:
            return None
        return timedelta(minutes=minutes)
    except (TypeError, ValueError, AttributeError, OverflowError):
        return None


def expected_grade(
    conn: sqlite3.Connection, decision: sqlite3.Row, observed_at: datetime
) -> dict[str, object]:
    """Compute a grade from current source state without writing ledger data."""
    if observed_at.utcoffset() is None:
        raise ValueError("Settlement observation time must include a timezone")
    observed = observed_at.astimezone(timezone.utc)
    grade: dict[str, object] = {
        "outcome": "pending",
        "profit_units": 0.0,
        "home_score": decision["home_score"],
        "away_score": decision["away_score"],
        "close_snapshot_id": None,
        "close_fetched_at": None,
        "close_line": None,
        "close_price": None,
        "clv_points": None,
    }
    captured_at = _utc_timestamp(decision["captured_at"])
    start = _utc_timestamp(decision["start_time_utc"])
    current_start = _utc_timestamp(decision["current_start"])
    minimum_lead = _minimum_lead(decision)
    if (
        captured_at is None
        or start is None
        or current_start is None
        or minimum_lead is None
        or not captured_at < min(start, observed)
    ):
        return grade
    identity_changed = decision["current_league"] != "NBA" or any(
        field in decision.keys()
        and decision[field] is not None
        and decision[field] != decision[f"current_{field}"]
        for field in ("home_team_id", "away_team_id")
    )
    status = str(decision["status"] or "").lower()
    if identity_changed or status in _CANCELLED_STATUSES:
        grade["outcome"] = "void"
        return grade
    if observed <= max(start, current_start):
        return grade
    if (
        abs(current_start - start) > _START_TOLERANCE
        or min(current_start, start) - captured_at < minimum_lead
    ):
        grade["outcome"] = "void"
        return grade
    if (
        status != "final"
        or _final_nba_scores(decision) is None
        or decision["side"] not in {"over", "under"}
        or not _valid_total_line(decision["line"])
        or not _valid_american_odds(decision["price"])
    ):
        return grade
    won = settle_total_side(
        decision["home_score"] + decision["away_score"], decision["line"], decision["side"]
    )
    grade["outcome"] = "push" if won is None else ("win" if won else "loss")
    grade["profit_units"] = (
        0.0 if won is None else american_profit(decision["price"], won, stake=1.0)
    )
    close = _close_for_decision(conn, decision, captured_at, min(start, current_start))
    if close is not None:
        grade.update(zip(SETTLEMENT_FIELDS[7:], close))
    return grade


def _material_state(grade: sqlite3.Row | dict[str, object]) -> tuple[object, ...]:
    return tuple(
        _utc_timestamp(grade[field]) if field == "close_fetched_at" else grade[field]
        for field in SETTLEMENT_FIELDS[3:]
    )


def _needs_revision(
    previous: sqlite3.Row | None, grade: dict[str, object], observed: datetime
) -> bool:
    if previous is None:
        return grade["outcome"] != "pending"
    previous_time = _utc_timestamp(previous["settled_at"])
    return (
        previous_time is not None
        and previous_time <= observed
        and _material_state(previous) != _material_state(grade)
    )


def unreconciled_grades(conn: sqlite3.Connection, trial_id: str, observed_at: datetime) -> int:
    """Count current-source changes that have not been recorded in grade history."""
    if observed_at.utcoffset() is None:
        raise ValueError("Settlement observation time must include a timezone")
    observed = observed_at.astimezone(timezone.utc)
    latest = grades_as_of(conn, trial_id, datetime.max.replace(tzinfo=timezone.utc))
    return sum(
        _needs_revision(
            latest.get(decision["game_id"]), expected_grade(conn, decision, observed), observed
        )
        for decision in _decision_rows(conn, trial_id)
    )


def settle_open_trials(conn: sqlite3.Connection, observed_at: datetime) -> int:
    """Append changed grades and refresh the current cache in the caller's transaction."""
    if observed_at.utcoffset() is None:
        raise ValueError("Settlement observation time must include a timezone")
    observed = observed_at.astimezone(timezone.utc)
    ensure_settlement_history(conn)
    decisions = _decision_rows(conn)
    latest = {
        trial_id: grades_as_of(conn, trial_id, datetime.max.replace(tzinfo=timezone.utc))
        for trial_id in {decision["trial_id"] for decision in decisions}
    }
    inserted = 0
    for decision in decisions:
        grade = expected_grade(conn, decision, observed)
        previous = latest[decision["trial_id"]].get(decision["game_id"])
        if not _needs_revision(previous, grade, observed):
            continue
        values = (
            decision["trial_id"],
            decision["game_id"],
            observed.isoformat(),
            *[grade[field] for field in SETTLEMENT_FIELDS[3:]],
        )
        cursor = conn.execute(
            f"""
            INSERT INTO paper_settlement_history ({", ".join(SETTLEMENT_FIELDS)})
            VALUES ({", ".join("?" for _ in SETTLEMENT_FIELDS)})
            """,
            values,
        )
        if grade["outcome"] == "pending":
            conn.execute(
                "DELETE FROM paper_settlements WHERE trial_id = ? AND game_id = ?",
                (decision["trial_id"], decision["game_id"]),
            )
        else:
            conn.execute(
                f"""
                INSERT OR REPLACE INTO paper_settlements ({", ".join(SETTLEMENT_FIELDS)})
                VALUES ({", ".join("?" for _ in SETTLEMENT_FIELDS)})
                """,
                values,
            )
        inserted += cursor.rowcount
    return inserted
