"""Retain known paper-trial schedules and count unobserved decision windows."""

from __future__ import annotations

import sqlite3
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any

_START_TOLERANCE = timedelta(minutes=10)


def _utc(value: datetime | str) -> datetime:
    parsed = datetime.fromisoformat(value) if isinstance(value, str) else value
    if not isinstance(parsed, datetime) or parsed.utcoffset() is None:
        raise ValueError("Paper schedule timestamps must include a time zone")
    return parsed.astimezone(timezone.utc)


def record_known_fixtures(
    conn: sqlite3.Connection,
    trial_id: str,
    league: str,
    observed_at: datetime,
    review_at: datetime,
    max_lead_hours: int = 12,
) -> None:
    """Append the first observation of each distinct future fixture schedule."""
    observed_at, review_at = _utc(observed_at), _utc(review_at)
    if max_lead_hours <= 0:
        raise ValueError("The maximum lead must be positive")
    if observed_at >= review_at:
        return
    conn.execute(
        """
        INSERT OR IGNORE INTO paper_fixture_schedule (
            trial_id, game_id, observed_at, start_time_utc, home_team_id, away_team_id
        )
        SELECT ?, g.game_id, ?, g.start_time_utc, g.home_team_id, g.away_team_id
        FROM games g
        JOIN sports s ON s.sport_id = g.sport_id
        LEFT JOIN game_results r ON r.game_id = g.game_id
        WHERE s.league = ? AND LOWER(g.status) = 'scheduled'
          AND r.home_score IS NULL AND r.away_score IS NULL
          AND julianday(g.start_time_utc) > julianday(?)
          AND julianday(g.start_time_utc) < julianday(?)
        """,
        (
            trial_id,
            observed_at.isoformat(),
            league,
            observed_at.isoformat(),
            (review_at + timedelta(hours=max_lead_hours)).isoformat(),
        ),
    )


def known_window_coverage(
    conn: sqlite3.Connection,
    trial_id: str,
    registered_at: datetime,
    review_at: datetime,
    observed_at: datetime,
    min_lead_minutes: int = 15,
    max_lead_hours: int = 12,
) -> dict[str, Any]:
    """Count expired windows only for prospectively retained fixture schedules."""
    registered_at, review_at, observed_at = map(_utc, (registered_at, review_at, observed_at))
    if not 0 < min_lead_minutes < max_lead_hours * 60:
        raise ValueError("The lead bounds must define a positive decision window")
    cutoff = min(observed_at, review_at)
    min_lead = timedelta(minutes=min_lead_minutes)
    max_lead = timedelta(hours=max_lead_hours)
    tables = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        )
    }
    if "paper_fixture_schedule" not in tables:
        return {"available": False}

    observed_games = set()
    if {"paper_capture_runs", "paper_fixture_observations"} <= tables:
        for row in conn.execute(
            """
            SELECT o.game_id, o.start_time_utc, r.observed_at
            FROM paper_fixture_observations o JOIN paper_capture_runs r ON r.run_id = o.run_id
            WHERE r.trial_id = ?
            """,
            (trial_id,),
        ):
            try:
                start, seen = _utc(row[1]), _utc(row[2])
            except (TypeError, ValueError):
                continue
            if (
                registered_at <= seen <= cutoff
                and seen < review_at
                and min_lead <= start - seen <= max_lead
            ):
                observed_games.add(row[0])

    schedules: dict[str, list[tuple[datetime, datetime, int, int]]] = defaultdict(list)
    for row in conn.execute(
        """
        SELECT game_id, observed_at, start_time_utc, home_team_id, away_team_id
        FROM paper_fixture_schedule WHERE trial_id = ?
        ORDER BY observed_at, start_time_utc, home_team_id, away_team_id
        """,
        (trial_id,),
    ):
        try:
            seen, start = _utc(row[1]), _utc(row[2])
        except (TypeError, ValueError):
            continue
        if registered_at <= seen <= cutoff and seen < review_at:
            schedules[row[0]].append((seen, start, row[3], row[4]))

    counts = {
        "available": True,
        "known_fixtures": 0,
        "known_future_fixtures": 0,
        "passed_window_fixtures": 0,
        "observed_passed_window_fixtures": 0,
        "missed_known_window_fixtures": 0,
        "ambiguous_schedule_fixtures": 0,
    }
    for game_id, history in schedules.items():
        if not any(seen <= start - min_lead for seen, start, _, _ in history):
            continue
        counts["known_fixtures"] += 1
        history.sort()
        starts = [row[1] for row in history]
        identities = {(row[2], row[3]) for row in history}
        if len(identities) > 1 or max(starts) - min(starts) > _START_TOLERANCE:
            counts["ambiguous_schedule_fixtures"] += 1
        elif cutoff < review_at and history[-1][1] - min_lead >= cutoff:
            counts["known_future_fixtures"] += 1
        else:
            counts["passed_window_fixtures"] += 1
            outcome = (
                "observed_passed_window_fixtures"
                if game_id in observed_games
                else "missed_known_window_fixtures"
            )
            counts[outcome] += 1
    return counts
