"""Report observed coverage of prospective paper-capture runs."""

from __future__ import annotations

import sqlite3
from collections import Counter
from datetime import datetime, timezone
from typing import Any

from src.predict.paper_schedule import known_window_coverage

_FULLY_EVALUATED = {"recorded", "already_recorded", "below_edge_threshold"}
_COVERAGE_TABLES = {"paper_capture_runs", "paper_fixture_observations"}


def _utc(value: str | datetime) -> datetime:
    timestamp = datetime.fromisoformat(value) if isinstance(value, str) else value
    if timestamp.utcoffset() is None:
        raise ValueError("Paper coverage timestamps must include a time zone")
    return timestamp.astimezone(timezone.utc)


def capture_coverage(
    conn: sqlite3.Connection,
    trial_id: str,
    registered_at: datetime,
    review_at: datetime,
    observed_at: datetime,
    *,
    min_lead_minutes: int = 15,
    max_lead_hours: int = 12,
) -> dict[str, Any]:
    """Summarize retained observations without reconstructing past opportunities."""
    registered_at = _utc(registered_at)
    review_at = _utc(review_at)
    observed_at = _utc(observed_at)
    tables = {
        row["name"]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name IN (?, ?)",
            tuple(sorted(_COVERAGE_TABLES)),
        )
    }
    if tables != _COVERAGE_TABLES:
        return {"available": False}

    rows = conn.execute(
        """
        SELECT r.run_id, r.observed_at, r.status, o.game_id, o.outcome
        FROM paper_capture_runs r
        LEFT JOIN paper_fixture_observations o ON o.run_id = r.run_id
        WHERE r.trial_id = ?
        ORDER BY r.run_id, o.game_id
        """,
        (trial_id,),
    )
    runs = {}
    observed_fixtures = set()
    evaluated_fixtures = set()
    below_edge_fixtures = set()
    outcomes: Counter[str] = Counter()
    first_fixture_observed_at = None
    for row in rows:
        captured_at = _utc(row["observed_at"])
        if not registered_at <= captured_at <= observed_at or captured_at >= review_at:
            continue
        runs[row["run_id"]] = (captured_at, row["status"])
        game_id = row["game_id"]
        if game_id is None:
            continue
        if first_fixture_observed_at is None or captured_at < first_fixture_observed_at:
            first_fixture_observed_at = captured_at
        observed_fixtures.add(game_id)
        outcomes[row["outcome"]] += 1
        if row["outcome"] in _FULLY_EVALUATED:
            evaluated_fixtures.add(game_id)
        if row["outcome"] == "below_edge_threshold":
            below_edge_fixtures.add(game_id)

    first_run_at = min((value[0] for value in runs.values()), default=None)
    last_run_id = max(runs, key=lambda run_id: (runs[run_id][0], run_id), default=None)
    last_run_at, last_run_status = runs[last_run_id] if last_run_id is not None else (None, None)
    return {
        "available": True,
        "runs": len(runs),
        "first_run_at": first_run_at.isoformat() if first_run_at is not None else None,
        "first_fixture_observed_at": (
            first_fixture_observed_at.isoformat() if first_fixture_observed_at is not None else None
        ),
        "last_run_at": last_run_at.isoformat() if last_run_at is not None else None,
        "last_run_status": last_run_status,
        "attempts_by_outcome": dict(sorted(outcomes.items())),
        "observed_fixtures": len(observed_fixtures),
        "fully_evaluated_fixtures": len(evaluated_fixtures),
        "below_edge_fixtures": len(below_edge_fixtures),
        "fully_evaluated_share": (
            len(evaluated_fixtures) / len(observed_fixtures) if observed_fixtures else None
        ),
        "known_windows": known_window_coverage(
            conn,
            trial_id,
            registered_at,
            review_at,
            observed_at,
            min_lead_minutes=min_lead_minutes,
            max_lead_hours=max_lead_hours,
        ),
    }
