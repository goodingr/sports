"""Log aggregate timing for the scheduled pipeline's exact fresh NBA odds receipt."""

import argparse
import json
import sqlite3
from collections import Counter
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.db.core import resolve_db_path


def _utc(text: str) -> datetime:
    if not isinstance(text, str):
        raise ValueError("Expected a timestamp string")
    value = datetime.fromisoformat(text)
    if value.utcoffset() is None:
        raise ValueError("Expected an aware timestamp")
    return value.astimezone(timezone.utc)


def _age(fetched: datetime, observed: datetime) -> float:
    if observed < fetched:
        raise ValueError("An observed clock precedes the odds receipt")
    return (observed - fetched).total_seconds()


def timing_report(
    conn: sqlite3.Connection,
    receipt_path: str,
    *,
    paid_started: datetime | None = None,
    paid_finished: datetime | None = None,
) -> dict[str, Any]:
    snapshots = conn.execute(
        """
        SELECT sn.snapshot_id, sn.fetched_at_utc FROM odds_snapshots sn
        JOIN sports s USING (sport_id)
        WHERE sn.raw_path = ? AND sn.source = 'the-odds-api' AND s.league = 'NBA'
        LIMIT 2
        """,
        (receipt_path,),
    ).fetchall()
    if len(snapshots) != 1:
        return {
            "status": "unavailable",
            "reason": "ambiguous_receipt" if snapshots else "receipt_not_loaded",
        }
    snapshot_id, fetched_text = snapshots[0]
    fetched = _utc(fetched_text)
    tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
    attempts = {}
    outcomes: Counter[str] = Counter()
    if {"paper_fixture_observations", "paper_capture_runs"} <= tables:
        for run_id, observed, outcome in conn.execute(
            """
            SELECT r.run_id, r.observed_at, o.outcome
            FROM paper_fixture_observations o JOIN paper_capture_runs r USING (run_id)
            WHERE o.snapshot_id = ?
            """,
            (snapshot_id,),
        ):
            attempts[run_id] = _age(fetched, _utc(observed))
            outcomes[outcome] += 1
    decisions = []
    if "paper_decisions" in tables:
        decisions = [
            _age(fetched, _utc(captured))
            for (captured,) in conn.execute(
                "SELECT captured_at FROM paper_decisions WHERE snapshot_id = ? AND odds_fetched_at = ?",
                (snapshot_id, fetched_text),
            )
        ]

    paid: dict[str, Any] = {"status": "not_run"}
    if paid_started is not None:
        paid = {"status": "command_failed", "gate_started_age_seconds": _age(fetched, paid_started)}
    if paid_started is not None and paid_finished is not None:
        paid.update(status="release_unavailable", gate_finished_age_seconds=_age(fetched, paid_finished))
        releases = []
        if "published_releases" in tables:
            for release_id, published, generated in conn.execute(
                """
                SELECT release_id, published_at, json_extract(payload, '$.generated_at')
                FROM published_releases
                WHERE julianday(published_at) BETWEEN julianday(?) AND julianday(?)
                """,
                (paid_started.isoformat(), paid_finished.isoformat()),
            ):
                if generated and paid_started <= _utc(generated) <= _utc(published) <= paid_finished:
                    releases.append((release_id, _utc(published)))
        if len(releases) > 1:
            paid["status"] = "ambiguous_release"
        elif releases:
            release_id, published_at = releases[0]
            issued = conn.execute(
                """
                SELECT COUNT(*) FROM published_picks
                WHERE first_release_id = ? AND json_extract(payload, '$.quote_snapshot_id') = ?
                """,
                (release_id, snapshot_id),
            ).fetchone()[0] if "published_picks" in tables else None
            paid.update(
                status="release_observed",
                release_age_seconds=_age(fetched, published_at),
                new_picks_from_receipt=issued,
            )
    return {
        "status": "available",
        "fetched_at_utc": fetched.isoformat(),
        "paper_attempt_ages_seconds": sorted(attempts.values()),
        "paper_decision_ages_seconds": sorted(decisions),
        "paper_fixture_outcomes": dict(outcomes),
        "paid": paid,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--receipt-file", type=Path, required=True)
    parser.add_argument("--db", type=Path)
    parser.add_argument("--paid-started-at", type=_utc)
    parser.add_argument("--paid-finished-at", type=_utc)
    args = parser.parse_args()
    try:
        receipt_path = args.receipt_file.read_text(encoding="utf-8").strip()
        if not receipt_path:
            raise FileNotFoundError
        db = resolve_db_path(args.db).resolve().as_uri() + "?mode=ro"
        with closing(sqlite3.connect(db, uri=True)) as conn:
            conn.execute("BEGIN")
            report = timing_report(
                conn, receipt_path, paid_started=args.paid_started_at, paid_finished=args.paid_finished_at
            )
    except FileNotFoundError:
        report = {"status": "unavailable", "reason": "no_fresh_receipt"}
    except (OSError, sqlite3.Error, ValueError):
        report = {"status": "unavailable", "reason": "timing_read_failed"}
    print("NBA quote timing " + json.dumps(report, sort_keys=True))


if __name__ == "__main__":
    main()
