"""Preview aggregate quote coverage for the frozen NCAA basketball forecast."""

from __future__ import annotations

import argparse
import json
import sqlite3
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path

from reports.research.ncaab_imbalance_offset import experiment


def preview(db_path: Path, *, now: datetime) -> dict[str, int | str | None]:
    protocol = json.loads(experiment.PROTOCOL.read_text(encoding="utf-8"))
    holdout = protocol["prospective_holdout"]
    start = datetime.fromisoformat(holdout["first_quote_and_kickoff_not_before_utc"])
    end = datetime.fromisoformat(holdout["quote_and_kickoff_end_exclusive_utc"])
    quotes: dict[str, experiment.Quote] = {}
    counts: dict[str, int] = {}
    if now >= start:
        db_path = db_path.resolve()
        with closing(sqlite3.connect(f"{db_path.as_uri()}?mode=ro", uri=True)) as conn:
            quotes, counts = experiment._quotes(
                conn, db_path.parent, protocol, start, end, first_capture=start
            )
    capture_times = [quote.fetched for quote in quotes.values()]
    return {
        "verified_odds_receipts": counts.get("verified_odds_receipts", 0),
        "unverified_odds_registrations": counts.get("unverified_odds_registrations", 0),
        "duplicate_odds_registrations": counts.get("duplicate_odds_registrations", 0),
        "eligible_fixtures": len(quotes),
        "eastern_fixture_dates": len(
            {quote.kickoff.astimezone(experiment.EASTERN).date() for quote in quotes.values()}
        ),
        "ambiguous_quote_fixtures": counts.get("ambiguous_quote_fixtures", 0),
        "earliest_eligible_capture_utc": min(capture_times).isoformat() if capture_times else None,
        "latest_eligible_capture_utc": max(capture_times).isoformat() if capture_times else None,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, required=True)
    args = parser.parse_args()
    try:
        result = preview(args.db, now=datetime.now(timezone.utc))
    except (OSError, ValueError, sqlite3.Error) as exc:
        parser.exit(1, f"{exc}\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
