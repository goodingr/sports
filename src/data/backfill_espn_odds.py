"""Backfill historical ESPN odds snapshots (if available)."""

from __future__ import annotations

import argparse
import logging
from datetime import datetime, timedelta
from typing import Iterable

from src.data.sources.espn_odds import ingest_nfl, ingest_nba

LOGGER = logging.getLogger(__name__)


def _date_range(start: datetime, end: datetime, step_days: int = 1) -> Iterable[datetime]:
    current = start
    while current <= end:
        yield current
        current += timedelta(days=step_days)


def backfill_espn_odds(
    league: str,
    start_date: str,
    end_date: str,
    *,
    step_days: int = 1,
    sleep: float = 1.0,
) -> None:
    """Backfill ESPN odds for a date range."""
    start = datetime.fromisoformat(start_date.replace("Z", "+00:00"))
    end = datetime.fromisoformat(end_date.replace("Z", "+00:00"))
    total_days = (end - start).days + 1

    LOGGER.info(
        "Backfilling ESPN %s odds from %s to %s (%d days, step=%d)",
        league.upper(),
        start_date,
        end_date,
        total_days,
        step_days,
    )

    if league.lower() == "nfl":
        handler = ingest_nfl
    elif league.lower() == "nba":
        handler = ingest_nba
    else:
        raise ValueError(f"Unsupported league: {league}")

    dates = list(_date_range(start, end, step_days))
    LOGGER.info("Processing %d dates", len(dates))

    for i, date in enumerate(dates, 1):
        # ESPN API expects YYYYMMDD format, not YYYY-MM-DD
        date_str = date.strftime("%Y%m%d")
        date_display = date.strftime("%Y-%m-%d")
        LOGGER.info("[%d/%d] Fetching ESPN odds for %s", i, len(dates), date_display)

        try:
            handler(date=date_str, timeout=30)
            if i < len(dates):
                import time

                time.sleep(sleep)
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Failed to fetch ESPN odds for %s: %s", date_str, exc)

    LOGGER.info("Backfill complete")


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill historical ESPN odds")
    parser.add_argument("start_date", help="Start date (YYYY-MM-DD)")
    parser.add_argument("end_date", help="End date (YYYY-MM-DD)")
    parser.add_argument("--league", choices=["nfl", "nba"], required=True, help="League to backfill")
    parser.add_argument("--step-days", type=int, default=1, help="Days between requests")
    parser.add_argument("--sleep", type=float, default=1.0, help="Seconds to sleep between requests")

    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")

    backfill_espn_odds(
        args.league,
        args.start_date,
        args.end_date,
        step_days=args.step_days,
        sleep=args.sleep,
    )


if __name__ == "__main__":
    main()

