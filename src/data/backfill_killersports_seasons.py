"""Backfill Killersports odds season-by-season for a league."""

from __future__ import annotations

import argparse
import logging
import time
from typing import Optional

from src.data.sources import killersports

LOGGER = logging.getLogger(__name__)


def backfill_seasons(
    *,
    league: str,
    start_season: int,
    end_season: int,
    show: int = 5000,
    future: int = 0,
    timeout: int = 60,
    sleep: float = 2.0,
) -> None:
    if start_season > end_season:
        raise ValueError("start_season must be <= end_season")
    seasons = list(range(start_season, end_season + 1))
    LOGGER.info(
        "Fetching Killersports %s seasons %s-%s (%d batches)",
        league.upper(),
        start_season,
        end_season,
        len(seasons),
    )
    for idx, season in enumerate(seasons, 1):
        try:
            LOGGER.info("[%d/%d] Fetching season %s", idx, len(seasons), season)
            killersports.ingest(
                league=league,
                season=season,
                show=show,
                future=future,
                timeout=timeout,
            )
        except Exception as exc:  # noqa: BLE001
            LOGGER.exception("Failed to fetch %s season %s: %s", league.upper(), season, exc)
        if idx < len(seasons) and sleep > 0:
            time.sleep(sleep)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--league", required=True, help="League code (e.g., NBA, NHL, NCAAB).")
    parser.add_argument("--start-season", type=int, required=True, help="First season (e.g., 2018).")
    parser.add_argument("--end-season", type=int, required=True, help="Last season (e.g., 2024).")
    parser.add_argument("--show", type=int, default=5000, help="Killersports 'show' parameter (rows).")
    parser.add_argument("--future", type=int, default=0, help="Killersports 'future' parameter for upcoming games.")
    parser.add_argument("--timeout", type=int, default=60, help="HTTP timeout per request.")
    parser.add_argument("--sleep", type=float, default=2.0, help="Seconds to sleep between seasons.")
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging verbosity.",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level))
    backfill_seasons(
        league=args.league,
        start_season=args.start_season,
        end_season=args.end_season,
        show=args.show,
        future=args.future,
        timeout=args.timeout,
        sleep=args.sleep,
    )


if __name__ == "__main__":
    main()
