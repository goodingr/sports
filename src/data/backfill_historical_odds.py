"""Backfill historical NBA odds from multiple sources."""

from __future__ import annotations

import argparse
import logging
import time
from datetime import datetime, timedelta
from functools import partial
from typing import Callable, Iterable

LOGGER = logging.getLogger(__name__)


def _date_range(start: datetime, end: datetime, step_days: int = 1) -> Iterable[datetime]:
    current = start
    while current <= end:
        yield current
        current += timedelta(days=step_days)


def backfill_historical_odds(
    source_handler: Callable,
    league: str,
    start_date: str,
    end_date: str,
    *,
    step_days: int = 1,
    sleep: float = 2.0,
    date_param_name: str = "date",
) -> None:
    """Backfill historical odds from a source that supports date parameters."""
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    dates = list(_date_range(start, end, step_days=step_days))
    
    LOGGER.info(
        "Backfilling %s historical odds from %s to %s (%d days, step=%d)",
        league.upper(),
        start_date,
        end_date,
        len(dates),
        step_days,
    )
    
    for i, date in enumerate(dates, 1):
        date_str = date.strftime("%Y-%m-%d")
        LOGGER.info("[%d/%d] Fetching %s odds for %s", i, len(dates), league.upper(), date_str)
        
        try:
            kwargs = {date_param_name: date_str, "timeout": 30}
            source_handler(**kwargs)
            
            if i < len(dates):
                time.sleep(sleep)
        except Exception as exc:  # noqa: BLE001
            LOGGER.error("Error fetching %s for %s: %s", league.upper(), date_str, exc)
            continue
    
    LOGGER.info("Backfill complete")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill historical odds from multiple sources")
    parser.add_argument(
        "--source",
        choices=["oddsshark", "vegasinsider", "covers", "killersports", "teamrankings_trends", "espn", "all"],
        default="all",
        help="Source to backfill from",
    )
    parser.add_argument(
        "--league",
        choices=["nba", "nfl", "cfb", "nhl"],
        default="nba",
        help="League to backfill",
    )
    parser.add_argument(
        "start_date",
        type=str,
        help="Start date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "end_date",
        type=str,
        help="End date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--step-days",
        type=int,
        default=1,
        help="Step size in days (default: 1)",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=2.0,
        help="Sleep seconds between requests (default: 2.0)",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(levelname)s:%(name)s:%(message)s",
    )
    
    sources = []
    league = args.league

    if args.source == "all" or args.source == "espn":
        from src.data.sources.espn_odds import ingest_nba, ingest_nfl, ingest_cfb

        if league == "nba":
            sources.append(("ESPN", ingest_nba))
        elif league == "nfl":
            sources.append(("ESPN", ingest_nfl))
        elif league == "cfb":
            sources.append(("ESPN", ingest_cfb))

    if league in {"nba", "nfl"}:
        if args.source == "all" or args.source == "oddsshark":
            from src.data.sources.oddsshark import ingest as oddsshark_ingest

            sources.append(("OddsShark", oddsshark_ingest))

        if args.source == "all" or args.source == "vegasinsider":
            from src.data.sources.vegasinsider import ingest as vegasinsider_ingest

            sources.append(("VegasInsider", vegasinsider_ingest))

        if args.source == "all" or args.source == "covers":
            if league == "nba":
                from src.data.sources.covers import ingest_nba as covers_ingest
            else:
                from src.data.sources.covers import ingest_nfl as covers_ingest

            sources.append(("Covers", covers_ingest))

        if args.source == "all" or args.source == "teamrankings_trends":
            from src.data.sources.teamrankings_trends import ingest as trends_ingest

            sources.append(("TeamRankings Trends", trends_ingest))

    if league in {"nba", "nhl"} and (args.source == "all" or args.source == "killersports"):
        from src.data.sources.killersports import ingest as killersports_ingest

        ks_kwargs = {"league": league.upper()}
        if league == "nba":
            ks_kwargs.pop("league")
        ks_handler: Callable = partial(killersports_ingest, **ks_kwargs) if ks_kwargs else killersports_ingest
        sources.append(("Killersports", ks_handler))

    if not sources:
        LOGGER.warning("No sources available for league=%s and source=%s", league, args.source)
        return

    for source_name, handler in sources:
        LOGGER.info("Backfilling from %s", source_name)
        try:
            backfill_historical_odds(
                handler,
                args.league,
                args.start_date,
                args.end_date,
                step_days=args.step_days,
                sleep=args.sleep,
            )
        except Exception as exc:  # noqa: BLE001
            LOGGER.error("Failed to backfill from %s: %s", source_name, exc)
            continue


if __name__ == "__main__":
    main()

