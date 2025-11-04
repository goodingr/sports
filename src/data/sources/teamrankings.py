"""Scrape TeamRankings efficiency tables for NFL and NBA."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Iterable, List, Sequence

import pandas as pd
import requests

from .utils import DEFAULT_HEADERS, SourceDefinition, source_run, write_dataframe


LOGGER = logging.getLogger(__name__)

BASE_URL = "https://www.teamrankings.com/{league}/team/{stat}"

DEFAULT_STATS = {
    "nfl": ["points-per-game", "yards-per-play"],
    "nba": ["points-per-game", "opponent-points-per-game"],
}


def _normalize_stats(league: str, stats: Iterable[str] | None) -> Sequence[str]:
    if stats:
        return [stat.strip().lower().replace(" ", "-") for stat in stats]
    return DEFAULT_STATS.get(league, ["points-per-game"])


def _fetch_table(league: str, stat: str, *, timeout: int) -> pd.DataFrame:
    url = BASE_URL.format(league=league, stat=stat)
    LOGGER.info("Fetching TeamRankings %s %s", league.upper(), stat)
    response = requests.get(url, timeout=timeout, headers=DEFAULT_HEADERS)
    response.raise_for_status()
    tables = pd.read_html(response.text)
    if not tables:
        LOGGER.warning("No tables found for %s %s", league, stat)
        return pd.DataFrame()
    table = tables[0]
    table["source_url"] = url
    table["retrieved_at"] = datetime.utcnow().isoformat()
    return table


def _ingest_teamrankings(
    *,
    league: str,
    stats: Iterable[str] | None,
    timeout: int,
) -> str:
    definition = SourceDefinition(
        key=f"teamrankings_{league}",
        name=f"TeamRankings {league.upper()} metrics",
        league=league.upper(),
        category="advanced_metrics",
        url="https://www.teamrankings.com/",
        default_frequency="daily",
        storage_subdir=f"{league}/teamrankings",
    )

    stat_list: List[str] = list(_normalize_stats(league, stats))
    output_dir = ""
    with source_run(definition) as run:
        output_dir = str(run.storage_dir)
        total_rows = 0
        for stat in stat_list:
            df = _fetch_table(league, stat, timeout=timeout)
            if df.empty:
                continue

            filename = f"{stat.replace('/', '_')}.csv"
            path = run.make_path(filename)
            write_dataframe(df, path)
            run.record_file(
                path,
                metadata={"stat": stat, "row_count": len(df)},
                records=len(df),
            )
            total_rows += len(df)

        if total_rows:
            run.set_records(total_rows)
            run.set_message(f"Captured {total_rows} TeamRankings rows")
        run.set_raw_path(run.storage_dir)

    return output_dir


def ingest_nfl(*, stats: Iterable[str] | None = None, timeout: int = 30) -> str:
    return _ingest_teamrankings(league="nfl", stats=stats, timeout=timeout)


def ingest_nba(*, stats: Iterable[str] | None = None, timeout: int = 30) -> str:
    return _ingest_teamrankings(league="nba", stats=stats, timeout=timeout)


__all__ = ["ingest_nfl", "ingest_nba"]

