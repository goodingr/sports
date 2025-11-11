"""Normalize Understat JSON archives into structured parquet files."""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
from typing import Iterable, List

import pandas as pd

from .config import PROCESSED_DATA_DIR

LOGGER = logging.getLogger(__name__)

RAW_BASE = Path("data/raw/sources/understat")
OUT_BASE = PROCESSED_DATA_DIR / "external" / "understat"


def _available_leagues() -> List[str]:
    if not RAW_BASE.exists():
        return []
    return sorted({p.name for p in RAW_BASE.iterdir() if p.is_dir()})


def _available_seasons(league: str) -> List[str]:
    league_dir = RAW_BASE / league
    if not league_dir.exists():
        return []
    seasons = set()
    for path in league_dir.glob("*_dates.json"):
        seasons.add(path.name.split("_")[0])
    return sorted(seasons)


def _load_json(path: Path) -> list | dict:
    with path.open(encoding="utf-8") as fh:
        return json.load(fh)


def _write_parquet(df: pd.DataFrame, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_path, index=False)
    LOGGER.info("Wrote %s rows to %s", len(df), out_path)


def _ingest_dates(league: str, season: str) -> None:
    src = RAW_BASE / league / f"{season}_dates.json"
    if not src.exists():
        LOGGER.warning("Missing dates data for %s %s", league, season)
        return
    data = _load_json(src)
    df = pd.DataFrame(data)
    out = OUT_BASE / league / f"{season}_dates.parquet"
    _write_parquet(df, out)


def _ingest_teams(league: str, season: str) -> None:
    src = RAW_BASE / league / f"{season}_teams.json"
    if not src.exists():
        LOGGER.warning("Missing teams data for %s %s", league, season)
        return
    raw = _load_json(src)
    # Understat stores teams keyed by numeric id
    records = []
    for team in raw.values():
        team_id = team.get("id")
        title = team.get("title")
        history = team.get("history", [])
        for entry in history:
            entry = dict(entry)
            entry["team_id"] = team_id
            entry["team_title"] = title
            entry["league"] = league
            entry["season"] = season
            records.append(entry)
    df = pd.DataFrame(records)
    out = OUT_BASE / league / f"{season}_teams.parquet"
    _write_parquet(df, out)


def _ingest_players(league: str, season: str) -> None:
    src = RAW_BASE / league / f"{season}_players.json"
    if not src.exists():
        LOGGER.warning("Missing players data for %s %s", league, season)
        return
    data = _load_json(src)
    df = pd.DataFrame(data)
    df["league"] = league
    df["season"] = season
    out = OUT_BASE / league / f"{season}_players.parquet"
    _write_parquet(df, out)


def ingest(leagues: Iterable[str], seasons: Iterable[str] | None = None) -> None:
    for league in leagues:
        avail = _available_seasons(league)
        if not avail:
            LOGGER.warning("No raw Understat data found for league %s", league)
            continue
        target_seasons = list(seasons) if seasons else avail
        for season in target_seasons:
            if season not in avail:
                LOGGER.warning("Season %s missing for league %s", season, league)
                continue
            LOGGER.info("Processing Understat %s %s", league, season)
            _ingest_dates(league, season)
            _ingest_teams(league, season)
            _ingest_players(league, season)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Normalize Understat JSON archives")
    parser.add_argument(
        "--leagues",
        default=None,
        help="Comma-separated Understat league folder names (default: all detected)",
    )
    parser.add_argument(
        "--seasons",
        default=None,
        help="Comma-separated season identifiers (e.g., 2021,2022,2023); default=all available",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level))
    leagues = (
        [x.strip() for x in args.leagues.split(",") if x.strip()]
        if args.leagues
        else _available_leagues()
    )
    if not leagues:
        LOGGER.error("No leagues specified or detected in %s", RAW_BASE)
        raise SystemExit(1)
    seasons = (
        [x.strip() for x in args.seasons.split(",") if x.strip()]
        if args.seasons
        else None
    )
    ingest(leagues, seasons)


if __name__ == "__main__":
    main()
