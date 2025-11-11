"""Normalize Football-Data.co.uk CSV odds into parquet tables."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Iterable

import pandas as pd

from .config import PROCESSED_DATA_DIR

LOGGER = logging.getLogger(__name__)

RAW_BASE = Path("data/raw/sources/football-data")
OUT_BASE = PROCESSED_DATA_DIR / "external" / "football_data"


def _available_leagues() -> list[str]:
    if not RAW_BASE.exists():
        return []
    return sorted({p.name for p in RAW_BASE.iterdir() if p.is_dir()})


def _available_files(league: str) -> list[Path]:
    league_dir = RAW_BASE / league
    if not league_dir.exists():
        return []
    return sorted(league_dir.glob("*.csv"))


def _ingest_file(path: Path, league: str) -> None:
    LOGGER.info("Processing %s", path)
    df = pd.read_csv(path)
    df.columns = [c.strip() for c in df.columns]
    df["league"] = league
    # Many CSVs include an ISO or dd/mm/yy string; preserve raw string and parse date if possible.
    if "Date" in df.columns:
        df["match_date"] = pd.to_datetime(df["Date"], errors="coerce", dayfirst=True)
    out_path = OUT_BASE / league / (path.stem + ".parquet")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_path, index=False)
    LOGGER.info("Wrote %s rows to %s", len(df), out_path)


def ingest(leagues: Iterable[str] | None = None) -> None:
    target_leagues = list(leagues) if leagues else _available_leagues()
    if not target_leagues:
        LOGGER.error("No football-data leagues found at %s", RAW_BASE)
        raise SystemExit(1)
    for league in target_leagues:
        files = _available_files(league)
        if not files:
            LOGGER.warning("No CSV files for league %s", league)
            continue
        for path in files:
            _ingest_file(path, league)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Normalize football-data.co.uk odds CSVs")
    parser.add_argument(
        "--leagues",
        default=None,
        help="Comma-separated league folder names (default: all detected)",
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
        else None
    )
    ingest(leagues)


if __name__ == "__main__":
    main()
