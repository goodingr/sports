#!/usr/bin/env python
"""Batch-import Killersports odds CSVs into the database."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Dict

from src.db import loaders


def _accumulate(dest: Dict[str, int], stats: Dict[str, int]) -> None:
    for key, value in stats.items():
        if value is None:
            continue
        dest[key] = dest.get(key, 0) + int(value)


def main() -> None:
    parser = argparse.ArgumentParser(description="Load Killersports odds CSVs into the DB")
    parser.add_argument(
        "--root",
        type=Path,
        default=Path("data/raw/sources/nhl/killersports"),
        help="Root directory containing Killersports snapshot folders",
    )
    parser.add_argument(
        "--pattern",
        default="*/odds.csv",
        help="Glob pattern relative to --root for locating CSVs",
    )
    parser.add_argument(
        "--league",
        default="NHL",
        help="League code to associate with the loaded data (default: NHL)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional limit on the number of files to process",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
    )
    args = parser.parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level))

    files = sorted(args.root.glob(args.pattern))
    if args.limit:
        files = files[: args.limit]

    if not files:
        logging.error("No Killersports CSVs found under %s matching %s", args.root, args.pattern)
        raise SystemExit(1)

    totals: Dict[str, int] = {}

    for csv_path in files:
        stats = loaders.import_killersports_odds(csv_path, league=args.league)
        logging.info("Loaded %s -> %s", csv_path, stats)
        _accumulate(totals, stats)

    logging.info("Overall totals: %s", totals)


if __name__ == "__main__":
    main()

