"""CLI for initializing the SQLite warehouse."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from .core import DB_PATH, initialize, vacuum


LOGGER = logging.getLogger(__name__)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Initialize the betting analytics SQLite database")
    parser.add_argument(
        "--db-path",
        type=Path,
        default=DB_PATH,
        help="Target SQLite database file (default: data/betting.db)",
    )
    parser.add_argument(
        "--vacuum",
        action="store_true",
        help="Run VACUUM after applying schema",
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
    initialize(args.db_path)
    LOGGER.info("Applied schema to %s", args.db_path)
    if args.vacuum:
        vacuum(args.db_path)
        LOGGER.info("Vacuum completed")


if __name__ == "__main__":
    main()

