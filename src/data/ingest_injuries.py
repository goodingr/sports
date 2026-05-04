"""Ingest NBA injuries from ESPN."""

from __future__ import annotations

import argparse
import logging
import sys

from src.data.availability_quality import (
    DEFAULT_LOOKAHEAD_DAYS,
    DEFAULT_MAX_STALE_DAYS,
    DEFAULT_MIN_COVERAGE,
    warn_if_low_availability_coverage,
)
from src.data.sources import nba_injuries_espn


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest NBA injuries from ESPN.")
    parser.add_argument("--timeout", type=int, default=60)
    parser.add_argument("--lookahead-days", type=int, default=DEFAULT_LOOKAHEAD_DAYS)
    parser.add_argument("--max-stale-days", type=int, default=DEFAULT_MAX_STALE_DAYS)
    parser.add_argument("--min-coverage", type=float, default=DEFAULT_MIN_COVERAGE)
    parser.add_argument(
        "--skip-availability-quality",
        action="store_true",
        help="Skip the warning-only NBA availability coverage report after ingestion.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level))
    try:
        output_dir = nba_injuries_espn.ingest(timeout=args.timeout)
    except Exception as exc:  # noqa: BLE001
        logging.exception("Failed to ingest NBA injuries: %s", exc)
        return 1
    logging.info("NBA injury ingestion completed: %s", output_dir)
    if not args.skip_availability_quality:
        warn_if_low_availability_coverage(
            leagues=["NBA"],
            lookahead_days=args.lookahead_days,
            max_stale_days=args.max_stale_days,
            min_coverage=args.min_coverage,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
