"""CLI helper for loading external historical datasets into the database."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, Optional, Sequence

from src.db.loaders import (
    import_football_data_totals,
    import_sbr_odds,
    import_teamrankings_picks,
)


def _run_import(
    source: str,
    *,
    leagues: Optional[Sequence[str]],
    seasons: Optional[Sequence[str]],
    base_dir: Optional[Path],
    tolerance_seconds: int,
) -> Dict[str, Dict[str, int]]:
    if source == "football-data":
        return import_football_data_totals(
            leagues=leagues,
            base_dir=base_dir,
            tolerance_seconds=tolerance_seconds,
        )
    if source == "teamrankings":
        return import_teamrankings_picks(
            leagues=leagues,
            base_dir=base_dir,
            tolerance_seconds=tolerance_seconds,
        )
    if source == "sbr":
        league = leagues[0] if leagues else "NBA"
        return import_sbr_odds(
            league=league,
            seasons=seasons,
            base_dir=base_dir,
            tolerance_seconds=tolerance_seconds,
        )
    raise ValueError(f"Unsupported source {source}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Load external datasets (football-data, teamrankings) into the DB",
    )
    parser.add_argument(
        "--source",
        choices=("football-data", "teamrankings", "sbr"),
        required=True,
        help="Which external dataset to import",
    )
    parser.add_argument(
        "--leagues",
        nargs="+",
        help="Optional list of leagues to load (ex: EPL, premier-league, NBA, nfl)",
    )
    parser.add_argument(
        "--seasons",
        nargs="+",
        help="Optional list of seasons (for sources that support it, e.g., 2022-23 for SBR)",
    )
    parser.add_argument(
        "--base-dir",
        type=Path,
        default=None,
        help="Override the default data directory for the selected source",
    )
    parser.add_argument(
        "--tolerance-seconds",
        type=int,
        default=6 * 3600,
        help="Matching tolerance for kickoff timestamps (default: 6 hours)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print summary as JSON instead of key=value pairs",
    )
    args = parser.parse_args()

    stats = _run_import(
        args.source,
        leagues=args.leagues,
        seasons=args.seasons,
        base_dir=args.base_dir,
        tolerance_seconds=args.tolerance_seconds,
    )

    if args.json:
        print(json.dumps(stats, indent=2, sort_keys=True))
        return

    if not stats:
        print("No records loaded.")
        return

    for league, league_stats in stats.items():
        formatted = ", ".join(f"{key}={value}" for key, value in league_stats.items())
        print(f"{league}: {formatted}")


if __name__ == "__main__":
    main()
