"""Load NCAA men's regular-season schedules/results from the March Madness dataset."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Optional

import pandas as pd

from src.db.loaders import load_ncaab_regular_season_results

LOGGER = logging.getLogger(__name__)


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing required file: {path}")
    return pd.read_csv(path)


def load_ncaab_from_kaggle(
    base_dir: Path,
    *,
    start_season: Optional[int] = None,
    end_season: Optional[int] = None,
) -> None:
    seasons_path = base_dir / "MSeasons.csv"
    results_path = base_dir / "MRegularSeasonCompactResults.csv"

    LOGGER.info("Loading NCAA data from %s", base_dir)
    seasons_df = _read_csv(seasons_path)
    compact_df = _read_csv(results_path)

    if start_season and end_season and start_season > end_season:
        raise ValueError("start_season cannot be greater than end_season")

    if start_season:
        compact_df = compact_df[compact_df["Season"] >= start_season]
    if end_season:
        compact_df = compact_df[compact_df["Season"] <= end_season]

    if compact_df.empty:
        LOGGER.warning("No NCAA rows after filtering seasons. Nothing to load.")
        return

    selected_seasons = sorted(compact_df["Season"].unique().tolist())
    LOGGER.info(
        "Preparing to load %d rows covering %d seasons (%s – %s)",
        len(compact_df),
        len(selected_seasons),
        min(selected_seasons),
        max(selected_seasons),
    )

    seasons_subset = seasons_df[seasons_df["Season"].isin(selected_seasons)]
    load_ncaab_regular_season_results(
        compact_df,
        seasons_subset,
        league="NCAAB",
        sport_name="College Basketball",
        default_market="spread",
        source_version=f"mm2025[{min(selected_seasons)}-{max(selected_seasons)}]",
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--base-dir",
        type=Path,
        default=Path("data/external/mm2025"),
        help="Directory containing Kaggle March Madness CSV files.",
    )
    parser.add_argument("--start-season", type=int, help="First season (e.g., 2015) to load.")
    parser.add_argument("--end-season", type=int, help="Last season (e.g., 2024) to load.")
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity.",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level.upper()))
    load_ncaab_from_kaggle(
        args.base_dir,
        start_season=args.start_season,
        end_season=args.end_season,
    )


if __name__ == "__main__":
    main()
