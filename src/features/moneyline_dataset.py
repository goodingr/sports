"""
Feature engineering for moneyline betting models.
Generates training/inference datasets from raw data sources.
"""
import argparse
import logging
from typing import Iterable

import pandas as pd

import src.features.dataset.cfb as cfb
import src.features.dataset.nba as nba
import src.features.dataset.ncaab as ncaab
import src.features.dataset.nfl as nfl
import src.features.dataset.nhl as nhl
import src.features.dataset.shared as shared
import src.features.dataset.soccer as soccer

LOGGER = logging.getLogger(__name__)


def build_dataset(seasons: Iterable[int], league: str = "NFL") -> pd.DataFrame:
    paths = shared.DatasetPaths(shared.seasons_tuple(seasons), league)
    league_code = league.upper()
    
    if league_code == "NFL":
        return nfl.build_dataset(paths, seasons)
    elif league_code == "NBA":
        return nba.build_dataset(paths, seasons)
    elif league_code == "CFB":
        return cfb.build_dataset(paths, seasons)
    elif league_code == "NCAAB":
        return ncaab.build_dataset(paths, seasons)
    elif league_code == "NHL":
        return nhl.build_dataset(paths, seasons)
    elif league_code in shared.SOCCER_LEAGUES:
        return soccer.build_dataset(paths, seasons)
    else:
        raise ValueError(f"Unsupported league: {league}")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build processed dataset for moneyline modeling")
    parser.add_argument(
        "--seasons",
        nargs="+",
        default=list(range(1999, 2024)),
        help="Seasons to include",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
    )
    parser.add_argument(
        "--league",
        default="NFL",
        choices=["NFL", "NBA", "NHL", "NCAAB", "CFB", "EPL", "LALIGA", "BUNDESLIGA", "SERIEA", "LIGUE1"],
        help="League to build the dataset for",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level))
    seasons = [int(season) for season in args.seasons]
    dataset = build_dataset(seasons, league=args.league)
    if dataset.empty:
        LOGGER.warning("No rows produced for %s seasons %s", args.league.upper(), seasons)
        raise SystemExit(2)


if __name__ == "__main__":
    main()
