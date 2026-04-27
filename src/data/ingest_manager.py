"""
Ingestion Manager
Orchestrates data ingestion by checking existing data and deciding whether to 
run a full backfill or a partial update.
"""

import argparse
import logging
import subprocess
import sys
from datetime import date, datetime
from pathlib import Path
from typing import List, Optional

import pandas as pd

from src.db.core import connect

LOGGER = logging.getLogger(__name__)

# Map leagues to their backfill modules. Current season is inferred at runtime.
LEAGUE_CONFIG = {
    "NFL": {"module": "src.data.backfill_nfl", "start_year": 1999},
    "NBA": {"module": "src.data.backfill_nba", "start_year": 2015},
    "CFB": {"module": "src.data.backfill_cfb", "start_year": 2018},
    "MLB": {"module": "src.data.backfill_mlb", "start_year": 2015},
    "SOCCER": {"module": "src.data.backfill_soccer", "start_year": 2018},
    "NCAAB": {"module": "src.data.backfill_killersports_seasons", "start_year": 2018},
    "NHL": {"module": "src.data.backfill_killersports_seasons", "start_year": 2016},
}

SOCCER_LEAGUES = ["EPL", "LALIGA", "BUNDESLIGA", "SERIEA", "LIGUE1"]

def get_latest_game_date(league: str) -> Optional[datetime]:
    """Get the date of the latest game in the database for a given league."""
    try:
        # Query to find the latest game date for a league
        query = """
            SELECT MAX(g.start_time_utc) as last_date 
            FROM games g
            JOIN sports s ON g.sport_id = s.sport_id
            WHERE s.league = ?
        """
        with connect() as conn:
            df = pd.read_sql_query(query, conn, params=(league,))
        
        if not df.empty and df.iloc[0]["last_date"]:
            return pd.to_datetime(df.iloc[0]["last_date"])
        return None
    except Exception as e:
        LOGGER.warning(f"Could not check history for {league}: {e}")
        return None

def run_module(module: str, args: List[str]):
    """Run a python module with arguments."""
    cmd = [sys.executable, "-m", module] + args
    LOGGER.info(f"Running: {' '.join(cmd)}")
    subprocess.check_call(cmd)


def infer_current_season(league: str, today: Optional[date] = None) -> int:
    """Infer the active season label used by each league's backfill scripts."""
    today = today or date.today()
    league_upper = league.upper()
    if league_upper in {"NBA", "NHL", "NCAAB"} or league_upper in SOCCER_LEAGUES:
        return today.year if today.month >= 7 else today.year - 1
    if league_upper in {"NFL", "CFB"}:
        return today.year if today.month >= 3 else today.year - 1
    return today.year


def ingest_league(league: str, force_backfill: bool = False):
    """
    Ingest data for a league.
    - If no history found or force_backfill=True: Run full backfill.
    - If history found: Run update for current season/recent days.
    """
    config = LEAGUE_CONFIG.get(league)
    if not config and league in SOCCER_LEAGUES:
        config = LEAGUE_CONFIG["SOCCER"]
    
    if not config:
        LOGGER.error(f"No configuration for league {league}")
        return

    last_date = get_latest_game_date(league)
    current_season = infer_current_season(league)
    start_year = config["start_year"]
    module = config["module"]

    if force_backfill or not last_date:
        LOGGER.info(f"[{league}] No history found or forced. Starting full backfill ({start_year}-{current_season})...")
        
        if league in SOCCER_LEAGUES:
             # Soccer script takes --leagues and --seasons
             run_module(module, ["--leagues", league, "--seasons"] + [str(y) for y in range(start_year, current_season + 1)])
        elif league == "NFL":
             # NFL script takes --seasons
             run_module(module, ["--seasons"] + [str(y) for y in range(start_year, current_season + 1)])
        elif league == "NBA":
             # NBA script takes --seasons
             run_module(module, ["--seasons"] + [str(y) for y in range(start_year, current_season + 1)])
        elif league == "CFB":
             # CFB script takes positional args for seasons
             run_module(module, [str(y) for y in range(start_year, current_season + 1)])
        elif league == "MLB":
             # MLB script takes --seasons
             run_module(module, ["--seasons"] + [str(y) for y in range(start_year, current_season + 1)])
        elif league in ("NCAAB", "NHL"):
             # Killersports script takes --league, --start-season, --end-season
             run_module(module, ["--league", league, "--start-season", str(start_year), "--end-season", str(current_season)])
             
    else:
        LOGGER.info(f"[{league}] Found history up to {last_date.date()}. Running update for current season...")
        
        # For update, we typically just fetch the current season or last X days
        # Some scripts support days_back, others just season
        
        if league in SOCCER_LEAGUES:
            # Soccer supports days_back via ESPN, or just fetch current season
            run_module(module, ["--leagues", league, "--days-back", "14"]) 
        elif league == "NBA":
            # NBA supports days_back
            run_module(module, ["--seasons", str(current_season), "--days-back", "7"])
        elif league == "NFL":
            # NFL script takes --seasons
            run_module(module, ["--seasons", str(current_season)])

def main():
    """Main entry point for ingestion manager."""
    parser = argparse.ArgumentParser(description="Manage data ingestion for all leagues")
    parser.add_argument("--leagues", nargs="+", help="Specific leagues to ingest (default: all)")
    parser.add_argument("--force-backfill", action="store_true", help="Force full backfill even if history exists")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    
    args = parser.parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level))
    
    leagues = args.leagues or ["NFL", "NBA", "CFB", "MLB"] + SOCCER_LEAGUES
    if len(leagues) == 1 and "," in leagues[0]:
        leagues = [league.strip() for league in leagues[0].split(",") if league.strip()]
    
    for league in leagues:
        try:
            ingest_league(league.upper(), args.force_backfill)
        except Exception as e:
            LOGGER.error(f"Failed to ingest {league}: {e}")


if __name__ == "__main__":
    main()
