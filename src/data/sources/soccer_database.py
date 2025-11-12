"""Load and process historical soccer database (database.sqlite)."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import pandas as pd
import sqlite3

from .utils import SourceDefinition, source_run

LOGGER = logging.getLogger(__name__)

# Default location for soccer database
DEFAULT_SOCCER_DB = Path("database.sqlite")

# League ID to our league code mapping
LEAGUE_MAPPING = {
    1729: "EPL",      # England Premier League
    4769: "LIGUE1",   # France Ligue 1
    7809: "BUNDESLIGA",  # Germany 1. Bundesliga
    10257: "SERIEA",  # Italy Serie A
    21518: "LALIGA",  # Spain LIGA BBVA
}


def ingest(
    *,
    db_path: Optional[str] = None,
    timeout: int = 300,  # noqa: ARG001
) -> str:
    """Load historical soccer database and register matches/odds in the warehouse.
    
    Args:
        db_path: Path to database.sqlite file (default: database.sqlite in project root)
        timeout: Not used, kept for API consistency
    """
    definition = SourceDefinition(
        key="soccer_database",
        name="Historical soccer database",
        league="SOCCER",
        category="historical_stats",
        url="https://www.kaggle.com/datasets/hugomathien/soccer",
        default_frequency="manual",
        storage_subdir="soccer/database",
    )
    
    db_file = Path(db_path) if db_path else DEFAULT_SOCCER_DB
    
    if not db_file.exists():
        raise FileNotFoundError(f"Soccer database not found: {db_file}")
    
    output_dir = ""
    with source_run(definition) as run:
        output_dir = str(run.storage_dir)
        
        conn = sqlite3.connect(db_file)
        
        try:
            # Get matches for our target leagues
            query = """
                SELECT 
                    m.id,
                    m.league_id,
                    l.name as league_name,
                    m.season,
                    m.stage,
                    m.date,
                    m.match_api_id,
                    m.home_team_api_id,
                    m.away_team_api_id,
                    m.home_team_goal,
                    m.away_team_goal,
                    m.B365H, m.B365D, m.B365A,
                    m.BWH, m.BWD, m.BWA,
                    m.IWH, m.IWD, m.IWA,
                    m.LBH, m.LBD, m.LBA,
                    m.PSH, m.PSD, m.PSA,
                    m.WHH, m.WHD, m.WHA,
                    m.SJH, m.SJD, m.SJA,
                    m.VCH, m.VCD, m.VCA,
                    m.GBH, m.GBD, m.GBA,
                    m.BSH, m.BSD, m.BSA,
                    ht.team_long_name as home_team_name,
                    ht.team_short_name as home_team_short,
                    at.team_long_name as away_team_name,
                    at.team_short_name as away_team_short
                FROM Match m
                JOIN League l ON m.league_id = l.id
                JOIN Team ht ON m.home_team_api_id = ht.team_api_id
                JOIN Team at ON m.away_team_api_id = at.team_api_id
                WHERE m.league_id IN (1729, 4769, 7809, 10257, 21518)
                ORDER BY m.season, m.date
            """
            
            matches_df = pd.read_sql_query(query, conn)
            
            if matches_df.empty:
                LOGGER.warning("No matches found in database for target leagues")
                run.set_message("No matches found")
                return output_dir
            
            # Save raw matches data
            matches_path = run.make_path("matches.parquet")
            matches_df.to_parquet(matches_path, index=False)
            run.record_file(
                matches_path,
                metadata={"rows": len(matches_df), "leagues": matches_df["league_name"].unique().tolist()},
                records=len(matches_df),
            )
            
            LOGGER.info("Loaded %d matches from soccer database", len(matches_df))
            run.set_message(f"Loaded {len(matches_df)} matches from {len(matches_df['league_name'].unique())} leagues")
            run.set_records(len(matches_df))
            run.set_raw_path(run.storage_dir)
            
        finally:
            conn.close()
    
    return output_dir


__all__ = ["ingest"]


