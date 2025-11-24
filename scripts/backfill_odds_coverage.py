import logging
import sqlite3
from pathlib import Path
import pandas as pd
from src.db.core import connect

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

def backfill_odds_coverage():
    """
    Backfill missing odds in game_results table from the odds table.
    This handles cases where odds were ingested before game_results rows were created.
    """
    with connect() as conn:
        # Find games with missing odds in game_results but present in odds table
        query = """
        SELECT DISTINCT g.game_id
        FROM games g
        JOIN game_results r ON g.game_id = r.game_id
        JOIN odds o ON g.game_id = o.game_id
        WHERE r.home_moneyline_close IS NULL
           OR r.away_moneyline_close IS NULL
        """
        games_to_update = pd.read_sql_query(query, conn)
        
        if games_to_update.empty:
            LOGGER.info("No games found requiring odds backfill.")
            return

        LOGGER.info("Found %d games requiring odds backfill", len(games_to_update))
        
        # Update logic similar to load_odds_snapshot but in batch or per game
        # We can use a correlated subquery update for efficiency
        
        update_query = """
        UPDATE game_results
        SET 
            home_moneyline_close = (
                SELECT price_american 
                FROM odds o 
                JOIN odds_snapshots s ON o.snapshot_id = s.snapshot_id
                WHERE o.game_id = game_results.game_id 
                AND o.market = 'h2h' AND o.outcome = 'home'
                ORDER BY s.fetched_at_utc DESC 
                LIMIT 1
            ),
            away_moneyline_close = (
                SELECT price_american 
                FROM odds o 
                JOIN odds_snapshots s ON o.snapshot_id = s.snapshot_id
                WHERE o.game_id = game_results.game_id 
                AND o.market = 'h2h' AND o.outcome = 'away'
                ORDER BY s.fetched_at_utc DESC 
                LIMIT 1
            ),
            total_close = (
                SELECT line 
                FROM odds o 
                JOIN odds_snapshots s ON o.snapshot_id = s.snapshot_id
                WHERE o.game_id = game_results.game_id 
                AND o.market = 'totals' AND o.outcome = 'Over'
                ORDER BY s.fetched_at_utc DESC 
                LIMIT 1
            )
        WHERE game_id IN (
            SELECT DISTINCT g.game_id
            FROM games g
            JOIN game_results r ON g.game_id = r.game_id
            JOIN odds o ON g.game_id = o.game_id
            WHERE r.home_moneyline_close IS NULL
               OR r.away_moneyline_close IS NULL
        )
        """
        
        cursor = conn.execute(update_query)
        LOGGER.info("Updated %d rows in game_results", cursor.rowcount)

if __name__ == "__main__":
    backfill_odds_coverage()
