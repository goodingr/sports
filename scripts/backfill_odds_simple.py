import sqlite3
import logging
import sys
from pathlib import Path

# Add project root to path
sys.path.append(str(Path.cwd()))

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

DB_PATH = "data/betting.db"

def backfill_odds_by_id():
    """
    Updates 'predictions' table with odds from 'odds' table strictly matching by game_id.
    This fixes the issue where merged games have predictions but NULL moneyline columns.
    """
    LOGGER.info("Starting Backfill Odds (Simple ID Match)...")
    
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")
    cursor = conn.cursor()
    
    # We need to map 'odds.outcome' to 'home/away'.
    # Odds table: outcome='home', 'away', 'Draw'.
    # Predictions table: home_moneyline, away_moneyline.
    
    # Update Home Odds
    sql_home = """
    UPDATE predictions
    SET home_moneyline = (
        SELECT price_american 
        FROM odds 
        WHERE odds.game_id = predictions.game_id 
        AND odds.outcome = 'home'
        ORDER BY snapshot_id DESC LIMIT 1
    )
    WHERE home_moneyline IS NULL OR home_moneyline = 0;
    """
    
    LOGGER.info("Updating Home Moneylines...")
    cursor.execute(sql_home)
    LOGGER.info(f"Updated {cursor.rowcount} rows.")
    
    # Update Away Odds
    sql_away = """
    UPDATE predictions
    SET away_moneyline = (
        SELECT price_american 
        FROM odds 
        WHERE odds.game_id = predictions.game_id 
        AND odds.outcome = 'away'
        ORDER BY snapshot_id DESC LIMIT 1
    )
    WHERE away_moneyline IS NULL OR away_moneyline = 0;
    """
    
    LOGGER.info("Updating Away Moneylines...")
    cursor.execute(sql_away)
    LOGGER.info(f"Updated {cursor.rowcount} rows.")
    
    # Update Draw Odds (if exists column)
    # Check if draw_moneyline exists
    cursor.execute("PRAGMA table_info(predictions)")
    cols = [r[1] for r in cursor.fetchall()]
    if "draw_moneyline" in cols:
        sql_draw = """
        UPDATE predictions
        SET draw_moneyline = (
            SELECT price_american 
            FROM odds 
            WHERE odds.game_id = predictions.game_id 
            AND odds.outcome = 'Draw'
            ORDER BY snapshot_id DESC LIMIT 1
        )
        WHERE draw_moneyline IS NULL OR draw_moneyline = 0;
        """
        LOGGER.info("Updating Draw Moneylines...")
        cursor.execute(sql_draw)
        LOGGER.info(f"Updated {cursor.rowcount} rows.")

    conn.commit()
    conn.close()
    LOGGER.info("Backfill Complete.")

if __name__ == "__main__":
    backfill_odds_by_id()
