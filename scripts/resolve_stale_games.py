import sqlite3
import sys
import logging
from pathlib import Path
from datetime import datetime, timedelta
import pytz

# Add project root to path to import backfill script
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(PROJECT_ROOT))

# Import the backfill logic
# We need to rely on the side effects of 'run' which updates the DB
from scripts.backfill_scores_espn import run as run_backfill

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
LOGGER = logging.getLogger("stale_resolver")

DB_PATH = PROJECT_ROOT / "data" / "betting.db"

def get_stale_games():
    """Find games that started in the past but are not 'final'."""
    # Using a buffer of 6 hours to ensure the game is actually over
    # (e.g. a game starting at 1PM is likely done by 7PM)
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.execute("""
            SELECT 
                g.game_id, 
                g.start_time_utc, 
                s.league,
                g.home_team_id,
                g.away_team_id
            FROM games g
            JOIN sports s ON g.sport_id = s.sport_id
            WHERE g.start_time_utc < datetime('now', '-6 hours')
            AND g.status != 'final'
            ORDER BY g.start_time_utc ASC
        """)
        return cursor.fetchall()

def resolve():
    stale_games = get_stale_games()
    
    if not stale_games:
        LOGGER.info("No stale games found! Everything is clean.")
        return

    LOGGER.info(f"Found {len(stale_games)} stale games.")
    
    # Group by (league, date) to minimize API calls
    # We convert start_time_utc to a date. 
    # To be safe with timezones (e.g. late night games counting as next day or previous day in "scoreboard" terms),
    # we might want to check the specific date of the game.
    # ESPN API usually uses the local date of the event or US ET.
    # Let's target the date of the game in ET.
    
    targets = set()
    
    et_tz = pytz.timezone('US/Eastern')
    utc_tz = pytz.timezone('UTC')

    for _, start_time_str, league, _, _ in stale_games:
        # start_time_str can be 'YYYY-MM-DDTHH:MM:SS+ZZ:ZZ' (ISO) 
        # or potentially 'YYYY-MM-DD HH:MM:SS' (legacy DBs sometimes)
        try:
            # Try ISO format first (most likely given DB check)
            dt_utc = datetime.fromisoformat(start_time_str)
            
            # If it's offset-naive, assume UTC
            if dt_utc.tzinfo is None:
                dt_utc = utc_tz.localize(dt_utc)
                
            dt_et = dt_utc.astimezone(et_tz)
            
            # Add the date to targets
            targets.add((league, dt_et.date()))
            
        except ValueError:
            # Fallback for space separated
            try:
                 dt_utc = datetime.strptime(start_time_str, "%Y-%m-%d %H:%M:%S")
                 dt_utc = utc_tz.localize(dt_utc)
                 dt_et = dt_utc.astimezone(et_tz)
                 targets.add((league, dt_et.date()))
            except ValueError:
                LOGGER.warning(f"Could not parse date: {start_time_str}")
                continue

    LOGGER.info(f"Identified {len(targets)} unique (league, date) combinations to check.")
    
    # Sort targets to process chronologically
    sorted_targets = sorted(list(targets), key=lambda x: (x[0], x[1]))

    for league, target_date in sorted_targets:
        LOGGER.info(f"Attempting to resolve {league} games for {target_date}...")
        try:
            # We call run for a single day
            run_backfill([league], target_date, target_date)
        except Exception as e:
            LOGGER.error(f"Failed to run backfill for {league} on {target_date}: {e}")

    # Final check
    remaining = get_stale_games()
    LOGGER.info(f"Resolution complete. Stale games remaining: {len(remaining)}")
    if remaining:
        LOGGER.info("Sample of remaining stale games:")
        for g in remaining[:5]:
            LOGGER.info(f" - ID: {g[0]}, League: {g[2]}, Start: {g[1]}")

if __name__ == "__main__":
    resolve()
