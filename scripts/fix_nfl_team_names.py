
import sqlite3
import logging
import sys
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(PROJECT_ROOT))

from src.db.core import connect

# Configure logging
logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

NFL_TEAMS = {
    "ARI": "Arizona Cardinals",
    "ATL": "Atlanta Falcons",
    "BAL": "Baltimore Ravens",
    "BUF": "Buffalo Bills",
    "CAR": "Carolina Panthers",
    "CHI": "Chicago Bears",
    "CIN": "Cincinnati Bengals",
    "CLE": "Cleveland Browns",
    "DAL": "Dallas Cowboys",
    "DEN": "Denver Broncos",
    "DET": "Detroit Lions",
    "GB": "Green Bay Packers",
    "HOU": "Houston Texans",
    "IND": "Indianapolis Colts",
    "JAX": "Jacksonville Jaguars",
    "KC": "Kansas City Chiefs",
    "LV": "Las Vegas Raiders",
    "LAC": "Los Angeles Chargers",
    "LAR": "Los Angeles Rams",
    "MIA": "Miami Dolphins",
    "MIN": "Minnesota Vikings",
    "NE": "New England Patriots",
    "NO": "New Orleans Saints",
    "NYG": "New York Giants",
    "NYJ": "New York Jets",
    "PHI": "Philadelphia Eagles",
    "PIT": "Pittsburgh Steelers",
    "SF": "San Francisco 49ers",
    "SEA": "Seattle Seahawks",
    "TB": "Tampa Bay Buccaneers",
    "TEN": "Tennessee Titans",
    "WAS": "Washington Commanders"
}

def fix_nfl_names():
    with connect() as conn:
        # Get NFL sport_id
        cursor = conn.execute("SELECT sport_id FROM sports WHERE league = 'NFL'")
        row = cursor.fetchone()
        if not row:
            LOGGER.error("NFL sport not found in DB")
            return
        sport_id = row[0]
        
        updated_count = 0
        for code, full_name in NFL_TEAMS.items():
            # Update name where code matches and sport is NFL
            # Note: We update `name` but keep `code` as the abbreviation
            res = conn.execute(
                "UPDATE teams SET name = ? WHERE code = ? AND sport_id = ?",
                (full_name, code, sport_id)
            )
            updated_count += res.rowcount
            
        LOGGER.info(f"Updated {updated_count} NFL team names to full format.")

if __name__ == "__main__":
    fix_nfl_names()
