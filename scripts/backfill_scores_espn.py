
import logging
import sys
import argparse
import requests
import time
import sqlite3
import json
from pathlib import Path
from datetime import date, datetime, timedelta
from typing import List, Dict, Optional, Tuple

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(PROJECT_ROOT))

# Import project modules
from src.data.config import RAW_DATA_DIR, ensure_directories
from src.data.team_mappings import normalize_team_code
from src.db.core import connect

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
LOGGER = logging.getLogger(__name__)

# ESPN API Config
ESPN_BASE_URL = "https://site.api.espn.com/apis/site/v2/sports"
LEAGUE_PATH_MAP = {
    "NHL": "hockey/nhl",
    "NBA": "basketball/nba",
    "NCAAB": "basketball/mens-college-basketball",
    "CFB": "football/college-football",
    "NFL": "football/nfl",
    "EPL": "soccer/eng.1",
    "LALIGA": "soccer/esp.1",
    "BUNDESLIGA": "soccer/ger.1",
    "SERIEA": "soccer/ita.1",
    "LIGUE1": "soccer/fra.1",
    "MLS": "soccer/usa.1"
}

def iter_dates(start_date: date, end_date: date):
    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)

def fetch_espn_scoreboard(league: str, target_date: date) -> Dict:
    path = LEAGUE_PATH_MAP.get(league.upper())
    if not path:
        raise ValueError(f"Unsupported league: {league}")
    
    url = f"{ESPN_BASE_URL}/{path}/scoreboard"
    date_str = target_date.strftime("%Y%m%d")
    
    try:
        LOGGER.debug(f"Fetching {league} scores for {date_str}...")
        params = {"dates": date_str, "limit": 1000}
        if league == "NCAAB":
            params["groups"] = 50 # Division I
            
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:
        LOGGER.error(f"Failed to fetch {league} scores for {date_str}: {e}")
        return {}

def parse_event(event: Dict, league: str) -> Optional[Dict]:
    """Parse a single ESPN event into a standardized game result."""
    try:
        completed = event.get("status", {}).get("type", {}).get("completed", False)
        # We only care about completed games for backfilling scores
        # BUT the user also mentioned "Ongoing" in their request, so maybe we want to update those too?
        # The user request showed "Ongoing" with no scores, relying on this to update them if they are now finished.
        # Even if they are still ongoing, updating the score is good.
        
        competitors = event.get("competitions", [{}])[0].get("competitors", [])
        if len(competitors) != 2:
            return None
        
        home_comp = next((c for c in competitors if c.get("homeAway") == "home"), None)
        away_comp = next((c for c in competitors if c.get("homeAway") == "away"), None)
        
        if not home_comp or not away_comp:
            return None
            
        home_team_raw = home_comp.get("team", {}).get("displayName")
        away_team_raw = away_comp.get("team", {}).get("displayName")
        
        # Normalize team names
        home_team_code = normalize_team_code(league, home_team_raw)
        away_team_code = normalize_team_code(league, away_team_raw)
        
        if not home_team_code or not away_team_code:
            LOGGER.warning(f"Could not map teams: {home_team_raw} ({home_team_code}) vs {away_team_raw} ({away_team_code})")
            return None
            
        home_score = home_comp.get("score")
        away_score = away_comp.get("score")
        
        status_desc = event.get("status", {}).get("type", {}).get("description")
        
        return {
            "home_team": home_team_code,
            "away_team": away_team_code,
            "home_team_raw": home_team_raw,
            "away_team_raw": away_team_raw,
            "home_score": int(home_score) if home_score is not None else None,
            "away_score": int(away_score) if away_score is not None else None,
            "status": status_desc,
            "completed": completed,
            "date": event.get("date"), # ISO string
            "espn_id": event.get("id")
        }
    except Exception as e:
        LOGGER.error(f"Error parsing event: {e}")
        return None

def update_scores_in_db(games: List[Dict], league: str):
    updated_count = 0
    with connect() as conn:
        for game in games:
            if game["home_score"] is None or game["away_score"] is None:
                continue
                
            # Try to match with DB game
            # We match on (league, home_team, away_team) and roughly the date
            # Since dates can shift slightly due to timezones, we'll try a flexible match if exact fails, 
            # but for simplicity let's rely on the team codes and a 48h window.
            
            # Using strftime to match date part might be tricky with TZ.
            # Let's try to find a game ID first.
            
            # Find game_id
            cursor = conn.execute(
                """
                SELECT g.game_id 
                FROM games g
                JOIN teams ht ON g.home_team_id = ht.team_id
                JOIN teams at ON g.away_team_id = at.team_id
                JOIN sports s ON g.sport_id = s.sport_id
                WHERE s.league = ? 
                AND ht.code = ? 
                AND at.code = ?
                AND ABS(julianDay(g.start_time_utc) - julianDay(?)) < 1.0
                """,
                (league, game["home_team"], game["away_team"], game["date"])
            )
            
            row = cursor.fetchone()
            if row:
                game_id = row[0]

                # Upsert results
                conn.execute(
                    """
                    INSERT INTO game_results (game_id, home_score, away_score)
                    VALUES (?, ?, ?)
                    ON CONFLICT(game_id) DO UPDATE SET
                        home_score = excluded.home_score,
                        away_score = excluded.away_score
                    """,
                    (game_id, game["home_score"], game["away_score"])
                )
                
                # Update status to final if completed
                if game.get("completed"):
                   conn.execute("UPDATE games SET status = 'final' WHERE game_id = ?", (game_id,))
                
                updated_count += 1 
                LOGGER.debug(f"Updated {league} game {game_id}: {game['home_score']}-{game['away_score']} (Status: {game['status']})")
                   
                LOGGER.debug(f"Updated {league} game {game_id}: {game['home_score']}-{game['away_score']} (Status: {game['status']})")
            else:
                LOGGER.debug(f"No DB match for {league} {game['home_team']} (raw: {game['home_team_raw']}) vs {game['away_team']} (raw: {game['away_team_raw']}) on {game['date']}")
                
    LOGGER.info(f"Updated {updated_count} {league} games in database.")

def run(leagues: List[str], start_date: date, end_date: date):
    ensure_directories()
    
    for league in leagues:
        LOGGER.info(f"Processing {league} from {start_date} to {end_date}...")
        all_games = []
        for d in iter_dates(start_date, end_date):
            data = fetch_espn_scoreboard(league, d)
            events = data.get("events", [])
            LOGGER.info(f"Found {len(events)} events for {league} on {d}")
            
            for event in events:
                parsed = parse_event(event, league)
                if parsed:
                    all_games.append(parsed)
            
            # Rate limit politeness
            time.sleep(0.5)
            
        if all_games:
            update_scores_in_db(all_games, league)
        else:
            LOGGER.info(f"No games found for {league}.")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--leagues", nargs="+", default=["NHL", "NBA", "NCAAB", "CFB", "NFL", "EPL", "LALIGA", "BUNDESLIGA", "SERIEA", "LIGUE1"], help="Leagues to backfill")
    parser.add_argument("--start", default="2025-12-01", help="Start date YYYY-MM-DD")
    parser.add_argument("--end", default="2025-12-31", help="End date YYYY-MM-DD")
    
    args = parser.parse_args()
    
    try:
        start = datetime.strptime(args.start, "%Y-%m-%d").date()
        end = datetime.strptime(args.end, "%Y-%m-%d").date()
    except ValueError:
        LOGGER.error("Invalid date format. Use YYYY-MM-DD.")
        return

    run(args.leagues, start, end)

if __name__ == "__main__":
    main()
