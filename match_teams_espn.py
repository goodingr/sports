
import sqlite3
import requests
import logging
from datetime import datetime, timedelta
import difflib
from typing import Dict, List, Set

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(message)s')
LOGGER = logging.getLogger(__name__)

DB_PATH = "data/betting.db"

ESPN_ENDPOINTS = {
    "NBA": "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard",
    "NFL": "https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard",
    "NHL": "https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/scoreboard",
    "NCAAB": "https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/scoreboard",
    "CFB": "https://site.api.espn.com/apis/site/v2/sports/football/college-football/scoreboard",
    "EPL": "https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/scoreboard",
    "LALIGA": "https://site.api.espn.com/apis/site/v2/sports/soccer/esp.1/scoreboard",
    "BUNDESLIGA": "https://site.api.espn.com/apis/site/v2/sports/soccer/ger.1/scoreboard",
    "SERIEA": "https://site.api.espn.com/apis/site/v2/sports/soccer/ita.1/scoreboard",
    "LIGUE1": "https://site.api.espn.com/apis/site/v2/sports/soccer/fra.1/scoreboard",
}

def fetch_espn_teams(league: str, date_str: str) -> Set[str]:
    url = ESPN_ENDPOINTS.get(league)
    if not url:
        return set()
    try:
        resp = requests.get(url, params={"dates": date_str}, timeout=5)
        if resp.status_code != 200:
            return set()
        data = resp.json()
        teams = set()
        for event in data.get("events", []):
            for comp in event.get("competitions", [{}])[0].get("competitors", []):
                teams.add(comp.get("team", {}).get("displayName"))
        return teams
    except:
        return set()

def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Get all games with missing scores in the range
    cursor.execute("""
        SELECT g.start_time_utc, s.league, ht.name as home_team
        FROM games g 
        JOIN sports s ON g.sport_id = s.sport_id 
        JOIN teams ht ON g.home_team_id = ht.team_id 
        LEFT JOIN game_results r ON g.game_id = r.game_id 
        WHERE g.start_time_utc BETWEEN '2025-11-02' AND '2025-12-02' 
          AND (r.home_score IS NULL OR r.away_score IS NULL)
        ORDER BY g.start_time_utc
    """)
    
    missing_games = cursor.fetchall()
    
    # Group by (league, date) -> list of DB home teams
    grouped = {}
    for row in missing_games:
        utc_time = datetime.fromisoformat(row['start_time_utc'].replace("Z", "+00:00"))
        local_time = utc_time - timedelta(hours=5)
        date_str = local_time.strftime("%Y%m%d")
        key = (row['league'], date_str)
        if key not in grouped:
            grouped[key] = []
        grouped[key].append(row['home_team'])
        
    LOGGER.info(f"Analyzing {len(missing_games)} missing games across {len(grouped)} league/date combinations...")
    
    suggestions = {}
    
    for (league, date_str), db_teams in grouped.items():
        espn_teams = fetch_espn_teams(league, date_str)
        if not espn_teams:
            continue
            
        for db_team in db_teams:
            # Find best match in espn_teams
            matches = difflib.get_close_matches(db_team, espn_teams, n=1, cutoff=0.4)
            if matches:
                best_match = matches[0]
                # Store as suggestion
                if db_team not in suggestions:
                    suggestions[db_team] = best_match
                else:
                    # If we already have a suggestion, check if this one is better or same
                    pass
            else:
                # Try simpler matching (substring)
                for et in espn_teams:
                    # Check if one contains the other (ignoring case/punctuation)
                    d_norm = db_team.lower().replace(".","")
                    e_norm = et.lower().replace(".","")
                    if d_norm in e_norm or e_norm in d_norm:
                        suggestions[db_team] = et
                        break
    
    LOGGER.info("\nSUGGESTED MAPPINGS:")
    LOGGER.info("===================")
    for db, espn in sorted(suggestions.items()):
        LOGGER.info(f'"{db}": "{espn}",')

if __name__ == "__main__":
    main()
