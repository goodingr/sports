
import sqlite3
import requests
import logging
from datetime import datetime, timedelta
import pandas as pd
from typing import Dict, List, Tuple, Optional
import difflib

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
LOGGER = logging.getLogger(__name__)

DB_PATH = "data/betting.db"

# ESPN API Endpoints
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

# Manual mappings for tricky teams
TEAM_MAPPINGS = {
    "Loyola (MD)": "Loyola Maryland",
    "St.": "State",
    "St": "State",
    "NC State": "North Carolina State",
    "Ole Miss": "Mississippi",
    "UConn": "Connecticut",
    "UMass": "Massachusetts",
    "UPenn": "Pennsylvania",
    "Pitt": "Pittsburgh",
    "VMI": "Virginia Military",
}

def normalize_name(name: str) -> str:
    """Normalize team name for matching."""
    # Replace common abbreviations
    name = name.replace("St.", "State").replace("St ", "State ")
    name = name.replace("&", "and")
    
    # Remove mascots from common teams if needed, or just rely on fuzzy match
    # But for "Fresno St Bulldogs" vs "Fresno State", the "Bulldogs" part might hurt if ESPN is just "Fresno State"
    # and we look for "Fresno State Bulldogs".
    # Actually, if DB is "Fresno St Bulldogs" -> "Fresno State Bulldogs"
    # and ESPN is "Fresno State", then "Fresno State" is a substring of "Fresno State Bulldogs".
    # My fuzzy matcher handles substring.
    
    return name.lower().replace(" ", "").replace(".", "").replace("-", "").replace("'", "")

def fuzzy_match(name: str, candidates: List[str], threshold: float = 0.8) -> Optional[str]:
    """Find best match for a name in a list of candidates."""
    norm_name = normalize_name(name)
    best_match = None
    best_score = 0.0
    
    for candidate in candidates:
        norm_cand = normalize_name(candidate)
        if norm_name == norm_cand:
            return candidate
        
        # Check for substring match (e.g. "Florida" in "Florida Gators")
        if norm_name in norm_cand or norm_cand in norm_name:
             # Boost score for substring match
             score = 0.9
        else:
            score = difflib.SequenceMatcher(None, norm_name, norm_cand).ratio()
            
        if score > best_score:
            best_score = score
            best_match = candidate
            
    if best_score >= threshold:
        return best_match
    return None

def fetch_espn_scores(league: str, date_str: str) -> List[Dict]:
    """Fetch scores from ESPN for a given league and date (YYYYMMDD)."""
    url = ESPN_ENDPOINTS.get(league)
    if not url:
        LOGGER.warning(f"No ESPN endpoint for {league}")
        return []
        
    try:
        params = {"dates": date_str}
        if league == "NCAAB":
            params["groups"] = "50"
            params["limit"] = "1000"
        elif league == "CFB":
            params["groups"] = "80" # FBS
            params["limit"] = "1000"
            
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        
        games = []
        for event in data.get("events", []):
            competition = event.get("competitions", [{}])[0]
            competitors = competition.get("competitors", [])
            status = event.get("status", {}).get("type", {}).get("state")
            
            if status != "post":
                continue # Only interested in completed games
                
            home_team = None
            away_team = None
            home_score = None
            away_score = None
            
            for comp in competitors:
                team_name = comp.get("team", {}).get("displayName")
                score = comp.get("score")
                
                if comp.get("homeAway") == "home":
                    home_team = team_name
                    home_score = score
                else:
                    away_team = team_name
                    away_score = score
                    
            if home_team and away_team and home_score is not None and away_score is not None:
                games.append({
                    "home_team": home_team,
                    "away_team": away_team,
                    "home_score": int(home_score),
                    "away_score": int(away_score)
                })
                
        return games
        
    except Exception as e:
        LOGGER.error(f"Error fetching ESPN scores for {league} on {date_str}: {e}")
        return []

def backfill_scores():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    # 1. Identify missing games
    LOGGER.info("Identifying games with missing scores...")
    cursor.execute("""
        SELECT g.game_id, g.start_time_utc, ht.name as home_team, at.name as away_team, s.league
        FROM games g 
        JOIN sports s ON g.sport_id = s.sport_id 
        JOIN teams ht ON g.home_team_id = ht.team_id 
        JOIN teams at ON g.away_team_id = at.team_id
        LEFT JOIN game_results r ON g.game_id = r.game_id 
        WHERE g.start_time_utc BETWEEN '2025-11-02' AND '2025-12-05' 
          AND (r.home_score IS NULL OR r.away_score IS NULL)
        ORDER BY g.start_time_utc
    """)
    
    missing_games = cursor.fetchall()
    LOGGER.info(f"Found {len(missing_games)} games with missing scores.")
    
    # Group by League and Date
    games_by_league_date = {}
    for game in missing_games:
        league = game['league']
        # Convert UTC to local date (approximate for grouping)
        # Most ESPN APIs work well with the local date of the game.
        # For US sports, UTC-5 (EST) is a safe bet.
        utc_time = datetime.fromisoformat(game['start_time_utc'].replace("Z", "+00:00"))
        local_time = utc_time - timedelta(hours=5) 
        date_str = local_time.strftime("%Y%m%d")
        
        key = (league, date_str)
        if key not in games_by_league_date:
            games_by_league_date[key] = []
        games_by_league_date[key].append(game)
        
    # 2. Fetch and Update
    updated_count = 0
    
    for (league, date_str), games in games_by_league_date.items():
        LOGGER.info(f"Processing {league} for {date_str} ({len(games)} games)...")
        
        espn_games = fetch_espn_scores(league, date_str)
        if not espn_games:
            LOGGER.warning(f"No scores found on ESPN for {league} {date_str}")
            continue
            
        # Create lookup map for ESPN games (Team -> Game)
        # Index both home and away teams to handle neutral site discrepancies
        espn_team_map = {}
        for g in espn_games:
            espn_team_map[g['home_team']] = g
            espn_team_map[g['away_team']] = g
            
        espn_team_names = list(espn_team_map.keys())
        
        for game in games:
            db_home = game['home_team']
            db_away = game['away_team']
            
            # Check manual mappings first
            for k, v in TEAM_MAPPINGS.items():
                if k in db_home:
                    db_home = db_home.replace(k, v)
            
            # Try to match DB home team against ANY ESPN team
            match_name = fuzzy_match(db_home, espn_team_names)
            
            if match_name:
                espn_game = espn_team_map[match_name]
                
                # We found the game. Now we need to determine which score corresponds to DB home/away.
                # If DB Home == ESPN Home (matched), then DB Home Score = ESPN Home Score.
                # If DB Home == ESPN Away (matched), then DB Home Score = ESPN Away Score.
                # But we can just use the team names in espn_game to figure it out.
                
                # Actually, simpler:
                # We have espn_game which has 'home_team', 'away_team', 'home_score', 'away_score'.
                # We need to map these to DB's 'home_score' and 'away_score'.
                
                # If our matched team (db_home matched to match_name) is espn_game['home_team']:
                if match_name == espn_game['home_team']:
                    final_home_score = espn_game['home_score']
                    final_away_score = espn_game['away_score']
                else:
                    # Matched team is away in ESPN, so DB Home is ESPN Away
                    final_home_score = espn_game['away_score']
                    final_away_score = espn_game['home_score']
                
                # Update DB
                try:
                    # Check if result row exists
                    cursor.execute("SELECT 1 FROM game_results WHERE game_id = ?", (game['game_id'],))
                    exists = cursor.fetchone()
                    
                    if exists:
                        cursor.execute("""
                            UPDATE game_results 
                            SET home_score = ?, away_score = ?
                            WHERE game_id = ?
                        """, (final_home_score, final_away_score, game['game_id']))
                    else:
                        cursor.execute("""
                            INSERT INTO game_results (game_id, home_score, away_score)
                            VALUES (?, ?, ?)
                        """, (game['game_id'], final_home_score, final_away_score))
                        
                    # Also update games table status
                    cursor.execute("UPDATE games SET status = 'final' WHERE game_id = ?", (game['game_id'],))
                    
                    updated_count += 1
                    # LOGGER.info(f"Updated {game['game_id']}: {db_home} vs {db_away} -> {espn_game['home_score']}-{espn_game['away_score']}")
                    
                except Exception as e:
                    LOGGER.error(f"Failed to update {game['game_id']}: {e}")
            else:
                LOGGER.warning(f"UNMATCHED: {db_home} (DB) not found in ESPN data for {league} {date_str}")
                # Log potential candidates to help build mappings
                # LOGGER.info(f"  Candidates: {', '.join(espn_home_names)}")
                
    conn.commit()
    conn.close()
    LOGGER.info(f"Successfully backfilled {updated_count} games.")

if __name__ == "__main__":
    backfill_scores()
