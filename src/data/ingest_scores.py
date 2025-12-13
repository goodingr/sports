"""Fetch recent scores from The Odds API and update the database."""

import argparse
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests

import json
from src.data.config import OddsAPISettings, OddsAPIKeyManager, RAW_DATA_DIR
from src.db.core import connect
from src.predict.config import LEAGUE_SPORT_KEYS, SUPPORTED_LEAGUES

def _get_sport_key(league: str) -> str:
    try:
        return LEAGUE_SPORT_KEYS[league.upper()]
    except KeyError as exc:
        raise ValueError(f"Unknown league: {league}") from exc

LOGGER = logging.getLogger(__name__)

def fetch_scores(league: str, days_from: int = 3, dotenv_path: Optional[Path] = None) -> List[Dict]:
    """Fetch recent scores for a league."""
    try:
        settings = OddsAPISettings.from_env(dotenv_path)
    except RuntimeError as exc:
        LOGGER.warning("Unable to load Odds API settings for %s scores: %s", league, exc)
        return []

    sport_key = _get_sport_key(league)
    url = f"{settings.base_url}/sports/{sport_key}/scores/"
    params = {
        "apiKey": settings.api_key,
        "daysFrom": min(max(1, days_from), 3), # API limit is usually 3 days for free/standard
        "dateFormat": "iso"
    }

    max_attempts = len(OddsAPIKeyManager.get_available_keys())
    for attempt in range(max_attempts):
        params["apiKey"] = OddsAPIKeyManager.get_current_key()
        
        try:
            response = requests.get(url, params=params, timeout=20)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as exc:
            if isinstance(exc, requests.HTTPError) and response.status_code == 401:
                LOGGER.warning("Odds API returned 401 Unauthorized with key ending in ...%s", params["apiKey"][-4:])
                if attempt < max_attempts - 1:
                    LOGGER.info("Rotating API key and retrying...")
                    OddsAPIKeyManager.rotate_key()
                    continue
            
            # Non-401 error or exhausted retries
            LOGGER.warning("Failed to fetch %s scores from The Odds API: %s", league, exc)
            return []
    return []

def write_score_snapshot(data: List[Dict], league: str) -> None:
    """Save raw score data to data/raw/scores/<league>_<timestamp>.json."""
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
    out_dir = RAW_DATA_DIR / "scores" / league
    out_dir.mkdir(parents=True, exist_ok=True)
    output_path = out_dir / f"scores_{timestamp}.json"
    output_path.write_text(json.dumps(data, indent=2), encoding="utf-8")
    LOGGER.info("Saved raw scores to %s", output_path)

def update_database(scores_data: List[Dict], league: str) -> int:
    """Update game results in the database."""
    if not scores_data:
        return 0
        
    updated_count = 0
    
    with connect() as conn:
        for event in scores_data:
            # Process both completed and live games
            is_completed = event.get("completed", False)
            
            event_id = event.get("id")
            if not event_id:
                continue

            # Extract scores
            scores_list = event.get("scores") or []
            home_score = None
            away_score = None
            
            home_team = event.get("home_team")
            away_team = event.get("away_team")
            
            for entry in scores_list:
                name = entry.get("name")
                score_val = entry.get("score")
                if name is None or score_val is None:
                    continue
                try:
                    parsed = int(score_val)
                except (ValueError, TypeError):
                    continue
                    
                if name == home_team:
                    home_score = parsed
                elif name == away_team:
                    away_score = parsed
            
            # Fallback for 2-item list if names don't match exactly (sometimes happens)
            if (home_score is None or away_score is None) and len(scores_list) == 2:
                try:
                    # Assuming order matches home/away or we can't tell. 
                    # Actually, API usually returns them in order but relying on name is safer.
                    # If names fail, we skip to be safe, or check if one matched.
                    pass 
                except Exception:
                    pass

            if home_score is None or away_score is None:
                continue

            # Update DB
            # We match by odds_api_id (event_id)
            # We need to find the game_id first
            
            # First, check if we have this game by odds_api_id
            cursor = conn.execute(
                "SELECT game_id FROM games WHERE odds_api_id = ?", 
                (event_id,)
            )
            row = cursor.fetchone()
            
            game_id = None
            if row:
                game_id = row[0]
            else:
                # Fallback: Match by teams and date?
                # For now, let's stick to odds_api_id as primary linkage for API data
                # If we ingested the game from API, it has the ID.
                LOGGER.debug("No game found for odds_api_id %s (Home: %s, Away: %s)", event_id, home_team, away_team)
                continue
                
            if game_id:
                # Upsert into game_results
                conn.execute(
                    """
                    INSERT INTO game_results (game_id, home_score, away_score)
                    VALUES (?, ?, ?)
                    ON CONFLICT(game_id) DO UPDATE SET
                        home_score = excluded.home_score,
                        away_score = excluded.away_score
                    """,
                    (game_id, home_score, away_score)
                )
                
                # Update status
                new_status = 'final' if is_completed else 'in_progress'
                conn.execute(
                    "UPDATE games SET status = ? WHERE game_id = ?",
                    (new_status, game_id)
                )
                updated_count += 1
                
    return updated_count

def run(leagues: List[str], days_from: int = 3, dotenv_path: Optional[Path] = None) -> None:
    """Run ingestion for specified leagues."""
    total_updated = 0
    for league in leagues:
        LOGGER.info("Fetching scores for %s...", league)
        data = fetch_scores(league, days_from=days_from, dotenv_path=dotenv_path)
        if data:
            write_score_snapshot(data, league)
            count = update_database(data, league)
            LOGGER.info("Updated %d games for %s", count, league)
            total_updated += count
        else:
            LOGGER.info("No score data returned for %s", league)
            
    LOGGER.info("Total games updated: %d", total_updated)

def main():
    parser = argparse.ArgumentParser(description="Ingest scores from The Odds API")
    parser.add_argument("--leagues", nargs="+", help="Specific leagues to update (default: all supported)")
    parser.add_argument("--days-from", type=int, default=3, help="Days back to fetch (max 3)")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    parser.add_argument("--dotenv", type=Path, default=None)
    
    args = parser.parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level))
    
    leagues = args.leagues or SUPPORTED_LEAGUES
    # Handle comma-separated strings if passed as a single argument
    if len(leagues) == 1 and "," in leagues[0]:
        leagues = [l.strip() for l in leagues[0].split(",")]
        
    run(leagues, days_from=args.days_from, dotenv_path=args.dotenv)

if __name__ == "__main__":
    main()
