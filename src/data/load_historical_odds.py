"""Load historical odds CSV files into the database."""

from __future__ import annotations

import argparse
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd

from src.db.core import connect
from src.db.loaders import load_odds_snapshot
from src.db.loaders import normalize_team_code

LOGGER = logging.getLogger(__name__)


def _convert_csv_to_payload(csv_path: Path, league: str, source: str) -> dict:
    """Convert odds CSV to The Odds API payload format."""
    df = pd.read_csv(csv_path)
    if df.empty:
        return {"results": []}
    
    # Normalize team codes
    if "team" in df.columns:
        df["team"] = df["team"].apply(lambda x: normalize_team_code(league, x) or x)
    if "opponent" in df.columns:
        df["opponent"] = df["opponent"].apply(lambda x: normalize_team_code(league, x) or x)
    
    # Group by date and game (if available)
    results = []
    group_by_cols = ["date"] if "date" in df.columns else []
    
    if "event_id" in df.columns:
        group_by_cols.append("event_id")
    elif "game_id" in df.columns:
        group_by_cols.append("game_id")
    
    if group_by_cols:
        for group_key, group in df.groupby(group_by_cols):
            # Create event dict
            if isinstance(group_key, tuple):
                date_str = group_key[0]
                event_id = str(group_key[1]) if len(group_key) > 1 else None
            else:
                date_str = str(group_key)
                event_id = None
            
            # Get unique teams
            teams = group["team"].unique() if "team" in group.columns else []
            if len(teams) < 2:
                continue
            
            # Determine home/away (first team is often home, but this is approximate)
            home_team = teams[0]
            away_team = teams[1] if len(teams) > 1 else None
            
            # Extract moneylines
            home_row = group[group["team"] == home_team].iloc[0] if len(group[group["team"] == home_team]) > 0 else None
            away_row = group[group["team"] == away_team].iloc[0] if len(group[group["team"] == away_team]) > 0 else None
            
            if home_row is None or away_row is None:
                continue
            
            home_ml = home_row.get("moneyline")
            away_ml = away_row.get("moneyline")
            
            # Build bookmakers structure
            bookmakers = [{
                "key": source.lower(),
                "title": source,
                "markets": [{
                    "key": "h2h",
                    "outcomes": [
                        {
                            "name": home_team,
                            "price": home_ml
                        },
                        {
                            "name": away_team,
                            "price": away_ml
                        }
                    ]
                }]
            }]
            
            # Construct commence_time from date
            try:
                commence_dt = datetime.strptime(date_str, "%Y-%m-%d")
                commence_time = commence_dt.replace(tzinfo=timezone.utc).isoformat()
            except ValueError:
                commence_time = None
            
            results.append({
                "id": event_id or f"{league.upper()}_{date_str}_{home_team}_{away_team}",
                "sport_key": "basketball_nba" if league.upper() == "NBA" else "americanfootball_nfl",
                "commence_time": commence_time,
                "home_team": home_team,
                "away_team": away_team,
                "bookmakers": bookmakers
            })
    else:
        # Fallback: treat each row as a separate outcome
        for _, row in df.iterrows():
            team = row.get("team")
            opponent = row.get("opponent")
            moneyline = row.get("moneyline")
            
            if pd.isna(moneyline) or not team or not opponent:
                continue
            
            # Create event for this team/opponent pair
            event_id = f"{league.upper()}_{row.get('date', 'unknown')}_{team}_{opponent}"
            
            results.append({
                "id": event_id,
                "sport_key": "basketball_nba" if league.upper() == "NBA" else "americanfootball_nfl",
                "commence_time": None,
                "home_team": team,  # Approximate - may need refinement
                "away_team": opponent,
                "bookmakers": [{
                    "key": source.lower(),
                    "title": source,
                    "markets": [{
                        "key": "h2h",
                        "outcomes": [{
                            "name": team,
                            "price": moneyline
                        }]
                    }]
                }]
            })
    
    return {
        "results": results,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "source": source.lower()
    }


def load_historical_odds_csv(csv_path: Path, league: str, source: str) -> None:
    """Load a historical odds CSV file into the database."""
    LOGGER.info("Loading %s %s odds from %s", source, league.upper(), csv_path)
    
    payload = _convert_csv_to_payload(csv_path, league, source)
    if not payload["results"]:
        LOGGER.warning("No odds data found in CSV")
        return
    
    sport_key = "basketball_nba" if league.upper() == "NBA" else "americanfootball_nfl"
    load_odds_snapshot(payload, raw_path=str(csv_path), sport_key=sport_key)
    LOGGER.info("Loaded %d games from %s CSV", len(payload["results"]), source)


def main() -> None:
    parser = argparse.ArgumentParser(description="Load historical odds CSV files into database")
    parser.add_argument("csv_path", type=Path, help="Path to odds CSV file")
    parser.add_argument("--league", choices=["nfl", "nba"], required=True, help="League")
    parser.add_argument("--source", required=True, help="Source name (e.g., oddsshark, vegasinsider)")
    
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
    
    load_historical_odds_csv(args.csv_path, args.league, args.source)


if __name__ == "__main__":
    main()

