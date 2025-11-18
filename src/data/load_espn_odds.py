"""Load ESPN odds CSV files into the database."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
from src.db.core import connect
from src.db.loaders import load_odds_snapshot
from src.db.loaders import normalize_team_code

LOGGER = logging.getLogger(__name__)


def _game_id_from_espn(event_id: str, league: str) -> str:
    """Convert ESPN event ID to our game_id format."""
    if league.upper() == "NBA":
        return f"NBA_{event_id}"
    return f"NFL_{event_id}"


def _convert_espn_csv_to_payload(csv_path: Path, league: str) -> dict:
    """Convert ESPN CSV to The Odds API payload format."""
    df = pd.read_csv(csv_path)
    if df.empty:
        return {"results": []}
    
    # Normalize team codes
    df["team"] = df["team"].apply(lambda x: normalize_team_code(league, x) or x)
    
    # Group by game (event_id)
    results = []
    for event_id, group in df.groupby("event_id"):
        game_id = _game_id_from_espn(event_id, league)
        
        # Get home and away teams
        home_row = group[group["is_home"] == 1].iloc[0] if len(group[group["is_home"] == 1]) > 0 else None
        away_row = group[group["is_home"] == 0].iloc[0] if len(group[group["is_home"] == 0]) > 0 else None
        
        if home_row is None or away_row is None:
            continue
        
        # Build odds data
        markets = [{
            "key": "h2h",
            "outcomes": [
                {
                    "name": home_row["team"],
                    "price": home_row.get("moneyline_close") or home_row.get("moneyline_open"),
                },
                {
                    "name": away_row["team"],
                    "price": away_row.get("moneyline_close") or away_row.get("moneyline_open"),
                },
            ],
        }]

        total_line = (
            home_row.get("total_close")
            or away_row.get("total_close")
            or home_row.get("total_open")
            or away_row.get("total_open")
        )
        if pd.notna(total_line):
            markets.append(
                {
                    "key": "totals",
                    "outcomes": [
                        {"name": "over", "point": total_line},
                        {"name": "under", "point": total_line},
                    ],
                }
            )

        bookmakers = [
            {
                "key": "espn",
                "title": "ESPN",
                "markets": markets,
            }
        ]
        
        results.append({
            "id": str(event_id),  # Use event_id as odds_api_id for matching
            "sport_key": "basketball_nba" if league.upper() == "NBA" else "americanfootball_nfl",
            "commence_time": group.iloc[0]["start_time"],
            "home_team": home_row["team"],
            "away_team": away_row["team"],
            "bookmakers": bookmakers
        })
    
    return {
        "results": results,
        "fetched_at": datetime.now(timezone.utc).isoformat()
    }


def load_espn_odds_csv(csv_path: Path, league: str) -> None:
    """Load an ESPN odds CSV file into the database."""
    LOGGER.info("Loading ESPN %s odds from %s", league.upper(), csv_path)
    
    payload = _convert_espn_csv_to_payload(csv_path, league)
    if not payload["results"]:
        LOGGER.warning("No odds data found in CSV")
        return
    
    sport_key = "basketball_nba" if league.upper() == "NBA" else "americanfootball_nfl"
    # Update source to "espn" instead of "the-odds-api"
    payload["source"] = "espn"
    load_odds_snapshot(payload, raw_path=str(csv_path), sport_key=sport_key)
    LOGGER.info("Loaded %d games from ESPN CSV", len(payload["results"]))


def main() -> None:
    parser = argparse.ArgumentParser(description="Load ESPN odds CSV files into database")
    parser.add_argument("csv_path", type=Path, help="Path to ESPN odds CSV file")
    parser.add_argument("--league", choices=["nfl", "nba"], required=True, help="League")
    
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
    
    load_espn_odds_csv(args.csv_path, args.league)


if __name__ == "__main__":
    main()

