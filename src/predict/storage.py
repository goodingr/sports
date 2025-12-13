"""Storage utilities for the prediction system."""

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

import pandas as pd
from src.db.core import connect
from src.predict.config import PREDICTIONS_DIR

LOGGER = logging.getLogger(__name__)

def load_games_from_database(league: str, days_ahead: int = 7) -> pd.DataFrame:
    """
    Load upcoming games from the database for a specific league.
    
    Args:
        league: League code (e.g., "NFL", "NBA").
        days_ahead: Number of days into the future to look for games.
        
    Returns:
        DataFrame containing game details.
    """
    with connect() as conn:
        # 1. Get sport_id
        sport_row = conn.execute("SELECT sport_id FROM sports WHERE league = ?", (league,)).fetchone()
        if not sport_row:
            LOGGER.warning(f"League {league} not found in database")
            return pd.DataFrame()
        sport_id = sport_row[0]
        
        # 2. Get latest snapshot
        snapshot_row = conn.execute(
            """
            SELECT snapshot_id 
            FROM odds_snapshots 
            WHERE sport_id = ? 
            ORDER BY fetched_at_utc DESC 
            LIMIT 1
            """, 
            (sport_id,)
        ).fetchone()
        
        if not snapshot_row:
            LOGGER.warning(f"No odds snapshots found for {league}")
            return pd.DataFrame()
        snapshot_id = snapshot_row[0]
        
        # 3. Get games
        query = """
            SELECT 
                g.game_id,
                g.start_time_utc as commence_time,
                ht.name as home_team,
                at.name as away_team,
                g.status
            FROM games g
            JOIN teams ht ON g.home_team_id = ht.team_id
            JOIN teams at ON g.away_team_id = at.team_id
            WHERE g.sport_id = ?
              AND g.start_time_utc >= datetime('now')
              AND g.start_time_utc <= datetime('now', ?)
              AND g.status != 'final'
            ORDER BY g.start_time_utc
        """
        games_df = pd.read_sql_query(query, conn, params=(sport_id, f"+{days_ahead} days"))
        
        if games_df.empty:
            return games_df
            
        # 4. Get odds for these games (from ANY snapshot, prioritizing latest)
        game_ids = games_df["game_id"].tolist()
        placeholders = ",".join("?" * len(game_ids))
        
        # Fetch H2H (Moneyline), Totals, and Spreads
        # Join with snapshots to get timestamp for deduplication
        odds_query = f"""
            SELECT o.game_id, o.market, o.outcome, o.price_american, o.line, s.fetched_at_utc
            FROM odds o
            JOIN odds_snapshots s ON o.snapshot_id = s.snapshot_id
            WHERE o.game_id IN ({placeholders})
              AND o.market IN ('h2h', 'totals', 'spreads')
        """
        odds_df = pd.read_sql_query(odds_query, conn, params=game_ids)
        
        if not odds_df.empty:
            # Sort by time desc and keep latest per game/market/outcome
            odds_df["fetched_at_utc"] = pd.to_datetime(odds_df["fetched_at_utc"])
            odds_df = odds_df.sort_values("fetched_at_utc", ascending=False)
            odds_df = odds_df.drop_duplicates(subset=["game_id", "market", "outcome"], keep="first")
        
        # Initialize columns
        games_df["home_moneyline"] = None
        games_df["away_moneyline"] = None
        games_df["total_line"] = None
        games_df["over_moneyline"] = None
        games_df["under_moneyline"] = None
        games_df["spread_line"] = None
        
        if odds_df.empty:
            return games_df
            
        for idx, row in games_df.iterrows():
            game_id = row["game_id"]
            home_team = row["home_team"]
            away_team = row["away_team"]
            
            # Normalize names for matching
            home_norm = home_team.lower().strip()
            away_norm = away_team.lower().strip()
            
            game_odds = odds_df[odds_df["game_id"] == game_id]
            
            # Moneyline
            h2h_odds = game_odds[game_odds["market"] == "h2h"]
            for _, odd in h2h_odds.iterrows():
                outcome = str(odd["outcome"]).lower().strip()
                price = odd["price_american"]
                
                # Enhanced matching: Exact, or "home"/"away", or substring
                if outcome == "home" or outcome == home_norm or outcome in home_norm or home_norm in outcome:
                    games_df.at[idx, "home_moneyline"] = price
                elif outcome == "away" or outcome == away_norm or outcome in away_norm or away_norm in outcome:
                    games_df.at[idx, "away_moneyline"] = price
            
            # Spreads
            spread_odds = game_odds[game_odds["market"] == "spreads"]
            for _, odd in spread_odds.iterrows():
                outcome = str(odd["outcome"]).lower().strip()
                line = odd["line"]
                
                if outcome == "home" or outcome == home_norm or outcome in home_norm or home_norm in outcome:
                     games_df.at[idx, "spread_line"] = line
                     
            # Totals
            totals_odds = game_odds[game_odds["market"] == "totals"]
            for _, odd in totals_odds.iterrows():
                outcome = str(odd["outcome"]).lower().strip() # 'over' or 'under'
                price = odd["price_american"]
                line = odd["line"]
                
                if pd.notna(line):
                    games_df.at[idx, "total_line"] = line
                
                if outcome == "over":
                    games_df.at[idx, "over_moneyline"] = price
                elif outcome == "under":
                    games_df.at[idx, "under_moneyline"] = price
            
    # Convert commence_time to datetime
    games_df["commence_time"] = pd.to_datetime(games_df["commence_time"])
    
    # Add league column
    games_df["league"] = league
    
    return games_df

def save_predictions(df: pd.DataFrame, model_type: str, timestamp: Optional[datetime] = None) -> None:
    """
    Save predictions to the database.
    
    Args:
        df: DataFrame containing predictions.
        model_type: Type of model (e.g., "ensemble", "random_forest").
        timestamp: Timestamp for the prediction (defaults to now).
    """
    if df.empty:
        return

    if timestamp is None:
        timestamp = datetime.now(timezone.utc)
        
    # Prepare records for insertion
    records = []
    for _, row in df.iterrows():
        record = {
            "game_id": row.get("game_id"),
            "model_type": model_type,
            "predicted_at": timestamp.isoformat(),
            "home_prob": row.get("home_predicted_prob"),
            "away_prob": row.get("away_predicted_prob"),
            "home_moneyline": row.get("home_moneyline"),
            "away_moneyline": row.get("away_moneyline"),
            "home_edge": row.get("home_edge"),
            "away_edge": row.get("away_edge"),
            "home_implied_prob": row.get("home_implied_prob"),
            "away_implied_prob": row.get("away_implied_prob"),
            # Totals
            "total_line": row.get("total_line"),
            "over_prob": row.get("over_predicted_prob"),
            "under_prob": row.get("under_predicted_prob"),
            "over_moneyline": row.get("over_moneyline"),
            "under_moneyline": row.get("under_moneyline"),
            "over_edge": row.get("over_edge"),
            "under_edge": row.get("under_edge"),
            "over_implied_prob": row.get("over_implied_prob"),
            "under_implied_prob": row.get("under_implied_prob"),
            "predicted_total_points": row.get("predicted_total_points"),
        }
        records.append(record)
        
    with connect() as conn:
        cursor = conn.cursor()
        cursor.executemany("""
            INSERT OR REPLACE INTO predictions (
                game_id, model_type, predicted_at, 
                home_prob, away_prob, home_moneyline, away_moneyline, 
                home_edge, away_edge, home_implied_prob, away_implied_prob,
                total_line, over_prob, under_prob, over_moneyline, under_moneyline,
                over_edge, under_edge, over_implied_prob, under_implied_prob,
                predicted_total_points
            ) VALUES (
                :game_id, :model_type, :predicted_at, 
                :home_prob, :away_prob, :home_moneyline, :away_moneyline, 
                :home_edge, :away_edge, :home_implied_prob, :away_implied_prob,
                :total_line, :over_prob, :under_prob, :over_moneyline, :under_moneyline,
                :over_edge, :under_edge, :over_implied_prob, :under_implied_prob,
                :predicted_total_points
            )
        """, records)
        
        LOGGER.info(f"Saved {cursor.rowcount} predictions to database for {model_type}")
