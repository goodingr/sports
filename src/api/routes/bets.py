from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional
import pandas as pd
from datetime import datetime
import pytz
import numpy as np

from src.dashboard.data import (
    load_forward_test_data, 
    _expand_totals, 
    get_totals_odds_for_recommended, 
    get_game_odds,
    filter_by_version,
    get_default_version_value
)
from src.data.team_mappings import get_full_team_name
from src.data.sportsbook_urls import get_sportsbook_url

router = APIRouter(prefix="/api/bets", tags=["bets"])

def get_totals_data(model_type: str = "ensemble", version: Optional[str] = "all") -> pd.DataFrame:
    """Load and filter data for Over/Under bets."""
    # Load raw data
    raw_df = load_forward_test_data(model_type=model_type)
    
    # Filter by version
    raw_df = filter_by_version(raw_df, version)
    
    # Expand to get totals specific columns (profit, result, etc.)
    df = _expand_totals(raw_df)
    
    if df.empty:
        return df
        
    # Add status column
    # If 'won' is not null, it's completed
    # Otherwise it's pending
    df["status"] = df["won"].apply(lambda x: "Completed" if pd.notna(x) else "Pending")
    
    # Filter for recommended bets (positive edge)
    if "edge" in df.columns:
        df = df[df["edge"] >= 0.06].copy()
        
    # Fix team names (NCAAB abbreviations and other prefixes)
    # Apply get_full_team_name to home_team and away_team columns
    if "league" in df.columns and "home_team" in df.columns and "away_team" in df.columns:
        # Define a helper to apply mapping
        def fix_team_name(row, team_col):
            team_name = row[team_col]
            # Strip common prefixes
            if isinstance(team_name, str):
                # Strip "Sv " prefix (e.g., "Sv Werder Bremen" -> "Werder Bremen")
                if team_name.startswith("Sv "):
                    team_name = team_name[3:]
            return get_full_team_name(row["league"], team_name)
            
        df["home_team"] = df.apply(lambda row: fix_team_name(row, "home_team"), axis=1)
        df["away_team"] = df.apply(lambda row: fix_team_name(row, "away_team"), axis=1)
    
    # Deduplicate by physical game (same matchup + time = same game)
    # This prevents games with both game_id and odds_api_id from appearing twice
    if not df.empty and all(col in df.columns for col in ['home_team', 'away_team', 'commence_time', 'league']):
        before_count = len(df)
        df = df.drop_duplicates(
            subset=['home_team', 'away_team', 'commence_time', 'league', 'side'],
            keep='first'  # Keep the first occurrence
        )
        after_count = len(df)
        if before_count > after_count:
            print(f"DEBUG: Removed {before_count - after_count} duplicate game records")
    
    # Add sportsbook information for pending bets
    # Initialize columns for all rows
    df['book'] = ""
    df['book_url'] = ""
    
    if not df.empty and "status" in df.columns:
        pending = df[df["status"] == "Pending"].copy()
        if not pending.empty:
            try:
                print(f"\nDEBUG: Processing {len(pending)} pending bets")
                
                # Get sportsbook odds from database
                odds_df = get_totals_odds_for_recommended(pending)
                
                print(f"DEBUG: Found {len(odds_df)} odds records for {len(pending)} pending bets")
                
                if not odds_df.empty:
                    # Group by game_id and side to get the best odds (highest moneyline for each side)
                    best_odds = odds_df.loc[odds_df.groupby(['forward_game_id', 'outcome'])['moneyline'].idxmax()]
                    
                    print(f"DEBUG: Best odds found for {len(best_odds)} game+side combinations")
                    
                    # Merge sportsbook data back to main dataframe
                    # Match on game_id and side
                    df = df.merge(
                        best_odds[['forward_game_id', 'outcome', 'book', 'moneyline', 'line', 'home_team_full', 'away_team_full']],
                        left_on=['game_id', 'side'],
                        right_on=['forward_game_id', 'outcome'],
                        how='left',
                        suffixes=('', '_sportsbook')
                    )
                    
                    # Check column naming (pandas may not add suffix if no collision)
                    line_col = 'line_sportsbook' if 'line_sportsbook' in df.columns else 'line'
                    
                    # Update columns where we found sportsbook matches
                    has_sportsbook_data = df[line_col].notna()
                    
                    # Update book column
                    if 'book_sportsbook' in df.columns:
                        df['book'] = df['book_sportsbook'].fillna(df['book'])
                    
                    # Overwrite the displayed odds (moneyline) with the best available sportsbook odds
                    if 'moneyline_sportsbook' in df.columns:
                        df['moneyline'] = df['moneyline_sportsbook'].fillna(df['moneyline'])
                    
                    # CRITICAL: Update total_line with sportsbook's line
                    if line_col in df.columns:
                        df.loc[has_sportsbook_data, 'total_line'] = df.loc[has_sportsbook_data, line_col]
                        
                        # Also update description to match the sportsbook line
                        if 'description' in df.columns and 'side' in df.columns:
                            df.loc[has_sportsbook_data, 'description'] = df.loc[has_sportsbook_data].apply(
                                lambda row: f"{row['side'].title()} {row['total_line']:.1f}" if pd.notna(row['total_line']) else row['side'].title(),
                                axis=1
                            )
                            
                    # Update team names with full names from odds data if available
                    if 'home_team_full' in df.columns:
                        df.loc[has_sportsbook_data, 'home_team'] = df.loc[has_sportsbook_data, 'home_team_full'].fillna(df.loc[has_sportsbook_data, 'home_team'])
                    
                    if 'away_team_full' in df.columns:
                        df.loc[has_sportsbook_data, 'away_team'] = df.loc[has_sportsbook_data, 'away_team_full'].fillna(df.loc[has_sportsbook_data, 'away_team'])
                    
                    # Add sportsbook URL
                    df['book_url'] = df['book'].apply(lambda x: get_sportsbook_url(x) if pd.notna(x) and x != "" else "")
                    
                    # Clean up merge columns
                    df = df.drop(columns=['forward_game_id', 'outcome', 'book_sportsbook', 'moneyline_sportsbook', line_col, 'home_team_full', 'away_team_full'], errors='ignore')
                    
                    matched_count = df[df['book'] != ""].shape[0]
                    unmatched = df[(df['status'] == 'Pending') & (df['book'] == "")]
                    print(f"DEBUG: Successfully matched {matched_count} bets with sportsbooks")
                    if not unmatched.empty:
                        print(f"DEBUG: Unmatched bets ({len(unmatched)}):")
                        for _, row in unmatched.head(5).iterrows():
                            print(f"  - {row['game_id']}: {row['home_team']} vs {row['away_team']}, {row['side']} {row.get('total_line', 'N/A')}")
                else:
                    print("DEBUG: No odds data returned from get_totals_odds_for_recommended")
            except Exception as e:
                # If sportsbook data fetch fails, just continue without it
                print(f"ERROR: Could not fetch sportsbook data: {e}")
                import traceback
                traceback.print_exc()
    
    return df

@router.get("/stats")
async def get_stats(model_type: str = "ensemble"):
    """Get aggregate statistics for Over/Under bets."""
    df = get_totals_data(model_type)
    
    if df.empty:
        return {
            "roi": 0.0,
            "win_rate": 0.0,
            "total_profit": 0.0,
            "total_bets": 0
        }
    
    # Filter for completed bets
    completed = df[df["status"] == "Completed"].copy()
    
    if completed.empty:
        return {
            "roi": 0.0,
            "win_rate": 0.0,
            "total_profit": 0.0,
            "total_bets": 0
        }
        
    total_bets = len(completed)
    wins = len(completed[completed["won"] == True])
    win_rate = (wins / total_bets) * 100 if total_bets > 0 else 0
    
    # Calculate profit
    total_profit = completed["profit"].sum() if "profit" in completed.columns else 0.0
    total_staked = completed["stake"].sum() if "stake" in completed.columns else (total_bets * 100) # Default stake
    
    roi = (total_profit / total_staked) * 100 if total_staked > 0 else 0.0
    
    return {
        "roi": round(roi, 2),
        "win_rate": round(win_rate, 2),
        "total_profit": round(total_profit, 2),
        "total_bets": total_bets
    }

@router.get("/history")
async def get_history(
    page: int = 1, 
    limit: int = 20, 
    model_type: str = "ensemble"
):
    """Get historical completed bets."""
    df = get_totals_data(model_type)
    
    if df.empty:
        return {
            "data": [],
            "total": 0,
            "page": page,
            "limit": limit
        }

    # Filter for completed bets OR ongoing bets (started but not finished)
    # Ongoing bets are Pending but commence_time <= now
    now = pd.Timestamp.now(tz="UTC")
    
    if "commence_time" in df.columns:
        mask = (df["status"] == "Completed") | (
            (df["status"] == "Pending") & 
            (df["commence_time"] <= now)
        )
        completed = df[mask].copy()
        
        # Sort by date desc
        completed = completed.sort_values("commence_time", ascending=False)
    else:
        completed = df[df["status"] == "Completed"].copy()
        
    # Pagination
    start = (page - 1) * limit
    end = start + limit
    
    paginated = completed.iloc[start:end]
    
    # Convert to list of dicts
    # Handle NaN values for JSON serialization
    # Helper to safely get float values
    def safe_get(row, col):
        val = row.get(col)
        if pd.isna(val) or (isinstance(val, float) and (np.isinf(val) or np.isnan(val))):
            return None
        return val

    records = []
    for _, row in paginated.iterrows():
        record = row.fillna("").to_dict()
        
        # Add prediction details
        record["predicted_total_points"] = safe_get(row, "predicted_total_points")
        record["edge"] = safe_get(row, "edge")
        record["profit"] = safe_get(row, "profit")
        record["home_score"] = safe_get(row, "home_score")
        record["away_score"] = safe_get(row, "away_score")
        
        # Construct recommended bet string
        if pd.notna(row.get("side")) and pd.notna(row.get("total_line")):
            record["recommended_bet"] = f"{row['side'].title()} {row['total_line']}"
        else:
            record["recommended_bet"] = None
            
        # 3. Get full odds data for this game
        if "game_id" in row:
            df_odds = get_game_odds(row["game_id"])
            if not df_odds.empty:
                record["odds_data"] = df_odds.fillna("").to_dict(orient="records")
            else:
                record["odds_data"] = []
        else:
            record["odds_data"] = []
            
        records.append(record)
    
    return {
        "data": records,
        "total": len(completed),
        "page": page,
        "limit": limit
    }

@router.get("/upcoming")
async def get_upcoming(model_type: str = "ensemble"):
    """Get upcoming active bets."""
    df = get_totals_data(model_type)
    
    if df.empty:
        return {
            "data": [],
            "count": 0
        }
    
    # Filter for pending/upcoming
    # We can use the status column we created
    upcoming = df[df["status"] == "Pending"].copy()
    
    # Filter out past games to ensure we only show truly upcoming bets
    now = pd.Timestamp.now(tz="UTC")
    if "commence_time" in upcoming.columns:
        upcoming = upcoming[upcoming["commence_time"] > now]
        upcoming = upcoming.sort_values("commence_time", ascending=True)
    
    # Helper to safely get float values
    def safe_get(row, col):
        val = row.get(col)
        if pd.isna(val) or (isinstance(val, float) and (np.isinf(val) or np.isnan(val))):
            return None
        return val

    records = []
    for _, row in upcoming.iterrows():
        record = row.fillna("").to_dict()
        
        # Add prediction details
        record["predicted_total_points"] = safe_get(row, "predicted_total_points")
        record["edge"] = safe_get(row, "edge")
        
        # Construct recommended bet string
        if pd.notna(row.get("side")) and pd.notna(row.get("total_line")):
            record["recommended_bet"] = f"{row['side'].title()} {row['total_line']}"
        else:
            record["recommended_bet"] = None
            
        # 3. Get full odds data for this game
        # Note: This is N+1 query pattern, but acceptable for limited page size
        if "game_id" in row:
            df_odds = get_game_odds(row["game_id"])
            if not df_odds.empty:
                record["odds_data"] = df_odds.fillna("").to_dict(orient="records")
            else:
                record["odds_data"] = []
        else:
            record["odds_data"] = []
            
        records.append(record)
    
    return {
        "data": records,
        "count": len(upcoming)
    }

@router.get("/game/{game_id}/odds")
async def get_odds_for_game(game_id: str, model_type: str = "ensemble"):
    """Get all sportsbook odds for a specific game, including prediction info."""
    # 1. Get sportsbook odds
    df_odds = get_game_odds(game_id)
    
    odds_data = []
    if not df_odds.empty:
        odds_data = df_odds.fillna("").to_dict(orient="records")

    # 2. Get prediction data for this game
    # We load the full dataset and filter (efficient enough for now given parquet speed)
    df_preds = get_totals_data(model_type)
    
    prediction_info = {
        "predicted_total_points": None,
        "recommended_bet": None,
        "edge": None,
        "home_score": None,
        "away_score": None,
        "profit": None,
        "won": None,
        "status": "Pending"
    }
    
    if not df_preds.empty:
        # Filter for this game
        game_pred = df_preds[df_preds["game_id"] == game_id]
        
        if not game_pred.empty:
            # Get the row with the highest edge (if multiple) or just the first one
            # Usually there are two rows (over and under), we want the one we recommended (highest edge)
            # If no recommendation (edge < threshold), we might just show the predicted score
            
            # Sort by edge desc to get the best bet first
            if "edge" in game_pred.columns:
                game_pred = game_pred.sort_values("edge", ascending=False)
            
            best_bet = game_pred.iloc[0]
            
            # Helper to safely get float values
            def safe_get(row, col):
                val = row.get(col)
                if pd.isna(val) or (isinstance(val, float) and (np.isinf(val) or np.isnan(val))):
                    return None
                return val

            prediction_info["predicted_total_points"] = safe_get(best_bet, "predicted_total_points")
            
            # Only show recommendation if it's a valid bet (has edge)
            # But user wants to see "recommended over/under", which implies the side we favor
            # even if it's not a strong "bet" recommendation? 
            # The prompt says "show the recommended over/under", usually implies the one with positive edge.
            # If both are negative edge, maybe just show the predicted score.
            
            # Let's send the side with the highest edge (or least negative) as the "lean"
            prediction_info["recommended_bet"] = f"{best_bet['side'].title()} {best_bet['total_line']}"
            prediction_info["edge"] = safe_get(best_bet, "edge")
            
            # Add result info
            prediction_info["home_score"] = safe_get(best_bet, "home_score")
            prediction_info["away_score"] = safe_get(best_bet, "away_score")
            prediction_info["profit"] = safe_get(best_bet, "profit")
            # won is boolean or None usually, but safe_get handles it if it's NaN
            prediction_info["won"] = safe_get(best_bet, "won")
            
            # Determine status
            if pd.notna(best_bet.get("home_score")) and pd.notna(best_bet.get("away_score")):
                prediction_info["status"] = "Completed"
            
            # If we have a predicted total, we can also explicitly say "Over" or "Under" based on that
            # vs the current line. But the 'side' column already captures this logic relative to the line.

    return {
        "data": odds_data,
        "count": len(odds_data),
        "prediction": prediction_info
    }
