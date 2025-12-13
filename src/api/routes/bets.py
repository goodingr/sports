from fastapi import APIRouter, HTTPException, Query, Depends
from typing import List, Optional
import pandas as pd
from datetime import datetime
import pytz
import numpy as np

from src.api.auth import get_current_user, is_user_premium

from src.dashboard.data import (
    load_forward_test_data, 
    _expand_totals, 
    get_totals_odds_for_recommended, 
    get_game_odds,
    get_batch_game_odds,
    filter_by_version,
    get_default_version_value
)
from src.data.team_mappings import get_full_team_name
from src.data.sportsbook_urls import get_sportsbook_url

router = APIRouter(prefix="/api/bets", tags=["bets"])

import time

def get_totals_data(model_type: str = "ensemble", version: Optional[str] = "all") -> pd.DataFrame:
    """Load and filter data for Over/Under bets."""
    t0 = time.time()
    # Load raw data
    raw_df = load_forward_test_data(model_type=model_type)
    t1 = time.time()
    print(f"DEBUG: load_forward_test_data took {t1-t0:.4f}s")
    
    # Filter by version
    raw_df = filter_by_version(raw_df, version)
    
    # Expand to get totals specific columns (profit, result, etc.)
    df = _expand_totals(raw_df)
    t2 = time.time()
    print(f"DEBUG: _expand_totals took {t2-t1:.4f}s")
    
    if df.empty:
        return df
        
    # Add status column
    # If 'profit' is not null (Win/Loss/Push), it's completed
    # If 'won' is not null, it's completed
    # Otherwise it's pending
    df["status"] = np.where(df["profit"].notna() | df["won"].notna(), "Completed", "Pending")
    
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
    
    # Add empty book/book_url columns to satisfy schema
    df['book'] = ""
    df['book_url'] = ""
    
    # NOTE: Sportsbook fetching moved to endpoint-level (deferred) for performance
    
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
    
    # Ensure commence_time is UTC aware for comparison
    if "commence_time" in df.columns:
        # Convert to datetime if not already
        if not pd.api.types.is_datetime64_any_dtype(df["commence_time"]):
            df["commence_time"] = pd.to_datetime(df["commence_time"], utc=True)
        elif df["commence_time"].dt.tz is None:
            df["commence_time"] = df["commence_time"].dt.tz_localize("UTC")
        else:
            df["commence_time"] = df["commence_time"].dt.tz_convert("UTC")

    if "commence_time" in df.columns:
        # Identify Live games (Started but still Pending)
        # We start by defaulting is_live to False
        df["is_live"] = False
        
        mask_live = (df["status"] == "Pending") & (df["commence_time"] <= now)
        df.loc[mask_live, "is_live"] = True
        
        # Filter for History: Completed OR Live
        mask = (df["status"] == "Completed") | mask_live
        completed = df[mask].copy()
        
        # Sort: Live games first, then by date descending
        completed = completed.sort_values(["is_live", "commence_time"], ascending=[False, False])
    else:
        completed = df[df["status"] == "Completed"].copy()
        
    # Pagination
    start = (page - 1) * limit
    end = start + limit
    
    paginated = completed.iloc[start:end]
    
    # BATCH LOADING: Get all odds for the paginated games at once
    game_ids = [row["game_id"] for _, row in paginated.iterrows() if "game_id" in row]
    df_all_odds = get_batch_game_odds(game_ids)
    
    # Creates lookup map: game_id -> list of odds records
    odds_map = {}
    if not df_all_odds.empty:
        # Fill NaN for JSON safety
        df_all_odds = df_all_odds.fillna("")
        for game_id, group in df_all_odds.groupby("game_id"):
             odds_map[game_id] = group.to_dict(orient="records")

    # DEFERRED FETCH: Get totals odds (sportsbook lines) for these specific games
    # This replaces the global fetch
    sportsbook_map = {}
    if not paginated.empty:
        try:
             # Only fetch for pending bets in the current view? 
             # Or all bets to show historical lines?
             # For history, we probably want to show what the line WAS or IS?
             # The original code only fetched for PENDING bets.
             
             # Filter primarily for pending or recently completed?
             # Let's stick to the original logic: fetch for pending.
             pending_slice = paginated[paginated["status"] == "Pending"].copy()
             
             if not pending_slice.empty:
                odds_df = get_totals_odds_for_recommended(pending_slice)
                if not odds_df.empty:
                    # We need to map back to game_id
                    # The odds_df has forward_game_id
                    for _, row_odds in odds_df.iterrows():
                        gid = row_odds.get("forward_game_id")
                        if gid:
                            sportsbook_map[gid] = row_odds.to_dict()
        except Exception as e:
            print(f"ERROR: Deferred sportsbook fetch failed: {e}")

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
        
        # Merge sportsbook data if available (DEFERRED)
        game_id = row.get("game_id")
        if game_id and game_id in sportsbook_map:
            sb_data = sportsbook_map[game_id]
            # Update fields
            if sb_data.get("book"): record["book"] = sb_data.get("book")
            if sb_data.get("moneyline"): record["moneyline"] = sb_data.get("moneyline")
            if sb_data.get("line"): 
                record["total_line"] = sb_data.get("line")
                record["recommended_bet"] = f"{str(row.get('side')).title()} {sb_data.get('line')}"
            
            # Add book_url
            book_name = sb_data.get("book", "")
            if book_name:
                record["book_url"] = get_sportsbook_url(book_name)

        # Construct recommended bet string (fallback)
        if "recommended_bet" not in record or not record["recommended_bet"]:
            if pd.notna(row.get("side")) and pd.notna(row.get("total_line")):
                record["recommended_bet"] = f"{row['side'].title()} {row['total_line']}"
            else:
                record["recommended_bet"] = None
            
        # 3. Get full odds data for this game using MAP
        if "game_id" in row and row["game_id"] in odds_map:
             record["odds_data"] = odds_map[row["game_id"]]
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
async def get_upcoming(
    model_type: str = "ensemble",
    user: Optional[dict] = Depends(get_current_user)
):
    """Get upcoming active bets. Premium users see odds and recommendations."""
    is_premium = is_user_premium(user)
    
    df = get_totals_data(model_type)
    
    if df.empty:
        return {
            "data": [],
            "count": 0,
            "is_premium": is_premium
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

    # BATCH LOADING: Get all odds
    game_ids = [row["game_id"] for _, row in upcoming.iterrows() if "game_id" in row]
    df_all_odds = get_batch_game_odds(game_ids)
    
    odds_map = {}
    if not df_all_odds.empty:
        df_all_odds = df_all_odds.fillna("")
        for game_id, group in df_all_odds.groupby("game_id"):
             odds_map[game_id] = group.to_dict(orient="records")

    # DEFERRED FETCH: Get totals odds for these specific games
    # ONLY FETCH IF PREMIUM
    sportsbook_map = {}
    if is_premium and not upcoming.empty:
        try:
             # Fetch for ALL upcoming (usually small number < 50)
             odds_df = get_totals_odds_for_recommended(upcoming)
             if not odds_df.empty:
                for _, row_odds in odds_df.iterrows():
                    gid = row_odds.get("forward_game_id")
                    if gid:
                        sportsbook_map[gid] = row_odds.to_dict()
        except Exception as e:
            print(f"ERROR: Deferred sportsbook fetch failed: {e}")

    records = []
    for _, row in upcoming.iterrows():
        record = row.fillna("").to_dict()
        
        # BASIC INFO (Always visible)
        # game_id, home_team, away_team, commence_time, league
        
        if is_premium:
            # PREMIUM INFO
            record["predicted_total_points"] = safe_get(row, "predicted_total_points")
            record["edge"] = safe_get(row, "edge")
            
            # Merge sportsbook data if available
            game_id = row.get("game_id")
            if game_id and game_id in sportsbook_map:
                sb_data = sportsbook_map[game_id]
                # Update fields
                if sb_data.get("book"): record["book"] = sb_data.get("book")
                if sb_data.get("moneyline"): record["moneyline"] = sb_data.get("moneyline")
                if sb_data.get("line"): 
                    record["total_line"] = sb_data.get("line")
                    record["recommended_bet"] = f"{str(row.get('side')).title()} {sb_data.get('line')}"
                
                # Add book_url
                book_name = sb_data.get("book", "")
                if book_name:
                    record["book_url"] = get_sportsbook_url(book_name)

            # Construct recommended bet string (fallback)
            if "recommended_bet" not in record or not record["recommended_bet"]:
                if pd.notna(row.get("side")) and pd.notna(row.get("total_line")):
                    record["recommended_bet"] = f"{row['side'].title()} {row['total_line']}"
                else:
                    record["recommended_bet"] = None
                
            # 3. Get full odds data for this game using MAP
            if "game_id" in row and row["game_id"] in odds_map:
                 record["odds_data"] = odds_map[row["game_id"]]
            else:
                 record["odds_data"] = []
        else:
            # NON-PREMIUM: Mask sensitive data
            record["predicted_total_points"] = None
            record["edge"] = None
            record["recommended_bet"] = "Premium Only"
            record["odds_data"] = []
            record["book"] = None
            record["book_url"] = None
            record["moneyline"] = None
            record["total_line"] = None
            
            # CRITICAL: Prevent leaking prediction via 'side' or 'description'
            record["side"] = None
            record["description"] = None

        # CLEANUP: Remove unnecessary debug fields for ALL users
        keys_to_remove = ["predicted_prob", "implied_prob", "implied_decimal", "actual_total", "home_score", "away_score", "winner", "won", "profit", "stake", "settled_at"]
        for key in keys_to_remove:
            if key in record:
                del record[key]
            
        records.append(record)
    
    return {
        "data": records,
        "count": len(upcoming),
        "is_premium": is_premium
    }

@router.get("/game/{game_id}/odds")
async def get_odds_for_game(
    game_id: str, 
    model_type: str = "ensemble",
    user: Optional[dict] = Depends(get_current_user)
):
    """Get all sportsbook odds for a specific game. Restricted to Premium users."""
    if not is_user_premium(user):
        raise HTTPException(status_code=403, detail="Premium subscription required to view detailed odds.")
        
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
