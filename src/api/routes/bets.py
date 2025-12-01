from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional
import pandas as pd
from datetime import datetime
import pytz
import numpy as np

from src.dashboard.data import load_forward_test_data, _expand_totals, get_totals_odds_for_recommended, get_game_odds
from src.data.team_mappings import get_full_team_name
from src.data.sportsbook_urls import get_sportsbook_url

router = APIRouter(prefix="/api/bets", tags=["bets"])

def get_totals_data(model_type: str = "ensemble") -> pd.DataFrame:
    """Load and filter data for Over/Under bets."""
    # Load raw data
    raw_df = load_forward_test_data(model_type=model_type)
    
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

    completed = df[df["status"] == "Completed"].copy()
    
    # Sort by date desc
    if "commence_time" in completed.columns:
        completed = completed.sort_values("commence_time", ascending=False)
        
    # Pagination
    start = (page - 1) * limit
    end = start + limit
    
    paginated = completed.iloc[start:end]
    
    # Convert to list of dicts
    # Handle NaN values for JSON serialization
    records = paginated.fillna("").to_dict(orient="records")
    
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
    
    records = upcoming.fillna("").to_dict(orient="records")
    
    return {
        "data": records,
        "count": len(upcoming)
    }

@router.get("/game/{game_id}/odds")
async def get_odds_for_game(game_id: str):
    """Get all sportsbook odds for a specific game."""
    df = get_game_odds(game_id)
    
    if df.empty:
        return {
            "data": [],
            "count": 0
        }
        
    # Convert to list of dicts
    # Handle NaN values
    records = df.fillna("").to_dict(orient="records")
    
    return {
        "data": records,
        "count": len(df)
    }
