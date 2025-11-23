"""Feature engineering for player-level statistics."""

import pandas as pd
from src.db.core import connect

def load_player_stats_df(league: str = "NBA") -> pd.DataFrame:
    """Load all player stats for a league."""
    with connect() as conn:
        query = """
        SELECT 
            ps.game_id,
            t.code as team,
            ps.player_id,
            ps.min,
            ps.pts,
            ps.reb,
            ps.ast,
            ps.plus_minus,
            g.start_time_utc
        FROM player_stats ps
        JOIN games g ON ps.game_id = g.game_id
        JOIN teams t ON ps.team_id = t.team_id
        JOIN sports s ON g.sport_id = s.sport_id
        WHERE s.league = ?
        ORDER BY g.start_time_utc
        """
        return pd.read_sql_query(query, conn, params=(league,))

def calculate_player_rolling_features(df: pd.DataFrame, windows: list[int] = [10]) -> pd.DataFrame:
    """Calculate rolling features for each player."""
    
    # Ensure sorted by time
    df = df.sort_values("start_time_utc")
    
    features = df[["game_id", "team", "player_id"]].copy()
    
    metrics = ["pts", "reb", "ast", "min", "plus_minus"]
    
    for player_id, group in df.groupby("player_id"):
        # Shift to avoid leakage (stats from *previous* games)
        shifted = group[metrics].shift(1)
        
        for window in windows:
            rolling = shifted.rolling(window=window, min_periods=1).mean()
            
            for metric in metrics:
                col_name = f"player_{metric}_rolling_{window}"
                # Assign back to the original index
                features.loc[group.index, col_name] = rolling[metric]
                
    return features

def aggregate_team_player_features(player_features: pd.DataFrame) -> pd.DataFrame:
    """Aggregate player rolling features into team-level features."""
    
    # Identify feature columns
    feature_cols = [c for c in player_features.columns if "rolling" in c]
    
    # Group by game and team, then sum (assuming we want total potential output of the roster)
    # Note: Summing rolling averages of *participating* players gives an estimate of the team's strength 
    # based on who actually played. This is valid for training if we assume we can predict the roster.
    
    team_features = player_features.groupby(["game_id", "team"])[feature_cols].sum().reset_index()
    
    # Rename columns to indicate team aggregation
    rename_map = {c: f"team_{c}_sum" for c in feature_cols}
    team_features = team_features.rename(columns=rename_map)
    
    return team_features

def build_player_features(league: str = "NBA") -> pd.DataFrame:
    """End-to-end builder for player-derived team features."""
    df = load_player_stats_df(league)
    if df.empty:
        return pd.DataFrame()
        
    player_feats = calculate_player_rolling_features(df)
    team_feats = aggregate_team_player_features(player_feats)
    
    return team_feats
