
from src.db.core import connect
import json
import logging
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, Optional
from src.data.team_mappings import normalize_team_code
from src.data.config import RAW_DATA_DIR

LOGGER = logging.getLogger(__name__)

class FeatureLoader:
    """Loads features from the database at prediction time."""
    
    def __init__(self, league: str):
        self.league = league.upper()
    
    def load_team_metrics(self, season: Optional[int] = None) -> pd.DataFrame:
        """Load season-level team metrics from team_features table."""
        with connect() as conn:
            query = """
                SELECT tf.feature_json 
                FROM team_features tf
                JOIN teams t ON tf.team_id = t.team_id
                JOIN sports s ON t.sport_id = s.sport_id
                WHERE s.league = ? AND tf.feature_set = 'season_stats'
            """
            params = [self.league]
            
            # Since JSON blobs are stored, we can't easily filter by season in SQL unless we extract it.
            # We'll load all for league and filter in memory for now, or use JSON extraction if available.
            # SQLite supports json_extract but pandas read_sql is easier for bulk load.
            
            cursor = conn.execute(query, params)
            rows = cursor.fetchall()
            
        if not rows:
            return pd.DataFrame()
            
        data = [json.loads(r[0]) for r in rows]
        df = pd.DataFrame(data)
        
        if df.empty:
            return df

        if season is not None and "season" in df.columns:
            filtered = df[df["season"] == season]
            if filtered.empty:
                 latest_season = df["season"].max()
                 df = df[df["season"] == latest_season].copy()
            else:
                 df = filtered.copy()
        
        # Normalize team column
        if "team" not in df.columns:
             if "TEAM_ABBREVIATION" in df.columns:
                 df = df.rename(columns={"TEAM_ABBREVIATION": "team"})
        
        if "team" in df.columns:
            df["team"] = df["team"].astype(str).str.upper()
            
        return df

    def load_rolling_metrics(self) -> pd.DataFrame:
        """Load rolling game-by-game metrics from team_features table."""
        with connect() as conn:
             # We want all game stats for the league
             query = """
                SELECT tf.feature_json
                FROM team_features tf
                JOIN teams t ON tf.team_id = t.team_id
                JOIN sports s ON t.sport_id = s.sport_id
                WHERE s.league = ? AND tf.feature_set = 'game_stats'
             """
             cursor = conn.execute(query, (self.league,))
             rows = cursor.fetchall()
             
        if not rows:
            return pd.DataFrame()
            
        data = [json.loads(r[0]) for r in rows]
        df = pd.DataFrame(data)
        
        # Ensure timestamp conversion
        if "game_date" in df.columns:
             df["game_date"] = pd.to_datetime(df["game_date"])
             
        if "team" in df.columns:
             df["team"] = df["team"].astype(str).str.upper()
             
        return df

    def load_injuries(self, game_date: Optional[datetime] = None) -> pd.DataFrame:
        """Load injury data from injury_reports table."""
        with connect() as conn:
            query = """
                SELECT 
                    team_code as team,
                    player_name as player,
                    position,
                    status,
                    report_date as game_date,
                    detail as description
                FROM injury_reports
                WHERE league = ?
            """
            params = [self.league]
            if game_date:
                # We can filter in SQL if we trust the format, but let's load all active injuries?
                # Actually, filtering by date is tricky if we don't have exact match.
                # Assuming requester handles filtering logic similar to before.
                pass
                
            df = pd.read_sql_query(query, conn, params=params)
            
        if df.empty:
            return df
            
        if "team" in df.columns:
            df["team"] = df["team"].astype(str).str.upper()
            
        if game_date is not None and "game_date" in df.columns:
             df["game_date"] = pd.to_datetime(df["game_date"], errors="coerce")
             # Filter logic from original: df[df["game_date"] <= game_date]
             df = df[df["game_date"] <= game_date].copy()
             
        return df

    def load_weather(self, game_date: Optional[datetime] = None) -> pd.DataFrame:
        # DB doesn't have weather table yet, returning empty default
        return pd.DataFrame()

    def load_advanced_stats(self) -> pd.DataFrame:
         # Not migrated yet
         return pd.DataFrame()

    # Reuse helper methods but ensure they call the new load_* methods
    def get_advanced_metric(self, team: str, metric_name: str, season: Optional[int] = None, default: float = np.nan) -> float:
        return default # Placeholder

    def get_team_metric(self, team: str, metric_name: str, season: Optional[int] = None, default: float = np.nan) -> float:
        metrics_df = self.load_team_metrics(season=season)
        if metrics_df.empty: return default
        
        team_str = str(team).upper()
        # Normalize column checking
        if "team" not in metrics_df.columns: return default
        
        team_row = metrics_df[metrics_df["team"] == team_str]
        if team_row.empty: return default
        
        val = team_row.iloc[0].get(metric_name)
        return float(val) if pd.notnull(val) else default

    def get_rolling_metric(self, team: str, metric_name: str, game_date: Optional[datetime] = None, window: int = 3, default: float = np.nan) -> float:
        rolling_df = self.load_rolling_metrics()
        if rolling_df.empty: return default
        
        team_str = str(team).upper()
        if "team" not in rolling_df.columns: return default
        
        team_data = rolling_df[rolling_df["team"] == team_str].copy()
        if team_data.empty: return default
        
        if game_date is not None and "game_date" in team_data.columns:
             # Ensure types match
             if team_data["game_date"].dt.tz is not None:
                 team_data["game_date"] = team_data["game_date"].dt.tz_localize(None)
             compare_date = pd.to_datetime(game_date)
             if compare_date.tz is not None:
                 compare_date = compare_date.tz_localize(None)
                 
             team_data = team_data[team_data["game_date"] < compare_date]
             
        if team_data.empty: return default
        
        team_data = team_data.sort_values("game_date", ascending=False).head(window)
        val = team_data[metric_name].mean()
        return float(val) if pd.notnull(val) else default
        
    def get_injury_count(self, team: str, game_date: Optional[datetime] = None, status: Optional[str] = None, position: Optional[str] = None) -> int:
        df = self.load_injuries(game_date)
        if df.empty: return 0
        
        team_str = str(team).upper()
        if "team" not in df.columns: return 0
        
        df = df[df["team"] == team_str]
        if status: df = df[df["status"] == status]
        if position: df = df[df["position"] == position]
        
        return len(df)

    def get_weather_features(self, game_id: Optional[str] = None, game_date: Optional[datetime] = None, venue: Optional[str] = None) -> Dict[str, float]:
        return self._default_weather()

    def _default_weather(self) -> Dict[str, float]:
        return {
            "game_temperature_f": np.nan, "game_wind_mph": np.nan,
            "is_weather_precip": 0.0, "is_weather_dome": 0.0,
        }
        
    def clear_cache(self):
        pass
