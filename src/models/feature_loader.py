"""Feature loader for prediction time - loads team metrics, rolling stats, injuries, weather, etc."""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

import numpy as np
import pandas as pd

from src.data.config import RAW_DATA_DIR
from src.data.team_mappings import normalize_team_code

LOGGER = logging.getLogger(__name__)

RAW_SOURCES_DIR = RAW_DATA_DIR / "sources"
SOCCER_LEAGUES = {"EPL", "LALIGA", "BUNDESLIGA", "SERIEA", "LIGUE1"}
LEAGUE_STORAGE_OVERRIDES = {league: "soccer" for league in SOCCER_LEAGUES}


def _latest_source_directory(league: str, source_subdir: str) -> Optional[Path]:
    """Find the most recent source directory for a given league and subdirectory."""
    search_roots = [RAW_SOURCES_DIR / league.lower() / source_subdir]

    override_root = LEAGUE_STORAGE_OVERRIDES.get(league.upper())
    if override_root:
        search_roots.append(RAW_SOURCES_DIR / override_root / source_subdir)

    for base_dir in search_roots:
        if not base_dir.exists():
            continue

        subdirs = [d for d in base_dir.iterdir() if d.is_dir()]
        if not subdirs:
            continue

        subdirs.sort(key=lambda x: x.name, reverse=True)
        return subdirs[0]

    return None


class FeatureLoader:
    """Loads features from parquet files at prediction time."""
    
    def __init__(self, league: str):
        """Initialize feature loader for a specific league."""
        self.league = league.upper()
        self._cache: Dict[str, pd.DataFrame] = {}
    
    def _load_latest_parquet(self, source_subdir: str, filename: str) -> pd.DataFrame:
        """Load the latest parquet file from a source directory."""
        cache_key = f"{source_subdir}/{filename}"
        if cache_key in self._cache:
            return self._cache[cache_key]
        
        directory = _latest_source_directory(self.league, source_subdir)
        if not directory:
            LOGGER.debug("No directory found for %s/%s", self.league, source_subdir)
            df = pd.DataFrame()
        else:
            path = directory / filename
            if path.exists():
                try:
                    df = pd.read_parquet(path)
                    LOGGER.debug("Loaded %s from %s", filename, path)
                except Exception as e:
                    LOGGER.warning("Failed to load %s: %s", path, e)
                    df = pd.DataFrame()
            else:
                LOGGER.debug("File not found: %s", path)
                df = pd.DataFrame()
        
        self._cache[cache_key] = df
        return df
    
    def load_team_metrics(self, season: Optional[int] = None) -> pd.DataFrame:
        """Load season-level team metrics."""
        df = self._load_latest_parquet("team_metrics", "team_metrics.parquet")
        if df.empty:
            return df
        
        if season is not None and "season" in df.columns:
            filtered = df[df["season"] == season]
            if filtered.empty:
                latest_season = df["season"].max()
                df = df[df["season"] == latest_season].copy()
            else:
                df = filtered.copy()

        if "team" not in df.columns:
            if "TEAM_ABBREVIATION" in df.columns:
                df = df.rename(columns={"TEAM_ABBREVIATION": "team"})
            elif "TEAM_NAME" in df.columns:
                df = df.copy()
                df["team"] = df["TEAM_NAME"].apply(
                    lambda name: normalize_team_code(self.league, name) if name else None
                )

        if "team" in df.columns:
            df["team"] = df["team"].astype(str).str.upper()

        return df
    
    def load_rolling_metrics(self) -> pd.DataFrame:
        """Load rolling game-by-game metrics."""
        df = self._load_latest_parquet("rolling_metrics", "rolling_metrics.parquet")
        return df
    
    def load_injuries(self, game_date: Optional[datetime] = None) -> pd.DataFrame:
        """Load injury data. If game_date is provided, filter to that date."""
        df = pd.DataFrame()
        loaded_filename = None
        
        # Try ESPN source first (alternative to blocked NBA CDN)
        if self.league == "NBA":
            df = self._load_latest_parquet("injuries_espn", "injuries.parquet")
            if not df.empty:
                loaded_filename = "injuries.parquet"
        
        # Fallback to regular injuries source
        if df.empty:
            # Try different possible filenames
            for filename in ["injuries.parquet", "injury_reports.parquet", "injuries.csv"]:
                df = self._load_latest_parquet("injuries", filename)
                if not df.empty:
                    loaded_filename = filename
                    break
        
        if df.empty:
            return df

        # If CSV, try to read it directly
        if loaded_filename and "csv" in loaded_filename.lower():
            directory = _latest_source_directory(self.league, "injuries")
            if directory:
                path = directory / loaded_filename
                if path.exists():
                    try:
                        df = pd.read_csv(path)
                    except Exception as e:
                        LOGGER.warning("Failed to load injuries CSV: %s", e)
                        return pd.DataFrame()

        # Normalize team/status columns after loading
        team_col = None
        for candidate in ("team", "team_code", "club_code", "team_abbreviation"):
            if candidate in df.columns:
                team_col = candidate
                break
        if team_col and team_col != "team":
            df["team"] = df[team_col].astype(str)
        elif team_col is None and "team" in df.columns:
            df["team"] = df["team"].astype(str)

        if "status" not in df.columns:
            for status_col in (
                "report_status",
                "injury_status",
                "injury_game_status",
                "game_status",
                "status_description",
            ):
                if status_col in df.columns:
                    df["status"] = df[status_col]
                    break

        if "position" not in df.columns:
            for pos_col in ("player_position", "pos"):
                if pos_col in df.columns:
                    df["position"] = df[pos_col]
                    break

        if "team" in df.columns:
            df["team"] = df["team"].astype(str).str.upper()
        if "status" in df.columns:
            df["status"] = df["status"].astype(str)
        if "position" in df.columns:
            df["position"] = df["position"].astype(str).str.upper()
        
        if game_date is not None and "game_date" in df.columns:
            df["game_date"] = pd.to_datetime(df["game_date"], errors="coerce")
            df = df[df["game_date"] <= game_date].copy()
        
        return df
    
    def load_weather(self, game_date: Optional[datetime] = None) -> pd.DataFrame:
        """Load weather data. If game_date is provided, filter to that date."""
        df = self._load_latest_parquet("weather", "weather.parquet")
        if df.empty:
            return df
        
        if game_date is not None and "game_date" in df.columns:
            df["game_date"] = pd.to_datetime(df["game_date"], errors="coerce")
            df = df[df["game_date"] == game_date].copy()
        
        return df
    
    def load_advanced_stats(self) -> pd.DataFrame:
        """Load advanced stats (xG, efficiency, etc.) - sport-specific."""
        df = self._load_latest_parquet("advanced_stats", "advanced_stats.parquet")
        if df.empty:
            return df

        df = df.copy()
        if "league" in df.columns:
            df["league"] = df["league"].astype(str).str.upper()
            league_mask = (
                (df["league"] == self.league)
                | df["league"].isna()
                | (df["league"] == "")
            )
            df = df[league_mask]
        return df

    def get_advanced_metric(
        self,
        team: str,
        metric_name: str,
        season: Optional[int] = None,
        default: float = np.nan,
    ) -> float:
        """Get an advanced stat value for a team (e.g., xG, possession)."""
        df = self.load_advanced_stats()
        if df.empty or metric_name not in df.columns:
            return default

        working = df.copy()
        if season is not None and "season" in working.columns:
            working = working[
                pd.to_numeric(working["season"], errors="coerce") == season
            ]
        if working.empty:
            return default

        team_code = normalize_team_code(self.league, team)
        masks = []

        if "team_code" in working.columns:
            masks.append(working["team_code"].astype(str).str.upper() == team_code)

        if "team" in working.columns:
            normalized = working["team"].astype(str).apply(
                lambda name: normalize_team_code(self.league, name)
            )
            masks.append(normalized == team_code)

        if not masks:
            return default

        team_mask = masks[0]
        for mask in masks[1:]:
            team_mask = team_mask | mask

        matches = working[team_mask]
        if matches.empty:
            return default

        value = matches.iloc[0].get(metric_name)
        if value is None or pd.isna(value):
            return default

        try:
            return float(value)
        except (TypeError, ValueError):
            return default
    
    def get_team_metric(
        self,
        team: str,
        metric_name: str,
        season: Optional[int] = None,
        default: float = np.nan
    ) -> float:
        """Get a specific team metric value."""
        metrics_df = self.load_team_metrics(season=season)
        if metrics_df.empty:
            return default
        
        # Normalize team code
        team_str = str(team).upper()
        if "team" in metrics_df.columns:
            pass
        elif "TEAM_ABBREVIATION" in metrics_df.columns:
            metrics_df = metrics_df.rename(columns={"TEAM_ABBREVIATION": "team"})
        else:
            return default
        
        metrics_df["team"] = metrics_df["team"].astype(str).str.upper()
        team_row = metrics_df[metrics_df["team"] == team_str]
        
        if team_row.empty:
            return default
        
        if metric_name not in team_row.columns:
            return default
        
        value = team_row.iloc[0][metric_name]
        if pd.isna(value):
            return default
        
        return float(value)
    
    def get_rolling_metric(
        self,
        team: str,
        metric_name: str,
        game_date: Optional[datetime] = None,
        window: int = 3,
        default: float = np.nan
    ) -> float:
        """Get a rolling metric value for a team."""
        rolling_df = self.load_rolling_metrics()
        if rolling_df.empty:
            return default
        
        # Normalize team code
        team_str = str(team).upper()
        if rolling_df.empty or "team" not in rolling_df.columns:
            return default
        
        rolling_df = rolling_df.copy()
        rolling_df["team"] = rolling_df["team"].astype(str).str.upper()
        team_data = rolling_df[rolling_df["team"] == team_str].copy()
        
        if team_data.empty:
            return default
        
        # Filter by date if provided
        if game_date is not None and "game_date" in team_data.columns:
            team_data["game_date"] = pd.to_datetime(team_data["game_date"], errors="coerce")
            # Ensure both are timezone-naive for comparison
            compare_date = pd.to_datetime(game_date)
            if compare_date.tz is not None:
                compare_date = compare_date.tz_localize(None)
            team_data = team_data[team_data["game_date"] < compare_date].copy()
        
        # Sort by date descending
        if "game_date" in team_data.columns:
            team_data = team_data.sort_values("game_date", ascending=False)
        
        # Get last N games
        if len(team_data) > window:
            team_data = team_data.head(window)
        
        if metric_name not in team_data.columns:
            return default
        
        # Calculate mean of last N games
        values = team_data[metric_name].dropna()
        if values.empty:
            return default
        
        return float(values.mean())
    
    def get_injury_count(
        self,
        team: str,
        game_date: Optional[datetime] = None,
        status: Optional[str] = None,
        position: Optional[str] = None
    ) -> int:
        """Get count of injuries for a team."""
        injuries_df = self.load_injuries(game_date=game_date)
        if injuries_df.empty:
            return 0
        
        # Normalize team code
        team_str = str(team).upper()
        if "team" in injuries_df.columns or "team_code" in injuries_df.columns:
            team_col = "team" if "team" in injuries_df.columns else "team_code"
            injuries_df = injuries_df.rename(columns={team_col: "team"})
        else:
            return 0
        
        injuries_df["team"] = injuries_df["team"].astype(str).str.upper()
        team_injuries = injuries_df[injuries_df["team"] == team_str].copy()
        
        if team_injuries.empty:
            return 0
        
        # Filter by status if provided
        if status is not None and "status" in team_injuries.columns:
            team_injuries = team_injuries[team_injuries["status"] == status]
        
        # Filter by position if provided
        if position is not None and "position" in team_injuries.columns:
            team_injuries = team_injuries[team_injuries["position"] == position]
        
        return len(team_injuries)
    
    def get_weather_features(
        self,
        game_id: Optional[str] = None,
        game_date: Optional[datetime] = None,
        venue: Optional[str] = None
    ) -> Dict[str, float]:
        """Get weather features for a game."""
        weather_df = self.load_weather(game_date=game_date)
        if weather_df.empty:
            return {
                "game_temperature_f": np.nan,
                "game_wind_mph": np.nan,
                "is_weather_precip": 0.0,
                "is_weather_dome": 0.0,
            }
        
        # Try to match by game_id, game_date, or venue
        if game_id is not None and "game_id" in weather_df.columns:
            match = weather_df[weather_df["game_id"] == game_id]
            if not match.empty:
                row = match.iloc[0]
            else:
                return self._default_weather()
        elif game_date is not None and "game_date" in weather_df.columns:
            weather_df["game_date"] = pd.to_datetime(weather_df["game_date"], errors="coerce")
            match = weather_df[weather_df["game_date"] == game_date]
            if not match.empty:
                row = match.iloc[0]
            else:
                return self._default_weather()
        elif venue is not None and "venue" in weather_df.columns:
            match = weather_df[weather_df["venue"] == venue]
            if not match.empty:
                row = match.iloc[0]
            else:
                return self._default_weather()
        else:
            return self._default_weather()
        
        result = {
            "game_temperature_f": float(row.get("temperature_f", np.nan)) if "temperature_f" in row else np.nan,
            "game_wind_mph": float(row.get("wind_mph", np.nan)) if "wind_mph" in row else np.nan,
            "is_weather_precip": 1.0 if row.get("precipitation", False) else 0.0,
            "is_weather_dome": 1.0 if row.get("dome", False) else 0.0,
        }
        return result
    
    def _default_weather(self) -> Dict[str, float]:
        """Return default weather features."""
        return {
            "game_temperature_f": np.nan,
            "game_wind_mph": np.nan,
            "is_weather_precip": 0.0,
            "is_weather_dome": 0.0,
        }
    
    def clear_cache(self) -> None:
        """Clear the feature cache."""
        self._cache.clear()
