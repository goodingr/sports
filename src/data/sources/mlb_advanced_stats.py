"""Fetch advanced team statistics for MLB using pybaseball."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Iterable, List

import pandas as pd

from .utils import SourceDefinition, source_run, write_dataframe

try:
    import pybaseball as pyb  # type: ignore import-not-found
except ModuleNotFoundError as exc:  # pragma: no cover - import guard
    raise SystemExit(
        "pybaseball is required for MLB advanced stats. Install it with `poetry add pybaseball`."
    ) from exc


LOGGER = logging.getLogger(__name__)


def _fetch_team_batting_stats(year: int) -> pd.DataFrame:
    """Fetch team batting statistics for a year."""
    try:
        # pybaseball's team_batting function returns team stats
        df = pyb.team_batting(year, qual=1)  # qual=1 means minimum 1 PA
        if df.empty:
            return pd.DataFrame()
        df["season"] = year
        return df
    except Exception as e:
        LOGGER.error("Error fetching team batting stats for year %s: %s", year, e)
        return pd.DataFrame()


def _fetch_team_pitching_stats(year: int) -> pd.DataFrame:
    """Fetch team pitching statistics for a year."""
    try:
        # pybaseball's team_pitching function returns team stats
        df = pyb.team_pitching(year, qual=1)  # qual=1 means minimum 1 IP
        if df.empty:
            return pd.DataFrame()
        df["season"] = year
        return df
    except Exception as e:
        LOGGER.error("Error fetching team pitching stats for year %s: %s", year, e)
        return pd.DataFrame()


def _calculate_advanced_metrics(batting_df: pd.DataFrame, pitching_df: pd.DataFrame) -> pd.DataFrame:
    """Calculate advanced metrics from batting and pitching stats."""
    if batting_df.empty and pitching_df.empty:
        return pd.DataFrame()
    
    # Merge batting and pitching stats
    if not batting_df.empty and not pitching_df.empty:
        # Find common team identifier column
        team_col = None
        for col in ["Tm", "Team", "team", "name"]:
            if col in batting_df.columns and col in pitching_df.columns:
                team_col = col
                break
        
        if team_col:
            merged = batting_df.merge(
                pitching_df,
                on=[team_col, "season"],
                how="outer",
                suffixes=("_bat", "_pit")
            )
        else:
            # If no common column, just concatenate
            merged = pd.concat([batting_df, pitching_df], axis=1)
    elif not batting_df.empty:
        merged = batting_df.copy()
    else:
        merged = pitching_df.copy()
    
    # Calculate advanced metrics if we have the data
    # wOBA requires specific columns that may not be available
    # For now, we'll store what we have and calculate what we can
    
    # Normalize team column
    if "Tm" in merged.columns:
        merged = merged.rename(columns={"Tm": "team"})
    elif "Team" in merged.columns:
        merged = merged.rename(columns={"Team": "team"})
    elif "name" in merged.columns:
        merged = merged.rename(columns={"name": "team"})
    
    if "team" not in merged.columns:
        LOGGER.warning("Could not find team column in merged stats")
        return pd.DataFrame()
    
    merged["team"] = merged["team"].astype(str)
    
    return merged


def ingest(*, seasons: Iterable[int] | None = None, timeout: int = 120) -> str:  # noqa: ARG001
    """Fetch advanced team statistics for MLB."""
    definition = SourceDefinition(
        key="mlb_advanced_stats",
        name="MLB advanced team statistics",
        league="MLB",
        category="advanced_metrics",
        url="https://github.com/jldbc/pybaseball",
        default_frequency="daily",
        storage_subdir="mlb/advanced_stats",
    )
    
    season_list: List[int]
    if seasons:
        season_list = sorted({int(season) for season in seasons})
    else:
        current = datetime.utcnow().year
        season_list = list(range(current - 4, current + 1))
    
    output_dir = ""
    with source_run(definition) as run:
        output_dir = str(run.storage_dir)
        all_batting: List[pd.DataFrame] = []
        all_pitching: List[pd.DataFrame] = []
        
        for season in season_list:
            LOGGER.info("Fetching MLB advanced stats for season %s", season)
            
            batting_df = _fetch_team_batting_stats(season)
            if not batting_df.empty:
                all_batting.append(batting_df)
            
            pitching_df = _fetch_team_pitching_stats(season)
            if not pitching_df.empty:
                all_pitching.append(pitching_df)
            
            # Small delay to avoid rate limiting
            import time
            time.sleep(1)
        
        if not all_batting and not all_pitching:
            run.set_message("No MLB stats retrieved")
            run.set_raw_path(run.storage_dir)
            return output_dir
        
        # Combine all seasons
        combined_batting = pd.concat(all_batting, ignore_index=True) if all_batting else pd.DataFrame()
        combined_pitching = pd.concat(all_pitching, ignore_index=True) if all_pitching else pd.DataFrame()
        
        # Calculate advanced metrics
        stats = _calculate_advanced_metrics(combined_batting, combined_pitching)
        
        if stats.empty:
            run.set_message("No advanced stats calculated")
            run.set_raw_path(run.storage_dir)
            return output_dir
        
        path = run.make_path("advanced_stats.parquet")
        write_dataframe(stats, path)
        run.record_file(path, metadata={"rows": len(stats)}, records=len(stats))
        
        run.set_records(len(stats))
        run.set_message(f"Captured {len(stats)} MLB advanced stat rows")
        run.set_raw_path(run.storage_dir)
    
    return output_dir


__all__ = ["ingest"]

