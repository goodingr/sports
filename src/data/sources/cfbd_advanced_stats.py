"""Fetch advanced team statistics from CollegeFootballData API."""

from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Iterable, List

import pandas as pd
import requests

from .utils import SourceDefinition, source_run, write_dataframe
from src.data.team_mappings import normalize_team_code


LOGGER = logging.getLogger(__name__)


def _fetch_team_stats(season: int, api_key: str) -> pd.DataFrame:
    """Fetch team stats for a season."""
    url = "https://api.collegefootballdata.com/stats/season/advanced"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Accept": "application/json",
    }
    params = {
        "year": season,
    }
    
    try:
        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        if not data:
            return pd.DataFrame()
        
        df = pd.DataFrame(data)
        df["season"] = season

        # Normalize team codes
        if "team" in df.columns:
            df["team"] = df["team"].apply(lambda name: normalize_team_code("CFB", str(name)))

        # Flatten offense/defense dicts into scalar columns
        for prefix in ("offense", "defense"):
            if prefix not in df.columns:
                continue
            df[f"{prefix}_ppa"] = df[prefix].apply(
                lambda value: value.get("ppa") if isinstance(value, dict) else None
            )
            df[f"{prefix}_successRate"] = df[prefix].apply(
                lambda value: value.get("successRate") if isinstance(value, dict) else None
            )
            df[f"{prefix}_explosiveness"] = df[prefix].apply(
                lambda value: value.get("explosiveness") if isinstance(value, dict) else None
            )
            df[f"{prefix}_pointsPerOpportunity"] = df[prefix].apply(
                lambda value: value.get("pointsPerOpportunity") if isinstance(value, dict) else None
            )
        df = df.drop(columns=["offense", "defense"], errors="ignore")
        return df
        
    except Exception as e:
        LOGGER.error("Error fetching CFBD advanced stats for season %s: %s", season, e)
        return pd.DataFrame()


def ingest(*, seasons: Iterable[int] | None = None, timeout: int = 60) -> str:  # noqa: ARG001
    """Fetch advanced team statistics from CollegeFootballData API."""
    definition = SourceDefinition(
        key="cfbd_advanced_stats",
        name="CollegeFootballData advanced team stats",
        league="CFB",
        category="advanced_metrics",
        url="https://api.collegefootballdata.com/",
        default_frequency="daily",
        storage_subdir="cfb/advanced_stats",
    )
    
    api_key = os.getenv("CFBD_API_KEY")
    if not api_key:
        raise SystemExit(
            "CFBD_API_KEY environment variable is required. "
            "Get a free API key from https://collegefootballdata.com/"
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
        all_frames: List[pd.DataFrame] = []
        
        for season in season_list:
            LOGGER.info("Fetching CFBD advanced stats for season %s", season)
            df = _fetch_team_stats(season, api_key)
            
            if not df.empty:
                all_frames.append(df)
                LOGGER.info("Fetched %d team stat rows for season %s", len(df), season)
            else:
                LOGGER.warning("No stats found for season %s", season)
        
        if not all_frames:
            run.set_message("No advanced stats retrieved")
            run.set_raw_path(run.storage_dir)
            return output_dir
        
        stats = pd.concat(all_frames, ignore_index=True)
        
        # Normalize team column name
        if "team" in stats.columns:
            stats["team"] = stats["team"].astype(str)
        elif "school" in stats.columns:
            stats = stats.rename(columns={"school": "team"})
            stats["team"] = stats["team"].astype(str)
        
        path = run.make_path("advanced_stats.parquet")
        write_dataframe(stats, path)
        run.record_file(path, metadata={"rows": len(stats)}, records=len(stats))
        
        run.set_records(len(stats))
        run.set_message(f"Captured {len(stats)} CFBD advanced stat rows")
        run.set_raw_path(run.storage_dir)
    
    return output_dir


__all__ = ["ingest"]

