"""Calculate rolling game-by-game metrics for NBA teams."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Iterable, List

import pandas as pd

from .utils import SourceDefinition, source_run, write_dataframe

try:
    from nba_api.stats.endpoints import leaguegamelog  # type: ignore import-not-found
except ModuleNotFoundError as exc:  # pragma: no cover - import guard
    raise SystemExit(
        "nba_api is required for NBA rolling metrics. Install it with `poetry add nba-api`."
    ) from exc


LOGGER = logging.getLogger(__name__)


def _season_string(season_year: int) -> str:
    return f"{season_year}-{str(season_year + 1)[-2:]}"


def _calculate_rolling_metrics(games_df: pd.DataFrame) -> pd.DataFrame:
    """Calculate rolling metrics for each team."""
    if games_df.empty:
        return pd.DataFrame()
    
    # Ensure we have required columns
    required_cols = ["SEASON_ID", "TEAM_ID", "TEAM_ABBREVIATION", "GAME_DATE", "WL", "PTS"]
    missing_cols = [col for col in required_cols if col not in games_df.columns]
    if missing_cols:
        LOGGER.warning("Missing required columns: %s", missing_cols)
        return pd.DataFrame()
    
    # Sort by team and date
    games_df = games_df.sort_values(["TEAM_ABBREVIATION", "GAME_DATE"]).copy()
    
    # Parse game date (can be in different formats)
    games_df["game_date"] = pd.to_datetime(games_df["GAME_DATE"], errors="coerce")
    games_df = games_df[games_df["game_date"].notna()].copy()
    
    if games_df.empty:
        LOGGER.warning("No valid game dates found after parsing")
        return pd.DataFrame()
    
    # Extract season year
    games_df["season"] = games_df["game_date"].dt.year
    # Adjust for seasons that start in one year and end in the next
    games_df.loc[games_df["game_date"].dt.month >= 10, "season"] = games_df.loc[games_df["game_date"].dt.month >= 10, "game_date"].dt.year
    games_df.loc[games_df["game_date"].dt.month < 10, "season"] = games_df.loc[games_df["game_date"].dt.month < 10, "game_date"].dt.year - 1
    
    # Calculate win (1) or loss (0)
    games_df["win"] = (games_df["WL"] == "W").astype(int)
    
    # Calculate point differential by matching games with opponents
    # Group by GAME_ID to get both teams' scores
    games_df["point_diff"] = None
    for game_id in games_df["GAME_ID"].unique():
        game_rows = games_df[games_df["GAME_ID"] == game_id]
        if len(game_rows) == 2:
            # Two teams in the same game
            team1_pts = game_rows.iloc[0]["PTS"]
            team2_pts = game_rows.iloc[1]["PTS"]
            games_df.loc[games_df["GAME_ID"] == game_id, "point_diff"] = games_df.loc[games_df["GAME_ID"] == game_id, "PTS"] - (team1_pts + team2_pts - games_df.loc[games_df["GAME_ID"] == game_id, "PTS"])
    
    results = []
    
    for team in games_df["TEAM_ABBREVIATION"].unique():
        team_games = games_df[games_df["TEAM_ABBREVIATION"] == team].copy()
        team_games = team_games.sort_values("game_date")
        
        for idx, row in team_games.iterrows():
            # Get games before this one
            prior_games = team_games[team_games["game_date"] < row["game_date"]]
            
            result_row = {
                "team": team,
                "game_date": row["game_date"],
                "season": row["season"],
                "game_id": f"NBA_{row['GAME_ID']}" if "GAME_ID" in row else None,
            }
            
            # Calculate rolling win percentage
            for window in [3, 5, 10]:
                window_games = prior_games.tail(window)
                if len(window_games) > 0:
                    result_row[f"rolling_win_pct_{window}"] = window_games["win"].mean()
                else:
                    result_row[f"rolling_win_pct_{window}"] = None
            
            # Calculate rolling point differential
            for window in [3, 5, 10]:
                window_games = prior_games.tail(window)
                if len(window_games) > 0 and "point_diff" in window_games.columns:
                    point_diffs = window_games["point_diff"].dropna()
                    if len(point_diffs) > 0:
                        result_row[f"rolling_point_diff_{window}"] = point_diffs.mean()
                    else:
                        result_row[f"rolling_point_diff_{window}"] = None
                else:
                    result_row[f"rolling_point_diff_{window}"] = None
            
            results.append(result_row)
    
    return pd.DataFrame(results)


def ingest(*, seasons: Iterable[int] | None = None, timeout: int = 30) -> str:  # noqa: ARG001
    """Calculate rolling metrics for NBA teams."""
    definition = SourceDefinition(
        key="nba_rolling_metrics",
        name="NBA rolling game-by-game metrics",
        league="NBA",
        category="advanced_metrics",
        url="https://stats.nba.com/",
        default_frequency="hourly",
        storage_subdir="nba/rolling_metrics",
    )
    
    season_list: List[int]
    if seasons:
        season_list = sorted({int(season) for season in seasons})
    else:
        current = datetime.utcnow().year
        season_list = [current - 1, current]
    
    output_dir = ""
    with source_run(definition) as run:
        output_dir = str(run.storage_dir)
        all_frames: List[pd.DataFrame] = []
        
        for season_year in season_list:
            season_str = _season_string(season_year)
            LOGGER.info("Fetching NBA game logs for %s to calculate rolling metrics", season_str)
            
            try:
                endpoint = leaguegamelog.LeagueGameLog(season=season_str, season_type_all_star="Regular Season")
                df = endpoint.get_data_frames()[0]
                
                if df.empty:
                    LOGGER.warning("No game logs found for season %s", season_str)
                    continue
                
                # Calculate rolling metrics
                rolling_df = _calculate_rolling_metrics(df)
                
                if not rolling_df.empty:
                    all_frames.append(rolling_df)
                    LOGGER.info("Calculated rolling metrics for %d games in season %s", len(rolling_df), season_str)
                else:
                    LOGGER.warning("No rolling metrics calculated for season %s", season_str)
                    
            except Exception as e:
                LOGGER.error("Error fetching/processing season %s: %s", season_str, e)
                continue
        
        if not all_frames:
            run.set_message("No rolling metrics calculated")
            run.set_raw_path(run.storage_dir)
            return output_dir
        
        metrics = pd.concat(all_frames, ignore_index=True)
        metrics["team"] = metrics["team"].astype(str)
        
        path = run.make_path("rolling_metrics.parquet")
        write_dataframe(metrics, path)
        run.record_file(path, metadata={"rows": len(metrics)}, records=len(metrics))
        
        run.set_records(len(metrics))
        run.set_message(f"Calculated rolling metrics for {len(metrics)} games")
        run.set_raw_path(run.storage_dir)
    
    return output_dir


__all__ = ["ingest"]

