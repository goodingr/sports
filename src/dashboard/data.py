"""Data loading and analytics helpers for the forward testing dashboard."""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Iterable, Optional

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover - fallback for Python <3.9
    from backports.zoneinfo import ZoneInfo  # type: ignore

import numpy as np
import pandas as pd

FORWARD_TEST_DIR = Path("data/forward_test")
MASTER_PREDICTIONS_PATH = FORWARD_TEST_DIR / "predictions_master.parquet"

DEFAULT_EDGE_THRESHOLD = 0.06
DEFAULT_STAKE = 100.0
DEFAULT_STARTING_BANKROLL = 10_000.0
DISPLAY_TIMEZONE = ZoneInfo("America/New_York")


@dataclass(frozen=True)
class SummaryMetrics:
    """Container for high-level dashboard metrics."""

    total_predictions: int
    completed_games: int
    pending_games: int
    recommended_bets: int
    recommended_completed: int
    win_rate: Optional[float]
    roi: Optional[float]
    net_profit: float
    max_drawdown: Optional[float]
    cumulative_profit: float
    last_updated: Optional[pd.Timestamp]
    starting_bankroll: float
    current_bankroll: float
    total_staked: float
    bankroll_growth: Optional[float]


def _to_datetime(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce") if series is not None else series


def _convert_to_display_timezone(series: pd.Series) -> pd.Series:
    if series is None or series.empty:
        return series
    if not pd.api.types.is_datetime64_any_dtype(series):
        return series

    tz = getattr(series.dt, "tz", None)
    if tz is None:
        return series.dt.tz_localize(DISPLAY_TIMEZONE)
    return series.dt.tz_convert(DISPLAY_TIMEZONE)


@lru_cache(maxsize=1)
def _read_predictions_cached(path_str: str, cache_buster: int) -> pd.DataFrame:
    path = Path(path_str)
    if not path.exists():
        return pd.DataFrame()

    df = pd.read_parquet(path)

    def _infer_league_from_game_id(game_id: object) -> str:
        if not isinstance(game_id, str):
            return "NBA"
        upper = game_id.upper()
        if upper.startswith("NFL_"):
            return "NFL"
        if upper.startswith("CFB_"):
            return "CFB"
        if upper.startswith("NBA_"):
            return "NBA"
        if upper.startswith("EPL_"):
            return "EPL"
        if upper.startswith("LALIGA_"):
            return "LALIGA"
        if upper.startswith("BUNDESLIGA_"):
            return "BUNDESLIGA"
        if upper.startswith("SERIEA_"):
            return "SERIEA"
        if upper.startswith("LIGUE1_"):
            return "LIGUE1"
        return "NBA"

    # Add or fix league column (for backward compatibility)
    # Default to NBA for old predictions, or infer from game_id
    if "league" not in df.columns:
        if "game_id" in df.columns:
            # Infer league from game_id prefix
            df["league"] = df["game_id"].apply(_infer_league_from_game_id)
        else:
            # Default to NBA for old predictions without game_id info
            df["league"] = "NBA"
    else:
        # Fix None/NaN values in existing league column
        if "game_id" in df.columns:
            mask = df["league"].isna() | (df["league"] == "None") | (df["league"].astype(str).str.lower() == "none")
            df.loc[mask, "league"] = df.loc[mask, "game_id"].apply(_infer_league_from_game_id)
        else:
            # Fill any None/NaN with NBA as default
            df["league"] = df["league"].fillna("NBA")
            df.loc[df["league"].astype(str).str.lower() == "none", "league"] = "NBA"

    for column in ("commence_time", "predicted_at", "result_updated_at"):
        if column in df.columns:
            df[column] = _to_datetime(df[column])

    # Ensure numeric columns are floats
    numeric_cols = [
        "home_moneyline",
        "away_moneyline",
        "home_predicted_prob",
        "away_predicted_prob",
        "home_implied_prob",
        "away_implied_prob",
        "home_edge",
        "away_edge",
    ]
    for column in numeric_cols:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")

    for column in ("commence_time", "predicted_at", "result_updated_at"):
        if column in df.columns:
            df[column] = _convert_to_display_timezone(df[column])

    return df


def load_forward_test_data(*, force_refresh: bool = False, path: Path = MASTER_PREDICTIONS_PATH, league: Optional[str] = None) -> pd.DataFrame:
    """Load forward test predictions with optional cache busting and league filtering."""

    path_str = str(path.resolve())
    if force_refresh:
        _read_predictions_cached.cache_clear()
    try:
        cache_buster = path.stat().st_mtime_ns
    except FileNotFoundError:
        cache_buster = 0
    df = _read_predictions_cached(path_str, cache_buster)
    
    # Filter by league if specified
    if not df.empty and league and "league" in df.columns:
        df = df[df["league"].str.upper() == league.upper()].copy()
    
    return df.copy() if not df.empty else df


def _american_to_decimal(ml: float) -> float:
    if ml is None or np.isnan(ml) or ml == 0:
        return np.nan
    if ml > 0:
        return 1.0 + (ml / 100.0)
    return 1.0 + (100.0 / abs(ml))


def _bet_profit(ml: float, stake: float, won: Optional[bool]) -> float:
    if ml is None or np.isnan(ml) or ml == 0 or stake <= 0:
        return np.nan if won is None else 0.0
    if won is None:
        return 0.0
    if won:
        if ml > 0:
            return stake * (ml / 100.0)
        return stake * (100.0 / abs(ml))
    return -stake


def _max_drawdown(cumulative: pd.Series) -> Optional[float]:
    if cumulative.empty:
        return None
    running_max = cumulative.cummax()
    drawdowns = running_max - cumulative
    return float(drawdowns.max()) if not drawdowns.empty else None


def _expand_predictions(df: pd.DataFrame, *, stake: float = DEFAULT_STAKE) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    # Required columns
    required_columns = [
        "game_id",
        "commence_time",
        "predicted_at",
        "home_team",
        "away_team",
        "home_moneyline",
        "away_moneyline",
        "home_predicted_prob",
        "away_predicted_prob",
        "home_implied_prob",
        "away_implied_prob",
        "home_edge",
        "away_edge",
        "result",
        "home_score",
        "away_score",
    ]
    
    # Optional columns (may not exist in older predictions or non-soccer leagues)
    optional_columns = ["result_updated_at", "draw_moneyline", "draw_predicted_prob", 
                       "draw_implied_prob", "draw_edge"]

    missing = [column for column in required_columns if column not in df.columns]
    if missing:
        raise KeyError(f"Missing expected columns in predictions: {missing}")

    records: list[dict[str, object]] = []
    for _, row in df.iterrows():
        # Check if this is a soccer league (has draw_moneyline)
        has_draw = pd.notna(row.get("draw_moneyline")) and row.get("draw_moneyline") is not None
        
        sides = ["home", "away"]
        if has_draw:
            sides.append("draw")
        
        for side in sides:
            if side == "draw":
                # Handle draw bet
                team = "Draw"
                opponent = f"{row.get('home_team')} vs {row.get('away_team')}"
                moneyline_col = "draw_moneyline"
                prob_col = "draw_predicted_prob"
                implied_col = "draw_implied_prob"
                edge_col = "draw_edge"
                
                moneyline = row.get(moneyline_col)
                predicted_prob = row.get(prob_col)
                implied_prob = row.get(implied_col)
                edge = row.get(edge_col)
                result = row.get("result")
                
                # Draw wins if result is "tie"
                if result == "tie":
                    won = True
                elif result in ("home", "away"):
                    won = False
                else:
                    won = None
            else:
                # Handle home/away bets (existing logic)
                team_col = f"{side}_team"
                opp_col = "away_team" if side == "home" else "home_team"
                moneyline_col = f"{side}_moneyline"
                prob_col = f"{side}_predicted_prob"
                implied_col = f"{side}_implied_prob"
                edge_col = f"{side}_edge"

                team = row.get(team_col)
                opponent = row.get(opp_col)
                moneyline = row.get(moneyline_col)
                predicted_prob = row.get(prob_col)
                implied_prob = row.get(implied_col)
                edge = row.get(edge_col)
                result = row.get("result")

                if result == "tie":
                    won = False
                elif result in ("home", "away"):
                    won = result == side
                else:
                    won = None

            profit = _bet_profit(float(moneyline) if moneyline is not None else np.nan, stake, won)

            records.append(
                {
                    "game_id": row.get("game_id"),
                    "side": side,
                    "team": team,
                    "opponent": opponent,
                    "moneyline": float(moneyline) if moneyline is not None else np.nan,
                    "predicted_prob": float(predicted_prob) if predicted_prob is not None else np.nan,
                    "implied_prob": float(implied_prob) if implied_prob is not None else np.nan,
                    "edge": float(edge) if edge is not None else np.nan,
                    "result": result,
                    "won": won,
                    "profit": profit,
                    "stake": stake if won is not None else np.nan,
                    "commence_time": row.get("commence_time"),
                    "predicted_at": row.get("predicted_at"),
                    "settled_at": row.get("result_updated_at") if "result_updated_at" in row.index else row.get("commence_time"),
                    "home_score": row.get("home_score"),
                    "away_score": row.get("away_score"),
                    "implied_decimal": _american_to_decimal(float(moneyline)) if moneyline is not None else np.nan,
                }
            )

    bets = pd.DataFrame(records)
    if not bets.empty:
        # Convert to datetime but don't convert timezone yet - we'll do that after sorting
        bets["commence_time"] = _to_datetime(bets["commence_time"])
        bets["predicted_at"] = _to_datetime(bets["predicted_at"])
        # Handle settled_at - use result_updated_at if available, otherwise commence_time
        if "result_updated_at" in bets.columns:
            bets["settled_at"] = _to_datetime(bets["result_updated_at"])
        else:
            bets["settled_at"] = bets["commence_time"].copy()
    return bets


def calculate_summary_metrics(
    df: pd.DataFrame,
    *,
    edge_threshold: float = DEFAULT_EDGE_THRESHOLD,
    stake: float = DEFAULT_STAKE,
) -> SummaryMetrics:
    if df.empty:
        return SummaryMetrics(
            total_predictions=0,
            completed_games=0,
            pending_games=0,
            recommended_bets=0,
            recommended_completed=0,
            win_rate=None,
            roi=None,
            net_profit=0.0,
            max_drawdown=None,
            cumulative_profit=0.0,
            last_updated=None,
            starting_bankroll=DEFAULT_STARTING_BANKROLL,
            current_bankroll=DEFAULT_STARTING_BANKROLL,
            total_staked=0.0,
            bankroll_growth=None,
        )

    bets = _expand_predictions(df, stake=stake)
    
    if bets.empty or "won" not in bets.columns:
        return SummaryMetrics(
            total_predictions=len(df),
            completed_games=int(df["result"].notna().sum()) if "result" in df.columns else 0,
            pending_games=len(df) - (int(df["result"].notna().sum()) if "result" in df.columns else 0),
            recommended_bets=0,
            recommended_completed=0,
            win_rate=None,
            roi=None,
            net_profit=0.0,
            max_drawdown=None,
            cumulative_profit=0.0,
            last_updated=None,
            starting_bankroll=DEFAULT_STARTING_BANKROLL,
            current_bankroll=DEFAULT_STARTING_BANKROLL,
            total_staked=0.0,
            bankroll_growth=None,
        )

    total_predictions = len(df)
    completed_games = int(df["result"].notna().sum()) if "result" in df.columns else 0
    pending_games = total_predictions - completed_games

    recommended_mask = bets["edge"].notna() & (bets["edge"] >= edge_threshold)
    recommended = bets.loc[recommended_mask].copy()

    recommended_completed = recommended[recommended["won"].notna()]
    wins = recommended_completed[recommended_completed["won"] == True]  # noqa: E712

    total_staked = stake * len(recommended_completed)
    net_profit = float(recommended_completed["profit"].fillna(0.0).sum())
    cumulative_profit = net_profit
    win_rate = (len(wins) / len(recommended_completed)) if len(recommended_completed) else None
    roi = (net_profit / total_staked) if total_staked else None

    # Calculate bankroll stats
    starting_bankroll = DEFAULT_STARTING_BANKROLL
    current_bankroll = starting_bankroll + net_profit
    bankroll_growth = ((current_bankroll - starting_bankroll) / starting_bankroll) if starting_bankroll > 0 else None

    if not recommended_completed.empty:
        recommended_completed = recommended_completed.sort_values("settled_at")
        cum_profit_series = recommended_completed["profit"].fillna(0.0).cumsum()
        max_drawdown = _max_drawdown(cum_profit_series)
    else:
        max_drawdown = None

    last_updated = None
    if "result_updated_at" in df.columns and df["result_updated_at"].notna().any():
        last_updated = df["result_updated_at"].dropna().max()
    elif "predicted_at" in df.columns and df["predicted_at"].notna().any():
        last_updated = df["predicted_at"].dropna().max()

    return SummaryMetrics(
        total_predictions=total_predictions,
        completed_games=completed_games,
        pending_games=pending_games,
        recommended_bets=int(recommended_mask.sum()),
        recommended_completed=len(recommended_completed),
        win_rate=win_rate,
        roi=roi,
        net_profit=net_profit,
        max_drawdown=max_drawdown,
        cumulative_profit=cumulative_profit,
        last_updated=last_updated,
        starting_bankroll=starting_bankroll,
        current_bankroll=current_bankroll,
        total_staked=float(total_staked),
        bankroll_growth=bankroll_growth,
    )


def get_performance_over_time(
    df: pd.DataFrame,
    *,
    edge_threshold: float = DEFAULT_EDGE_THRESHOLD,
    stake: float = DEFAULT_STAKE,
    freq: str = "D",
) -> pd.DataFrame:
    bets = _expand_predictions(df, stake=stake)
    if bets.empty or "won" not in bets.columns:
        return pd.DataFrame(columns=["date", "bets", "wins", "profit", "roi", "win_rate", "cumulative_profit"])

    mask = bets["edge"].notna() & (bets["edge"] >= edge_threshold)
    completed = bets.loc[mask & bets["won"].notna()].copy()
    if completed.empty:
        return pd.DataFrame(columns=["date", "bets", "wins", "profit", "roi", "win_rate", "cumulative_profit"])

    if "settled_at" not in completed.columns:
        completed["settled_at"] = completed["commence_time"]

    completed = completed.sort_values("settled_at")
    completed["date"] = completed["settled_at"].dt.to_period(freq).dt.to_timestamp()

    grouped = completed.groupby("date")

    summary = grouped.agg(
        bets=("won", "count"),
        wins=("won", lambda x: int((x == True).sum())),  # noqa: E712
        profit=("profit", "sum"),
    ).reset_index()

    summary["roi"] = np.where(summary["bets"] > 0, summary["profit"] / (stake * summary["bets"]), np.nan)
    summary["win_rate"] = np.where(summary["bets"] > 0, summary["wins"] / summary["bets"], np.nan)
    summary["cumulative_profit"] = summary["profit"].cumsum().round(2)

    return summary


def get_recent_predictions(
    df: pd.DataFrame,
    *,
    limit: int = 50,
    edge_threshold: Optional[float] = None,
) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    bets = _expand_predictions(df)
    # Filter out finished games (only show games where won is None/NaN)
    bets = bets[bets["won"].isna()]
    
    # Convert to datetime and sort BEFORE timezone conversion to ensure proper sorting
    if "commence_time" in bets.columns:
        # Convert to datetime if not already
        if not pd.api.types.is_datetime64_any_dtype(bets["commence_time"]):
            bets["commence_time"] = _to_datetime(bets["commence_time"])
        # Sort by datetime value (ascending - nearest upcoming first)
        bets = bets.sort_values("commence_time", ascending=True, na_position='last')
        # Now convert to display timezone AFTER sorting
        bets["commence_time"] = _convert_to_display_timezone(bets["commence_time"])
    else:
        bets = bets.sort_values("predicted_at", ascending=True, na_position='last')

    if edge_threshold is not None:
        bets = bets[bets["edge"].notna() & (bets["edge"] >= edge_threshold)]

    return bets.head(limit)


def get_recommended_bets(
    df: pd.DataFrame,
    *,
    edge_threshold: float = DEFAULT_EDGE_THRESHOLD,
) -> pd.DataFrame:
    bets = _expand_predictions(df)
    if bets.empty or "won" not in bets.columns:
        return pd.DataFrame()

    upcoming = bets[bets["won"].isna()]
    upcoming = upcoming[upcoming["edge"].notna() & (upcoming["edge"] >= edge_threshold)]

    if upcoming.empty:
        return pd.DataFrame()

    # Prefer the strongest edge per game_id to avoid duplicate rows (e.g., both sides recommended)
    upcoming = upcoming.sort_values(
        by=["edge", "commence_time"], ascending=[False, True], na_position="last"
    )

    if "game_id" in upcoming.columns:
        upcoming = upcoming.drop_duplicates(subset=["game_id"], keep="first")
    else:
        dedupe_cols = [col for col in ["league", "commence_time", "team", "opponent"] if col in upcoming.columns]
        if dedupe_cols:
            upcoming = upcoming.drop_duplicates(subset=dedupe_cols, keep="first")

    return upcoming.sort_values("commence_time", ascending=True, na_position="last")


def get_performance_by_threshold(
    df: pd.DataFrame,
    *,
    thresholds: Optional[Iterable[float]] = None,
    stake: float = DEFAULT_STAKE,
) -> pd.DataFrame:
    bets = _expand_predictions(df, stake=stake)
    if bets.empty or "won" not in bets.columns:
        return pd.DataFrame(columns=["bucket", "bets", "wins", "win_rate", "profit", "roi"])
    
    completed = bets[bets["won"].notna()].copy()
    if completed.empty:
        return pd.DataFrame(columns=["bucket", "bets", "wins", "win_rate", "profit", "roi"])

    if thresholds is None:
        thresholds = [0.0, 0.02, 0.04, 0.06, 0.08, 0.1, 0.15, 0.2]

    bins = sorted(set(float(t) for t in thresholds))
    if bins[0] > 0.0:
        bins.insert(0, 0.0)

    buckets = pd.IntervalIndex.from_breaks(bins + [np.inf], closed="left")

    completed = completed[completed["edge"].notna()].copy()
    completed["bucket"] = pd.cut(completed["edge"], bins=buckets)

    grouped = completed.groupby("bucket")
    summary = grouped.agg(
        bets=("won", "count"),
        wins=("won", lambda x: int((x == True).sum())),  # noqa: E712
        profit=("profit", "sum"),
    ).reset_index()

    summary["win_rate"] = np.where(summary["bets"] > 0, summary["wins"] / summary["bets"], np.nan)
    summary["roi"] = np.where(summary["bets"] > 0, summary["profit"] / (stake * summary["bets"]), np.nan)
    summary["bucket_label"] = summary["bucket"].astype(str)

    return summary[["bucket_label", "bets", "wins", "win_rate", "profit", "roi"]]


def get_completed_bets(
    df: pd.DataFrame,
    *,
    edge_threshold: float = DEFAULT_EDGE_THRESHOLD,
    stake: float = DEFAULT_STAKE,
) -> pd.DataFrame:
    """Get all completed bets with results, win/loss, and profit.
    
    Shows bets that have results (won.notna() and scores), regardless of database status.
    The database status check is used to exclude games that are still in progress.
    """
    from datetime import datetime, timezone
    from src.db.core import connect
    
    # Only consider games that have already started
    if "commence_time" in df.columns:
        commence_times = pd.to_datetime(df["commence_time"], errors="coerce", utc=True)
        now = datetime.now(timezone.utc)
        df = df.loc[(commence_times.notna()) & (commence_times <= now)].copy()

    bets = _expand_predictions(df, stake=stake)
    if bets.empty or "won" not in bets.columns:
        return pd.DataFrame()

    # Filter to recommended bets that have results (won is not null means result was determined)
    # Also check if scores exist as a secondary indicator
    has_results = bets["won"].notna()
    if "home_score" in bets.columns and "away_score" in bets.columns:
        has_results = has_results & bets["home_score"].notna() & bets["away_score"].notna()
    
    completed = bets[has_results & bets["edge"].notna() & (bets["edge"] >= edge_threshold)].copy()

    if completed.empty:
        return pd.DataFrame()

    # Check game status from database to exclude games that are still in progress
    # If a game has results but status is not 'final', exclude it (likely still ongoing)
    if "game_id" in completed.columns:
        try:
            with connect() as conn:
                # Get all game_ids from predictions (these are The Odds API event IDs)
                prediction_game_ids = completed["game_id"].unique().tolist()
                if not prediction_game_ids:
                    # If we can't query, show all bets with results (safer than hiding everything)
                    pass
                else:
                    # Build all possible ID formats to check
                    all_ids_to_check = []
                    for pred_id in prediction_game_ids:
                        all_ids_to_check.append(pred_id)  # Original ID (as odds_api_id)
                        all_ids_to_check.append(f"NBA_{pred_id}")  # NBA prefixed (as game_id)
                        all_ids_to_check.append(f"NFL_{pred_id}")  # NFL prefixed (as game_id)
                        all_ids_to_check.append(f"CFB_{pred_id}")  # CFB prefixed (as game_id)
                        all_ids_to_check.append(f"EPL_{pred_id}")  # EPL prefixed (as game_id)
                        all_ids_to_check.append(f"LALIGA_{pred_id}")  # LALIGA prefixed (as game_id)
                        all_ids_to_check.append(f"BUNDESLIGA_{pred_id}")  # BUNDESLIGA prefixed (as game_id)
                        all_ids_to_check.append(f"SERIEA_{pred_id}")  # SERIEA prefixed (as game_id)
                        all_ids_to_check.append(f"LIGUE1_{pred_id}")  # LIGUE1 prefixed (as game_id)
                    
                    # Query game statuses - check both game_id and odds_api_id
                    # Use all_ids_to_check for game_id check and prediction_game_ids for odds_api_id check
                    game_id_placeholders = ",".join("?" * len(all_ids_to_check))
                    odds_api_id_placeholders = ",".join("?" * len(prediction_game_ids))
                    status_query = f"""
                        SELECT game_id, status, odds_api_id
                        FROM games
                        WHERE game_id IN ({game_id_placeholders})
                        OR odds_api_id IN ({odds_api_id_placeholders})
                    """
                    # Query with both the original IDs and prefixed versions
                    status_results = conn.execute(status_query, all_ids_to_check + prediction_game_ids).fetchall()
                    
                    # Create mapping: prediction_id -> status
                    game_status_map = {}  # prediction_id -> status or None if not in DB
                    
                    for row in status_results:
                        db_game_id, status, db_odds_api_id = row
                        # Track which prediction IDs are in the database and their status
                        for pred_id in prediction_game_ids:
                            if (db_game_id == pred_id or 
                                db_game_id == f"NBA_{pred_id}" or 
                                db_game_id == f"NFL_{pred_id}" or
                                db_game_id == f"CFB_{pred_id}" or
                                db_odds_api_id == pred_id):
                                game_status_map[pred_id] = status
                    
                    # Filter: Exclude games that are in DB but NOT final (they're still in progress)
                    # Keep games that are:
                    # 1. Not in DB at all (show them - they have results so likely completed)
                    # 2. In DB with status='final' (show them - definitely completed)
                    # Exclude games that are in DB but status != 'final' (still in progress)
                    final_game_ids = []
                    for pred_id in prediction_game_ids:
                        status = game_status_map.get(pred_id)
                        if status is None or status == "final":
                            # Not in DB or definitely final - show it
                            final_game_ids.append(pred_id)
                        # If status exists and is not 'final', exclude it
                    
                    if final_game_ids:
                        completed = completed[completed["game_id"].isin(final_game_ids)].copy()
                    else:
                        # No games passed the filter, return empty
                        return pd.DataFrame()
        except Exception as e:
            # If database query fails, show all bets with results
            # (this is safer than hiding all data due to a DB error)
            import logging
            logging.getLogger(__name__).warning("Failed to check game status: %s", e)
            # Continue with all completed bets
            pass

    if completed.empty:
        return pd.DataFrame()

    # Sort by settled date (most recent first)
    if "settled_at" in completed.columns:
        completed = completed.sort_values("settled_at", ascending=False)
    elif "commence_time" in completed.columns:
        completed = completed.sort_values("commence_time", ascending=False)

    return completed


def get_upcoming_calendar(
    df: pd.DataFrame,
    *,
    edge_threshold: float = DEFAULT_EDGE_THRESHOLD,
) -> pd.DataFrame:
    bets = get_recommended_bets(df, edge_threshold=edge_threshold)
    if bets.empty:
        return pd.DataFrame(columns=["date", "team", "opponent", "edge", "commence_time"])

    bets = bets.copy()
    bets["date"] = bets["commence_time"].dt.date
    return bets[["date", "team", "opponent", "edge", "commence_time", "moneyline"]]


__all__ = [
    "SummaryMetrics",
    "DEFAULT_EDGE_THRESHOLD",
    "DEFAULT_STAKE",
    "DEFAULT_STARTING_BANKROLL",
    "DISPLAY_TIMEZONE",
    "load_forward_test_data",
    "calculate_summary_metrics",
    "get_performance_over_time",
    "get_recent_predictions",
    "get_recommended_bets",
    "get_completed_bets",
    "get_performance_by_threshold",
    "get_upcoming_calendar",
]
