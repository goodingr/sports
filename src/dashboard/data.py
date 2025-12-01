"""Data loading and analytics helpers for the forward testing dashboard."""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple, Union

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover - fallback for Python <3.9
    from backports.zoneinfo import ZoneInfo  # type: ignore

import numpy as np
import pandas as pd
import yaml
import logging

LOGGER = logging.getLogger(__name__)

from src.data.team_mappings import normalize_team_code, get_full_team_name
from src.db.core import connect
from src.data.sportsbook_urls import get_sportsbook_url

FORWARD_TEST_DIR = Path("data/forward_test")
MASTER_PREDICTIONS_PATH = FORWARD_TEST_DIR / "predictions_master.parquet"
VERSION_CONFIG_PATH = Path("config/versions.yml")

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


@dataclass(frozen=True)
class PredictionComparisonStats:
    total_games: int
    agreement_rate: Optional[float]
    we_right_books_wrong: int
    books_right_we_wrong: int
    both_correct: int
    both_wrong: int
    pending: int
    our_accuracy: Optional[float]
    book_accuracy: Optional[float]


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


def _ensure_utc(ts: pd.Timestamp) -> pd.Timestamp:
    """Ensure a timestamp is timezone-aware UTC."""
    if pd.isna(ts):
        return ts
    if ts.tzinfo is None:
        return ts.tz_localize("UTC")
    return ts.tz_convert("UTC")


@lru_cache(maxsize=1)
def _load_version_config() -> dict:
    if not VERSION_CONFIG_PATH.exists():
        return {"versions": []}
    try:
        data = yaml.safe_load(VERSION_CONFIG_PATH.read_text(encoding="utf-8")) or {}
    except Exception:
        return {"versions": []}

    parsed = []
    for entry in data.get("versions", []):
        name = str(entry.get("name") or "").strip()
        if not name:
            continue
        start_value = entry.get("start")
        start_ts = pd.to_datetime(start_value, utc=True, errors="coerce")
        if start_ts is None or pd.isna(start_ts):
            start_ts = pd.Timestamp.min.tz_localize("UTC")
        elif start_ts.tzinfo is None:
            start_ts = start_ts.tz_localize("UTC")
        parsed.append({"name": name, "start": start_ts})

    parsed.sort(key=lambda item: item["start"])
    return {"versions": parsed, "current": data.get("current")}


def get_version_options() -> list[str]:
    config = _load_version_config()
    return [entry["name"] for entry in config["versions"]]


def get_default_version_value() -> str:
    config = _load_version_config()
    versions = config.get("versions", [])
    current = config.get("current")
    if current and any(entry.get("name") == current for entry in versions):
        return current
    if versions:
        return versions[-1]["name"]
    return "all"


def _ensure_utc(ts: Optional[pd.Timestamp]) -> Optional[pd.Timestamp]:
    if ts is None or pd.isna(ts):
        return None
    if ts.tzinfo is None:
        return ts.tz_localize("UTC")
    return ts.tz_convert("UTC")


def _assign_versions(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        df["version"] = []
        return df

    config = _load_version_config()
    versions = config["versions"]
    if not versions:
        df["version"] = "v0.1"
        return df

    comparison_series = (
        df["predicted_at"]
        if "predicted_at" in df.columns
        else df.get("commence_time")
    )
    if comparison_series is None:
        df["version"] = versions[-1]["name"]
        return df

    timestamps = comparison_series.apply(_ensure_utc)

    def lookup(ts: Optional[pd.Timestamp]) -> str:
        if ts is None:
            return versions[0]["name"]
        for entry in reversed(versions):
            if ts >= entry["start"]:
                return entry["name"]
        return versions[0]["name"]

    df["version"] = timestamps.apply(lookup)
    return df


def filter_by_version(df: pd.DataFrame, version: Optional[str]) -> pd.DataFrame:
    if df.empty or not version or version == "all" or "version" not in df.columns:
        return df
    return df[df["version"] == version].copy()


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
        if upper.startswith("NHL_"):
            return "NHL"
        if upper.startswith("NCAAB_"):
            return "NCAAB"
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


def load_forward_test_data(
    *, 
    force_refresh: bool = False, 
    path: Path = MASTER_PREDICTIONS_PATH, 
    league: Optional[str] = None,
    model_type: str = "ensemble",
) -> pd.DataFrame:
    """Load forward test predictions with optional cache busting, league filtering, and model type selection."""
    
    # If using default path, update it to include model_type subdirectory
    if path == MASTER_PREDICTIONS_PATH:
        path = FORWARD_TEST_DIR / model_type / "predictions_master.parquet"
    
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
    df = _assign_versions(df)
    return df.copy() if not df.empty else df


def compare_model_predictions(
    league: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    model_types: list[str] = ["gradient_boosting", "random_forest", "ensemble"],
) -> pd.DataFrame:
    """Load and merge predictions from multiple models for comparison."""
    dfs = {}
    for model_type in model_types:
        df = load_forward_test_data(league=league, model_type=model_type)
        if df.empty:
            continue
        
        # Filter by date if specified
        if start_date:
            df = df[df["commence_time"] >= pd.to_datetime(start_date, utc=True)]
        if end_date:
            df = df[df["commence_time"] <= pd.to_datetime(end_date, utc=True)]
            
        dfs[model_type] = df
    
    if not dfs:
        return pd.DataFrame()
        
    # Create a base dataframe with unique game_ids from all models
    all_games = []
    for model_type, df in dfs.items():
        # Keep metadata columns
        meta_cols = ["game_id", "home_team", "away_team", "commence_time", "result", "home_score", "away_score", "league"]
        subset = df[[c for c in meta_cols if c in df.columns]].copy()
        all_games.append(subset)
        
    if not all_games:
        return pd.DataFrame()
        
    # Concatenate and drop duplicates to get master list of games
    master_df = pd.concat(all_games, ignore_index=True)
    if "game_id" in master_df.columns:
        # Sort by commence_time to keep the most recent metadata if duplicates exist (though they should be identical)
        if "commence_time" in master_df.columns:
            master_df = master_df.sort_values("commence_time")
        master_df = master_df.drop_duplicates(subset=["game_id"], keep="last")
    else:
        # Fallback if no game_id (shouldn't happen with new data)
        master_df = master_df.drop_duplicates()
        
    # Merge each model's predictions onto the master dataframe
    for model_type, df in dfs.items():
        # Select prediction columns
        pred_cols = ["game_id", "home_predicted_prob", "away_predicted_prob", "home_edge", "away_edge"]
        subset = df[[c for c in pred_cols if c in df.columns]].copy()
        
        # Rename columns
        rename_map = {
            "home_predicted_prob": f"{model_type}_home_prob",
            "away_predicted_prob": f"{model_type}_away_prob",
            "home_edge": f"{model_type}_home_edge",
            "away_edge": f"{model_type}_away_edge",
        }
        subset = subset.rename(columns=rename_map)
        
        if "game_id" in subset.columns:
            master_df = pd.merge(master_df, subset, on="game_id", how="left")
            
    return master_df


def _american_to_decimal(ml: float) -> float:
    if ml is None or np.isnan(ml) or ml == 0:
        return np.nan
    if ml > 0:
        return 1.0 + (ml / 100.0)
    return 1.0 + (100.0 / abs(ml))


def _american_to_probability(ml: Optional[float]) -> float:
    if ml is None or (isinstance(ml, float) and np.isnan(ml)) or ml == 0:
        return np.nan
    if ml > 0:
        return 100.0 / (ml + 100.0)
    return abs(ml) / (abs(ml) + 100.0)


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
    optional_columns = ["predicted_at", "result_updated_at", "draw_moneyline", "draw_predicted_prob", 
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
            # Resolve full team names
            home_full = get_full_team_name(row.get("league"), row.get("home_team")) or row.get("home_team")
            away_full = get_full_team_name(row.get("league"), row.get("away_team")) or row.get("away_team")

            if side == "draw":
                # Handle draw bet
                team = "Draw"
                opponent = f"{home_full} vs {away_full}"
                moneyline_col = "draw_moneyline"
                prob_col = "draw_predicted_prob"
                implied_col = "draw_implied_prob"
                edge_col = "draw_edge"
                
                moneyline = row.get(moneyline_col)
                predicted_prob = row.get(prob_col)
                implied_prob = row.get(implied_col)
                edge = row.get(edge_col)
                result = row.get("result")
                
                # Draw wins if result is "tie" or "draw"
                if result in ("tie", "draw"):
                    won = True
                elif result in ("home", "away"):
                    won = False
                else:
                    won = None
            else:
                # Handle home/away bets (existing logic)
                moneyline_col = f"{side}_moneyline"
                prob_col = f"{side}_predicted_prob"
                implied_col = f"{side}_implied_prob"
                edge_col = f"{side}_edge"

                if side == "home":
                    team = home_full
                    opponent = away_full
                else:
                    team = away_full
                    opponent = home_full

                moneyline = row.get(moneyline_col)
                predicted_prob = row.get(prob_col)
                implied_prob = row.get(implied_col)
                edge = row.get(edge_col)
                result = row.get("result")

                if result in ("tie", "draw"):
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
                    "league": row.get("league"),
                    "home_team_name": home_full,
                    "away_team_name": away_full,
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


def _expand_totals(df: pd.DataFrame, *, stake: float = DEFAULT_STAKE) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    required = [
        "game_id",
        "league",
        "commence_time",

        "total_line",
        "over_moneyline",
        "under_moneyline",
        "over_predicted_prob",
        "under_predicted_prob",
        "over_implied_prob",
        "under_implied_prob",
        "over_edge",
        "under_edge",
        "home_score",
        "away_score",
    ]

    missing = [col for col in required if col not in df.columns]
    if missing:
        LOGGER.warning("Totals expansion skipped, missing columns: %s", missing)
        return pd.DataFrame()

    records: list[dict[str, object]] = []
    for _, row in df.iterrows():
        line = row.get("total_line")
        over_ml = row.get("over_moneyline")
        under_ml = row.get("under_moneyline")
        if pd.isna(line) or pd.isna(over_ml) or pd.isna(under_ml):
            continue

        for side in ("over", "under"):
            price = row.get(f"{side}_moneyline")
            pred = row.get(f"{side}_predicted_prob")
            implied = row.get(f"{side}_implied_prob")
            edge = row.get(f"{side}_edge")

            actual_total = None
            if pd.notna(row.get("home_score")) and pd.notna(row.get("away_score")):
                actual_total = float(row.get("home_score") + row.get("away_score"))

            if actual_total is None:
                won = None
            else:
                if actual_total > line:
                    winner = "over"
                elif actual_total < line:
                    winner = "under"
                else:
                    winner = None
                won = winner == side if winner is not None else None

            profit = _bet_profit(float(price), stake, won)
            description = f"{side.title()} {line:.1f}" if pd.notna(line) else side.title()

            # Get full team names
            home_full = get_full_team_name(row.get("league"), row.get("home_team"))
            away_full = get_full_team_name(row.get("league"), row.get("away_team"))

            records.append(
                {
                    "game_id": row.get("game_id"),
                    "league": row.get("league"),
                    "home_team": home_full or row.get("home_team"),
                    "away_team": away_full or row.get("away_team"),
                    "side": side,
                    "description": description,
                    "total_line": float(line),
                    "moneyline": float(price) if price is not None else np.nan,
                    "predicted_prob": float(pred) if pred is not None else np.nan,
                    "implied_prob": float(implied) if implied is not None else np.nan,
                    "edge": float(edge) if edge is not None else np.nan,
                    "won": won,
                    "profit": profit,
                    "stake": stake if won is not None else np.nan,
                    "commence_time": row.get("commence_time"),
                    "predicted_at": row.get("predicted_at"),
                    "settled_at": row.get("result_updated_at") if "result_updated_at" in row.index else row.get("commence_time"),
                    "total_points": actual_total,
                    "predicted_total_points": row.get("predicted_total_points"),
                    "home_score": row.get("home_score"),
                    "away_score": row.get("away_score"),
                }
            )

    totals_df = pd.DataFrame(records)
    if not totals_df.empty:
        totals_df["commence_time"] = _to_datetime(totals_df["commence_time"])
        totals_df["predicted_at"] = _to_datetime(totals_df["predicted_at"])
        totals_df["settled_at"] = _to_datetime(totals_df["settled_at"])
    return totals_df


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


def calculate_totals_metrics(
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

    totals = _expand_totals(df, stake=stake)
    if totals.empty or "edge" not in totals.columns:
        total_predictions = int(df["total_line"].notna().sum()) if "total_line" in df.columns else 0
        completed_games = int(df["home_score"].notna().sum()) if "home_score" in df.columns else 0
        pending_games = total_predictions - completed_games
        return SummaryMetrics(
            total_predictions=total_predictions,
            completed_games=max(completed_games, 0),
            pending_games=max(pending_games, 0),
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

    totals = totals.copy()
    total_predictions = len(totals)
    completed_games = int(totals["won"].notna().sum())
    pending_games = total_predictions - completed_games

    mask = totals["edge"].notna() & (totals["edge"] >= edge_threshold)
    recommended = totals.loc[mask].copy()
    recommended_completed = recommended[recommended["won"].notna()]
    wins = recommended_completed[recommended_completed["won"] == True]  # noqa: E712

    total_staked = stake * len(recommended_completed)
    net_profit = float(recommended_completed["profit"].fillna(0.0).sum())
    cumulative_profit = net_profit
    win_rate = (len(wins) / len(recommended_completed)) if len(recommended_completed) else None
    roi = (net_profit / total_staked) if total_staked else None

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
    elif "predicted_at" in totals.columns and totals["predicted_at"].notna().any():
        last_updated = totals["predicted_at"].dropna().max()

    return SummaryMetrics(
        total_predictions=total_predictions,
        completed_games=completed_games,
        pending_games=pending_games,
        recommended_bets=int(mask.sum()),
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


def get_totals_performance_over_time(
    df: pd.DataFrame,
    *,
    edge_threshold: float = DEFAULT_EDGE_THRESHOLD,
    stake: float = DEFAULT_STAKE,
    freq: str = "D",
) -> pd.DataFrame:
    totals = _expand_totals(df, stake=stake)
    if totals.empty or "won" not in totals.columns:
        return pd.DataFrame(columns=["date", "bets", "wins", "profit", "roi", "win_rate", "cumulative_profit"])

    mask = totals["edge"].notna() & (totals["edge"] >= edge_threshold)
    completed = totals.loc[mask & totals["won"].notna()].copy()
    if completed.empty:
        return pd.DataFrame(columns=["date", "bets", "wins", "profit", "roi", "win_rate", "cumulative_profit"])

    if "settled_at" not in completed.columns or completed["settled_at"].isna().all():
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

    # 1. Filter for upcoming games
    # Use the _ensure_utc helper to safely compare times
    now = datetime.now(timezone.utc)
    
    # Create a mask for future games
    # We need to handle the case where commence_time might be a string or datetime
    # So we'll convert the column to UTC datetime first if it isn't already
    if "commence_time" in bets.columns:
        if not pd.api.types.is_datetime64_any_dtype(bets['commence_time']):
            bets['commence_time'] = pd.to_datetime(bets['commence_time'], utc=True, errors='coerce')
        
        # Ensure column is tz-aware (UTC)
        if bets['commence_time'].dt.tz is None:
            bets['commence_time'] = bets['commence_time'].dt.tz_localize('UTC')
        else:
            bets['commence_time'] = bets['commence_time'].dt.tz_convert('UTC')
            
        bets = bets[bets["commence_time"] > now].copy()
    else:
        # If no commence_time, we can't filter by future, so we proceed with all pending bets
        pass

    # --- MATCHUP-LEVEL DEDUPLICATION ---
    # Ensure we only have ONE game_id per matchup (Teams + Time) to prevent inconsistent predictions
    dedupe_cols = ["league", "commence_time", "home_team", "away_team"]
    available_cols = [c for c in dedupe_cols if c in bets.columns]
    
    if len(available_cols) == 4:
        # 1. Identify the "best" game_id for each matchup
        # Create a view of unique games with their timestamp
        unique_games = bets[["game_id"] + available_cols].drop_duplicates("game_id")
        
        if "predicted_at" in bets.columns:
            # Add predicted_at for sorting
            unique_games = unique_games.merge(
                bets[["game_id", "predicted_at"]].drop_duplicates("game_id"), 
                on="game_id", 
                how="left"
            )
            unique_games = unique_games.sort_values("predicted_at", ascending=False)
        
        # Keep the first game_id per matchup
        best_games = unique_games.drop_duplicates(subset=available_cols, keep="first")
        valid_game_ids = set(best_games["game_id"])
        
        # 2. Filter the original bets dataframe to keep ONLY the valid game_ids
        bets = bets[bets["game_id"].isin(valid_game_ids)].copy()
    # -----------------------------------

    if bets.empty:
        return pd.DataFrame()

    # Sort by datetime value (ascending - nearest upcoming first)
    if "commence_time" in bets.columns:
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
    
    # Filter out past games
    now = pd.Timestamp.now(tz="UTC")
    if "commence_time" in upcoming.columns:
        # Ensure commence_time is UTC for comparison
        upcoming["commence_time_utc"] = upcoming["commence_time"].apply(_ensure_utc)
        upcoming = upcoming[upcoming["commence_time_utc"] > now]
        # Drop temp column
        upcoming = upcoming.drop(columns=["commence_time_utc"])

    upcoming = upcoming[upcoming["edge"].notna() & (upcoming["edge"] >= edge_threshold)]

    if upcoming.empty:
        return pd.DataFrame()

    # Prefer the strongest edge per game_id to avoid duplicate rows (e.g., both sides recommended)
    # Deduplicate by game_id to ensure we don't show the same bet twice
    if "game_id" in upcoming.columns and "side" in upcoming.columns:
        # Sort by edge descending so we keep the best one
        upcoming = upcoming.sort_values("edge", ascending=False)
        upcoming = upcoming.drop_duplicates(subset=["game_id"], keep="first")
        
    # Also deduplicate by team/time/side to catch cases where game_ids differ for same match
    dedupe_cols = [col for col in ["league", "commence_time", "home_team", "away_team", "side"] if col in upcoming.columns]
    if len(dedupe_cols) >= 4:  # Ensure we have enough columns to safely dedupe
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


def get_totals_performance_by_threshold(
    df: pd.DataFrame,
    *,
    thresholds: Optional[Iterable[float]] = None,
    stake: float = DEFAULT_STAKE,
) -> pd.DataFrame:
    totals = _expand_totals(df, stake=stake)
    if totals.empty or "won" not in totals.columns:
        return pd.DataFrame(columns=["bucket_label", "bets", "wins", "win_rate", "profit", "roi"])

    completed = totals[totals["won"].notna()].copy()
    if completed.empty:
        return pd.DataFrame(columns=["bucket_label", "bets", "wins", "win_rate", "profit", "roi"])

    if thresholds is None:
        thresholds = [0.0, 0.02, 0.04, 0.06, 0.08, 0.1, 0.15, 0.2]

    bins = sorted(set(float(t) for t in thresholds))
    if bins[0] > 0.0:
        bins.insert(0, 0.0)

    buckets = pd.IntervalIndex.from_breaks(bins + [np.inf], closed="left")

    completed = completed[completed["edge"].notna()].copy()
    if completed.empty:
        return pd.DataFrame(columns=["bucket_label", "bets", "wins", "win_rate", "profit", "roi"])

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
    # If a game has results but status is clearly live/upcoming, exclude it
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
                    # Map commence_time per game_id for smarter filtering
                    commence_lookup: dict[str, Optional[pd.Timestamp]] = {}
                    if "commence_time" in completed.columns:
                        commence_lookup = (
                            completed[["game_id", "commence_time"]]
                            .dropna(subset=["game_id"])
                            .groupby("game_id")["commence_time"]
                            .first()
                            .to_dict()
                        )

                    final_game_ids = []
                    for pred_id in prediction_game_ids:
                        status = game_status_map.get(pred_id)
                        if status is None:
                            final_game_ids.append(pred_id)
                            continue

                        status_normalized = str(status).strip().lower()
                        commence_ts = commence_lookup.get(pred_id)
                        if isinstance(commence_ts, pd.Timestamp):
                            if commence_ts.tzinfo is None:
                                commence_ts = commence_ts.tz_localize("UTC")
                            else:
                                commence_ts = commence_ts.tz_convert("UTC")

                        # Explicit in-progress statuses should still be hidden
                        if status_normalized in {"in_progress", "live", "halftime"}:
                            continue

                        # Upcoming games still scheduled should remain hidden if they have not started yet
                        if status_normalized == "scheduled":
                            if commence_ts is not None and commence_ts > now:
                                continue
                            # Game should have started; allow it to display since we have results
                            final_game_ids.append(pred_id)
                            continue

                        if status_normalized == "final":
                            final_game_ids.append(pred_id)
                            continue

                        # Default: allow other statuses (postponed/cancelled/etc.) to surface since we have results
                        final_game_ids.append(pred_id)
                    
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
    
    # Filter out past games
    now = pd.Timestamp.now(tz="UTC")
    if "commence_time" in bets.columns:
        bets = bets[bets["commence_time"] > now]
        
    bets["date"] = bets["commence_time"].dt.date
    return bets[["date", "team", "opponent", "edge", "commence_time", "moneyline"]]


def get_overunder_recommendations(
    df: pd.DataFrame,
    *,
    edge_threshold: float = DEFAULT_EDGE_THRESHOLD,
) -> pd.DataFrame:
    totals = _expand_totals(df)
    if totals.empty or "edge" not in totals.columns:
        return pd.DataFrame()

    upcoming = totals[totals["won"].isna()]
    
    # Filter out past games
    now = pd.Timestamp.now(tz="UTC")
    if "commence_time" in upcoming.columns:
        # Ensure commence_time is UTC for comparison
        upcoming["commence_time_utc"] = upcoming["commence_time"].apply(_ensure_utc)
        upcoming = upcoming[upcoming["commence_time_utc"] > now]
        # Drop temp column
        upcoming = upcoming.drop(columns=["commence_time_utc"])

    upcoming = upcoming[upcoming["edge"].notna() & (upcoming["edge"] >= edge_threshold)]
    if upcoming.empty:
        return pd.DataFrame()

    upcoming = upcoming.sort_values(by=["edge", "commence_time"], ascending=[False, True], na_position="last")
    
    # Deduplicate by game_id and side
    if "game_id" in upcoming.columns and "side" in upcoming.columns:
        upcoming = upcoming.drop_duplicates(subset=["game_id", "side"], keep="first")
        
    # --- MATCHUP-LEVEL DEDUPLICATION ---
    # Ensure we only have ONE game_id per matchup (Teams + Time) to prevent inconsistent predictions
    dedupe_cols = ["league", "commence_time", "home_team", "away_team"]
    available_cols = [c for c in dedupe_cols if c in upcoming.columns]
    
    if len(available_cols) == 4:
        # 1. Identify the "best" game_id for each matchup
        # Create a view of unique games with their timestamp
        unique_games = upcoming[["game_id"] + available_cols].drop_duplicates("game_id")
        
        if "predicted_at" in upcoming.columns:
            # Add predicted_at for sorting
            unique_games = unique_games.merge(
                upcoming[["game_id", "predicted_at"]].drop_duplicates("game_id"), 
                on="game_id", 
                how="left"
            )
            unique_games = unique_games.sort_values("predicted_at", ascending=False)
        
        # Keep the first game_id per matchup
        best_games = unique_games.drop_duplicates(subset=available_cols, keep="first")
        valid_game_ids = set(best_games["game_id"])
        
        # 2. Filter the original bets dataframe to keep ONLY the valid game_ids
        upcoming = upcoming[upcoming["game_id"].isin(valid_game_ids)].copy()
    # -----------------------------------
        
    return upcoming


def get_overunder_completed(
    df: pd.DataFrame,
    *,
    edge_threshold: float = DEFAULT_EDGE_THRESHOLD,
    stake: float = DEFAULT_STAKE,
) -> pd.DataFrame:
    totals = _expand_totals(df, stake=stake)
    if totals.empty:
        return pd.DataFrame()
    completed = totals[totals["won"].notna() & totals["edge"].notna() & (totals["edge"] >= edge_threshold)].copy()
    if completed.empty:
        return pd.DataFrame()
    if "commence_time" in completed.columns:
        completed = completed.sort_values("commence_time", ascending=False)
    return completed


def _map_game_ids_by_odds_api(recommended: pd.DataFrame) -> pd.DataFrame:
    if recommended.empty or "game_id" not in recommended.columns:
        return pd.DataFrame(columns=["prediction_game_id", "db_game_id"])

    prediction_ids = [gid for gid in recommended["game_id"] if isinstance(gid, str) and gid.strip()]
    if not prediction_ids:
        return pd.DataFrame(columns=["prediction_game_id", "db_game_id"])

    unique_ids = sorted(set(prediction_ids))
    placeholders = ",".join("?" for _ in unique_ids)

    with connect() as conn:
        rows = conn.execute(
            f"""
            SELECT odds_api_id, game_id
            FROM games
            WHERE odds_api_id IS NOT NULL
              AND odds_api_id IN ({placeholders})
            """,
            unique_ids,
        ).fetchall()

    if not rows:
        return pd.DataFrame(columns=["prediction_game_id", "db_game_id"])

    mapping = pd.DataFrame(
        [{"prediction_game_id": row[0], "db_game_id": row[1]} for row in rows if row[0] and row[1]]
    )
    return mapping.drop_duplicates(subset=["prediction_game_id"])


def _to_utc_timestamp(value: object) -> Optional[pd.Timestamp]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    ts = pd.to_datetime(value, errors="coerce")
    if ts is None or pd.isna(ts):
        return None
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    else:
        ts = ts.tz_convert("UTC")
    return ts


def _lookup_team_id(conn, sport_id: int, team_code: str, cache: Dict[Tuple[int, str], Optional[int]]) -> Optional[int]:
    key = (sport_id, team_code)
    if key in cache:
        return cache[key]
    row = conn.execute(
        "SELECT team_id FROM teams WHERE sport_id = ? AND code = ?",
        (sport_id, team_code),
    ).fetchone()
    team_id = row[0] if row else None
    cache[key] = team_id
    return team_id


def _match_games_to_db(recommended: pd.DataFrame) -> pd.DataFrame:
    if recommended.empty or "league" not in recommended.columns:
        return pd.DataFrame(columns=["prediction_game_id", "db_game_id"])

    leagues = sorted({str(val).upper() for val in recommended["league"].dropna() if str(val).strip()})
    if not leagues:
        return pd.DataFrame(columns=["prediction_game_id", "db_game_id"])

    placeholders = ",".join("?" for _ in leagues)
    league_params = tuple(leagues)

    with connect() as conn:
        sport_rows = conn.execute(
            f"SELECT league, sport_id FROM sports WHERE UPPER(league) IN ({placeholders})",
            league_params,
        ).fetchall()
        sport_ids = {str(row[0]).upper(): row[1] for row in sport_rows}
        team_cache: Dict[Tuple[int, str], Optional[int]] = {}
        mapping: list[dict[str, object]] = []

        for _, row in recommended.iterrows():
            league = str(row.get("league") or "").upper()
            sport_id = sport_ids.get(league)
            if not sport_id:
                continue

            home_name = row.get("home_team_name") or row.get("home_team") or row.get("team")
            away_name = row.get("away_team_name") or row.get("away_team") or row.get("opponent")
            if not home_name or not away_name:
                continue

            # Try to expand abbreviations to full names first
            from src.data.team_mappings import get_full_team_name
            home_expanded = get_full_team_name(league, str(home_name))
            away_expanded = get_full_team_name(league, str(away_name))
            
            # Use expanded names if available, otherwise use original
            home_to_normalize = home_expanded if home_expanded else home_name
            away_to_normalize = away_expanded if away_expanded else away_name

            home_code = normalize_team_code(league, home_to_normalize)
            away_code = normalize_team_code(league, away_to_normalize)
            if not home_code or not away_code:
                continue

            home_team_id = _lookup_team_id(conn, sport_id, home_code.upper(), team_cache)
            away_team_id = _lookup_team_id(conn, sport_id, away_code.upper(), team_cache)
            if not home_team_id or not away_team_id:
                continue

            commence_ts = _to_utc_timestamp(row.get("commence_time"))
            if commence_ts is None:
                continue
            target_epoch = int(commence_ts.timestamp())

            game_row = conn.execute(
                """
                SELECT game_id, start_time_utc
                FROM games
                WHERE sport_id = ?
                  AND home_team_id = ?
                  AND away_team_id = ?
                ORDER BY ABS(strftime('%s', start_time_utc) - ?)
                LIMIT 1
                """,
                (sport_id, home_team_id, away_team_id, target_epoch),
            ).fetchone()

            if not game_row:
                continue

            start_iso = game_row[1]
            if start_iso:
                try:
                    start_epoch = int(pd.Timestamp(start_iso).tz_convert("UTC").timestamp())
                except Exception:
                    start_epoch = None
                if start_epoch is not None and abs(start_epoch - target_epoch) > 3 * 24 * 3600:
                    continue

            mapping.append(
                {
                    "prediction_game_id": row.get("game_id"),
                    "db_game_id": game_row[0],
                }
            )

    if not mapping:
        return pd.DataFrame(columns=["prediction_game_id", "db_game_id"])

    mapping_df = pd.DataFrame(mapping).dropna()
    mapping_df = mapping_df.drop_duplicates(subset=["prediction_game_id"])
    return mapping_df



def get_performance_by_league(
    df: pd.DataFrame,
    *,
    edge_threshold: float = DEFAULT_EDGE_THRESHOLD,
    stake: float = DEFAULT_STAKE,
) -> dict[str, pd.DataFrame]:
    """
    Calculate cumulative profit over time for each league.
    """
    if df.empty:
        return {}
        
    bets = _expand_predictions(df, stake=stake)
    if bets.empty or "won" not in bets.columns:
        return {}
        
    # Filter for completed bets only
    completed = bets[bets["won"].notna()].copy()
    if completed.empty:
        return {}
        
    # Filter for recommended bets
    mask = completed["edge"].notna() & (completed["edge"] >= edge_threshold)
    recommended = completed.loc[mask].copy()
    
    if recommended.empty:
        return {}
        
    # Group by league
    leagues = recommended["league"].unique()
    results = {}
    
    for league in leagues:
        league_bets = recommended[recommended["league"] == league].copy()
        if league_bets.empty:
            continue
            
        league_bets = league_bets.sort_values("settled_at")
        league_bets["cumulative_profit"] = league_bets["profit"].fillna(0.0).cumsum()
        
        # Calculate ROI: (Cumulative Profit / Cumulative Stake) * 100
        league_bets["bet_count"] = range(1, len(league_bets) + 1)
        league_bets["cumulative_stake"] = league_bets["bet_count"] * stake
        league_bets["roi"] = (league_bets["cumulative_profit"] / league_bets["cumulative_stake"]) * 100.0
        
        # Create daily summary for the chart
        league_bets["date"] = league_bets["settled_at"].dt.date
        daily = (
            league_bets.groupby("date")
            .agg(
                cumulative_profit=("cumulative_profit", "last"),
                roi=("roi", "last")
            )
            .reset_index()
        )
        results[league] = daily
        
    return results


def get_totals_performance_by_league(
    df: pd.DataFrame,
    *,
    edge_threshold: float = DEFAULT_EDGE_THRESHOLD,
    stake: float = DEFAULT_STAKE,
) -> dict[str, pd.DataFrame]:
    """
    Calculate cumulative profit over time for each league for totals (over/under) bets.
    """
    if df.empty:
        return {}
        
    totals = _expand_totals(df, stake=stake)
    if totals.empty or "won" not in totals.columns:
        return {}
        
    # Filter for completed bets only
    completed = totals[totals["won"].notna()].copy()
    if completed.empty:
        return {}
        
    # Filter for recommended bets
    mask = completed["edge"].notna() & (completed["edge"] >= edge_threshold)
    recommended = completed.loc[mask].copy()
    
    if recommended.empty:
        return {}
        
    # Group by league
    leagues = recommended["league"].unique()
    results = {}
    
    for league in leagues:
        league_bets = recommended[recommended["league"] == league].copy()
        if league_bets.empty:
            continue
            
        league_bets = league_bets.sort_values("settled_at")
        league_bets["cumulative_profit"] = league_bets["profit"].fillna(0.0).cumsum()
        
        # Calculate ROI: (Cumulative Profit / Cumulative Stake) * 100
        league_bets["bet_count"] = range(1, len(league_bets) + 1)
        league_bets["cumulative_stake"] = league_bets["bet_count"] * stake
        league_bets["roi"] = (league_bets["cumulative_profit"] / league_bets["cumulative_stake"]) * 100.0
        
        # Create daily summary for the chart
        league_bets["date"] = league_bets["settled_at"].dt.date
        daily = (
            league_bets.groupby("date")
            .agg(
                cumulative_profit=("cumulative_profit", "last"),
                roi=("roi", "last")
            )
            .reset_index()
        )
        results[league] = daily
        
    return results


def _select_best_option(options: Dict[str, Tuple[Optional[float], Optional[str]]]) -> Tuple[Optional[str], Optional[str], Optional[float]]:
    best_side = None
    best_label = None
    best_prob = None
    for side, (prob, label) in options.items():
        if prob is None or (isinstance(prob, float) and np.isnan(prob)):
            continue
        if best_prob is None or prob > best_prob:
            best_side = side
            best_label = label
            best_prob = prob
    return best_side, best_label, best_prob


def build_prediction_comparison(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    records: list[dict[str, object]] = []

    for _, row in df.iterrows():
        league = str(row.get("league") or "").upper()
        is_soccer = league in {"EPL", "LALIGA", "BUNDESLIGA", "SERIEA", "LIGUE1"}

        side_info: Dict[str, Dict[str, object]] = {
            "home": {
                "team": row.get("home_team"),
                "pred_prob": row.get("home_predicted_prob"),
                "implied_prob": row.get("home_implied_prob"),
                "moneyline": row.get("home_moneyline"),
            },
            "away": {
                "team": row.get("away_team"),
                "pred_prob": row.get("away_predicted_prob"),
                "implied_prob": row.get("away_implied_prob"),
                "moneyline": row.get("away_moneyline"),
            },
        }

        if is_soccer and "draw_predicted_prob" in row.index:
            side_info["draw"] = {
                "team": "Draw",
                "pred_prob": row.get("draw_predicted_prob"),
                "implied_prob": row.get("draw_implied_prob"),
                "moneyline": row.get("draw_moneyline"),
            }

        our_options = {
            side: (info.get("pred_prob"), info.get("team") or side.title())
            for side, info in side_info.items()
        }
        our_side, our_team, our_prob = _select_best_option(our_options)

        sportsbook_probabilities: Dict[str, Tuple[Optional[float], Optional[str]]] = {}
        for side, info in side_info.items():
            implied = info.get("implied_prob")
            if implied is None or (isinstance(implied, float) and np.isnan(implied)):
                implied = _american_to_probability(info.get("moneyline"))
            sportsbook_probabilities[side] = (implied, info.get("team") or side.title())

        book_side, book_team, book_prob = _select_best_option(sportsbook_probabilities)

        actual_side_raw = str(row.get("result") or "").lower()
        actual_side = None
        if actual_side_raw in {"home", "away"}:
            actual_side = actual_side_raw
        elif actual_side_raw in {"tie", "draw"}:
            actual_side = "draw"

        def _team_for_side(side: Optional[str]) -> Optional[str]:
            if side is None:
                return None
            if side == "home":
                return row.get("home_team")
            if side == "away":
                return row.get("away_team")
            if side == "draw":
                return "Draw"
            return None

        actual_team = _team_for_side(actual_side)

        book_probs_by_side = {side: prob for side, (prob, _) in sportsbook_probabilities.items()}
        gap = np.nan
        if our_side:
            gap = (our_prob if our_prob is not None else np.nan) - (
                book_probs_by_side.get(our_side) if book_probs_by_side.get(our_side) is not None else np.nan
            )

        our_correct = actual_side is not None and our_side == actual_side
        book_correct = actual_side is not None and book_side == actual_side
        agreement = bool(our_side and book_side and our_side == book_side)

        if actual_side is None:
            outcome = "Pending"
        elif our_correct and not book_correct:
            outcome = "We beat the books"
        elif book_correct and not our_correct:
            outcome = "Books beat us"
        elif our_correct and book_correct:
            outcome = "Both correct"
        else:
            outcome = "Both wrong"

        records.append(
            {
                "game_id": row.get("game_id"),
                "league": league or None,
                "commence_time": row.get("commence_time"),
                "home_team": row.get("home_team"),
                "away_team": row.get("away_team"),
                "our_pick_side": our_side,
                "our_pick_team": our_team,
                "our_pick_prob": our_prob,
                "book_pick_side": book_side,
                "book_pick_team": book_team,
                "book_pick_prob": book_prob,
                "agreement": agreement,
                "actual_winner_side": actual_side,
                "actual_winner_team": actual_team,
                "our_correct": our_correct if actual_side is not None else None,
                "book_correct": book_correct if actual_side is not None else None,
                "comparison_outcome": outcome,
                "probability_gap": gap,
            }
        )

    comparison_df = pd.DataFrame.from_records(records)
    if "commence_time" in comparison_df.columns:
        comparison_df = comparison_df.sort_values("commence_time", ascending=True)
    return comparison_df


def summarize_prediction_comparison(df: pd.DataFrame) -> PredictionComparisonStats:
    if df.empty:
        return PredictionComparisonStats(
            total_games=0,
            agreement_rate=None,
            we_right_books_wrong=0,
            books_right_we_wrong=0,
            both_correct=0,
            both_wrong=0,
            pending=0,
            our_accuracy=None,
            book_accuracy=None,
        )

    agreement_rate = float(df["agreement"].mean()) if "agreement" in df.columns else None
    completed = df[df["actual_winner_side"].notna()] if "actual_winner_side" in df.columns else pd.DataFrame()
    pending = len(df) - len(completed)
    completed_total = len(completed)

    def _count(mask: pd.Series) -> int:
        if completed.empty:
            return 0
        return int(mask.sum())

    we_right_books_wrong = _count((completed["our_correct"] == True) & (completed["book_correct"] != True))  # noqa: E712
    books_right_we_wrong = _count((completed["book_correct"] == True) & (completed["our_correct"] != True))  # noqa: E712
    both_correct = _count((completed["our_correct"] == True) & (completed["book_correct"] == True))  # noqa: E712
    both_wrong = _count((completed["our_correct"] == False) & (completed["book_correct"] == False))  # noqa: E712

    if completed_total > 0:
        our_accuracy = float((completed["our_correct"] == True).mean())  # noqa: E712
        book_accuracy = float((completed["book_correct"] == True).mean())  # noqa: E712
    else:
        our_accuracy = None
        book_accuracy = None

    return PredictionComparisonStats(
        total_games=len(df),
        agreement_rate=agreement_rate,
        we_right_books_wrong=we_right_books_wrong,
        books_right_we_wrong=books_right_we_wrong,
        both_correct=both_correct,
        both_wrong=both_wrong,
        pending=pending,
        our_accuracy=our_accuracy,
        book_accuracy=book_accuracy,
    )


def get_moneylines_for_recommended(recommended: pd.DataFrame) -> pd.DataFrame:
    mapping = _map_game_ids_by_odds_api(recommended)
    remaining = recommended
    if not mapping.empty:
        matched_ids = set(mapping["prediction_game_id"])
        remaining = recommended[~recommended["game_id"].isin(matched_ids)]

    if not remaining.empty:
        extra = _match_games_to_db(remaining)
        if not extra.empty:
            mapping = pd.concat([mapping, extra], ignore_index=True) if not mapping.empty else extra

    if mapping.empty:
        return pd.DataFrame(columns=["forward_game_id", "outcome", "book", "moneyline", "fetched_at_utc"])

    db_ids = mapping["db_game_id"].dropna().unique().tolist()
    if not db_ids:
        return pd.DataFrame(columns=["forward_game_id", "outcome", "book", "moneyline", "fetched_at_utc"])

    placeholders = ",".join("?" for _ in db_ids)
    query = f"""
        WITH latest_snapshot AS (
            SELECT
                o.game_id,
                o.book_id,
                o.outcome,
                MAX(s.fetched_at_utc) AS max_fetched
            FROM odds o
            JOIN odds_snapshots s ON o.snapshot_id = s.snapshot_id
            WHERE o.market = 'h2h'
              AND o.game_id IN ({placeholders})
              AND o.outcome IN ('home', 'away', 'draw')
            GROUP BY o.game_id, o.book_id, o.outcome
        )
        SELECT
            o.game_id,
            o.outcome,
            b.name AS book,
            o.price_american AS moneyline,
            s.fetched_at_utc
        FROM odds o
        JOIN odds_snapshots s ON o.snapshot_id = s.snapshot_id
        JOIN books b ON o.book_id = b.book_id
        JOIN latest_snapshot ls
          ON ls.game_id = o.game_id
         AND ls.book_id = o.book_id
         AND ls.outcome = o.outcome
         AND ls.max_fetched = s.fetched_at_utc
        WHERE o.market = 'h2h'
          AND o.price_american IS NOT NULL
          AND o.game_id IN ({placeholders})
    """

    params = db_ids + db_ids
    with connect() as conn:
        rows = conn.execute(query, params).fetchall()

    if not rows:
        return pd.DataFrame(columns=["forward_game_id", "outcome", "book", "moneyline", "fetched_at_utc"])

    odds_df = pd.DataFrame([dict(row) for row in rows])
    if not odds_df.empty and "book" in odds_df.columns:
        odds_df = odds_df[~odds_df["book"].astype(str).str.contains("kaggle", case=False, na=False)].copy()

    if odds_df.empty:
        return pd.DataFrame(columns=["forward_game_id", "outcome", "book", "moneyline", "fetched_at_utc"])

    odds_df = odds_df.merge(mapping, how="left", left_on="game_id", right_on="db_game_id")
    odds_df = odds_df.rename(columns={"prediction_game_id": "forward_game_id"})
    return odds_df[["forward_game_id", "outcome", "book", "moneyline", "fetched_at_utc"]]


def get_totals_odds_for_recommended(recommended: pd.DataFrame) -> pd.DataFrame:
    mapping = _map_game_ids_by_odds_api(recommended)
    remaining = recommended
    if not mapping.empty:
        matched_ids = set(mapping["prediction_game_id"])
        remaining = recommended[~recommended["game_id"].isin(matched_ids)]

    if not remaining.empty:
        extra = _match_games_to_db(remaining)
        if not extra.empty:
            mapping = pd.concat([mapping, extra], ignore_index=True) if not mapping.empty else extra

    if mapping.empty:
        return pd.DataFrame(
            columns=["forward_game_id", "db_game_id", "book", "outcome", "moneyline", "line", "fetched_at_utc"]
        )

    db_ids = mapping["db_game_id"].dropna().unique().tolist()
    if not db_ids:
        return pd.DataFrame(
            columns=["forward_game_id", "db_game_id", "book", "outcome", "moneyline", "line", "fetched_at_utc"]
        )

    placeholders = ",".join("?" for _ in db_ids)
    query = f"""
        WITH latest_snapshot AS (
            SELECT
                o.game_id,
                o.book_id,
                MAX(s.fetched_at_utc) AS max_fetched
            FROM odds o
            JOIN odds_snapshots s ON o.snapshot_id = s.snapshot_id
            WHERE o.market = 'totals'
              AND o.game_id IN ({placeholders})
              AND LOWER(o.outcome) IN ('over', 'under')
            GROUP BY o.game_id, o.book_id
        )
        SELECT
            o.game_id,
            o.outcome,
            o.line,
            o.price_american AS moneyline,
            b.name AS book,
            s.fetched_at_utc,
            t1.name AS home_team_full,
            t2.name AS away_team_full
        FROM odds o
        JOIN odds_snapshots s ON o.snapshot_id = s.snapshot_id
        JOIN books b ON o.book_id = b.book_id
        JOIN latest_snapshot ls
          ON ls.game_id = o.game_id
         AND ls.book_id = o.book_id
         AND ls.max_fetched = s.fetched_at_utc
        JOIN games g ON o.game_id = g.game_id
        JOIN teams t1 ON g.home_team_id = t1.team_id
        JOIN teams t2 ON g.away_team_id = t2.team_id
        WHERE o.market = 'totals'
          AND LOWER(o.outcome) IN ('over', 'under')
          AND o.price_american IS NOT NULL
          AND o.game_id IN ({placeholders})
    """

    params = db_ids + db_ids
    with connect() as conn:
        rows = conn.execute(query, params).fetchall()

    if not rows:
        return pd.DataFrame(
            columns=["forward_game_id", "db_game_id", "book", "outcome", "moneyline", "line", "fetched_at_utc", "home_team_full", "away_team_full"]
        )

    odds_df = pd.DataFrame(rows, columns=["db_game_id", "outcome", "line", "moneyline", "book", "fetched_at_utc", "home_team_full", "away_team_full"])
    odds_df["outcome"] = odds_df["outcome"].astype(str).str.lower()
    odds_df = odds_df.merge(mapping, how="left", left_on="db_game_id", right_on="db_game_id")
    odds_df = odds_df.rename(columns={"prediction_game_id": "forward_game_id"})
    odds_df = odds_df.dropna(subset=["forward_game_id"])
    return odds_df[["forward_game_id", "db_game_id", "book", "outcome", "moneyline", "line", "fetched_at_utc", "home_team_full", "away_team_full"]]


def get_game_odds(game_id: str) -> pd.DataFrame:
    """Get all sportsbook odds for a specific game ID."""
    # Try to resolve the game_id to a DB ID
    # game_id could be the prediction ID (e.g. "e1b2...") or prefixed (e.g. "NBA_e1b2...")
    
    # We can reuse the mapping logic but simplified for a single ID
    prediction_id = game_id
    if "_" in game_id and not game_id.startswith("0"): # Simple heuristic, might need robust parsing if IDs vary
         # If it looks like LEAGUE_ID, strip league? 
         # Actually, prediction IDs in parquet are usually just the hash or UUID.
         # But let's handle the case where we get passed the ID from the frontend which matches the parquet game_id.
         pass

    # Create a dummy dataframe to use existing mapping functions
    dummy_df = pd.DataFrame([{"game_id": game_id}])
    mapping = _map_game_ids_by_odds_api(dummy_df)
    
    db_game_id = None
    if not mapping.empty:
        db_game_id = mapping.iloc[0]["db_game_id"]
    
    # If not found via odds_api_id, try direct lookup if it looks like a DB ID (integer?) 
    # Our DB game_ids are integers? Let's check schema. 
    # Actually, looking at _match_games_to_db, it seems we match prediction IDs to DB IDs.
    
    if not db_game_id:
        # If we couldn't map it, maybe it's already a DB ID? 
        # Or maybe we need to try the _match_games_to_db logic if we had team names.
        # For now, let's assume the frontend passes the prediction game_id.
        return pd.DataFrame()

    query = """
        WITH latest_snapshot AS (
            SELECT
                o.game_id,
                o.book_id,
                o.market,
                o.outcome,
                MAX(s.fetched_at_utc) AS max_fetched
            FROM odds o
            JOIN odds_snapshots s ON o.snapshot_id = s.snapshot_id
            WHERE o.game_id = ?
            GROUP BY o.game_id, o.book_id, o.market, o.outcome
        )
        SELECT
            o.market,
            o.outcome,
            o.line,
            o.price_american AS moneyline,
            b.name AS book,
            s.fetched_at_utc
        FROM odds o
        JOIN odds_snapshots s ON o.snapshot_id = s.snapshot_id
        JOIN books b ON o.book_id = b.book_id
        JOIN latest_snapshot ls
          ON ls.game_id = o.game_id
         AND ls.book_id = o.book_id
         AND ls.market = o.market
         AND ls.outcome = o.outcome
         AND ls.max_fetched = s.fetched_at_utc
        WHERE o.game_id = ?
          AND o.price_american IS NOT NULL
    """
    
    with connect() as conn:
        rows = conn.execute(query, (db_game_id, db_game_id)).fetchall()
        
    if not rows:
        return pd.DataFrame()
        
    df = pd.DataFrame(rows, columns=["market", "outcome", "line", "moneyline", "book", "fetched_at_utc"])
    
    # Add book_url
    if "book" in df.columns:
        df["book_url"] = df["book"].apply(lambda x: get_sportsbook_url(x) if pd.notna(x) else "")
        
    return df


__all__ = [
    "SummaryMetrics",
    "PredictionComparisonStats",
    "DEFAULT_EDGE_THRESHOLD",
    "DEFAULT_STAKE",
    "DEFAULT_STARTING_BANKROLL",
    "DISPLAY_TIMEZONE",
    "load_forward_test_data",
    "calculate_summary_metrics",
    "calculate_totals_metrics",
    "get_performance_over_time",
    "get_totals_performance_over_time",
    "get_recent_predictions",
    "get_recommended_bets",
    "get_completed_bets",
    "get_performance_by_threshold",
    "get_totals_performance_by_threshold",
    "get_upcoming_calendar",
    "get_overunder_recommendations",
    "get_overunder_completed",
    "build_prediction_comparison",
    "summarize_prediction_comparison",
    "get_moneylines_for_recommended",
    "get_totals_odds_for_recommended",
    "get_game_odds",
    "get_version_options",
    "get_default_version_value",
    "filter_by_version",
]
