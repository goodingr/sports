"""Data loading and analytics helpers for the forward testing dashboard."""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Iterable, Optional

import numpy as np
import pandas as pd

FORWARD_TEST_DIR = Path("data/forward_test")
MASTER_PREDICTIONS_PATH = FORWARD_TEST_DIR / "predictions_master.parquet"

DEFAULT_EDGE_THRESHOLD = 0.06
DEFAULT_STAKE = 100.0
DEFAULT_STARTING_BANKROLL = 10_000.0


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


def _to_datetime(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce") if series is not None else series


@lru_cache(maxsize=1)
def _read_predictions_cached(path_str: str) -> pd.DataFrame:
    path = Path(path_str)
    if not path.exists():
        return pd.DataFrame()

    df = pd.read_parquet(path)

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

    return df


def load_forward_test_data(*, force_refresh: bool = False, path: Path = MASTER_PREDICTIONS_PATH) -> pd.DataFrame:
    """Load forward test predictions with optional cache busting."""

    path_str = str(path.resolve())
    if force_refresh:
        _read_predictions_cached.cache_clear()
    df = _read_predictions_cached(path_str)
    return df.copy() if not df.empty else df


def _american_to_decimal(ml: float) -> float:
    if ml is None or np.isnan(ml) or ml == 0:
        return np.nan
    if ml > 0:
        return 1.0 + (ml / 100.0)
    return 1.0 + (100.0 / abs(ml))


def _bet_profit(ml: float, stake: float, won: Optional[bool]) -> float:
    if ml is None or np.isnan(ml) or stake <= 0:
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

    columns = [
        "game_id",
        "commence_time",
        "predicted_at",
        "result_updated_at",
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

    missing = [column for column in columns if column not in df.columns]
    if missing:
        raise KeyError(f"Missing expected columns in predictions: {missing}")

    records: list[dict[str, object]] = []
    for _, row in df.iterrows():
        for side in ("home", "away"):
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
                won: Optional[bool] = None
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
                    "settled_at": row.get("result_updated_at") or row.get("commence_time"),
                    "home_score": row.get("home_score"),
                    "away_score": row.get("away_score"),
                    "implied_decimal": _american_to_decimal(float(moneyline)) if moneyline is not None else np.nan,
                }
            )

    bets = pd.DataFrame(records)
    if not bets.empty:
        bets["commence_time"] = _to_datetime(bets["commence_time"])
        bets["predicted_at"] = _to_datetime(bets["predicted_at"])
        bets["settled_at"] = _to_datetime(bets["settled_at"])
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
        )

    bets = _expand_predictions(df, stake=stake)

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
    )


def get_performance_over_time(
    df: pd.DataFrame,
    *,
    edge_threshold: float = DEFAULT_EDGE_THRESHOLD,
    stake: float = DEFAULT_STAKE,
    freq: str = "D",
) -> pd.DataFrame:
    bets = _expand_predictions(df, stake=stake)
    if bets.empty:
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
    summary["cumulative_profit"] = summary["profit"].cumsum()

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
    bets = bets.sort_values("predicted_at", ascending=False)

    if edge_threshold is not None:
        bets = bets[bets["edge"].notna() & (bets["edge"] >= edge_threshold)]

    return bets.head(limit)


def get_recommended_bets(
    df: pd.DataFrame,
    *,
    edge_threshold: float = DEFAULT_EDGE_THRESHOLD,
) -> pd.DataFrame:
    bets = _expand_predictions(df)
    if bets.empty:
        return bets

    upcoming = bets[bets["won"].isna()]
    upcoming = upcoming[upcoming["edge"].notna() & (upcoming["edge"] >= edge_threshold)]

    return upcoming.sort_values("commence_time")


def get_performance_by_threshold(
    df: pd.DataFrame,
    *,
    thresholds: Optional[Iterable[float]] = None,
    stake: float = DEFAULT_STAKE,
) -> pd.DataFrame:
    bets = _expand_predictions(df, stake=stake)
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
    "load_forward_test_data",
    "calculate_summary_metrics",
    "get_performance_over_time",
    "get_recent_predictions",
    "get_recommended_bets",
    "get_performance_by_threshold",
    "get_upcoming_calendar",
]

