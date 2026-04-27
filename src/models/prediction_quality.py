"""Prediction quality reporting for betting model launch gates.

This module evaluates the thing the paid product depends on: whether a
predeclared betting rule would have made money using only predictions created
before game start.
"""

from __future__ import annotations

import argparse
import json
import logging
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Literal, Optional

import numpy as np
import pandas as pd
import yaml
from sklearn.metrics import brier_score_loss

from src.db.core import DB_PATH

LOGGER = logging.getLogger(__name__)

Market = Literal["totals", "moneyline"]
DEFAULT_RELEASE_LEAGUES = ("NBA", "NHL", "EPL", "LALIGA", "BUNDESLIGA", "SERIEA", "LIGUE1")
DEFAULT_STAKE = 100.0


@dataclass(frozen=True)
class QualityGate:
    min_bets_narrow: int = 150
    min_bets_multi_league: int = 300
    min_roi: float = 0.05
    min_bootstrap_roi_low: float = 0.0
    bootstrap_samples: int = 3000
    random_seed: int = 42


def american_to_decimal(moneyline: float | int | None) -> Optional[float]:
    """Convert American odds to decimal odds."""
    if moneyline is None or pd.isna(moneyline):
        return None
    ml = float(moneyline)
    if ml == 0:
        return None
    if ml > 0:
        return 1.0 + ml / 100.0
    return 1.0 + 100.0 / abs(ml)


def american_profit(moneyline: float | int | None, won: bool, stake: float = DEFAULT_STAKE) -> float:
    """Return profit for a settled American-odds bet."""
    decimal = american_to_decimal(moneyline)
    if decimal is None:
        return np.nan
    if won:
        return stake * (decimal - 1.0)
    return -stake


def implied_probability(moneyline: float | int | None) -> Optional[float]:
    decimal = american_to_decimal(moneyline)
    if decimal is None:
        return None
    return 1.0 / decimal


def no_vig_pair(first_moneyline: float, second_moneyline: float) -> tuple[float, float]:
    """Normalize two implied probabilities to remove sportsbook hold."""
    first = implied_probability(first_moneyline)
    second = implied_probability(second_moneyline)
    if first is None or second is None or first + second <= 0:
        return np.nan, np.nan
    total = first + second
    return first / total, second / total


def expected_value(probability: float, moneyline: float, stake: float = DEFAULT_STAKE) -> float:
    """Expected profit in dollars for a one-unit bet."""
    decimal = american_to_decimal(moneyline)
    if decimal is None or pd.isna(probability):
        return np.nan
    return probability * stake * (decimal - 1.0) - (1.0 - probability) * stake


def settle_total_side(actual_total: float, line: float, side: str) -> Optional[bool]:
    """Settle an over/under side. Returns None for pushes."""
    if pd.isna(actual_total) or pd.isna(line):
        return None
    side_norm = side.lower()
    if np.isclose(actual_total, line):
        return None
    if side_norm == "over":
        return bool(actual_total > line)
    if side_norm == "under":
        return bool(actual_total < line)
    raise ValueError(f"Unsupported totals side: {side}")


def settle_moneyline_side(home_score: float, away_score: float, side: str) -> Optional[bool]:
    """Settle home/away/draw moneyline. Returns None for unresolved/tie push cases."""
    if pd.isna(home_score) or pd.isna(away_score):
        return None
    side_norm = side.lower()
    if home_score == away_score:
        if side_norm == "draw":
            return True
        if side_norm in {"home", "away"}:
            return False
        return None
    winner = "home" if home_score > away_score else "away"
    if side_norm not in {"home", "away", "draw"}:
        raise ValueError(f"Unsupported moneyline side: {side}")
    return side_norm == winner


def _read_sql(db_path: Path, query: str, params: Iterable[Any] = ()) -> pd.DataFrame:
    with sqlite3.connect(str(db_path)) as conn:
        return pd.read_sql_query(query, conn, params=list(params))


def _league_filter_sql(leagues: Optional[Iterable[str]]) -> tuple[str, list[str]]:
    if not leagues:
        return "", []
    normalized = [league.upper() for league in leagues]
    placeholders = ",".join("?" for _ in normalized)
    return f" AND UPPER(s.league) IN ({placeholders})", normalized


def _coerce_prediction_times(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["predicted_at"] = pd.to_datetime(df["predicted_at"], utc=True, errors="coerce")
    df["start_time_utc"] = pd.to_datetime(df["start_time_utc"], utc=True, errors="coerce")
    return df


def _latest_pre_game(df: pd.DataFrame, group_cols: list[str]) -> pd.DataFrame:
    df = _coerce_prediction_times(df)
    df = df[df["predicted_at"].notna() & df["start_time_utc"].notna()].copy()
    df = df[df["predicted_at"] <= df["start_time_utc"]].copy()
    return df.sort_values("predicted_at").drop_duplicates(group_cols, keep="last")


def load_totals_model_input(
    db_path: Path = DB_PATH,
    leagues: Optional[Iterable[str]] = DEFAULT_RELEASE_LEAGUES,
    latest_only: bool = True,
) -> pd.DataFrame:
    """Load canonical settled totals model input from SQLite.

    The returned frame contains only rows that can be evaluated without leakage:
    settled games, available total line/odds/probabilities, and predictions made
    before game start.
    """
    league_sql, params = _league_filter_sql(leagues)
    query = f"""
        SELECT
            p.prediction_id,
            p.game_id,
            p.model_type,
            p.predicted_at,
            p.total_line,
            p.over_prob,
            p.under_prob,
            p.over_moneyline,
            p.under_moneyline,
            p.over_edge,
            p.under_edge,
            p.over_implied_prob,
            p.under_implied_prob,
            p.predicted_total_points,
            g.start_time_utc,
            s.league,
            gr.home_score,
            gr.away_score,
            gr.total_close
        FROM predictions p
        JOIN games g ON p.game_id = g.game_id
        JOIN sports s ON g.sport_id = s.sport_id
        JOIN game_results gr ON p.game_id = gr.game_id
        WHERE gr.home_score IS NOT NULL
          AND gr.away_score IS NOT NULL
          AND p.total_line IS NOT NULL
          AND p.over_prob IS NOT NULL
          AND p.under_prob IS NOT NULL
          AND p.over_moneyline IS NOT NULL
          AND p.under_moneyline IS NOT NULL
          {league_sql}
    """
    df = _read_sql(db_path, query, params)
    if df.empty:
        return df
    numeric_cols = [
        "total_line",
        "over_prob",
        "under_prob",
        "over_moneyline",
        "under_moneyline",
        "over_edge",
        "under_edge",
        "over_implied_prob",
        "under_implied_prob",
        "predicted_total_points",
        "home_score",
        "away_score",
        "total_close",
    ]
    for column in numeric_cols:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")
    df = _latest_pre_game(df, ["game_id", "model_type"]) if latest_only else _coerce_prediction_times(df)
    df["actual_total"] = df["home_score"] + df["away_score"]
    df["model_total_error"] = df["predicted_total_points"] - df["actual_total"]
    df["market_line_error"] = df["total_line"] - df["actual_total"]
    no_vig = df.apply(
        lambda row: no_vig_pair(row["over_moneyline"], row["under_moneyline"]),
        axis=1,
        result_type="expand",
    )
    df["over_no_vig_prob"] = no_vig[0]
    df["under_no_vig_prob"] = no_vig[1]
    return df


def load_moneyline_model_input(
    db_path: Path = DB_PATH,
    leagues: Optional[Iterable[str]] = DEFAULT_RELEASE_LEAGUES,
    latest_only: bool = True,
) -> pd.DataFrame:
    """Load canonical settled moneyline model input from SQLite."""
    league_sql, params = _league_filter_sql(leagues)
    query = f"""
        SELECT
            p.prediction_id,
            p.game_id,
            p.model_type,
            p.predicted_at,
            p.home_prob,
            p.away_prob,
            p.home_moneyline,
            p.away_moneyline,
            p.home_edge,
            p.away_edge,
            p.home_implied_prob,
            p.away_implied_prob,
            g.start_time_utc,
            s.league,
            gr.home_score,
            gr.away_score
        FROM predictions p
        JOIN games g ON p.game_id = g.game_id
        JOIN sports s ON g.sport_id = s.sport_id
        JOIN game_results gr ON p.game_id = gr.game_id
        WHERE gr.home_score IS NOT NULL
          AND gr.away_score IS NOT NULL
          AND p.home_prob IS NOT NULL
          AND p.away_prob IS NOT NULL
          AND p.home_moneyline IS NOT NULL
          AND p.away_moneyline IS NOT NULL
          {league_sql}
    """
    df = _read_sql(db_path, query, params)
    if df.empty:
        return df
    for column in [
        "home_prob",
        "away_prob",
        "home_moneyline",
        "away_moneyline",
        "home_edge",
        "away_edge",
        "home_implied_prob",
        "away_implied_prob",
        "home_score",
        "away_score",
    ]:
        df[column] = pd.to_numeric(df[column], errors="coerce")
    df = _latest_pre_game(df, ["game_id", "model_type"]) if latest_only else _coerce_prediction_times(df)
    no_vig = df.apply(
        lambda row: no_vig_pair(row["home_moneyline"], row["away_moneyline"]),
        axis=1,
        result_type="expand",
    )
    df["home_no_vig_prob"] = no_vig[0]
    df["away_no_vig_prob"] = no_vig[1]
    return df


def expand_totals_bets(model_input: pd.DataFrame) -> pd.DataFrame:
    """Expand one totals prediction row into over and under candidate bets."""
    if model_input.empty:
        return pd.DataFrame()
    rows: list[pd.DataFrame] = []
    for side in ("over", "under"):
        side_df = model_input.copy()
        side_df["market"] = "totals"
        side_df["side"] = side
        side_df["predicted_prob"] = side_df[f"{side}_prob"]
        side_df["moneyline"] = side_df[f"{side}_moneyline"]
        side_df["edge"] = side_df[f"{side}_edge"]
        side_df["implied_prob"] = side_df[f"{side}_implied_prob"]
        side_df["no_vig_market_prob"] = side_df[f"{side}_no_vig_prob"]
        rows.append(side_df)
    bets = pd.concat(rows, ignore_index=True)
    bets["won"] = bets.apply(
        lambda row: settle_total_side(row["actual_total"], row["total_line"], row["side"]),
        axis=1,
    )
    bets = bets[bets["won"].notna()].copy()
    bets["won"] = bets["won"].astype(bool)
    bets["profit"] = bets.apply(lambda row: american_profit(row["moneyline"], row["won"]), axis=1)
    bets["expected_value"] = bets.apply(
        lambda row: expected_value(row["predicted_prob"], row["moneyline"]),
        axis=1,
    )
    bets["actual_value"] = bets["profit"]
    bets["closing_line_value"] = np.where(
        bets["side"] == "over",
        bets["total_close"] - bets["total_line"],
        bets["total_line"] - bets["total_close"],
    )
    return bets


def expand_moneyline_bets(model_input: pd.DataFrame) -> pd.DataFrame:
    """Expand one moneyline prediction row into home and away candidate bets."""
    if model_input.empty:
        return pd.DataFrame()
    rows: list[pd.DataFrame] = []
    for side in ("home", "away"):
        side_df = model_input.copy()
        side_df["market"] = "moneyline"
        side_df["side"] = side
        side_df["predicted_prob"] = side_df[f"{side}_prob"]
        side_df["moneyline"] = side_df[f"{side}_moneyline"]
        side_df["edge"] = side_df[f"{side}_edge"]
        side_df["implied_prob"] = side_df[f"{side}_implied_prob"]
        side_df["no_vig_market_prob"] = side_df[f"{side}_no_vig_prob"]
        rows.append(side_df)
    bets = pd.concat(rows, ignore_index=True)
    bets["won"] = bets.apply(
        lambda row: settle_moneyline_side(row["home_score"], row["away_score"], row["side"]),
        axis=1,
    )
    bets = bets[bets["won"].notna()].copy()
    bets["won"] = bets["won"].astype(bool)
    bets["profit"] = bets.apply(lambda row: american_profit(row["moneyline"], row["won"]), axis=1)
    bets["expected_value"] = bets.apply(
        lambda row: expected_value(row["predicted_prob"], row["moneyline"]),
        axis=1,
    )
    bets["actual_value"] = bets["profit"]
    return bets


def bootstrap_roi_interval(
    profits: pd.Series,
    stake: float = DEFAULT_STAKE,
    samples: int = 3000,
    seed: int = 42,
) -> tuple[float, float, float]:
    if profits.empty:
        return np.nan, np.nan, np.nan
    values = profits.astype(float).to_numpy()
    rng = np.random.default_rng(seed)
    sampled = rng.choice(values, size=(samples, len(values)), replace=True)
    rois = sampled.sum(axis=1) / (stake * len(values))
    low, median, high = np.percentile(rois, [2.5, 50.0, 97.5])
    return float(low), float(median), float(high)


def max_losing_streak(won: pd.Series) -> int:
    streak = 0
    worst = 0
    for value in won.astype(bool):
        if value:
            streak = 0
        else:
            streak += 1
            worst = max(worst, streak)
    return worst


def max_drawdown(profits: pd.Series) -> float:
    if profits.empty:
        return 0.0
    cumulative = profits.astype(float).cumsum()
    running_peak = cumulative.cummax()
    drawdown = running_peak - cumulative
    return float(drawdown.max()) if not drawdown.empty else 0.0


def calibration_bins(
    bets: pd.DataFrame,
    bins: Optional[list[float]] = None,
) -> list[dict[str, Any]]:
    if bets.empty:
        return []
    bins = bins or [0.0, 0.45, 0.5, 0.55, 0.6, 0.65, 0.7, 0.8, 1.0]
    frame = bets.dropna(subset=["predicted_prob", "won"]).copy()
    frame["prob_bin"] = pd.cut(frame["predicted_prob"], bins=bins, include_lowest=True)
    rows: list[dict[str, Any]] = []
    for prob_bin, group in frame.groupby("prob_bin", observed=True):
        rows.append(
            {
                "prob_bin": str(prob_bin),
                "bets": int(len(group)),
                "avg_predicted_prob": float(group["predicted_prob"].mean()),
                "actual_win_rate": float(group["won"].mean()),
                "calibration_error": float(group["predicted_prob"].mean() - group["won"].mean()),
            }
        )
    return rows


def summarize_bets(
    bets: pd.DataFrame,
    gate: QualityGate = QualityGate(),
) -> dict[str, Any]:
    """Summarize settled bets for a model/rule."""
    if bets.empty:
        return {
            "bets": 0,
            "roi": None,
            "win_rate": None,
            "profit": 0.0,
            "passes_launch_gate": False,
        }
    frame = bets.dropna(subset=["profit", "predicted_prob", "won"]).copy()
    total_staked = DEFAULT_STAKE * len(frame)
    roi = float(frame["profit"].sum() / total_staked) if total_staked > 0 else np.nan
    ci_low, ci_median, ci_high = bootstrap_roi_interval(
        frame["profit"],
        samples=gate.bootstrap_samples,
        seed=gate.random_seed,
    )
    brier = None
    if frame["won"].nunique() > 1:
        brier = float(brier_score_loss(frame["won"].astype(int), frame["predicted_prob"]))
    market_brier = None
    if frame["won"].nunique() > 1 and "no_vig_market_prob" in frame.columns:
        market_probs = frame["no_vig_market_prob"].fillna(frame["implied_prob"])
        market_brier = float(brier_score_loss(frame["won"].astype(int), market_probs))

    ordered_won = (
        frame.sort_values("start_time_utc")["won"]
        if "start_time_utc" in frame.columns
        else frame["won"]
    )

    return {
        "bets": int(len(frame)),
        "profit": float(frame["profit"].sum()),
        "roi": roi,
        "win_rate": float(frame["won"].mean()),
        "avg_edge": float(frame["edge"].mean()) if "edge" in frame.columns else None,
        "avg_expected_value": float(frame["expected_value"].mean()),
        "actual_value_per_bet": float(frame["actual_value"].mean()),
        "brier_score": brier,
        "market_brier_score": market_brier,
        "model_beats_market_brier": bool(
            brier is not None and market_brier is not None and brier < market_brier
        ),
        "bootstrap_roi_low": ci_low,
        "bootstrap_roi_median": ci_median,
        "bootstrap_roi_high": ci_high,
        "max_drawdown": max_drawdown(frame["profit"]),
        "max_losing_streak": max_losing_streak(ordered_won),
        "calibration_bins": calibration_bins(frame),
        "passes_launch_gate": bool(
            len(frame) >= gate.min_bets_narrow
            and roi >= gate.min_roi
            and ci_low > gate.min_bootstrap_roi_low
        ),
    }


def rule_is_disabled(rule: dict[str, Any]) -> bool:
    status = str(rule.get("status", "")).strip().lower()
    return bool(rule.get("disabled")) or rule.get("enabled") is False or status == "disabled"


def _empty_like(frame: pd.DataFrame) -> pd.DataFrame:
    return frame.iloc[0:0].copy()


def filter_bets_for_rule(bets: pd.DataFrame, rule: dict[str, Any]) -> pd.DataFrame:
    """Apply a rule's market/league/model/side/price filters to a bet table."""
    frame = bets.copy()
    if frame.empty or rule_is_disabled(rule):
        return _empty_like(frame)

    if rule.get("market"):
        if "market" not in frame.columns:
            return _empty_like(frame)
        frame = frame[frame["market"] == str(rule["market"]).lower()]
    if rule.get("league") and str(rule["league"]).upper() != "ALL":
        if "league" not in frame.columns:
            return _empty_like(frame)
        frame = frame[frame["league"].astype(str).str.upper() == str(rule["league"]).upper()]
    if rule.get("model_type"):
        if "model_type" not in frame.columns:
            return _empty_like(frame)
        frame = frame[frame["model_type"] == rule["model_type"]]
    if rule.get("side") and str(rule["side"]).lower() != "both":
        if "side" not in frame.columns:
            return _empty_like(frame)
        frame = frame[frame["side"] == str(rule["side"]).lower()]
    if rule.get("min_edge") is not None:
        if "edge" not in frame.columns:
            return _empty_like(frame)
        frame = frame[frame["edge"] >= float(rule["min_edge"])]
    if rule.get("min_moneyline") is not None:
        if "moneyline" not in frame.columns:
            return _empty_like(frame)
        frame = frame[frame["moneyline"] >= float(rule["min_moneyline"])]
    if rule.get("max_moneyline") is not None:
        if "moneyline" not in frame.columns:
            return _empty_like(frame)
        frame = frame[frame["moneyline"] <= float(rule["max_moneyline"])]
    return frame.copy()


def evaluate_rule(bets: pd.DataFrame, rule: dict[str, Any], gate: QualityGate) -> dict[str, Any]:
    """Apply a predeclared rule and return quality metrics."""
    frame = filter_bets_for_rule(bets, rule)

    summary = summarize_bets(frame, gate=gate)
    league = str(rule.get("league") or "ALL").upper()
    required_min_bets = gate.min_bets_multi_league if league == "ALL" else gate.min_bets_narrow
    summary["required_min_bets"] = required_min_bets
    summary["passes_launch_gate"] = bool(
        summary.get("bets", 0) >= required_min_bets
        and summary.get("roi") is not None
        and summary["roi"] >= gate.min_roi
        and summary.get("bootstrap_roi_low") is not None
        and summary["bootstrap_roi_low"] > gate.min_bootstrap_roi_low
        and summary.get("model_beats_market_brier") is True
    )
    summary["rule_id"] = rule.get("id")
    summary["status"] = rule.get("status", "candidate")
    summary["disabled"] = rule_is_disabled(rule)
    summary["rule"] = rule
    return summary


def _first_existing_column(df: pd.DataFrame, candidates: Iterable[str]) -> Optional[str]:
    for column in candidates:
        if column in df.columns:
            return column
    return None


def _best_or_selected(row: pd.Series, best_column: str, selected_column: str) -> float:
    best = row.get(best_column)
    if pd.notna(best):
        return float(best)
    selected = row.get(selected_column)
    if pd.notna(selected):
        return float(selected)
    return np.nan


def _coalesce_probability(frame: pd.DataFrame, columns: Iterable[str], fallback: float = np.nan) -> pd.Series:
    result = pd.Series(fallback, index=frame.index, dtype="float64")
    for column in columns:
        if column in frame.columns:
            result = result.where(result.notna(), pd.to_numeric(frame[column], errors="coerce"))
    return result


def _expand_totals_benchmark_predictions(predictions: pd.DataFrame) -> pd.DataFrame:
    if predictions.empty:
        return pd.DataFrame()
    frame = predictions.copy()
    line_column = _first_existing_column(frame, ["line", "total_line"])
    if line_column is None:
        return pd.DataFrame()
    frame["line"] = pd.to_numeric(frame[line_column], errors="coerce")
    if "actual_total" not in frame.columns:
        if {"home_score", "away_score"} <= set(frame.columns):
            frame["actual_total"] = (
                pd.to_numeric(frame["home_score"], errors="coerce")
                + pd.to_numeric(frame["away_score"], errors="coerce")
            )
        else:
            return pd.DataFrame()
    else:
        frame["actual_total"] = pd.to_numeric(frame["actual_total"], errors="coerce")

    over_prob = _coalesce_probability(frame, ["over_predicted_prob", "over_prob", "predicted_prob"])
    over_market = _coalesce_probability(
        frame,
        ["over_no_vig_prob", "over_market_prob", "market_prob", "no_vig_market_prob"],
    )
    under_market = _coalesce_probability(frame, ["under_no_vig_prob", "under_market_prob"])
    under_market = under_market.where(under_market.notna(), 1.0 - over_market)

    rows: list[pd.DataFrame] = []
    for side in ("over", "under"):
        side_df = frame.copy()
        side_df["market"] = "totals"
        side_df["side"] = side
        if side == "over":
            side_df["predicted_prob"] = over_prob
            side_df["no_vig_market_prob"] = over_market
            side_df["moneyline"] = side_df.apply(
                lambda row: _best_or_selected(row, "best_over_moneyline", "over_moneyline"),
                axis=1,
            )
        else:
            side_df["predicted_prob"] = 1.0 - over_prob
            side_df["no_vig_market_prob"] = under_market
            side_df["moneyline"] = side_df.apply(
                lambda row: _best_or_selected(row, "best_under_moneyline", "under_moneyline"),
                axis=1,
            )
        side_df["implied_prob"] = side_df["no_vig_market_prob"]
        edge_column = f"{side}_edge"
        if edge_column in side_df.columns:
            side_df["edge"] = pd.to_numeric(side_df[edge_column], errors="coerce")
        else:
            side_df["edge"] = side_df["predicted_prob"] - side_df["no_vig_market_prob"]
        rows.append(side_df)

    bets = pd.concat(rows, ignore_index=True)
    bets["won"] = bets.apply(
        lambda row: settle_total_side(row["actual_total"], row["line"], row["side"]),
        axis=1,
    )
    bets = bets[bets["won"].notna()].copy()
    if bets.empty:
        return bets
    bets["won"] = bets["won"].astype(bool)
    bets["profit"] = bets.apply(lambda row: american_profit(row["moneyline"], row["won"]), axis=1)
    bets["expected_value"] = bets.apply(
        lambda row: expected_value(row["predicted_prob"], row["moneyline"]),
        axis=1,
    )
    bets["actual_value"] = bets["profit"]
    if "snapshot_time_utc" in bets.columns and "predicted_at" not in bets.columns:
        bets["predicted_at"] = bets["snapshot_time_utc"]
    return bets


def _expand_moneyline_benchmark_predictions(predictions: pd.DataFrame) -> pd.DataFrame:
    if predictions.empty:
        return pd.DataFrame()
    frame = predictions.copy()
    if "predicted_prob" not in frame.columns:
        return pd.DataFrame()
    if "won" not in frame.columns:
        if "target_win" in frame.columns:
            frame["won"] = frame["target_win"].astype(bool)
        else:
            return pd.DataFrame()
    if "side" not in frame.columns and "is_home" in frame.columns:
        frame["side"] = np.where(frame["is_home"].astype(bool), "home", "away")
    if "side" not in frame.columns:
        return pd.DataFrame()

    frame["market"] = "moneyline"
    frame["predicted_prob"] = pd.to_numeric(frame["predicted_prob"], errors="coerce")
    frame["no_vig_market_prob"] = _coalesce_probability(
        frame,
        ["no_vig_market_prob", "no_vig_prob", "market_prob", "implied_prob"],
    )
    frame["implied_prob"] = frame["no_vig_market_prob"]
    frame["moneyline"] = frame.apply(
        lambda row: _best_or_selected(row, "best_moneyline", "moneyline"),
        axis=1,
    )
    if "edge" not in frame.columns:
        frame["edge"] = frame["predicted_prob"] - frame["no_vig_market_prob"]
    else:
        frame["edge"] = pd.to_numeric(frame["edge"], errors="coerce")
    if "snapshot_time_utc" in frame.columns and "predicted_at" not in frame.columns:
        frame["predicted_at"] = frame["snapshot_time_utc"]
    frame["won"] = frame["won"].astype(bool)
    frame["profit"] = frame.apply(lambda row: american_profit(row["moneyline"], row["won"]), axis=1)
    frame["expected_value"] = frame.apply(
        lambda row: expected_value(row["predicted_prob"], row["moneyline"]),
        axis=1,
    )
    frame["actual_value"] = frame["profit"]
    return frame


def expand_benchmark_predictions(
    predictions: pd.DataFrame,
    *,
    market: Optional[str] = None,
    source_id: Optional[str] = None,
) -> pd.DataFrame:
    """Convert validation/benchmark prediction rows into settled bet rows."""
    if predictions.empty:
        return pd.DataFrame()

    market_key = market.lower() if market else None
    if market_key is None and "market" in predictions.columns:
        markets = sorted({str(value).lower() for value in predictions["market"].dropna().unique()})
        if len(markets) == 1:
            market_key = markets[0]
        elif len(markets) > 1:
            frames = [
                expand_benchmark_predictions(
                    predictions[predictions["market"].astype(str).str.lower() == item],
                    market=item,
                    source_id=source_id,
                )
                for item in markets
            ]
            non_empty = [frame for frame in frames if not frame.empty]
            return pd.concat(non_empty, ignore_index=True) if non_empty else pd.DataFrame()

    if market_key is None:
        if "target_over" in predictions.columns or {"actual_total", "line"} <= set(predictions.columns):
            market_key = "totals"
        elif "target_win" in predictions.columns:
            market_key = "moneyline"

    if market_key == "totals":
        bets = _expand_totals_benchmark_predictions(predictions)
    elif market_key == "moneyline":
        bets = _expand_moneyline_benchmark_predictions(predictions)
    else:
        raise ValueError("Unable to infer benchmark prediction market")

    if source_id and not bets.empty:
        bets["source_id"] = source_id
    return bets


def _read_prediction_artifact(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix in {".parquet", ".pq"}:
        return pd.read_parquet(path)
    if suffix == ".csv":
        return pd.read_csv(path)
    if suffix in {".json", ".jsonl"}:
        return pd.read_json(path, lines=suffix == ".jsonl")
    raise ValueError(f"Unsupported benchmark prediction format: {path}")


def _quality_gate_from_config(config: dict[str, Any]) -> QualityGate:
    raw = config.get("launch_gate") or {}
    return QualityGate(
        min_bets_narrow=int(raw.get("min_bets_narrow", QualityGate.min_bets_narrow)),
        min_bets_multi_league=int(
            raw.get("min_bets_multi_league", QualityGate.min_bets_multi_league)
        ),
        min_roi=float(raw.get("min_roi", QualityGate.min_roi)),
        min_bootstrap_roi_low=float(
            raw.get("min_bootstrap_roi_low", QualityGate.min_bootstrap_roi_low)
        ),
        bootstrap_samples=int(raw.get("bootstrap_samples", QualityGate.bootstrap_samples)),
        random_seed=int(raw.get("random_seed", QualityGate.random_seed)),
    )


def load_rules(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"launch_gate": {}, "approved_rules": [], "candidate_rules": []}
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    data.setdefault("approved_rules", [])
    data.setdefault("candidate_rules", [])
    return data


def configured_rules(config: dict[str, Any]) -> list[dict[str, Any]]:
    """Return approved and candidate rules with section-derived default statuses."""
    approved = [
        {**rule, "status": rule.get("status", "approved")}
        for rule in config.get("approved_rules", []) or []
    ]
    candidates = [
        {**rule, "status": rule.get("status", "candidate")}
        for rule in config.get("candidate_rules", []) or []
    ]
    return [*approved, *candidates]


def build_quality_report(
    db_path: Path = DB_PATH,
    rules_path: Path = Path("config/published_rules.yml"),
    leagues: Optional[Iterable[str]] = DEFAULT_RELEASE_LEAGUES,
    benchmark_prediction_paths: Optional[Iterable[Path]] = None,
) -> dict[str, Any]:
    """Build a complete quality report for totals and moneyline candidates."""
    rules_config = load_rules(rules_path)
    gate = _quality_gate_from_config(rules_config)

    totals_input = load_totals_model_input(db_path=db_path, leagues=leagues)
    moneyline_input = load_moneyline_model_input(db_path=db_path, leagues=leagues)
    totals_bets = expand_totals_bets(totals_input)
    moneyline_bets = expand_moneyline_bets(moneyline_input)
    all_bets = pd.concat([totals_bets, moneyline_bets], ignore_index=True)

    rules = configured_rules(rules_config)
    rule_results = [evaluate_rule(all_bets, rule, gate) for rule in rules]
    approved_rule_results = [
        result
        for result in rule_results
        if result.get("status") == "approved" and not result.get("disabled")
    ]
    passing_approved_rule_ids = [
        str(result["rule_id"])
        for result in approved_rule_results
        if result.get("passes_launch_gate")
    ]

    evaluation_sources: list[dict[str, Any]] = [
        {
            "source_id": "existing_predictions",
            "source_type": "existing_prediction_output",
            "path": str(db_path),
            "dataset_counts": {
                "totals_prediction_rows": int(len(totals_input)),
                "moneyline_prediction_rows": int(len(moneyline_input)),
                "expanded_totals_bets": int(len(totals_bets)),
                "expanded_moneyline_bets": int(len(moneyline_bets)),
                "expanded_bets": int(len(all_bets)),
            },
            "rule_results": rule_results,
            "passing_approved_rule_ids": passing_approved_rule_ids,
            "publishable_profitable_list_exists": bool(passing_approved_rule_ids),
        }
    ]

    benchmark_results: list[dict[str, Any]] = []
    for path in benchmark_prediction_paths or []:
        artifact_path = Path(path)
        raw_predictions = _read_prediction_artifact(artifact_path)
        benchmark_bets = expand_benchmark_predictions(
            raw_predictions,
            source_id=artifact_path.stem,
        )
        source_rule_results = [evaluate_rule(benchmark_bets, rule, gate) for rule in rules]
        source_passing_approved = [
            str(result["rule_id"])
            for result in source_rule_results
            if result.get("status") == "approved"
            and not result.get("disabled")
            and result.get("passes_launch_gate")
        ]
        source = {
            "source_id": artifact_path.stem,
            "source_type": "candidate_benchmark_output",
            "path": str(artifact_path),
            "dataset_counts": {
                "prediction_rows": int(len(raw_predictions)),
                "expanded_bets": int(len(benchmark_bets)),
            },
            "rule_results": source_rule_results,
            "passing_approved_rule_ids": source_passing_approved,
            "publishable_profitable_list_exists": bool(source_passing_approved),
        }
        benchmark_results.append(source)
        evaluation_sources.append(source)

    grouped_rows: list[dict[str, Any]] = []
    group_columns = ["market", "league", "model_type", "side"]
    if not all_bets.empty and set(group_columns) <= set(all_bets.columns):
        for keys, group in all_bets.groupby(group_columns):
            row = dict(zip(group_columns, keys))
            row.update(summarize_bets(group, gate=gate))
            row.pop("calibration_bins", None)
            grouped_rows.append(row)

    totals_error_summary: list[dict[str, Any]] = []
    if not totals_input.empty:
        for keys, group in totals_input.groupby(["league", "model_type"]):
            league, model_type = keys
            totals_error_summary.append(
                {
                    "league": league,
                    "model_type": model_type,
                    "games": int(len(group)),
                    "model_mae": float(group["model_total_error"].abs().mean()),
                    "sportsbook_line_mae": float(group["market_line_error"].abs().mean()),
                    "model_mae_minus_line_mae": float(
                        group["model_total_error"].abs().mean()
                        - group["market_line_error"].abs().mean()
                    ),
                }
            )

    return {
        "generated_at": pd.Timestamp.utcnow().isoformat(),
        "db_path": str(db_path),
        "rules_path": str(rules_path),
        "release_leagues": list(leagues or []),
        "launch_gate": gate.__dict__,
        "dataset_counts": {
            "totals_prediction_rows": int(len(totals_input)),
            "moneyline_prediction_rows": int(len(moneyline_input)),
            "expanded_totals_bets": int(len(totals_bets)),
            "expanded_moneyline_bets": int(len(moneyline_bets)),
        },
        "rule_results": rule_results,
        "benchmark_results": benchmark_results,
        "evaluation_sources": evaluation_sources,
        "passing_approved_rule_ids": passing_approved_rule_ids,
        "publishable_profitable_list_exists": bool(passing_approved_rule_ids),
        "grouped_quality": grouped_rows,
        "totals_error_summary": totals_error_summary,
    }


def _json_default(value: Any) -> Any:
    if isinstance(value, (np.integer, np.floating)):
        return value.item()
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if pd.isna(value):
        return None
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate prediction quality launch-gate report.")
    parser.add_argument("--db", type=Path, default=DB_PATH, help="SQLite database path.")
    parser.add_argument(
        "--rules",
        type=Path,
        default=Path("config/published_rules.yml"),
        help="Published/candidate rule config.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("reports/prediction_quality/quality_report.json"),
        help="JSON report output path.",
    )
    parser.add_argument(
        "--leagues",
        default=",".join(DEFAULT_RELEASE_LEAGUES),
        help="Comma-separated leagues to include.",
    )
    parser.add_argument(
        "--benchmark-predictions",
        type=Path,
        action="append",
        default=[],
        help="Candidate benchmark validation predictions to evaluate with the same gate.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level))
    leagues = [league.strip().upper() for league in args.leagues.split(",") if league.strip()]
    report = build_quality_report(
        db_path=args.db,
        rules_path=args.rules,
        leagues=leagues,
        benchmark_prediction_paths=args.benchmark_predictions,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, default=_json_default), encoding="utf-8")
    approved = [r for r in report["rule_results"] if r.get("status") == "approved"]
    passing = [r for r in approved if r.get("passes_launch_gate")]
    LOGGER.info(
        "Wrote quality report to %s (%d approved rules, %d passing)",
        args.output,
        len(approved),
        len(passing),
    )


if __name__ == "__main__":
    main()
