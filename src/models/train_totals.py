"""Train gradient boosting models to predict game totals for over/under markets."""

from __future__ import annotations

import argparse
import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error

from src.db.core import DB_PATH, connect


@dataclass
class TotalsTrainingArtifacts:
    model_path: Path
    metrics_path: Path


FORWARD_TEST_MASTER = Path("data/forward_test/predictions_master.parquet")
FOOTBALL_DATA_DIR = Path("data/processed/external/football_data")
FOOTBALL_DATA_LEAGUE_MAP: Dict[str, str] = {
    "EPL": "premier-league",
    "LALIGA": "la-liga",
    "BUNDESLIGA": "bundesliga",
    "SERIEA": "serie-a",
    "LIGUE1": "ligue-1",
}
FOOTBALL_DEFAULT_TOTAL = {
    "EPL": 2.5,
    "LALIGA": 2.5,
    "BUNDESLIGA": 2.75,
    "SERIEA": 2.5,
    "LIGUE1": 2.5,
}


def _load_totals_from_db(league: str) -> pd.DataFrame:
    query = """
        SELECT
            g.game_id,
            g.start_time_utc,
            gr.home_score,
            gr.away_score,
            gr.home_moneyline_close,
            gr.away_moneyline_close,
            gr.spread_close,
            gr.total_close
        FROM games g
        JOIN sports s ON s.sport_id = g.sport_id
        JOIN game_results gr ON gr.game_id = g.game_id
        WHERE s.league = ?
          AND gr.home_score IS NOT NULL
          AND gr.away_score IS NOT NULL
          AND gr.total_close IS NOT NULL
    """
    with connect(DB_PATH) as conn:
        rows = conn.execute(query, (league.upper(),)).fetchall()
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows, columns=[
        "game_id",
        "start_time_utc",
        "home_score",
        "away_score",
        "home_moneyline_close",
        "away_moneyline_close",
        "spread_close",
        "total_close",
    ])
    df["start_time_utc"] = pd.to_datetime(df["start_time_utc"], errors="coerce")
    df = df.dropna(subset=["start_time_utc"])
    df["actual_total"] = df["home_score"] + df["away_score"]
    df = df.dropna(subset=["actual_total", "total_close"])
    return df


def _load_totals_from_forward_test(league: str) -> pd.DataFrame:
    if not FORWARD_TEST_MASTER.exists():
        return pd.DataFrame()

    df = pd.read_parquet(FORWARD_TEST_MASTER)
    if df.empty:
        return pd.DataFrame()

    df = df[df.get("league", "").astype(str).str.upper() == league.upper()].copy()
    if df.empty:
        return pd.DataFrame()

    required_cols = [
        "game_id",
        "commence_time",
        "home_score",
        "away_score",
        "home_moneyline",
        "away_moneyline",
        "spread_line",
        "total_line",
    ]
    for col in required_cols:
        if col not in df.columns:
            return pd.DataFrame()

    df = df.dropna(subset=["commence_time", "home_score", "away_score", "total_line"])
    if df.empty:
        return pd.DataFrame()

    df = df.rename(
        columns={
            "commence_time": "start_time_utc",
            "total_line": "total_close",
            "spread_line": "spread_close",
        }
    )
    df["start_time_utc"] = pd.to_datetime(df["start_time_utc"], errors="coerce")
    df = df.dropna(subset=["start_time_utc"])
    df["actual_total"] = df["home_score"] + df["away_score"]

    # Ensure moneylines exist (fill fallback 0 if missing)
    for col in ["home_moneyline_close", "away_moneyline_close", "spread_close"]:
        if col not in df.columns:
            df[col] = np.nan
    df["home_moneyline_close"] = df["home_moneyline"]
    df["away_moneyline_close"] = df["away_moneyline"]

    keep_cols = [
        "game_id",
        "start_time_utc",
        "home_score",
        "away_score",
        "home_moneyline_close",
        "away_moneyline_close",
        "spread_close",
        "total_close",
        "actual_total",
    ]
    return df[keep_cols]


def _decimal_to_american(value: Optional[float]) -> Optional[float]:
    if value is None or (isinstance(value, float) and (np.isnan(value) or value <= 1.0)):
        return None
    decimal = float(value)
    if decimal >= 2.0:
        return round((decimal - 1.0) * 100, 2)
    return round(-100 / (decimal - 1.0), 2)


def _select_decimal_column(df: pd.DataFrame, candidates: List[str]) -> Optional[pd.Series]:
    for col in candidates:
        if col in df.columns:
            return df[col]
    return None


def _load_totals_from_football_data(league: str) -> pd.DataFrame:
    folder = FOOTBALL_DATA_LEAGUE_MAP.get(league.upper())
    if not folder:
        return pd.DataFrame()
    league_dir = FOOTBALL_DATA_DIR / folder
    if not league_dir.exists():
        return pd.DataFrame()

    frames: List[pd.DataFrame] = []
    for path in sorted(league_dir.glob("*.parquet")):
        df = pd.read_parquet(path)
        if df.empty:
            continue
        date_series = None
        if "match_date" in df.columns:
            date_series = pd.to_datetime(df["match_date"], errors="coerce")
        elif "Date" in df.columns:
            date_series = pd.to_datetime(df["Date"], errors="coerce", dayfirst=True)
        else:
            continue
        if date_series is None:
            continue
        over_series = _select_decimal_column(
            df,
            ["AvgC>2.5", "Avg>2.5", "MaxC>2.5", "Max>2.5", "B365C>2.5", "B365>2.5"],
        )
        under_series = _select_decimal_column(
            df,
            ["AvgC<2.5", "Avg<2.5", "MaxC<2.5", "Max<2.5", "B365C<2.5", "B365<2.5"],
        )
        home_ml_series = _select_decimal_column(df, ["AvgCH", "AvgH", "MaxCH", "MaxH", "B365CH", "B365H"])
        away_ml_series = _select_decimal_column(df, ["AvgCA", "AvgA", "MaxCA", "MaxA", "B365CA", "B365A"])
        if over_series is None or under_series is None or home_ml_series is None or away_ml_series is None:
            continue

        start_times = pd.to_datetime(date_series, errors="coerce")
        if start_times.isna().all():
            continue
        records = pd.DataFrame(
            {
                "start_time_utc": start_times.dt.tz_localize("UTC"),
                "home_score": df.get("FTHG"),
                "away_score": df.get("FTAG"),
                "home_moneyline_close": home_ml_series.apply(_decimal_to_american),
                "away_moneyline_close": away_ml_series.apply(_decimal_to_american),
                "over_decimal": over_series,
                "under_decimal": under_series,
            }
        )
        records["start_time_utc"] = pd.to_datetime(records["start_time_utc"], errors="coerce")
        records = records.dropna(
            subset=["start_time_utc", "home_score", "away_score", "over_decimal", "under_decimal"]
        )
        if records.empty:
            continue
        total_line = FOOTBALL_DEFAULT_TOTAL.get(league.upper(), 2.5)
        records["total_close"] = float(total_line)
        records["spread_close"] = 0.0
        records["actual_total"] = records["home_score"] + records["away_score"]
        records["over_moneyline"] = records["over_decimal"].apply(_decimal_to_american)
        records["under_moneyline"] = records["under_decimal"].apply(_decimal_to_american)
        records.rename(
            columns={
                "over_moneyline": "over_moneyline_close",
                "under_moneyline": "under_moneyline_close",
            },
            inplace=True,
        )
        keep_cols = [
            "start_time_utc",
            "home_score",
            "away_score",
            "home_moneyline_close",
            "away_moneyline_close",
            "spread_close",
            "total_close",
            "actual_total",
        ]
        frames.append(records[keep_cols])

    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    return combined


def _load_totals_dataframe(league: str) -> pd.DataFrame:
    frames: List[pd.DataFrame] = []
    db_df = _load_totals_from_db(league)
    if not db_df.empty:
        frames.append(db_df)

    fd_df = _load_totals_from_football_data(league)
    if not fd_df.empty:
        frames.append(fd_df)

    fallback_df = _load_totals_from_forward_test(league)
    if not fallback_df.empty:
        frames.append(fallback_df)

    if not frames:
        raise RuntimeError(f"No historical totals found for league {league}")

    combined = pd.concat(frames, ignore_index=True)
    combined = combined.dropna(subset=["start_time_utc"])
    combined = combined.sort_values("start_time_utc")
    combined = combined.drop_duplicates(
        subset=["start_time_utc", "home_score", "away_score", "total_close"], keep="last"
    )
    return combined


def train_totals_model(league: str) -> TotalsTrainingArtifacts:
    df = _load_totals_dataframe(league)
    df = df.sort_values("start_time_utc")
    feature_cols = ["total_close", "spread_close", "home_moneyline_close", "away_moneyline_close"]
    X = df[feature_cols].copy().astype(float)
    for col in feature_cols[1:]:
        X[col] = X[col].fillna(0.0)
    y = df["actual_total"].astype(float)

    split_idx = int(len(df) * 0.8)
    if split_idx <= 0 or split_idx >= len(df):
        split_idx = len(df) - 1
    X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
    y_train, y_test = y.iloc[:split_idx], y.iloc[split_idx:]

    model = GradientBoostingRegressor(random_state=42)
    model.fit(X_train, y_train)

    preds = model.predict(X_test)
    rmse = np.sqrt(mean_squared_error(y_test, preds))
    mae = mean_absolute_error(y_test, preds)
    residual_std = float(np.std(y_train - model.predict(X_train)))

    total_lines = X_test["total_close"].to_numpy(copy=True)
    actual_diff = y_test.to_numpy(copy=True) - total_lines
    pred_diff = preds - total_lines
    push_mask = np.isclose(actual_diff, 0.0, atol=1e-6)
    ou_considered = (~push_mask).sum()
    if ou_considered > 0:
        actual_over = actual_diff[~push_mask] > 0
        pred_over = pred_diff[~push_mask] > 0
        ou_accuracy = float(np.mean(actual_over == pred_over))
    else:
        ou_accuracy = None

    model_bundle = {
        "regressor": model,
        "residual_std": residual_std,
        "feature_names": feature_cols,
    }

    models_dir = Path("models")
    models_dir.mkdir(exist_ok=True)
    model_path = models_dir / f"{league.lower()}_totals_gradient_boosting.pkl"
    joblib.dump(model_bundle, model_path)

    metrics_dir = Path("reports") / "backtests"
    metrics_dir.mkdir(parents=True, exist_ok=True)
    metrics_path = metrics_dir / f"{league.lower()}_totals_metrics.json"
    metrics = {
        "league": league.upper(),
        "samples": len(df),
        "train_samples": len(X_train),
        "test_samples": len(X_test),
        "rmse": float(rmse),
        "mae": float(mae),
        "residual_std": residual_std,
        "over_under_accuracy": ou_accuracy,
        "over_under_samples": int(ou_considered),
        "over_under_pushes": int(push_mask.sum()),
    }
    metrics_path.write_text(json.dumps(metrics, indent=2), encoding="utf-8")

    return TotalsTrainingArtifacts(model_path=model_path, metrics_path=metrics_path)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train totals (over/under) regression models.")
    parser.add_argument(
        "--league",
        required=True,
        help="League code (e.g., NBA, NFL, CFB, EPL).",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    logging_config = getattr(__import__("logging"), "basicConfig")
    logging_config(level=getattr(__import__("logging"), args.log_level))
    artifacts = train_totals_model(args.league)
    print(f"Totals model saved to {artifacts.model_path}")
    print(f"Metrics written to {artifacts.metrics_path}")


if __name__ == "__main__":
    main()
