"""Train regression models to predict game totals for over/under markets."""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import math
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from typing import Dict, List, Optional

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingRegressor, RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error

from src.db.core import DB_PATH, connect
from src.models.prediction_quality import no_vig_pair
from src.models.totals_calibration import calibration_residual_scale, split_totals_dates


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
          AND g.status = 'final'
          AND julianday(g.start_time_utc) < julianday('now', '-6 hours')
          AND gr.home_score IS NOT NULL
          AND gr.away_score IS NOT NULL
          AND gr.total_close IS NOT NULL
    """
    with connect(DB_PATH) as conn:
        rows = conn.execute(query, (league.upper(),)).fetchall()
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(
        rows,
        columns=[
            "game_id",
            "start_time_utc",
            "home_score",
            "away_score",
            "home_moneyline_close",
            "away_moneyline_close",
            "spread_close",
            "total_close",
        ],
    )
    df["start_time_utc"] = pd.to_datetime(df["start_time_utc"], errors="coerce", utc=True)
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
    df["start_time_utc"] = pd.to_datetime(df["start_time_utc"], errors="coerce", utc=True)
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
        home_ml_series = _select_decimal_column(
            df, ["AvgCH", "AvgH", "MaxCH", "MaxH", "B365CH", "B365H"]
        )
        away_ml_series = _select_decimal_column(
            df, ["AvgCA", "AvgA", "MaxCA", "MaxA", "B365CA", "B365A"]
        )
        if (
            over_series is None
            or under_series is None
            or home_ml_series is None
            or away_ml_series is None
        ):
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


def train_totals_model(
    league: str,
    model_type: str = "gradient_boosting",
    *,
    input_path: Optional[Path] = None,
    models_dir: Path = Path("models"),
    metrics_dir: Path = Path("reports/backtests"),
) -> TotalsTrainingArtifacts:
    input_bytes = input_path.read_bytes() if input_path is not None else None
    df = (
        pd.read_parquet(BytesIO(input_bytes))
        if input_bytes is not None
        else _load_totals_dataframe(league)
    )
    if input_path is not None:
        if "league" not in df:
            raise ValueError("Totals research input must identify its league")
        df = df[df["league"].astype(str).str.upper() == league.upper()].copy()
    df = df.sort_values("start_time_utc").reset_index(drop=True)
    feature_cols = ["total_close", "spread_close", "home_moneyline_close", "away_moneyline_close"]
    df[feature_cols] = df[feature_cols].astype(float)
    df[feature_cols[1:]] = df[feature_cols[1:]].fillna(0.0)
    df["actual_total"] = pd.to_numeric(df["actual_total"], errors="raise")
    if not np.isfinite(df[[*feature_cols, "actual_total"]].to_numpy()).all():
        raise ValueError("Totals training needs finite features and final totals")
    train, calibration, test = split_totals_dates(df)
    for partition in (train, calibration, test):
        partition["start_time_utc"] = pd.to_datetime(partition["start_time_utc"], utc=True)
    X_train, y_train = train[feature_cols], train["actual_total"]
    X_calibration, y_calibration = calibration[feature_cols], calibration["actual_total"]
    X_test, y_test = test[feature_cols], test["actual_total"]

    if model_type == "random_forest":
        model = RandomForestRegressor(
            n_estimators=200, max_depth=10, min_samples_leaf=4, random_state=42, n_jobs=-1
        )
    else:
        # Default to gradient boosting
        model = GradientBoostingRegressor(random_state=42)

    model.fit(X_train, y_train)

    preds = model.predict(X_test)
    rmse = np.sqrt(mean_squared_error(y_test, preds))
    mae = mean_absolute_error(y_test, preds)
    calibration_predictions = model.predict(X_calibration)
    residual_std = calibration_residual_scale(y_calibration.to_numpy(), calibration_predictions)
    training_residual_std = float(np.std(y_train - model.predict(X_train)))

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
        "model_type": model_type,
        "uncertainty_source": "chronological_calibration_rmse",
        "train_end": train["start_time_utc"].max().isoformat(),
        "calibration_start": calibration["start_time_utc"].min().isoformat(),
        "calibration_end": calibration["start_time_utc"].max().isoformat(),
    }

    models_dir.mkdir(parents=True, exist_ok=True)
    model_path = models_dir / f"{league.lower()}_totals_{model_type}.pkl"
    joblib.dump(model_bundle, model_path)

    metrics_dir.mkdir(parents=True, exist_ok=True)
    metrics_path = metrics_dir / f"{league.lower()}_totals_{model_type}_metrics.json"
    metrics = {
        "league": league.upper(),
        "model_type": model_type,
        "samples": len(df),
        "train_samples": len(X_train),
        "calibration_samples": len(X_calibration),
        "test_samples": len(X_test),
        "rmse": float(rmse),
        "mae": float(mae),
        "residual_std": residual_std,
        "uncertainty_source": "chronological_calibration_rmse",
        "training_residual_std_diagnostic": training_residual_std,
        "calibration_residual_mean": float(np.mean(y_calibration - calibration_predictions)),
        "train_end": train["start_time_utc"].max().isoformat(),
        "calibration_start": calibration["start_time_utc"].min().isoformat(),
        "calibration_end": calibration["start_time_utc"].max().isoformat(),
        "test_start": test["start_time_utc"].min().isoformat(),
        "test_end": test["start_time_utc"].max().isoformat(),
        "over_under_accuracy": ou_accuracy,
        "over_under_samples": int(ou_considered),
        "over_under_pushes": int(push_mask.sum()),
    }
    if input_bytes is not None:
        metrics["input_path"] = str(input_path)
        metrics["input_sha256"] = hashlib.sha256(input_bytes).hexdigest()
    scored = test.copy()
    scored["predicted_total"] = preds
    for label, scale in (("calibrated", residual_std), ("training", training_residual_std)):
        if scale <= 0 or not math.isfinite(scale):
            metrics[f"test_{label}_brier"] = None
            continue
        z = (preds - total_lines) / scale
        over_probability = np.array([0.5 * (1 + math.erf(value / math.sqrt(2))) for value in z])
        scored[f"{label}_over_probability"] = over_probability
        if ou_considered:
            truth = (actual_diff[~push_mask] > 0).astype(float)
            metrics[f"test_{label}_brier"] = float(
                np.mean((over_probability[~push_mask] - truth) ** 2)
            )
        else:
            metrics[f"test_{label}_brier"] = None
    if {"over_moneyline", "under_moneyline"} <= set(scored):
        market_probability = np.array(
            [
                no_vig_pair(over, under)[0]
                for over, under in scored[["over_moneyline", "under_moneyline"]].itertuples(
                    index=False, name=None
                )
            ]
        )
        comparable = np.isfinite(market_probability) & ~push_mask
        scored["no_vig_market_over_probability"] = market_probability
        metrics["test_market_comparison_samples"] = int(comparable.sum())
        if comparable.any():
            truth = (actual_diff[comparable] > 0).astype(float)
            metrics["test_market_brier"] = float(
                np.mean((market_probability[comparable] - truth) ** 2)
            )
            metrics["test_calibrated_brier_on_market_quotes"] = float(
                np.mean((scored["calibrated_over_probability"].to_numpy()[comparable] - truth) ** 2)
            )
    validation_path = metrics_dir / f"{league.lower()}_totals_{model_type}_validation.parquet"
    scored.to_parquet(validation_path, index=False)
    metrics["validation_predictions_path"] = str(validation_path)
    metrics_path.write_text(json.dumps(metrics, indent=2, allow_nan=False), encoding="utf-8")

    return TotalsTrainingArtifacts(model_path=model_path, metrics_path=metrics_path)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train totals (over/under) regression models.")
    parser.add_argument(
        "--input", type=Path, help="Optional audited totals rows in Parquet with a league column"
    )
    parser.add_argument("--models-dir", type=Path, default=Path("models"))
    parser.add_argument("--metrics-dir", type=Path, default=Path("reports/backtests"))
    parser.add_argument(
        "--league",
        required=True,
        help="League code (e.g., NBA, NFL, CFB, EPL).",
    )
    parser.add_argument(
        "--model-type",
        default="gradient_boosting",
        choices=["gradient_boosting", "random_forest"],
        help="Type of model to train.",
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
    artifacts = train_totals_model(
        args.league,
        model_type=args.model_type,
        input_path=args.input,
        models_dir=args.models_dir,
        metrics_dir=args.metrics_dir,
    )
    print(f"Totals model saved to {artifacts.model_path}")
    print(f"Metrics written to {artifacts.metrics_path}")


if __name__ == "__main__":
    main()
