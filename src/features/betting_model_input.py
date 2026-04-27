"""Canonical betting model input builders.

These builders create leakage-safe training/evaluation rows from raw odds,
games, and results. They are intentionally separate from current model
predictions so new models can be trained against market snapshots directly.
"""

from __future__ import annotations

import argparse
import json
import logging
import sqlite3
from pathlib import Path
from typing import Any, Iterable, Optional

import numpy as np
import pandas as pd

from src.data.config import RAW_DATA_DIR
from src.db.core import DB_PATH
from src.features import soccer_features

LOGGER = logging.getLogger(__name__)

DEFAULT_RELEASE_LEAGUES = ("NBA", "NHL", "EPL", "LALIGA", "BUNDESLIGA", "SERIEA", "LIGUE1")
SOCCER_LEAGUES = {"EPL", "LALIGA", "BUNDESLIGA", "SERIEA", "LIGUE1"}
GENERIC_ROLLING_WINDOWS = (3, 5, 10)
NBA_ROLLING_WINDOWS = (5, 10)
BOOK_PRIORITY = (
    "draftkings",
    "fanduel",
    "betmgm",
    "caesars",
    "betrivers",
    "pointsbet",
    "pinnacle",
    "bovada",
)

NBA_ROLLING_SOURCE_COLUMNS = tuple(
    f"rolling_{metric}_{window}"
    for window in NBA_ROLLING_WINDOWS
    for metric in ("win_pct", "point_diff", "off_rating", "def_rating", "net_rating", "pace")
)

INJURY_SOURCE_COLUMNS = (
    "injuries_out",
    "injuries_doubtful",
    "injuries_questionable",
    "injuries_total",
    "injuries_skill_out",
)

SOCCER_WAREHOUSE_ALIAS_COLUMNS = {
    "warehouse_goals_for_l5": "score_for_l5",
    "warehouse_goals_against_l5": "score_against_l5",
    "warehouse_total_goals_l5": "game_total_l5",
    "warehouse_win_pct_l5": "win_pct_l5",
    "warehouse_same_venue_goals_for_l5": "same_venue_score_for_l5",
    "warehouse_same_venue_goals_against_l5": "same_venue_score_against_l5",
}

SOCCER_WAREHOUSE_SOURCE_COLUMNS = tuple(SOCCER_WAREHOUSE_ALIAS_COLUMNS.keys())

SOCCER_FOOTBALL_DATA_COLUMNS = (
    "fd_goals_for_l5",
    "fd_goals_against_l5",
    "fd_shots_for_l5",
    "fd_shots_against_l5",
    "fd_shots_on_target_for_l5",
    "fd_shots_on_target_against_l5",
    "fd_same_venue_goals_for_l5",
    "fd_same_venue_goals_against_l5",
    "fd_same_venue_shots_for_l5",
    "fd_same_venue_shots_against_l5",
)

SOCCER_UNDERSTAT_TEAM_COLUMNS = (
    "ust_team_xg_avg_l5",
    "ust_team_xga_avg_l5",
    "ust_team_ppda_att_l3",
    "ust_team_ppda_allowed_att_l3",
    "ust_team_deep_entries_l3",
    "ust_team_deep_allowed_l3",
    "ust_team_goals_for_avg_l5",
    "ust_team_goals_against_avg_l5",
    "ust_team_xpts_avg_l5",
)

SOCCER_UNDERSTAT_SHOT_COLUMNS = (
    "ust_team_shot_open_play_share_l5",
    "ust_team_shot_set_piece_share_l5",
    "ust_team_avg_shot_distance_l5",
)

SOCCER_UNDERSTAT_LINEUP_COLUMNS = (
    "ust_xi_prior_minutes_avg",
    "ust_xi_prior_xg_per90_avg",
    "ust_xi_prior_xa_per90_avg",
    "ust_xi_prior_shots_per90_avg",
    "ust_xi_returning_starters_prev_match",
    "ust_xi_returning_starters_last3",
)

SOCCER_SOURCE_COLUMNS = (
    *SOCCER_WAREHOUSE_SOURCE_COLUMNS,
    *SOCCER_FOOTBALL_DATA_COLUMNS,
    *SOCCER_UNDERSTAT_TEAM_COLUMNS,
    *SOCCER_UNDERSTAT_SHOT_COLUMNS,
    *SOCCER_UNDERSTAT_LINEUP_COLUMNS,
)

FEATURE_GROUPS: dict[str, tuple[str, ...]] = {
    "market": (
        "hours_before_start",
        "market_hold",
        "line_movement",
        "home_moneyline_movement",
        "away_moneyline_movement",
        "over_moneyline_movement",
        "under_moneyline_movement",
    ),
    "team_form": tuple(
        f"{side}_{metric}_l{window}"
        for side in ("home", "away")
        for window in GENERIC_ROLLING_WINDOWS
        for metric in ("score_for", "score_against", "game_total", "win_pct")
    ),
    "rest": tuple(
        f"{side}_{metric}" for side in ("home", "away") for metric in ("rest_days", "back_to_back")
    )
    + ("rest_diff",),
    "home_away_splits": tuple(
        f"{side}_same_venue_{metric}_l5"
        for side in ("home", "away")
        for metric in ("score_for", "score_against", "win_pct")
    ),
    "nba_advanced": tuple(
        f"{side}_nba_{column}" for side in ("home", "away") for column in NBA_ROLLING_SOURCE_COLUMNS
    ),
    "availability": tuple(
        f"{side}_{column}" for side in ("home", "away") for column in INJURY_SOURCE_COLUMNS
    ),
    "soccer": tuple(
        f"{side}_soccer_{column}" for side in ("home", "away") for column in SOCCER_SOURCE_COLUMNS
    ),
    "soccer_warehouse": tuple(
        f"{side}_soccer_{column}"
        for side in ("home", "away")
        for column in SOCCER_WAREHOUSE_SOURCE_COLUMNS
    ),
    "soccer_football_data": tuple(
        f"{side}_soccer_{column}"
        for side in ("home", "away")
        for column in SOCCER_FOOTBALL_DATA_COLUMNS
    ),
    "soccer_understat_team": tuple(
        f"{side}_soccer_{column}"
        for side in ("home", "away")
        for column in SOCCER_UNDERSTAT_TEAM_COLUMNS
    ),
    "soccer_understat_shots": tuple(
        f"{side}_soccer_{column}"
        for side in ("home", "away")
        for column in SOCCER_UNDERSTAT_SHOT_COLUMNS
    ),
    "soccer_understat_lineup": tuple(
        f"{side}_soccer_{column}"
        for side in ("home", "away")
        for column in SOCCER_UNDERSTAT_LINEUP_COLUMNS
    ),
}

GAME_FEATURE_CONTRACT_COLUMNS = tuple(
    dict.fromkeys(
        [
            *(column for columns in FEATURE_GROUPS.values() for column in columns),
            *(
                f"{metric}_l{window}_diff"
                for window in GENERIC_ROLLING_WINDOWS
                for metric in ("score_for", "score_against", "game_total", "win_pct")
            ),
            "same_venue_score_for_l5_diff",
            "same_venue_score_against_l5_diff",
            *(f"nba_{metric}_diff" for metric in NBA_ROLLING_SOURCE_COLUMNS),
            *(f"soccer_{metric}_diff" for metric in SOCCER_SOURCE_COLUMNS),
        ]
    )
)

MONEYLINE_SIDE_FEATURE_CONTRACT_COLUMNS = tuple(
    dict.fromkeys(
        [
            *(
                f"team_{metric}"
                for window in GENERIC_ROLLING_WINDOWS
                for metric in (
                    f"score_for_l{window}",
                    f"score_against_l{window}",
                    f"game_total_l{window}",
                    f"win_pct_l{window}",
                    f"games_l{window}",
                )
            ),
            *(
                f"opponent_{metric}"
                for window in GENERIC_ROLLING_WINDOWS
                for metric in (
                    f"score_for_l{window}",
                    f"score_against_l{window}",
                    f"game_total_l{window}",
                    f"win_pct_l{window}",
                    f"games_l{window}",
                )
            ),
            "team_rest_days",
            "opponent_rest_days",
            "team_back_to_back",
            "opponent_back_to_back",
            "rest_diff",
            "team_same_venue_score_for_l5",
            "opponent_same_venue_score_for_l5",
            "team_same_venue_score_against_l5",
            "opponent_same_venue_score_against_l5",
            "team_same_venue_win_pct_l5",
            "opponent_same_venue_win_pct_l5",
            *(
                f"{metric}_l{window}_diff"
                for window in GENERIC_ROLLING_WINDOWS
                for metric in ("score_for", "score_against", "game_total", "win_pct")
            ),
            "same_venue_score_for_l5_diff",
            "same_venue_score_against_l5_diff",
            *(
                value
                for metric in INJURY_SOURCE_COLUMNS
                for value in (f"team_{metric}", f"opponent_{metric}", f"{metric}_diff")
            ),
            *(
                value
                for metric in NBA_ROLLING_SOURCE_COLUMNS
                for value in (f"team_nba_{metric}", f"opponent_nba_{metric}", f"nba_{metric}_diff")
            ),
            *(
                value
                for metric in SOCCER_SOURCE_COLUMNS
                for value in (
                    f"team_soccer_{metric}",
                    f"opponent_soccer_{metric}",
                    f"soccer_{metric}_diff",
                )
            ),
        ]
    )
)

MODEL_FEATURE_CONTRACTS = {
    "totals": GAME_FEATURE_CONTRACT_COLUMNS,
    "moneyline": MONEYLINE_SIDE_FEATURE_CONTRACT_COLUMNS,
}


def get_model_feature_contract(market: str) -> tuple[str, ...]:
    """Return the stable feature columns expected for a canonical model input."""
    key = market.lower()
    if key not in MODEL_FEATURE_CONTRACTS:
        raise ValueError(f"Unsupported model feature contract: {market}")
    return MODEL_FEATURE_CONTRACTS[key]


def _book_rank(book_name: Any) -> int:
    normalized = str(book_name or "").strip().lower()
    try:
        return BOOK_PRIORITY.index(normalized)
    except ValueError:
        return len(BOOK_PRIORITY)


def _american_to_decimal(value: float | int | None) -> Optional[float]:
    if value is None or pd.isna(value):
        return None
    moneyline = float(value)
    if moneyline == 0:
        return None
    if moneyline > 0:
        return 1.0 + moneyline / 100.0
    return 1.0 + 100.0 / abs(moneyline)


def _implied_prob(value: float | int | None) -> Optional[float]:
    decimal = _american_to_decimal(value)
    if decimal is None:
        return None
    return 1.0 / decimal


def _no_vig_pair(first_moneyline: float, second_moneyline: float) -> tuple[float, float]:
    first = _implied_prob(first_moneyline)
    second = _implied_prob(second_moneyline)
    if first is None or second is None or first + second <= 0:
        return np.nan, np.nan
    total = first + second
    return first / total, second / total


def _league_filter(leagues: Optional[Iterable[str]]) -> tuple[str, list[str]]:
    if not leagues:
        return "", []
    normalized = [league.upper() for league in leagues]
    placeholders = ",".join("?" for _ in normalized)
    return f" AND UPPER(s.league) IN ({placeholders})", normalized


def _read_sql(db_path: Path, query: str, params: Iterable[Any] = ()) -> pd.DataFrame:
    with sqlite3.connect(str(db_path)) as conn:
        return pd.read_sql_query(query, conn, params=list(params))


def _table_exists(db_path: Path, table_name: str) -> bool:
    with sqlite3.connect(str(db_path)) as conn:
        row = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type IN ('table', 'view') AND name = ?",
            (table_name,),
        ).fetchone()
    return row is not None


def _ensure_columns(
    df: pd.DataFrame, columns: Iterable[str], default: Any = np.nan
) -> pd.DataFrame:
    updated = df.copy()
    missing = [column for column in columns if column not in updated.columns]
    if missing:
        updated = pd.concat(
            [updated, pd.DataFrame({column: default for column in missing}, index=updated.index)],
            axis=1,
        )
    return updated


def _latest_source_parquet(league: str, source_subdir: str, filename: str) -> pd.DataFrame:
    base = RAW_DATA_DIR / "sources" / league.lower() / source_subdir
    if not base.exists():
        return pd.DataFrame()
    candidates = sorted(path for path in base.iterdir() if path.is_dir())
    for directory in reversed(candidates):
        path = directory / filename
        if path.exists():
            try:
                return pd.read_parquet(path)
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("Unable to read %s: %s", path, exc)
                return pd.DataFrame()
    return pd.DataFrame()


def _normalize_datetime_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["start_time_utc"] = pd.to_datetime(
        df["start_time_utc"],
        utc=True,
        errors="coerce",
        format="mixed",
    )
    df["snapshot_time_utc"] = pd.to_datetime(
        df["snapshot_time_utc"],
        utc=True,
        errors="coerce",
        format="mixed",
    )
    return df[df["start_time_utc"].notna() & df["snapshot_time_utc"].notna()].copy()


def _add_market_common_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["hours_before_start"] = (
        df["start_time_utc"] - df["snapshot_time_utc"]
    ).dt.total_seconds() / 3600.0
    df["book_rank"] = df["book"].map(_book_rank)
    return df


def _latest_per_game(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    return (
        df.sort_values(
            ["snapshot_time_utc", "book_rank", "book"],
            ascending=[False, True, True],
        )
        .drop_duplicates("game_id", keep="first")
        .copy()
    )


def _attach_line_dispersion(latest_by_book: pd.DataFrame, selected: pd.DataFrame) -> pd.DataFrame:
    if latest_by_book.empty or selected.empty:
        selected["book_line_count"] = 0
        selected["line_std"] = np.nan
        return selected
    dispersion = (
        latest_by_book.groupby("game_id")
        .agg(book_line_count=("line", "count"), line_std=("line", "std"))
        .reset_index()
    )
    return selected.merge(dispersion, on="game_id", how="left")


def _load_team_result_history(db_path: Path, leagues: Optional[Iterable[str]]) -> pd.DataFrame:
    league_sql, params = _league_filter(leagues)
    query = f"""
        SELECT
            g.game_id,
            s.league,
            g.start_time_utc,
            g.home_team_id,
            g.away_team_id,
            gr.home_score,
            gr.away_score
        FROM games g
        JOIN sports s ON g.sport_id = s.sport_id
        JOIN game_results gr ON g.game_id = gr.game_id
        WHERE gr.home_score IS NOT NULL
          AND gr.away_score IS NOT NULL
          {league_sql}
    """
    history = _read_sql(db_path, query, params)
    if history.empty:
        return history
    history["start_time_utc"] = pd.to_datetime(history["start_time_utc"], utc=True, errors="coerce")
    for column in ["home_score", "away_score", "home_team_id", "away_team_id"]:
        history[column] = pd.to_numeric(history[column], errors="coerce")
    return history.dropna(
        subset=["start_time_utc", "home_team_id", "away_team_id", "home_score", "away_score"]
    )


def _empty_team_prior_summary(prefix: str) -> dict[str, float]:
    summary: dict[str, float] = {}
    for window in GENERIC_ROLLING_WINDOWS:
        summary.update(
            {
                f"{prefix}_score_for_l{window}": np.nan,
                f"{prefix}_score_against_l{window}": np.nan,
                f"{prefix}_game_total_l{window}": np.nan,
                f"{prefix}_win_pct_l{window}": np.nan,
                f"{prefix}_games_l{window}": 0.0,
            }
        )
    summary.update(
        {
            f"{prefix}_same_venue_score_for_l5": np.nan,
            f"{prefix}_same_venue_score_against_l5": np.nan,
            f"{prefix}_same_venue_win_pct_l5": np.nan,
            f"{prefix}_same_venue_games_l5": 0.0,
            f"{prefix}_rest_days": np.nan,
            f"{prefix}_back_to_back": 0.0,
        }
    )
    return summary


def _team_prior_summary(
    history: pd.DataFrame,
    team_id: Any,
    before: pd.Timestamp,
    prefix: str,
    *,
    target_is_home: bool,
) -> dict[str, float]:
    if history.empty or pd.isna(team_id) or pd.isna(before):
        return _empty_team_prior_summary(prefix)

    team_value = float(team_id)
    before_games = history[history["start_time_utc"] < before]
    home_games = before_games[before_games["home_team_id"] == team_value][
        ["start_time_utc", "home_score", "away_score"]
    ].rename(columns={"home_score": "score_for", "away_score": "score_against"})
    home_games["is_home_game"] = True
    away_games = before_games[before_games["away_team_id"] == team_value][
        ["start_time_utc", "away_score", "home_score"]
    ].rename(columns={"away_score": "score_for", "home_score": "score_against"})
    away_games["is_home_game"] = False
    team_games = pd.concat([home_games, away_games], ignore_index=True).sort_values(
        "start_time_utc"
    )
    if team_games.empty:
        return _empty_team_prior_summary(prefix)

    team_games = team_games.copy()
    team_games["game_total"] = team_games["score_for"] + team_games["score_against"]
    team_games["win"] = (team_games["score_for"] > team_games["score_against"]).astype(float)
    rest_days = (before - team_games.iloc[-1]["start_time_utc"]).total_seconds() / 86400.0
    summary = _empty_team_prior_summary(prefix)
    for window in GENERIC_ROLLING_WINDOWS:
        recent = team_games.tail(window)
        summary[f"{prefix}_score_for_l{window}"] = float(recent["score_for"].mean())
        summary[f"{prefix}_score_against_l{window}"] = float(recent["score_against"].mean())
        summary[f"{prefix}_game_total_l{window}"] = float(recent["game_total"].mean())
        summary[f"{prefix}_win_pct_l{window}"] = float(recent["win"].mean())
        summary[f"{prefix}_games_l{window}"] = float(len(recent))

    same_venue = team_games[team_games["is_home_game"] == target_is_home].tail(5)
    if not same_venue.empty:
        summary[f"{prefix}_same_venue_score_for_l5"] = float(same_venue["score_for"].mean())
        summary[f"{prefix}_same_venue_score_against_l5"] = float(same_venue["score_against"].mean())
        summary[f"{prefix}_same_venue_win_pct_l5"] = float(same_venue["win"].mean())
        summary[f"{prefix}_same_venue_games_l5"] = float(len(same_venue))
    summary[f"{prefix}_rest_days"] = float(rest_days)
    summary[f"{prefix}_back_to_back"] = 1.0 if rest_days <= 1.5 else 0.0
    return summary


def _attach_team_history_features(
    selected: pd.DataFrame,
    db_path: Path,
    leagues: Optional[Iterable[str]],
) -> pd.DataFrame:
    if (
        selected.empty
        or "home_team_id" not in selected.columns
        or "away_team_id" not in selected.columns
    ):
        return selected

    history = _load_team_result_history(db_path, leagues)
    if history.empty:
        return selected

    feature_rows = []
    game_rows = selected[
        ["game_id", "home_team_id", "away_team_id", "start_time_utc"]
    ].drop_duplicates("game_id")
    for _, row in game_rows.iterrows():
        home = _team_prior_summary(
            history,
            row["home_team_id"],
            row["start_time_utc"],
            "home",
            target_is_home=True,
        )
        away = _team_prior_summary(
            history,
            row["away_team_id"],
            row["start_time_utc"],
            "away",
            target_is_home=False,
        )
        merged = {
            "game_id": row["game_id"],
            **home,
            **away,
        }
        if pd.notna(merged["home_rest_days"]) and pd.notna(merged["away_rest_days"]):
            merged["rest_diff"] = merged["home_rest_days"] - merged["away_rest_days"]
        else:
            merged["rest_diff"] = np.nan
        for window in GENERIC_ROLLING_WINDOWS:
            merged[f"score_for_l{window}_diff"] = (
                merged[f"home_score_for_l{window}"] - merged[f"away_score_for_l{window}"]
            )
            merged[f"score_against_l{window}_diff"] = (
                merged[f"home_score_against_l{window}"] - merged[f"away_score_against_l{window}"]
            )
            merged[f"game_total_l{window}_diff"] = (
                merged[f"home_game_total_l{window}"] - merged[f"away_game_total_l{window}"]
            )
            merged[f"win_pct_l{window}_diff"] = (
                merged[f"home_win_pct_l{window}"] - merged[f"away_win_pct_l{window}"]
            )
        merged["same_venue_score_for_l5_diff"] = (
            merged["home_same_venue_score_for_l5"] - merged["away_same_venue_score_for_l5"]
        )
        merged["same_venue_score_against_l5_diff"] = (
            merged["home_same_venue_score_against_l5"] - merged["away_same_venue_score_against_l5"]
        )
        feature_rows.append(merged)

    return selected.merge(pd.DataFrame(feature_rows), on="game_id", how="left")


def _attach_best_totals_prices(
    latest_by_book: pd.DataFrame, selected: pd.DataFrame
) -> pd.DataFrame:
    if latest_by_book.empty or selected.empty:
        return selected
    selected_lines = (
        selected[["game_id", "line"]]
        .drop_duplicates("game_id")
        .rename(columns={"line": "selected_line"})
    )
    scoped = latest_by_book.merge(selected_lines, on="game_id", how="inner")
    scoped = scoped[np.isclose(scoped["line"], scoped["selected_line"], equal_nan=False)].copy()
    if scoped.empty:
        scoped = latest_by_book.copy()

    rows = []
    for game_id, group in scoped.groupby("game_id"):
        row: dict[str, Any] = {"game_id": game_id}
        for side in ("over", "under"):
            price_col = f"{side}_moneyline"
            priced = group.dropna(subset=[price_col])
            if priced.empty:
                row[f"best_{side}_moneyline"] = np.nan
                row[f"best_{side}_book"] = None
                continue
            best = priced.sort_values(
                [price_col, "book_rank", "book"], ascending=[False, True, True]
            ).iloc[0]
            row[f"best_{side}_moneyline"] = best[price_col]
            row[f"best_{side}_book"] = best["book"]
        rows.append(row)
    return selected.merge(pd.DataFrame(rows), on="game_id", how="left")


def _attach_best_moneyline_prices(
    latest_by_book: pd.DataFrame, selected: pd.DataFrame
) -> pd.DataFrame:
    if latest_by_book.empty or selected.empty:
        return selected
    rows = []
    for game_id, group in latest_by_book.groupby("game_id"):
        row: dict[str, Any] = {"game_id": game_id}
        for side in ("home", "away"):
            price_col = f"{side}_moneyline"
            priced = group.dropna(subset=[price_col])
            if priced.empty:
                row[f"best_{side}_moneyline"] = np.nan
                row[f"best_{side}_book"] = None
                continue
            best = priced.sort_values(
                [price_col, "book_rank", "book"], ascending=[False, True, True]
            ).iloc[0]
            row[f"best_{side}_moneyline"] = best[price_col]
            row[f"best_{side}_book"] = best["book"]
        rows.append(row)
    return selected.merge(pd.DataFrame(rows), on="game_id", how="left")


def _attach_prefixed_team_features(
    selected: pd.DataFrame,
    features: pd.DataFrame,
    *,
    feature_columns: Iterable[str],
    output_namespace: str,
    team_key: str,
) -> pd.DataFrame:
    if selected.empty:
        return selected
    source_columns = [column for column in feature_columns if column in features.columns]
    output_columns = [
        f"{side}_{output_namespace}_{column}"
        for side in ("home", "away")
        for column in feature_columns
    ]
    if features.empty or not source_columns or team_key not in features.columns:
        return _ensure_columns(selected, output_columns)

    updated = selected.copy()
    for side in ("home", "away"):
        if team_key == "team_id":
            left_key = f"{side}_team_id"
            right_key = "team_id"
        else:
            left_key = f"{side}_team_code"
            right_key = team_key
        if left_key not in updated.columns:
            continue
        side_features = features[["game_id", right_key, *source_columns]].drop_duplicates(
            ["game_id", right_key],
            keep="last",
        )
        rename_map = {column: f"{side}_{output_namespace}_{column}" for column in source_columns}
        side_features = side_features.rename(columns=rename_map)
        updated = updated.merge(
            side_features,
            left_on=["game_id", left_key],
            right_on=["game_id", right_key],
            how="left",
        )
        if right_key != left_key and right_key in updated.columns:
            updated = updated.drop(columns=[right_key])

    updated = _ensure_columns(updated, output_columns)
    for column in feature_columns:
        home_col = f"home_{output_namespace}_{column}"
        away_col = f"away_{output_namespace}_{column}"
        if home_col in updated.columns and away_col in updated.columns:
            updated[f"{output_namespace}_{column}_diff"] = updated[home_col] - updated[away_col]
    return updated


def _load_nba_rolling_features_from_team_features(db_path: Path) -> pd.DataFrame:
    if not _table_exists(db_path, "team_features"):
        return pd.DataFrame()

    query = """
        SELECT game_id, team_id, feature_json
        FROM team_features
        WHERE feature_set IN ('game_stats', 'rolling_metrics', 'nba_rolling_metrics')
    """
    raw = _read_sql(db_path, query)
    if raw.empty:
        return raw

    records: list[dict[str, Any]] = []
    for row in raw.itertuples(index=False):
        try:
            payload = json.loads(row.feature_json or "{}")
        except json.JSONDecodeError:
            continue
        record: dict[str, Any] = {"game_id": row.game_id, "team_id": row.team_id}
        for column in NBA_ROLLING_SOURCE_COLUMNS:
            record[column] = payload.get(column)
        records.append(record)
    if not records:
        return pd.DataFrame()
    features = pd.DataFrame.from_records(records)
    for column in NBA_ROLLING_SOURCE_COLUMNS:
        if column in features.columns:
            features[column] = pd.to_numeric(features[column], errors="coerce")
    return features


def _load_nba_rolling_features_from_parquet() -> pd.DataFrame:
    raw = _latest_source_parquet("nba", "rolling_metrics", "rolling_metrics.parquet")
    if raw.empty:
        return raw
    if "team" not in raw.columns or "game_date" not in raw.columns:
        return pd.DataFrame()
    source_columns = [column for column in NBA_ROLLING_SOURCE_COLUMNS if column in raw.columns]
    if not source_columns:
        return pd.DataFrame()
    optional_keys = ["game_id"] if "game_id" in raw.columns else []
    features = raw[[*optional_keys, "game_date", "team", *source_columns]].copy()
    features["game_date"] = pd.to_datetime(features["game_date"], errors="coerce").dt.date
    features["team"] = features["team"].astype(str).str.upper()
    for column in source_columns:
        features[column] = pd.to_numeric(features[column], errors="coerce")
    return features


def _fill_nba_parquet_features_by_date(
    updated: pd.DataFrame,
    selected: pd.DataFrame,
    features: pd.DataFrame,
) -> pd.DataFrame:
    if features.empty or "game_date" not in features.columns:
        return updated
    source_columns = [column for column in NBA_ROLLING_SOURCE_COLUMNS if column in features.columns]
    if not source_columns:
        return updated

    filled = updated.copy()
    feature_subset = features[["game_date", "team", *source_columns]].dropna(
        subset=["game_date", "team"]
    )
    for side in ("home", "away"):
        side_key = f"{side}_team_code"
        if side_key not in selected.columns:
            continue
        base_lookup = selected[["game_id", "start_time_utc", side_key]].copy()
        base_lookup["start_time_utc"] = pd.to_datetime(
            base_lookup["start_time_utc"], utc=True, errors="coerce"
        )
        base_lookup[side_key] = base_lookup[side_key].astype(str).str.upper()
        for offset_hours in (0, 12):
            lookup = base_lookup.copy()
            lookup["game_date"] = (
                lookup["start_time_utc"] - pd.Timedelta(hours=offset_hours)
            ).dt.date
            merged = lookup.merge(
                feature_subset,
                left_on=["game_date", side_key],
                right_on=["game_date", "team"],
                how="left",
            )
            merged = merged.drop_duplicates("game_id", keep="last").set_index("game_id")
            for column in source_columns:
                target = f"{side}_nba_{column}"
                if target not in filled.columns:
                    filled[target] = np.nan
                filled[target] = filled[target].fillna(filled["game_id"].map(merged[column]))

    for column in source_columns:
        home_col = f"home_nba_{column}"
        away_col = f"away_nba_{column}"
        if home_col in filled.columns and away_col in filled.columns:
            filled[f"nba_{column}_diff"] = filled[home_col] - filled[away_col]
    return filled


def _attach_nba_advanced_features(selected: pd.DataFrame, db_path: Path) -> pd.DataFrame:
    if selected.empty:
        return selected
    updated = selected.copy()
    nba_mask = updated["league"].astype(str).str.upper() == "NBA"
    if not nba_mask.any():
        return _ensure_columns(
            updated,
            [
                f"{side}_nba_{column}"
                for side in ("home", "away")
                for column in NBA_ROLLING_SOURCE_COLUMNS
            ],
        )

    db_features = _load_nba_rolling_features_from_team_features(db_path)
    if not db_features.empty:
        updated = _attach_prefixed_team_features(
            updated,
            db_features,
            feature_columns=NBA_ROLLING_SOURCE_COLUMNS,
            output_namespace="nba",
            team_key="team_id",
        )

    parquet_features = _load_nba_rolling_features_from_parquet()
    if not parquet_features.empty:
        if "game_id" in parquet_features.columns:
            parquet_attached = _attach_prefixed_team_features(
                selected,
                parquet_features,
                feature_columns=NBA_ROLLING_SOURCE_COLUMNS,
                output_namespace="nba",
                team_key="team",
            )
            for column in parquet_attached.columns:
                if column not in updated.columns:
                    updated[column] = parquet_attached[column]
                elif column.startswith(("home_nba_", "away_nba_", "nba_")):
                    updated[column] = updated[column].fillna(parquet_attached[column])
        updated = _fill_nba_parquet_features_by_date(updated, selected, parquet_features)

    nba_columns = [
        column
        for column in updated.columns
        if column.startswith(("home_nba_", "away_nba_", "nba_rolling_"))
    ]
    if nba_columns:
        updated.loc[~nba_mask, nba_columns] = np.nan

    return _attach_prefixed_team_features(
        updated,
        pd.DataFrame(),
        feature_columns=NBA_ROLLING_SOURCE_COLUMNS,
        output_namespace="nba",
        team_key="team_id",
    )


def _status_category(status: Any, practice_status: Any = None) -> str:
    text = f"{status or ''} {practice_status or ''}".lower()
    if any(keyword in text for keyword in ("injured reserve", "out", "suspended", "inactive")):
        return "out"
    if "doubt" in text:
        return "doubtful"
    if any(keyword in text for keyword in ("question", "probable", "limited", "did not", "dnp")):
        return "questionable"
    return "other"


def _load_injury_reports(db_path: Path, leagues: Optional[Iterable[str]]) -> pd.DataFrame:
    if not _table_exists(db_path, "injury_reports"):
        return pd.DataFrame()
    league_sql, params = _league_filter(leagues)
    league_sql = league_sql.replace("s.league", "league")
    query = f"""
        SELECT
            UPPER(league) AS league,
            team_id,
            UPPER(team_code) AS team_code,
            player_name,
            position,
            status,
            practice_status,
            report_date,
            game_date
        FROM injury_reports
        WHERE 1 = 1
          {league_sql}
    """
    reports = _read_sql(db_path, query, params)
    if reports.empty:
        return reports
    reports["report_date"] = pd.to_datetime(reports["report_date"], utc=True, errors="coerce")
    reports["game_date"] = pd.to_datetime(reports["game_date"], utc=True, errors="coerce")
    reports["team_id"] = pd.to_numeric(reports["team_id"], errors="coerce")
    reports["status_category"] = reports.apply(
        lambda row: _status_category(row.get("status"), row.get("practice_status")),
        axis=1,
    )
    reports["position"] = reports["position"].fillna("").astype(str).str.upper()
    return reports.dropna(subset=["report_date"])


def _summarize_team_injuries(
    reports: pd.DataFrame,
    *,
    league: str,
    team_id: Any,
    team_code: Any,
    start_time: pd.Timestamp,
) -> dict[str, float]:
    empty = {column: np.nan for column in INJURY_SOURCE_COLUMNS}
    if reports.empty or pd.isna(start_time):
        return empty

    team_reports = reports[reports["league"] == str(league).upper()].copy()
    if pd.notna(team_id):
        team_reports = team_reports[
            (team_reports["team_id"] == float(team_id))
            | (team_reports["team_code"] == str(team_code or "").upper())
        ]
    else:
        team_reports = team_reports[team_reports["team_code"] == str(team_code or "").upper()]
    team_reports = team_reports[team_reports["report_date"] <= start_time]
    if team_reports.empty:
        return empty

    game_date = start_time.date()
    same_game = team_reports[team_reports["game_date"].dt.date == game_date]
    if not same_game.empty:
        team_reports = same_game
    else:
        min_report_time = start_time - pd.Timedelta(days=7)
        team_reports = team_reports[team_reports["report_date"] >= min_report_time]
    if team_reports.empty:
        return empty

    latest = (
        team_reports.sort_values("report_date").drop_duplicates(["player_name"], keep="last").copy()
    )
    severe = latest["status_category"].isin(["out", "doubtful"])
    skill_positions = {"G", "F", "C", "G-F", "F-G", "F-C", "C-F", "G-C"}
    return {
        "injuries_out": float((latest["status_category"] == "out").sum()),
        "injuries_doubtful": float((latest["status_category"] == "doubtful").sum()),
        "injuries_questionable": float((latest["status_category"] == "questionable").sum()),
        "injuries_total": float((latest["status_category"] != "other").sum()),
        "injuries_skill_out": float((severe & latest["position"].isin(skill_positions)).sum()),
    }


def _attach_injury_features(
    selected: pd.DataFrame,
    db_path: Path,
    leagues: Optional[Iterable[str]],
) -> pd.DataFrame:
    output_columns = [
        f"{side}_{column}" for side in ("home", "away") for column in INJURY_SOURCE_COLUMNS
    ]
    if selected.empty:
        return selected
    reports = _load_injury_reports(db_path, leagues)
    if reports.empty:
        return _ensure_columns(selected, output_columns)

    rows: list[dict[str, Any]] = []
    game_rows = selected[
        [
            "game_id",
            "league",
            "start_time_utc",
            "home_team_id",
            "away_team_id",
            "home_team_code",
            "away_team_code",
        ]
    ].drop_duplicates("game_id")
    for row in game_rows.itertuples(index=False):
        record: dict[str, Any] = {"game_id": row.game_id}
        for side in ("home", "away"):
            summary = _summarize_team_injuries(
                reports,
                league=row.league,
                team_id=getattr(row, f"{side}_team_id"),
                team_code=getattr(row, f"{side}_team_code"),
                start_time=row.start_time_utc,
            )
            record.update({f"{side}_{key}": value for key, value in summary.items()})
        rows.append(record)
    return selected.merge(pd.DataFrame(rows), on="game_id", how="left")


def _soccer_season_from_start(start_time: pd.Timestamp) -> int | None:
    if pd.isna(start_time):
        return None
    season = int(start_time.year)
    if int(start_time.month) < 7:
        season -= 1
    return season


def _load_soccer_feature_frame(selected: pd.DataFrame) -> pd.DataFrame:
    soccer_games = selected[selected["league"].astype(str).str.upper().isin(SOCCER_LEAGUES)].copy()
    if soccer_games.empty:
        return pd.DataFrame()
    soccer_games["season"] = soccer_games["start_time_utc"].apply(_soccer_season_from_start)
    games_lookup = soccer_games[
        [
            "game_id",
            "league",
            "season",
            "start_time_utc",
            "home_team_code",
            "away_team_code",
            "home_team",
            "away_team",
        ]
    ].drop_duplicates("game_id")

    frames: list[pd.DataFrame] = []
    for league, league_games in games_lookup.groupby("league"):
        seasons = sorted(season for season in league_games["season"].dropna().unique())
        if not seasons:
            continue
        games_df = league_games.rename(
            columns={
                "home_team_code": "home_team",
                "away_team_code": "away_team",
                "home_team": "home_team_name",
                "away_team": "away_team_name",
            }
        )[
            [
                "game_id",
                "season",
                "start_time_utc",
                "home_team",
                "away_team",
                "home_team_name",
                "away_team_name",
            ]
        ]

        try:
            fd_features = soccer_features.build_football_data_form_features(
                league, seasons, games_df
            )
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Unable to build Football-Data form features for %s: %s", league, exc)
            fd_features = pd.DataFrame()
        try:
            ust_features = soccer_features.build_understat_features(league, seasons, games_df)
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Unable to build Understat features for %s: %s", league, exc)
            ust_features = pd.DataFrame()

        if fd_features.empty and ust_features.empty:
            continue
        if fd_features.empty:
            combined = ust_features
        elif ust_features.empty:
            combined = fd_features
        else:
            combined = fd_features.merge(ust_features, on=["game_id", "team"], how="outer")
        frames.append(combined)

    if not frames:
        return pd.DataFrame()
    features = pd.concat(frames, ignore_index=True)
    features["team"] = features["team"].astype(str).str.upper()
    return features


def _attach_soccer_features(selected: pd.DataFrame) -> pd.DataFrame:
    output_columns = [
        f"{side}_soccer_{column}" for side in ("home", "away") for column in SOCCER_SOURCE_COLUMNS
    ]
    if selected.empty:
        return selected
    soccer_frame = _load_soccer_feature_frame(selected)
    if soccer_frame.empty:
        updated = _ensure_columns(selected, output_columns)
    else:
        updated = _attach_prefixed_team_features(
            selected,
            soccer_frame,
            feature_columns=SOCCER_SOURCE_COLUMNS,
            output_namespace="soccer",
            team_key="team",
        )

    soccer_mask = updated["league"].astype(str).str.upper().isin(SOCCER_LEAGUES)
    for side in ("home", "away"):
        for soccer_column, warehouse_column in SOCCER_WAREHOUSE_ALIAS_COLUMNS.items():
            target = f"{side}_soccer_{soccer_column}"
            source = f"{side}_{warehouse_column}"
            if target not in updated.columns:
                updated[target] = np.nan
            if source in updated.columns:
                updated.loc[soccer_mask, target] = updated.loc[soccer_mask, target].fillna(
                    updated.loc[soccer_mask, source]
                )

    diff_columns: dict[str, pd.Series] = {}
    for soccer_column in SOCCER_SOURCE_COLUMNS:
        home_col = f"home_soccer_{soccer_column}"
        away_col = f"away_soccer_{soccer_column}"
        if home_col in updated.columns and away_col in updated.columns:
            diff_columns[f"soccer_{soccer_column}_diff"] = updated[home_col] - updated[away_col]
    if diff_columns:
        updated = updated.drop(
            columns=[column for column in diff_columns if column in updated.columns],
            errors="ignore",
        )
        updated = pd.concat([updated, pd.DataFrame(diff_columns, index=updated.index)], axis=1)
    return updated


def _finalize_game_feature_contract(df: pd.DataFrame) -> pd.DataFrame:
    return _ensure_columns(df, GAME_FEATURE_CONTRACT_COLUMNS)


def build_totals_model_input(
    db_path: Path = DB_PATH,
    leagues: Optional[Iterable[str]] = DEFAULT_RELEASE_LEAGUES,
    latest_only: bool = True,
) -> pd.DataFrame:
    """Build leakage-safe over/under model input from odds snapshots and results."""
    league_sql, params = _league_filter(leagues)
    query = f"""
        SELECT
            g.game_id,
            s.league,
            g.start_time_utc,
            g.home_team_id,
            g.away_team_id,
            ht.code AS home_team_code,
            at.code AS away_team_code,
            ht.name AS home_team,
            at.name AS away_team,
            os.snapshot_id,
            os.fetched_at_utc AS snapshot_time_utc,
            b.name AS book,
            o.book_id,
            o.line,
            MAX(CASE WHEN LOWER(o.outcome) = 'over' THEN o.price_american END) AS over_moneyline,
            MAX(CASE WHEN LOWER(o.outcome) = 'under' THEN o.price_american END) AS under_moneyline,
            gr.home_score,
            gr.away_score,
            gr.total_close
        FROM odds o
        JOIN odds_snapshots os ON o.snapshot_id = os.snapshot_id
        JOIN games g ON o.game_id = g.game_id
        JOIN sports s ON g.sport_id = s.sport_id
        JOIN teams ht ON g.home_team_id = ht.team_id
        JOIN teams at ON g.away_team_id = at.team_id
        JOIN books b ON o.book_id = b.book_id
        JOIN game_results gr ON g.game_id = gr.game_id
        WHERE o.market = 'totals'
          AND o.price_american IS NOT NULL
          AND o.line IS NOT NULL
          AND gr.home_score IS NOT NULL
          AND gr.away_score IS NOT NULL
          {league_sql}
        GROUP BY
            g.game_id, s.league, g.start_time_utc, os.snapshot_id,
            g.home_team_id, g.away_team_id, ht.code, at.code, ht.name, at.name,
            os.fetched_at_utc, b.name, o.book_id, o.line,
            gr.home_score, gr.away_score, gr.total_close
        HAVING over_moneyline IS NOT NULL AND under_moneyline IS NOT NULL
    """
    df = _read_sql(db_path, query, params)
    if df.empty:
        return df
    df = _normalize_datetime_columns(df)
    df = df[df["snapshot_time_utc"] <= df["start_time_utc"]].copy()
    if df.empty:
        return df
    for column in ["line", "over_moneyline", "under_moneyline", "home_score", "away_score"]:
        df[column] = pd.to_numeric(df[column], errors="coerce")
    df = df.dropna(subset=["line", "over_moneyline", "under_moneyline"])
    df = _add_market_common_features(df)

    no_vig = df.apply(
        lambda row: _no_vig_pair(row["over_moneyline"], row["under_moneyline"]),
        axis=1,
        result_type="expand",
    )
    df["over_no_vig_prob"] = no_vig[0]
    df["under_no_vig_prob"] = no_vig[1]
    df["market_hold"] = (
        df["over_moneyline"].map(_implied_prob) + df["under_moneyline"].map(_implied_prob) - 1.0
    )
    df["actual_total"] = df["home_score"] + df["away_score"]
    df["target_over"] = np.where(
        df["actual_total"] > df["line"],
        1,
        np.where(df["actual_total"] < df["line"], 0, np.nan),
    )
    df["is_push"] = df["target_over"].isna()

    opening = (
        df.sort_values("snapshot_time_utc")
        .drop_duplicates(["game_id", "book"], keep="first")[
            ["game_id", "book", "line", "over_moneyline", "under_moneyline", "snapshot_time_utc"]
        ]
        .rename(
            columns={
                "line": "opening_line",
                "over_moneyline": "opening_over_moneyline",
                "under_moneyline": "opening_under_moneyline",
                "snapshot_time_utc": "opening_snapshot_time_utc",
            }
        )
    )
    df = df.merge(opening, on=["game_id", "book"], how="left")
    df["line_movement"] = df["line"] - df["opening_line"]
    df["over_moneyline_movement"] = df["over_moneyline"] - df["opening_over_moneyline"]
    df["under_moneyline_movement"] = df["under_moneyline"] - df["opening_under_moneyline"]

    latest_by_book = (
        df.sort_values("snapshot_time_utc").drop_duplicates(["game_id", "book"], keep="last").copy()
    )
    selected = _latest_per_game(df) if latest_only else df.copy()
    selected = _attach_line_dispersion(latest_by_book, selected)
    selected = _attach_best_totals_prices(latest_by_book, selected)
    selected = _attach_team_history_features(selected, db_path, leagues)
    selected = _attach_nba_advanced_features(selected, db_path)
    selected = _attach_injury_features(selected, db_path, leagues)
    selected = _attach_soccer_features(selected)
    selected = _finalize_game_feature_contract(selected)
    return selected.sort_values(["league", "start_time_utc", "game_id"]).reset_index(drop=True)


def _normalize_moneyline_outcome(outcome: Any, home_team: str, away_team: str) -> str:
    value = str(outcome or "").strip().lower()
    home = home_team.strip().lower()
    away = away_team.strip().lower()
    if value in {"home", home} or (value and (value in home or home in value)):
        return "home"
    if value in {"away", away} or (value and (value in away or away in value)):
        return "away"
    if value == "draw":
        return "draw"
    return value


def build_moneyline_model_input(
    db_path: Path = DB_PATH,
    leagues: Optional[Iterable[str]] = DEFAULT_RELEASE_LEAGUES,
    latest_only: bool = True,
) -> pd.DataFrame:
    """Build leakage-safe moneyline model input from odds snapshots and results."""
    league_sql, params = _league_filter(leagues)
    query = f"""
        SELECT
            g.game_id,
            s.league,
            g.start_time_utc,
            g.home_team_id,
            g.away_team_id,
            os.snapshot_id,
            os.fetched_at_utc AS snapshot_time_utc,
            b.name AS book,
            o.book_id,
            o.outcome,
            o.price_american,
            ht.code AS home_team_code,
            at.code AS away_team_code,
            ht.name AS home_team,
            at.name AS away_team,
            gr.home_score,
            gr.away_score
        FROM odds o
        JOIN odds_snapshots os ON o.snapshot_id = os.snapshot_id
        JOIN games g ON o.game_id = g.game_id
        JOIN sports s ON g.sport_id = s.sport_id
        JOIN books b ON o.book_id = b.book_id
        JOIN teams ht ON g.home_team_id = ht.team_id
        JOIN teams at ON g.away_team_id = at.team_id
        JOIN game_results gr ON g.game_id = gr.game_id
        WHERE o.market = 'h2h'
          AND o.price_american IS NOT NULL
          AND gr.home_score IS NOT NULL
          AND gr.away_score IS NOT NULL
          {league_sql}
    """
    raw = _read_sql(db_path, query, params)
    if raw.empty:
        return raw
    raw = _normalize_datetime_columns(raw)
    raw = raw[raw["snapshot_time_utc"] <= raw["start_time_utc"]].copy()
    if raw.empty:
        return raw
    raw["side"] = raw.apply(
        lambda row: _normalize_moneyline_outcome(
            row["outcome"], row["home_team"], row["away_team"]
        ),
        axis=1,
    )
    raw = raw[raw["side"].isin(["home", "away"])].copy()
    raw["price_american"] = pd.to_numeric(raw["price_american"], errors="coerce")
    raw = raw.dropna(subset=["price_american"])

    pivot_cols = [
        "game_id",
        "league",
        "start_time_utc",
        "home_team_id",
        "away_team_id",
        "snapshot_id",
        "snapshot_time_utc",
        "book",
        "book_id",
        "home_team_code",
        "away_team_code",
        "home_team",
        "away_team",
        "home_score",
        "away_score",
    ]
    paired = (
        raw.pivot_table(
            index=pivot_cols,
            columns="side",
            values="price_american",
            aggfunc="first",
        )
        .reset_index()
        .rename(columns={"home": "home_moneyline", "away": "away_moneyline"})
    )
    paired = paired.dropna(subset=["home_moneyline", "away_moneyline"])
    paired = _add_market_common_features(paired)
    no_vig = paired.apply(
        lambda row: _no_vig_pair(row["home_moneyline"], row["away_moneyline"]),
        axis=1,
        result_type="expand",
    )
    paired["home_no_vig_prob"] = no_vig[0]
    paired["away_no_vig_prob"] = no_vig[1]
    paired["market_hold"] = (
        paired["home_moneyline"].map(_implied_prob)
        + paired["away_moneyline"].map(_implied_prob)
        - 1.0
    )
    paired["target_home_win"] = np.where(
        paired["home_score"] > paired["away_score"],
        1,
        np.where(paired["home_score"] < paired["away_score"], 0, np.nan),
    )

    opening = (
        paired.sort_values("snapshot_time_utc")
        .drop_duplicates(["game_id", "book"], keep="first")[
            ["game_id", "book", "home_moneyline", "away_moneyline", "snapshot_time_utc"]
        ]
        .rename(
            columns={
                "home_moneyline": "opening_home_moneyline",
                "away_moneyline": "opening_away_moneyline",
                "snapshot_time_utc": "opening_snapshot_time_utc",
            }
        )
    )
    paired = paired.merge(opening, on=["game_id", "book"], how="left")
    paired["home_moneyline_movement"] = paired["home_moneyline"] - paired["opening_home_moneyline"]
    paired["away_moneyline_movement"] = paired["away_moneyline"] - paired["opening_away_moneyline"]

    latest_by_book = (
        paired.sort_values("snapshot_time_utc")
        .drop_duplicates(["game_id", "book"], keep="last")
        .copy()
    )
    selected = _latest_per_game(paired) if latest_only else paired.copy()
    selected = _attach_best_moneyline_prices(latest_by_book, selected)
    selected = _attach_team_history_features(selected, db_path, leagues)
    selected = _attach_nba_advanced_features(selected, db_path)
    selected = _attach_injury_features(selected, db_path, leagues)
    selected = _attach_soccer_features(selected)
    selected = _finalize_game_feature_contract(selected)
    return selected.sort_values(["league", "start_time_utc", "game_id"]).reset_index(drop=True)


def build_moneyline_side_model_input(
    db_path: Path = DB_PATH,
    leagues: Optional[Iterable[str]] = DEFAULT_RELEASE_LEAGUES,
    latest_only: bool = True,
) -> pd.DataFrame:
    """Build one leakage-safe moneyline row per bettable side/team."""
    games = build_moneyline_model_input(db_path=db_path, leagues=leagues, latest_only=latest_only)
    if games.empty:
        return games

    rows: list[dict[str, Any]] = []

    def _signed_metric(value: Any, sign: float) -> float:
        if pd.isna(value):
            return np.nan
        return float(value) * sign

    for _, row in games.iterrows():
        home_win = float(row["home_score"]) > float(row["away_score"])
        away_win = float(row["away_score"]) > float(row["home_score"])
        for side in ("home", "away"):
            is_home = side == "home"
            team_prefix = "home" if is_home else "away"
            opponent_prefix = "away" if is_home else "home"
            side_row = {
                "game_id": row["game_id"],
                "league": row["league"],
                "start_time_utc": row["start_time_utc"],
                "snapshot_id": row["snapshot_id"],
                "snapshot_time_utc": row["snapshot_time_utc"],
                "book": row["book"],
                "book_id": row["book_id"],
                "side": side,
                "is_home": 1.0 if is_home else 0.0,
                "team_id": row.get(f"{team_prefix}_team_id"),
                "opponent_team_id": row.get(f"{opponent_prefix}_team_id"),
                "team_code": row.get(f"{team_prefix}_team_code"),
                "opponent_code": row.get(f"{opponent_prefix}_team_code"),
                "team": row[f"{team_prefix}_team"],
                "opponent": row[f"{opponent_prefix}_team"],
                "moneyline": row[f"{team_prefix}_moneyline"],
                "opponent_moneyline": row[f"{opponent_prefix}_moneyline"],
                "best_moneyline": row.get(f"best_{team_prefix}_moneyline"),
                "best_book": row.get(f"best_{team_prefix}_book"),
                "no_vig_prob": row[f"{team_prefix}_no_vig_prob"],
                "opponent_no_vig_prob": row[f"{opponent_prefix}_no_vig_prob"],
                "market_hold": row["market_hold"],
                "hours_before_start": row["hours_before_start"],
                "opening_moneyline": row[f"opening_{team_prefix}_moneyline"],
                "moneyline_movement": row[f"{team_prefix}_moneyline_movement"],
                "target_win": 1 if (home_win if is_home else away_win) else 0,
            }
            base_metrics = ["rest_days", "back_to_back"]
            for window in GENERIC_ROLLING_WINDOWS:
                base_metrics.extend(
                    [
                        f"score_for_l{window}",
                        f"score_against_l{window}",
                        f"game_total_l{window}",
                        f"win_pct_l{window}",
                        f"games_l{window}",
                    ]
                )
            base_metrics.extend(
                [
                    "same_venue_score_for_l5",
                    "same_venue_score_against_l5",
                    "same_venue_win_pct_l5",
                    "same_venue_games_l5",
                ]
            )
            for metric in base_metrics:
                team_value = row.get(f"{team_prefix}_{metric}")
                opponent_value = row.get(f"{opponent_prefix}_{metric}")
                side_row[f"team_{metric}"] = team_value
                side_row[f"opponent_{metric}"] = opponent_value

            for metric in INJURY_SOURCE_COLUMNS:
                team_value = row.get(f"{team_prefix}_{metric}")
                opponent_value = row.get(f"{opponent_prefix}_{metric}")
                side_row[f"team_{metric}"] = team_value
                side_row[f"opponent_{metric}"] = opponent_value
                if pd.notna(team_value) and pd.notna(opponent_value):
                    side_row[f"{metric}_diff"] = float(team_value) - float(opponent_value)
                else:
                    side_row[f"{metric}_diff"] = np.nan

            for metric in NBA_ROLLING_SOURCE_COLUMNS:
                side_row[f"team_nba_{metric}"] = row.get(f"{team_prefix}_nba_{metric}")
                side_row[f"opponent_nba_{metric}"] = row.get(f"{opponent_prefix}_nba_{metric}")

            for metric in SOCCER_SOURCE_COLUMNS:
                side_row[f"team_soccer_{metric}"] = row.get(f"{team_prefix}_soccer_{metric}")
                side_row[f"opponent_soccer_{metric}"] = row.get(
                    f"{opponent_prefix}_soccer_{metric}"
                )

            sign = 1.0 if is_home else -1.0
            side_row["rest_diff"] = _signed_metric(row.get("rest_diff"), sign)
            for window in GENERIC_ROLLING_WINDOWS:
                for metric in ("score_for", "score_against", "game_total", "win_pct"):
                    side_row[f"{metric}_l{window}_diff"] = _signed_metric(
                        row.get(f"{metric}_l{window}_diff"),
                        sign,
                    )
            for metric in ("score_for", "score_against"):
                side_row[f"same_venue_{metric}_l5_diff"] = _signed_metric(
                    row.get(f"same_venue_{metric}_l5_diff"),
                    sign,
                )
            for metric in NBA_ROLLING_SOURCE_COLUMNS:
                side_row[f"nba_{metric}_diff"] = _signed_metric(
                    row.get(f"nba_{metric}_diff"),
                    sign,
                )
            for metric in SOCCER_SOURCE_COLUMNS:
                side_row[f"soccer_{metric}_diff"] = _signed_metric(
                    row.get(f"soccer_{metric}_diff"),
                    sign,
                )
            rows.append(side_row)

    side_df = pd.DataFrame(rows)
    side_df = _ensure_columns(side_df, MONEYLINE_SIDE_FEATURE_CONTRACT_COLUMNS)
    return side_df.sort_values(["league", "start_time_utc", "game_id", "side"]).reset_index(
        drop=True
    )


def _market_drop_reasons(
    db_path: Path,
    leagues: Optional[Iterable[str]],
    market: str,
    built_game_ids: set[Any],
) -> dict[str, dict[str, int]]:
    league_sql, params = _league_filter(leagues)
    games_query = f"""
        SELECT
            g.game_id,
            UPPER(s.league) AS league,
            g.start_time_utc,
            gr.home_score,
            gr.away_score
        FROM games g
        JOIN sports s ON g.sport_id = s.sport_id
        LEFT JOIN game_results gr ON g.game_id = gr.game_id
        WHERE 1 = 1
          {league_sql}
    """
    games = _read_sql(db_path, games_query, params)
    if games.empty:
        return {}
    games["start_time_utc"] = pd.to_datetime(games["start_time_utc"], utc=True, errors="coerce")
    games["has_result"] = games["home_score"].notna() & games["away_score"].notna()

    odds_query = f"""
        SELECT
            g.game_id,
            UPPER(s.league) AS league,
            g.start_time_utc,
            os.fetched_at_utc AS snapshot_time_utc,
            o.outcome,
            o.price_american,
            o.line
        FROM odds o
        JOIN odds_snapshots os ON o.snapshot_id = os.snapshot_id
        JOIN games g ON o.game_id = g.game_id
        JOIN sports s ON g.sport_id = s.sport_id
        WHERE o.market = ?
          AND o.price_american IS NOT NULL
          {league_sql}
    """
    odds = _read_sql(db_path, odds_query, [market, *params])
    if odds.empty:
        pregame_ids: set[Any] = set()
    else:
        odds["start_time_utc"] = pd.to_datetime(odds["start_time_utc"], utc=True, errors="coerce")
        odds["snapshot_time_utc"] = pd.to_datetime(
            odds["snapshot_time_utc"], utc=True, errors="coerce"
        )
        odds = odds[odds["snapshot_time_utc"] <= odds["start_time_utc"]].copy()
        if market == "totals":
            odds = odds[odds["line"].notna()].copy()
        pregame_ids = set(odds["game_id"].dropna().unique())

    reasons: dict[str, dict[str, int]] = {}
    for league, league_games in games.groupby("league"):
        all_ids = set(league_games["game_id"].dropna().unique())
        result_ids = set(league_games.loc[league_games["has_result"], "game_id"].dropna().unique())
        league_built_ids = built_game_ids & all_ids
        missing_result = all_ids - result_ids
        no_pre_game_odds = result_ids - pregame_ids
        incomplete_pair = result_ids - league_built_ids - no_pre_game_odds
        reasons[league] = {
            "drop_missing_result": len(missing_result),
            "drop_no_pre_game_odds": len(no_pre_game_odds),
            "drop_incomplete_market_pair": len(incomplete_pair),
        }
        reasons[league]["dropped_rows"] = sum(reasons[league].values())
    return reasons


def _feature_group_non_null_pct(df: pd.DataFrame, columns: Iterable[str]) -> float:
    if df.empty:
        return 0.0
    present = [column for column in columns if column in df.columns]
    if not present:
        return 0.0
    return round(float(df[present].notna().to_numpy().mean() * 100.0), 2)


def build_feature_coverage_report(
    db_path: Path = DB_PATH,
    leagues: Optional[Iterable[str]] = DEFAULT_RELEASE_LEAGUES,
    markets: Iterable[str] = ("totals", "moneyline"),
) -> pd.DataFrame:
    """Report model-input row coverage and feature density by market and league."""
    normalized_leagues = (
        [league.upper() for league in leagues] if leagues else list(DEFAULT_RELEASE_LEAGUES)
    )
    rows: list[dict[str, Any]] = []

    for market in markets:
        if market == "totals":
            model_input = build_totals_model_input(db_path=db_path, leagues=normalized_leagues)
            sql_market = "totals"
        elif market == "moneyline":
            model_input = build_moneyline_side_model_input(
                db_path=db_path, leagues=normalized_leagues
            )
            sql_market = "h2h"
        else:
            raise ValueError(f"Unsupported market for coverage report: {market}")

        built_game_ids = (
            set(model_input["game_id"].dropna().unique()) if "game_id" in model_input else set()
        )
        drop_reasons = _market_drop_reasons(db_path, normalized_leagues, sql_market, built_game_ids)

        for league in normalized_leagues:
            league_rows = (
                model_input[model_input["league"].astype(str).str.upper() == league]
                if not model_input.empty and "league" in model_input.columns
                else pd.DataFrame()
            )
            record: dict[str, Any] = {
                "market": market,
                "league": league,
                "row_count": int(len(league_rows)),
            }
            for group_name, group_columns in FEATURE_GROUPS.items():
                side_columns = []
                if market == "moneyline":
                    for column in group_columns:
                        for side_prefix in ("home_", "away_"):
                            if column.startswith(side_prefix):
                                suffix = column[len(side_prefix) :]
                                side_columns.extend([f"team_{suffix}", f"opponent_{suffix}"])
                                break
                        else:
                            side_columns.append(column)
                else:
                    side_columns = list(group_columns)
                record[f"{group_name}_non_null_pct"] = _feature_group_non_null_pct(
                    league_rows,
                    side_columns,
                )
            record.update(
                drop_reasons.get(
                    league,
                    {
                        "dropped_rows": 0,
                        "drop_missing_result": 0,
                        "drop_no_pre_game_odds": 0,
                        "drop_incomplete_market_pair": 0,
                    },
                )
            )
            rows.append(record)

    return pd.DataFrame(rows).sort_values(["market", "league"]).reset_index(drop=True)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build canonical betting model inputs.")
    parser.add_argument("--db", type=Path, default=DB_PATH)
    parser.add_argument(
        "--market",
        choices=["totals", "moneyline"],
        default="totals",
        help="Model input to build.",
    )
    parser.add_argument(
        "--leagues",
        default=",".join(DEFAULT_RELEASE_LEAGUES),
        help="Comma-separated leagues.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Optional parquet output path.",
    )
    parser.add_argument(
        "--all-snapshots",
        action="store_true",
        help="Keep every pre-game snapshot instead of latest pre-game row per game.",
    )
    parser.add_argument(
        "--coverage-report",
        action="store_true",
        help="Build feature coverage by market/league instead of model input rows.",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    logging.basicConfig(level=logging.INFO)
    leagues = [league.strip().upper() for league in args.leagues.split(",") if league.strip()]
    if args.coverage_report:
        df = build_feature_coverage_report(args.db, leagues=leagues)
    elif args.market == "totals":
        df = build_totals_model_input(args.db, leagues=leagues, latest_only=not args.all_snapshots)
    else:
        df = build_moneyline_side_model_input(
            args.db, leagues=leagues, latest_only=not args.all_snapshots
        )
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(args.output, index=False)
        LOGGER.info("Wrote %d rows to %s", len(df), args.output)
    else:
        print(df.head(20).to_string(index=False))
        LOGGER.info("Built %d %s model input rows", len(df), args.market)


if __name__ == "__main__":
    main()
