"""Audit settled-game odds coverage for benchmark sample-size diagnostics."""

from __future__ import annotations

import argparse
import json
import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

import numpy as np
import pandas as pd

from src.db.core import DB_PATH

LOGGER = logging.getLogger(__name__)

DEFAULT_RELEASE_LEAGUES = ("NBA", "NHL", "EPL", "LALIGA", "BUNDESLIGA", "SERIEA", "LIGUE1")
DEFAULT_MARKETS = ("totals", "moneyline")
DEFAULT_MAX_HOURS_BEFORE_START = 72.0
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
HOUR_BUCKETS = ("missing", "<1h", "1-6h", "6-24h", "24-72h", ">=72h")
MARKET_TO_SQL = {"totals": "totals", "moneyline": "h2h", "h2h": "h2h"}
SQL_TO_MARKET = {"totals": "totals", "h2h": "moneyline"}

SETTLED_COLUMNS = [
    "game_id",
    "league",
    "start_time_utc",
    "home_score",
    "away_score",
    "home_moneyline_close",
    "away_moneyline_close",
    "total_close",
]
ODDS_COLUMNS = [
    "game_id",
    "league",
    "start_time_utc",
    "snapshot_id",
    "snapshot_time_utc",
    "book_id",
    "book",
    "market",
    "outcome",
    "price_american",
    "line",
    "home_team",
    "away_team",
    "home_team_code",
    "away_team_code",
]
TOTALS_PAIR_COLUMNS = [
    "game_id",
    "league",
    "start_time_utc",
    "snapshot_id",
    "snapshot_time_utc",
    "book_id",
    "book",
    "line",
    "over_moneyline",
    "under_moneyline",
]
MONEYLINE_PAIR_COLUMNS = [
    "game_id",
    "league",
    "start_time_utc",
    "snapshot_id",
    "snapshot_time_utc",
    "book_id",
    "book",
    "home_moneyline",
    "away_moneyline",
]


def _split_csv(value: str | Iterable[str] | None, default: Iterable[str]) -> list[str]:
    if value is None:
        return [str(item).strip().upper() for item in default if str(item).strip()]
    if isinstance(value, str):
        raw = value.split(",")
    else:
        raw = list(value)
    return [str(item).strip().upper() for item in raw if str(item).strip()]


def _normalize_markets(value: str | Iterable[str] | None) -> list[str]:
    if value is None:
        raw = DEFAULT_MARKETS
    elif isinstance(value, str):
        raw = value.split(",")
    else:
        raw = list(value)
    markets: list[str] = []
    for item in raw:
        normalized = str(item).strip().lower()
        if not normalized:
            continue
        if normalized not in MARKET_TO_SQL:
            raise ValueError(f"Unsupported market: {item}. Expected totals or moneyline.")
        report_market = SQL_TO_MARKET[MARKET_TO_SQL[normalized]]
        if report_market not in markets:
            markets.append(report_market)
    return markets


def _league_filter_sql(leagues: Iterable[str]) -> tuple[str, list[str]]:
    normalized = [league.upper() for league in leagues]
    if not normalized:
        return "", []
    placeholders = ",".join("?" for _ in normalized)
    return f" AND UPPER(s.league) IN ({placeholders})", normalized


def _read_sql(db_path: Path, query: str, params: Iterable[Any] = ()) -> pd.DataFrame:
    with sqlite3.connect(str(db_path)) as conn:
        return pd.read_sql_query(query, conn, params=list(params))


def _empty_frame(columns: Iterable[str]) -> pd.DataFrame:
    return pd.DataFrame(columns=list(columns))


def _ensure_columns(df: pd.DataFrame, columns: Iterable[str]) -> pd.DataFrame:
    updated = df.copy()
    for column in columns:
        if column not in updated.columns:
            updated[column] = np.nan
    return updated


def _read_settled_games(db_path: Path, leagues: list[str]) -> pd.DataFrame:
    league_sql, params = _league_filter_sql(leagues)
    query = f"""
        SELECT
            g.game_id,
            UPPER(s.league) AS league,
            g.start_time_utc,
            gr.home_score,
            gr.away_score,
            gr.home_moneyline_close,
            gr.away_moneyline_close,
            gr.total_close
        FROM games g
        JOIN sports s ON g.sport_id = s.sport_id
        JOIN game_results gr ON g.game_id = gr.game_id
        WHERE gr.home_score IS NOT NULL
          AND gr.away_score IS NOT NULL
          {league_sql}
    """
    settled = _read_sql(db_path, query, params)
    if settled.empty:
        return _empty_frame(SETTLED_COLUMNS)
    settled["start_time_utc"] = pd.to_datetime(
        settled["start_time_utc"], utc=True, errors="coerce", format="mixed"
    )
    for column in [
        "home_score",
        "away_score",
        "home_moneyline_close",
        "away_moneyline_close",
        "total_close",
    ]:
        settled[column] = pd.to_numeric(settled[column], errors="coerce")
    return settled[SETTLED_COLUMNS]


def _read_odds_rows(db_path: Path, leagues: list[str], markets: list[str]) -> pd.DataFrame:
    league_sql, league_params = _league_filter_sql(leagues)
    sql_markets = sorted({MARKET_TO_SQL[market] for market in markets})
    market_placeholders = ",".join("?" for _ in sql_markets)
    query = f"""
        SELECT
            g.game_id,
            UPPER(s.league) AS league,
            g.start_time_utc,
            os.snapshot_id,
            os.fetched_at_utc AS snapshot_time_utc,
            o.book_id,
            b.name AS book,
            LOWER(o.market) AS market,
            o.outcome,
            o.price_american,
            o.line,
            ht.name AS home_team,
            at.name AS away_team,
            ht.code AS home_team_code,
            at.code AS away_team_code
        FROM odds o
        JOIN odds_snapshots os ON o.snapshot_id = os.snapshot_id
        JOIN games g ON o.game_id = g.game_id
        JOIN sports s ON g.sport_id = s.sport_id
        JOIN books b ON o.book_id = b.book_id
        JOIN teams ht ON g.home_team_id = ht.team_id
        JOIN teams at ON g.away_team_id = at.team_id
        JOIN game_results gr ON g.game_id = gr.game_id
        WHERE gr.home_score IS NOT NULL
          AND gr.away_score IS NOT NULL
          AND LOWER(o.market) IN ({market_placeholders})
          {league_sql}
    """
    odds = _read_sql(db_path, query, [*sql_markets, *league_params])
    if odds.empty:
        return _empty_frame(ODDS_COLUMNS)
    odds["start_time_utc"] = pd.to_datetime(
        odds["start_time_utc"], utc=True, errors="coerce", format="mixed"
    )
    odds["snapshot_time_utc"] = pd.to_datetime(
        odds["snapshot_time_utc"], utc=True, errors="coerce", format="mixed"
    )
    odds["price_american"] = pd.to_numeric(odds["price_american"], errors="coerce")
    odds["line"] = pd.to_numeric(odds["line"], errors="coerce")
    return odds[ODDS_COLUMNS]


def _book_rank(book_name: Any) -> int:
    normalized = str(book_name or "").strip().lower()
    try:
        return BOOK_PRIORITY.index(normalized)
    except ValueError:
        return len(BOOK_PRIORITY)


def _book_key(book_name: Any) -> str:
    return str(book_name or "").strip().lower()


def _hour_bucket(hours: Any) -> str:
    if pd.isna(hours):
        return "missing"
    value = float(hours)
    if value < 1:
        return "<1h"
    if value < 6:
        return "1-6h"
    if value < 24:
        return "6-24h"
    if value < 72:
        return "24-72h"
    return ">=72h"


def _american_to_decimal(value: float | int | None) -> Optional[float]:
    if value is None or pd.isna(value):
        return None
    moneyline = float(value)
    if moneyline == 0:
        return None
    if moneyline > 0:
        return 1.0 + moneyline / 100.0
    return 1.0 + 100.0 / abs(moneyline)


def _moneyline_clv(bet_moneyline: Any, close_moneyline: Any) -> float:
    bet_decimal = _american_to_decimal(bet_moneyline)
    close_decimal = _american_to_decimal(close_moneyline)
    if bet_decimal is None or close_decimal is None:
        return np.nan
    return float(bet_decimal - close_decimal)


def _normalize_moneyline_outcome(
    outcome: Any,
    home_team: Any,
    away_team: Any,
    home_team_code: Any,
    away_team_code: Any,
) -> str:
    value = str(outcome or "").strip().lower()
    home = str(home_team or "").strip().lower()
    away = str(away_team or "").strip().lower()
    home_code = str(home_team_code or "").strip().lower()
    away_code = str(away_team_code or "").strip().lower()
    if value in {"home", home, home_code} or (home and (value in home or home in value)):
        return "home"
    if value in {"away", away, away_code} or (away and (value in away or away in value)):
        return "away"
    if value == "draw":
        return "draw"
    return value


def _pair_totals(odds: pd.DataFrame) -> pd.DataFrame:
    if odds.empty:
        return _empty_frame(TOTALS_PAIR_COLUMNS)
    work = odds[odds["market"].astype(str).str.lower() == "totals"].copy()
    if work.empty:
        return _empty_frame(TOTALS_PAIR_COLUMNS)
    work["side"] = work["outcome"].astype(str).str.strip().str.lower()
    work = work[work["side"].isin(["over", "under"])].copy()
    work = work.dropna(subset=["price_american", "line"])
    if work.empty:
        return _empty_frame(TOTALS_PAIR_COLUMNS)

    index_cols = [
        "game_id",
        "league",
        "start_time_utc",
        "snapshot_id",
        "snapshot_time_utc",
        "book_id",
        "book",
        "line",
    ]
    paired = (
        work.pivot_table(
            index=index_cols,
            columns="side",
            values="price_american",
            aggfunc="first",
        )
        .reset_index()
        .rename(columns={"over": "over_moneyline", "under": "under_moneyline"})
    )
    paired.columns.name = None
    paired = _ensure_columns(paired, TOTALS_PAIR_COLUMNS)
    paired = paired.dropna(subset=["over_moneyline", "under_moneyline"])
    return paired[TOTALS_PAIR_COLUMNS]


def _pair_moneyline(odds: pd.DataFrame) -> pd.DataFrame:
    if odds.empty:
        return _empty_frame(MONEYLINE_PAIR_COLUMNS)
    work = odds[odds["market"].astype(str).str.lower() == "h2h"].copy()
    if work.empty:
        return _empty_frame(MONEYLINE_PAIR_COLUMNS)
    work["side"] = work.apply(
        lambda row: _normalize_moneyline_outcome(
            row["outcome"],
            row["home_team"],
            row["away_team"],
            row["home_team_code"],
            row["away_team_code"],
        ),
        axis=1,
    )
    work = work[work["side"].isin(["home", "away"])].copy()
    work = work.dropna(subset=["price_american"])
    if work.empty:
        return _empty_frame(MONEYLINE_PAIR_COLUMNS)

    index_cols = [
        "game_id",
        "league",
        "start_time_utc",
        "snapshot_id",
        "snapshot_time_utc",
        "book_id",
        "book",
    ]
    paired = (
        work.pivot_table(
            index=index_cols,
            columns="side",
            values="price_american",
            aggfunc="first",
        )
        .reset_index()
        .rename(columns={"home": "home_moneyline", "away": "away_moneyline"})
    )
    paired.columns.name = None
    paired = _ensure_columns(paired, MONEYLINE_PAIR_COLUMNS)
    paired = paired.dropna(subset=["home_moneyline", "away_moneyline"])
    return paired[MONEYLINE_PAIR_COLUMNS]


def _pairs_for_market(odds: pd.DataFrame, market: str) -> pd.DataFrame:
    if market == "totals":
        return _pair_totals(odds)
    if market == "moneyline":
        return _pair_moneyline(odds)
    raise ValueError(f"Unsupported market: {market}")


def _with_timing_columns(pairs: pd.DataFrame) -> pd.DataFrame:
    if pairs.empty:
        return pairs.copy()
    updated = pairs.copy()
    updated["book_rank"] = updated["book"].map(_book_rank)
    updated["hours_before_start"] = (
        updated["start_time_utc"] - updated["snapshot_time_utc"]
    ).dt.total_seconds() / 3600.0
    return updated


def _select_opening_current(
    pregame_pairs: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if pregame_pairs.empty:
        return pregame_pairs.copy(), pregame_pairs.copy(), pregame_pairs.copy()
    ranked = _with_timing_columns(pregame_pairs)
    opening = (
        ranked.sort_values(
            ["snapshot_time_utc", "book_rank", "book"],
            ascending=[True, True, True],
        )
        .drop_duplicates("game_id", keep="first")
        .copy()
    )
    current = (
        ranked.sort_values(
            ["snapshot_time_utc", "book_rank", "book"],
            ascending=[False, True, True],
        )
        .drop_duplicates("game_id", keep="first")
        .copy()
    )
    latest_by_book = (
        ranked.sort_values(
            ["snapshot_time_utc", "book_rank", "book"],
            ascending=[True, True, True],
        )
        .drop_duplicates(["game_id", "book"], keep="last")
        .copy()
    )
    return opening, current, latest_by_book


def _best_totals_by_game(latest_by_book: pd.DataFrame, current: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "game_id",
        "best_over_moneyline",
        "best_over_book",
        "best_under_moneyline",
        "best_under_book",
    ]
    if latest_by_book.empty or current.empty:
        return _empty_frame(columns)
    current_lines = current.set_index("game_id")["line"].to_dict()
    rows: list[dict[str, Any]] = []
    for game_id, group in latest_by_book.groupby("game_id"):
        selected_line = current_lines.get(game_id)
        scoped = group
        if pd.notna(selected_line):
            line_values = pd.to_numeric(group["line"], errors="coerce")
            scoped = group[np.isclose(line_values, float(selected_line), equal_nan=False)].copy()
        if scoped.empty:
            scoped = group

        row: dict[str, Any] = {"game_id": game_id}
        for side in ("over", "under"):
            price_col = f"{side}_moneyline"
            priced = scoped.dropna(subset=[price_col]).copy()
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
    return pd.DataFrame(rows, columns=columns)


def _best_moneyline_by_game(latest_by_book: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "game_id",
        "best_home_moneyline",
        "best_home_book",
        "best_away_moneyline",
        "best_away_book",
    ]
    if latest_by_book.empty:
        return _empty_frame(columns)
    rows: list[dict[str, Any]] = []
    for game_id, group in latest_by_book.groupby("game_id"):
        row: dict[str, Any] = {"game_id": game_id}
        for side in ("home", "away"):
            price_col = f"{side}_moneyline"
            priced = group.dropna(subset=[price_col]).copy()
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
    return pd.DataFrame(rows, columns=columns)


def _best_by_game(market: str, latest_by_book: pd.DataFrame, current: pd.DataFrame) -> pd.DataFrame:
    if market == "totals":
        return _best_totals_by_game(latest_by_book, current)
    return _best_moneyline_by_game(latest_by_book)


def _market_sides(market: str) -> tuple[str, str]:
    return ("over", "under") if market == "totals" else ("home", "away")


def _count_records(series: pd.Series, count_name: str) -> list[dict[str, Any]]:
    if series.empty:
        return []
    rows = []
    for key, count in series.items():
        if pd.isna(key):
            label = "unknown"
        else:
            label = str(key)
        rows.append({"book": label, count_name: int(count)})
    rows.sort(key=lambda row: (-row[count_name], row["book"]))
    return rows


def _book_coverage(
    pregame_pairs: pd.DataFrame,
    current: pd.DataFrame,
    best: pd.DataFrame,
    market: str,
) -> dict[str, Any]:
    usable_by_book = (
        pregame_pairs.groupby("book")["game_id"].nunique().sort_values(ascending=False)
        if not pregame_pairs.empty
        else pd.Series(dtype=int)
    )
    usable_rows_by_book = (
        pregame_pairs.groupby("book")["game_id"].size().sort_values(ascending=False)
        if not pregame_pairs.empty
        else pd.Series(dtype=int)
    )
    selected_by_book = (
        current.groupby("book")["game_id"].nunique().sort_values(ascending=False)
        if not current.empty
        else pd.Series(dtype=int)
    )

    sides = _market_sides(market)
    best_books: list[dict[str, Any]] = []
    if not best.empty:
        for side in sides:
            book_col = f"best_{side}_book"
            if book_col not in best.columns:
                continue
            for _, row in best[["game_id", book_col]].dropna(subset=[book_col]).iterrows():
                best_books.append({"game_id": row["game_id"], "book": row[book_col], "side": side})
    best_books_df = pd.DataFrame(best_books)
    best_side_counts = (
        best_books_df.groupby("book")["side"].size().sort_values(ascending=False)
        if not best_books_df.empty
        else pd.Series(dtype=int)
    )
    best_game_counts = (
        best_books_df.groupby("book")["game_id"].nunique().sort_values(ascending=False)
        if not best_books_df.empty
        else pd.Series(dtype=int)
    )

    return {
        "usable_games_by_book": _count_records(usable_by_book, "games"),
        "usable_snapshot_rows_by_book": _count_records(usable_rows_by_book, "rows"),
        "selected_games_by_book": _count_records(selected_by_book, "games"),
        "best_side_count_by_book": _count_records(best_side_counts, "sides"),
        "best_games_by_book": _count_records(best_game_counts, "games"),
    }


def _selected_vs_best(current: pd.DataFrame, best: pd.DataFrame, market: str) -> dict[str, Any]:
    sides = _market_sides(market)
    if current.empty:
        return {
            "selected_book_games": 0,
            "best_book_any_side_games": 0,
            "selected_book_is_best_any_side_games": 0,
            "selected_book_is_best_all_sides_games": 0,
            "best_book_differs_any_side_games": 0,
            "best_book_missing_games": 0,
            "side_availability": {
                side: {
                    "selected_price_games": 0,
                    "best_price_games": 0,
                    "selected_book_is_best_games": 0,
                    "best_book_differs_games": 0,
                }
                for side in sides
            },
        }

    frame = current.merge(best, on="game_id", how="left") if not best.empty else current.copy()
    side_availability: dict[str, dict[str, int]] = {}
    best_any = pd.Series(False, index=frame.index)
    match_any = pd.Series(False, index=frame.index)
    differ_any = pd.Series(False, index=frame.index)
    match_all = pd.Series(True, index=frame.index)

    for side in sides:
        selected_price_col = f"{side}_moneyline"
        best_price_col = f"best_{side}_moneyline"
        best_book_col = f"best_{side}_book"
        if best_price_col not in frame.columns:
            frame[best_price_col] = np.nan
        if best_book_col not in frame.columns:
            frame[best_book_col] = np.nan
        selected_present = frame["book"].notna() & frame[selected_price_col].notna()
        best_present = frame[best_book_col].notna() & frame[best_price_col].notna()
        matches = best_present & selected_present & (
            frame[best_book_col].map(_book_key) == frame["book"].map(_book_key)
        )
        differs = best_present & selected_present & (
            frame[best_book_col].map(_book_key) != frame["book"].map(_book_key)
        )
        best_any = best_any | best_present
        match_any = match_any | matches
        differ_any = differ_any | differs
        match_all = match_all & matches
        side_availability[side] = {
            "selected_price_games": int(selected_present.sum()),
            "best_price_games": int(best_present.sum()),
            "selected_book_is_best_games": int(matches.sum()),
            "best_book_differs_games": int(differs.sum()),
        }

    return {
        "selected_book_games": int(frame["book"].notna().sum()),
        "best_book_any_side_games": int(best_any.sum()),
        "selected_book_is_best_any_side_games": int(match_any.sum()),
        "selected_book_is_best_all_sides_games": int(match_all.sum()),
        "best_book_differs_any_side_games": int(differ_any.sum()),
        "best_book_missing_games": int((~best_any).sum()),
        "side_availability": side_availability,
    }


def _closing_masks(settled: pd.DataFrame, market: str) -> tuple[pd.Series, pd.Series]:
    if settled.empty:
        empty = pd.Series(dtype=bool)
        return empty, empty
    if market == "totals":
        has_close = settled["total_close"].notna()
        return has_close, has_close
    any_close = settled["home_moneyline_close"].notna() | settled["away_moneyline_close"].notna()
    complete_close = settled["home_moneyline_close"].notna() & settled["away_moneyline_close"].notna()
    return any_close, complete_close


def _annotate_clv(current: pd.DataFrame, settled: pd.DataFrame, market: str) -> pd.DataFrame:
    if current.empty:
        return current.copy()
    close_columns = ["game_id", "total_close", "home_moneyline_close", "away_moneyline_close"]
    frame = current.merge(settled[close_columns], on="game_id", how="left")
    sides = _market_sides(market)
    if market == "totals":
        frame["over_clv"] = frame["total_close"] - frame["line"]
        frame["under_clv"] = frame["line"] - frame["total_close"]
    else:
        frame["home_clv"] = [
            _moneyline_clv(bet, close)
            for bet, close in zip(frame["home_moneyline"], frame["home_moneyline_close"])
        ]
        frame["away_clv"] = [
            _moneyline_clv(bet, close)
            for bet, close in zip(frame["away_moneyline"], frame["away_moneyline_close"])
        ]
    for side in sides:
        frame[f"has_{side}_clv"] = frame[f"{side}_clv"].notna()
    frame["has_clv"] = frame[[f"has_{side}_clv" for side in sides]].any(axis=1)
    return frame


def _hours_bucket_counts(current: pd.DataFrame) -> dict[str, int]:
    counts = {bucket: 0 for bucket in HOUR_BUCKETS}
    if current.empty:
        return counts
    values = current["hours_before_start"].apply(_hour_bucket).value_counts()
    for bucket, count in values.items():
        counts[str(bucket)] = int(count)
    return counts


def _rate(numerator: int, denominator: int) -> Optional[float]:
    if denominator <= 0:
        return None
    return float(numerator / denominator)


def _summarize_market_league(
    *,
    league: str,
    market: str,
    settled: pd.DataFrame,
    odds: pd.DataFrame,
    max_hours_before_start: Optional[float],
) -> dict[str, Any]:
    league_settled = settled[settled["league"].astype(str).str.upper() == league].copy()
    league_odds = odds[odds["league"].astype(str).str.upper() == league].copy()
    settled_count = int(league_settled["game_id"].nunique()) if not league_settled.empty else 0

    sql_market = MARKET_TO_SQL[market]
    market_odds = league_odds[league_odds["market"].astype(str).str.lower() == sql_market].copy()
    any_market_odds_games = int(market_odds["game_id"].nunique()) if not market_odds.empty else 0
    complete_pairs = _pairs_for_market(market_odds, market)
    complete_pair_games = int(complete_pairs["game_id"].nunique()) if not complete_pairs.empty else 0

    if complete_pairs.empty:
        pregame_pairs = complete_pairs.copy()
    else:
        pregame_mask = (
            complete_pairs["snapshot_time_utc"].notna()
            & complete_pairs["start_time_utc"].notna()
            & (complete_pairs["snapshot_time_utc"] <= complete_pairs["start_time_utc"])
        )
        pregame_pairs = complete_pairs[pregame_mask].copy()

    opening, current, latest_by_book = _select_opening_current(pregame_pairs)
    best = _best_by_game(market, latest_by_book, current)
    current_with_clv = _annotate_clv(current, league_settled, market)

    any_close_mask, complete_close_mask = _closing_masks(league_settled, market)
    games_with_usable_odds = int(pregame_pairs["game_id"].nunique()) if not pregame_pairs.empty else 0
    games_with_current_odds = int(current["game_id"].nunique()) if not current.empty else 0
    games_with_clv = int(current_with_clv["has_clv"].sum()) if not current_with_clv.empty else 0

    if current.empty:
        timing_qualified = 0
        stale_excluded = 0
        missing_timing_excluded = 0
        current_timing_mask = pd.Series(dtype=bool)
    else:
        hours = pd.to_numeric(current["hours_before_start"], errors="coerce")
        if max_hours_before_start is None:
            current_timing_mask = hours.notna()
            stale_excluded = 0
        else:
            current_timing_mask = hours.notna() & (hours <= max_hours_before_start)
            stale_excluded = int((hours.notna() & (hours > max_hours_before_start)).sum())
        missing_timing_excluded = int(hours.isna().sum())
        timing_qualified = int(current_timing_mask.sum())

    if current_with_clv.empty:
        clv_after_timing = 0
    else:
        clv_hours = pd.to_numeric(current_with_clv["hours_before_start"], errors="coerce")
        if max_hours_before_start is None:
            clv_timing_mask = clv_hours.notna()
        else:
            clv_timing_mask = clv_hours.notna() & (clv_hours <= max_hours_before_start)
        clv_after_timing = int((current_with_clv["has_clv"] & clv_timing_mask).sum())

    complete_pair_game_ids = (
        set(complete_pairs["game_id"].dropna().unique()) if not complete_pairs.empty else set()
    )
    pregame_pair_game_ids = (
        set(pregame_pairs["game_id"].dropna().unique()) if not pregame_pairs.empty else set()
    )
    post_start_only_games = len(complete_pair_game_ids - pregame_pair_game_ids)
    no_usable_odds_games = max(settled_count - games_with_usable_odds, 0)

    side_clv_counts = {
        side: (
            int(current_with_clv[f"has_{side}_clv"].sum())
            if not current_with_clv.empty and f"has_{side}_clv" in current_with_clv.columns
            else 0
        )
        for side in _market_sides(market)
    }

    sample_ready_games = clv_after_timing
    return {
        "league": league,
        "market": market,
        "sql_market": sql_market,
        "settled_games": settled_count,
        "games_with_any_odds": any_market_odds_games,
        "games_with_complete_market_pair": complete_pair_games,
        "games_with_usable_odds": games_with_usable_odds,
        "games_with_opening_odds": int(opening["game_id"].nunique()) if not opening.empty else 0,
        "games_with_current_odds": games_with_current_odds,
        "games_with_closing_odds": int(any_close_mask.sum()) if not any_close_mask.empty else 0,
        "games_with_complete_closing_odds": (
            int(complete_close_mask.sum()) if not complete_close_mask.empty else 0
        ),
        "games_with_clv": games_with_clv,
        "games_with_clv_after_timing": clv_after_timing,
        "sample_ready_games": sample_ready_games,
        "sample_ready_rate": _rate(sample_ready_games, settled_count),
        "games_without_usable_odds": no_usable_odds_games,
        "games_with_complete_pair_but_no_pregame_pair": post_start_only_games,
        "games_excluded_by_stale_odds_timing_filters": stale_excluded
        + missing_timing_excluded,
        "timing_filters": {
            "max_hours_before_start": max_hours_before_start,
            "stale_odds_excluded": stale_excluded,
            "missing_timing_excluded": missing_timing_excluded,
            "timing_qualified_games": timing_qualified,
        },
        "clv_side_counts": side_clv_counts,
        "book_coverage": _book_coverage(pregame_pairs, current, best, market),
        "hours_before_start_buckets": _hours_bucket_counts(current),
        "selected_vs_best": _selected_vs_best(current, best, market),
    }


def _empty_report(
    *,
    db_path: Path,
    leagues: list[str],
    markets: list[str],
    max_hours_before_start: Optional[float],
    error: str,
) -> dict[str, Any]:
    rows = [
        _summarize_market_league(
            league=league,
            market=market,
            settled=_empty_frame(SETTLED_COLUMNS),
            odds=_empty_frame(ODDS_COLUMNS),
            max_hours_before_start=max_hours_before_start,
        )
        for market in markets
        for league in leagues
    ]
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "db_path": str(db_path),
        "leagues": leagues,
        "markets": markets,
        "max_hours_before_start": max_hours_before_start,
        "error": error,
        "rows": rows,
    }


def build_odds_coverage_report(
    db_path: Path = DB_PATH,
    leagues: Optional[Iterable[str]] = DEFAULT_RELEASE_LEAGUES,
    markets: Optional[Iterable[str]] = DEFAULT_MARKETS,
    max_hours_before_start: Optional[float] = DEFAULT_MAX_HOURS_BEFORE_START,
) -> dict[str, Any]:
    """Return league/market odds coverage diagnostics for settled games."""
    db_path = Path(db_path)
    normalized_leagues = _split_csv(leagues, DEFAULT_RELEASE_LEAGUES)
    normalized_markets = _normalize_markets(markets)
    if not db_path.exists():
        return _empty_report(
            db_path=db_path,
            leagues=normalized_leagues,
            markets=normalized_markets,
            max_hours_before_start=max_hours_before_start,
            error=f"database not found: {db_path}",
        )

    settled = _read_settled_games(db_path, normalized_leagues)
    odds = _read_odds_rows(db_path, normalized_leagues, normalized_markets)
    rows = [
        _summarize_market_league(
            league=league,
            market=market,
            settled=settled,
            odds=odds,
            max_hours_before_start=max_hours_before_start,
        )
        for market in normalized_markets
        for league in normalized_leagues
    ]
    summary_by_market: dict[str, dict[str, int]] = {}
    for market in normalized_markets:
        market_rows = [row for row in rows if row["market"] == market]
        summary_by_market[market] = {
            "settled_games": int(sum(row["settled_games"] for row in market_rows)),
            "games_with_usable_odds": int(sum(row["games_with_usable_odds"] for row in market_rows)),
            "games_with_clv": int(sum(row["games_with_clv"] for row in market_rows)),
            "sample_ready_games": int(sum(row["sample_ready_games"] for row in market_rows)),
            "stale_or_missing_timing_excluded": int(
                sum(row["games_excluded_by_stale_odds_timing_filters"] for row in market_rows)
            ),
        }

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "db_path": str(db_path),
        "leagues": normalized_leagues,
        "markets": normalized_markets,
        "max_hours_before_start": max_hours_before_start,
        "rows": rows,
        "summary_by_market": summary_by_market,
    }


def _format_table(rows: list[dict[str, Any]]) -> str:
    headers = [
        "league",
        "market",
        "settled",
        "usable",
        "current",
        "closing",
        "clv",
        "stale",
        "ready",
    ]
    values = []
    for row in rows:
        values.append(
            [
                row["league"],
                row["market"],
                row["settled_games"],
                row["games_with_usable_odds"],
                row["games_with_current_odds"],
                row["games_with_closing_odds"],
                row["games_with_clv"],
                row["timing_filters"]["stale_odds_excluded"],
                row["sample_ready_games"],
            ]
        )
    widths = [
        max(len(str(header)), *(len(str(row[index])) for row in values)) if values else len(header)
        for index, header in enumerate(headers)
    ]
    lines = [
        "  ".join(str(header).ljust(widths[index]) for index, header in enumerate(headers)),
        "  ".join("-" * width for width in widths),
    ]
    for value_row in values:
        lines.append(
            "  ".join(str(value).ljust(widths[index]) for index, value in enumerate(value_row))
        )
    return "\n".join(lines)


def format_console_summary(report: dict[str, Any]) -> str:
    """Format the JSON report as a compact operator summary."""
    lines = [
        "Odds coverage audit",
        f"DB: {report['db_path']}",
        f"Max hours before start: {report.get('max_hours_before_start')}",
    ]
    if report.get("error"):
        lines.append(f"Error: {report['error']}")
    lines.append(_format_table(report.get("rows", [])))
    return "\n".join(lines)


def _json_default(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, (np.integer, np.floating)):
        return value.item()
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if pd.isna(value):
        return None
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def write_report(report: dict[str, Any], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2, default=_json_default), encoding="utf-8")
    return output_path


def _parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Audit settled-game odds, timing, book, and CLV coverage."
    )
    parser.add_argument("--db", type=Path, default=DB_PATH, help="SQLite database path.")
    parser.add_argument(
        "--leagues",
        default=",".join(DEFAULT_RELEASE_LEAGUES),
        help="Comma-separated leagues to audit.",
    )
    parser.add_argument(
        "--markets",
        default=",".join(DEFAULT_MARKETS),
        help="Comma-separated markets: totals,moneyline.",
    )
    parser.add_argument(
        "--max-hours-before-start",
        type=float,
        default=DEFAULT_MAX_HOURS_BEFORE_START,
        help="Strict timing cutoff used to count stale selected odds.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Optional JSON output path.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
    )
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> None:
    args = _parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level))
    leagues = _split_csv(args.leagues, DEFAULT_RELEASE_LEAGUES)
    markets = _normalize_markets(args.markets)
    report = build_odds_coverage_report(
        db_path=args.db,
        leagues=leagues,
        markets=markets,
        max_hours_before_start=args.max_hours_before_start,
    )
    print(format_console_summary(report))
    if args.output:
        write_report(report, args.output)
        print(f"JSON written to {args.output}")


if __name__ == "__main__":
    main()
