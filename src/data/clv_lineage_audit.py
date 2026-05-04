"""Audit totals closing-line value lineage.

The strict benchmark now prefers same-book close lines from odds snapshots.
This audit checks whether odds-backed rows have traceable pregame close lines
and separates those rows from older settled games that have no odds snapshots.
"""

from __future__ import annotations

import argparse
import json
import logging
import sqlite3
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

import numpy as np
import pandas as pd

from src.db.core import DB_PATH

LOGGER = logging.getLogger(__name__)

DEFAULT_LEAGUES = ("NBA",)
DEFAULT_MARKET = "totals"
DEFAULT_MAX_HOURS_BEFORE_START = 72.0
DEFAULT_LINE_TOLERANCE = 0.01
DEFAULT_TOP_N = 25
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
EXPECTED_CLOSE_LINEAGE_FIELDS = (
    "close_snapshot_id",
    "close_snapshot_time_utc",
    "close_book",
    "close_book_id",
    "close_source",
)
SETTLED_COLUMNS = [
    "game_id",
    "league",
    "start_time_utc",
    "home_team",
    "away_team",
    "home_score",
    "away_score",
    "total_close",
    "total_close_snapshot_id",
    "total_close_snapshot_time_utc",
    "total_close_book_id",
    "total_close_book",
    "total_close_source",
    "source_version",
]
TOTALS_PAIR_COLUMNS = [
    "game_id",
    "league",
    "start_time_utc",
    "snapshot_id",
    "snapshot_time_utc",
    "snapshot_source",
    "raw_path",
    "book_id",
    "book",
    "line",
    "over_moneyline",
    "under_moneyline",
]


def _split_csv(value: str | Iterable[str] | None, default: Iterable[str]) -> list[str]:
    if value is None:
        raw = list(default)
    elif isinstance(value, str):
        raw = value.split(",")
    else:
        raw = list(value)
    return [str(item).strip().upper() for item in raw if str(item).strip()]


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


def _ensure_total_close_lineage_columns(db_path: Path) -> None:
    with sqlite3.connect(str(db_path)) as conn:
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
        if "game_results" not in tables:
            return
        existing = {
            row[1] for row in conn.execute("PRAGMA table_info(game_results)").fetchall()
        }
        ddl = {
            "total_close_snapshot_id": "TEXT",
            "total_close_snapshot_time_utc": "TEXT",
            "total_close_book_id": "INTEGER",
            "total_close_book": "TEXT",
            "total_close_source": "TEXT",
        }
        for column, column_ddl in ddl.items():
            if column not in existing:
                conn.execute(f"ALTER TABLE game_results ADD COLUMN {column} {column_ddl}")


def _book_rank(book_name: Any) -> int:
    normalized = str(book_name or "").strip().lower()
    try:
        return BOOK_PRIORITY.index(normalized)
    except ValueError:
        return len(BOOK_PRIORITY)


def _book_key(book_name: Any) -> str:
    return str(book_name or "").strip().lower()


def _iso(value: Any) -> str | None:
    if pd.isna(value):
        return None
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize("UTC")
    return timestamp.isoformat()


def _float_or_none(value: Any) -> float | None:
    if value is None or pd.isna(value):
        return None
    return float(value)


def _read_settled_games(db_path: Path, leagues: list[str]) -> pd.DataFrame:
    league_sql, params = _league_filter_sql(leagues)
    query = f"""
        SELECT
            g.game_id,
            UPPER(s.league) AS league,
            g.start_time_utc,
            ht.name AS home_team,
            at.name AS away_team,
            gr.home_score,
            gr.away_score,
            gr.total_close,
            gr.total_close_snapshot_id,
            gr.total_close_snapshot_time_utc,
            gr.total_close_book_id,
            gr.total_close_book,
            gr.total_close_source,
            gr.source_version
        FROM games g
        JOIN sports s ON g.sport_id = s.sport_id
        JOIN teams ht ON g.home_team_id = ht.team_id
        JOIN teams at ON g.away_team_id = at.team_id
        JOIN game_results gr ON g.game_id = gr.game_id
        WHERE gr.home_score IS NOT NULL
          AND gr.away_score IS NOT NULL
          {league_sql}
    """
    settled = _read_sql(db_path, query, params)
    if settled.empty:
        return _empty_frame(SETTLED_COLUMNS)
    settled = _ensure_columns(settled, SETTLED_COLUMNS)
    settled["start_time_utc"] = pd.to_datetime(
        settled["start_time_utc"],
        utc=True,
        errors="coerce",
        format="mixed",
    )
    settled["total_close_snapshot_time_utc"] = pd.to_datetime(
        settled["total_close_snapshot_time_utc"],
        utc=True,
        errors="coerce",
        format="mixed",
    )
    for column in ["home_score", "away_score", "total_close"]:
        settled[column] = pd.to_numeric(settled[column], errors="coerce")
    return settled[SETTLED_COLUMNS]


def _read_totals_pairs(db_path: Path, leagues: list[str]) -> pd.DataFrame:
    league_sql, params = _league_filter_sql(leagues)
    query = f"""
        SELECT
            g.game_id,
            UPPER(s.league) AS league,
            g.start_time_utc,
            os.snapshot_id,
            os.fetched_at_utc AS snapshot_time_utc,
            os.source AS snapshot_source,
            os.raw_path,
            o.book_id,
            b.name AS book,
            o.line,
            MAX(CASE WHEN LOWER(o.outcome) = 'over' THEN o.price_american END)
                AS over_moneyline,
            MAX(CASE WHEN LOWER(o.outcome) = 'under' THEN o.price_american END)
                AS under_moneyline
        FROM odds o
        JOIN odds_snapshots os ON o.snapshot_id = os.snapshot_id
        JOIN games g ON o.game_id = g.game_id
        JOIN sports s ON g.sport_id = s.sport_id
        JOIN books b ON o.book_id = b.book_id
        JOIN game_results gr ON g.game_id = gr.game_id
        WHERE LOWER(o.market) = 'totals'
          AND o.price_american IS NOT NULL
          AND o.line IS NOT NULL
          AND gr.home_score IS NOT NULL
          AND gr.away_score IS NOT NULL
          {league_sql}
        GROUP BY
            g.game_id, s.league, g.start_time_utc, os.snapshot_id,
            os.fetched_at_utc, os.source, os.raw_path, o.book_id, b.name, o.line
        HAVING over_moneyline IS NOT NULL AND under_moneyline IS NOT NULL
    """
    odds = _read_sql(db_path, query, params)
    if odds.empty:
        return _empty_frame(TOTALS_PAIR_COLUMNS)
    odds = _ensure_columns(odds, TOTALS_PAIR_COLUMNS)
    odds["start_time_utc"] = pd.to_datetime(
        odds["start_time_utc"],
        utc=True,
        errors="coerce",
        format="mixed",
    )
    odds["snapshot_time_utc"] = pd.to_datetime(
        odds["snapshot_time_utc"],
        utc=True,
        errors="coerce",
        format="mixed",
    )
    for column in ["line", "over_moneyline", "under_moneyline"]:
        odds[column] = pd.to_numeric(odds[column], errors="coerce")
    return odds.dropna(subset=["line", "over_moneyline", "under_moneyline"])[
        TOTALS_PAIR_COLUMNS
    ]


def _with_timing(pairs: pd.DataFrame) -> pd.DataFrame:
    if pairs.empty:
        return pairs.copy()
    updated = pairs.copy()
    updated["book_rank"] = updated["book"].map(_book_rank)
    updated["hours_before_start"] = (
        updated["start_time_utc"] - updated["snapshot_time_utc"]
    ).dt.total_seconds() / 3600.0
    return updated


def _select_current(pregame_pairs: pd.DataFrame) -> pd.DataFrame:
    if pregame_pairs.empty:
        return pregame_pairs.copy()
    ranked = _with_timing(pregame_pairs)
    return (
        ranked.sort_values(
            ["snapshot_time_utc", "book_rank", "book"],
            ascending=[False, True, True],
        )
        .drop_duplicates("game_id", keep="first")
        .copy()
    )


def _latest_by_book(pregame_pairs: pd.DataFrame) -> pd.DataFrame:
    if pregame_pairs.empty:
        return pregame_pairs.copy()
    ranked = _with_timing(pregame_pairs)
    return (
        ranked.sort_values(
            ["snapshot_time_utc", "book_rank", "book"],
            ascending=[True, True, True],
        )
        .drop_duplicates(["game_id", "book"], keep="last")
        .copy()
    )


def _best_side_books(latest_by_book: pd.DataFrame, current: pd.Series | None) -> dict[str, Any]:
    result: dict[str, Any] = {
        "best_over_book": None,
        "best_over_moneyline": None,
        "best_under_book": None,
        "best_under_moneyline": None,
        "best_book_differs_any_side": False,
    }
    if latest_by_book.empty or current is None:
        return result
    game_id = current["game_id"]
    selected_line = current.get("line")
    group = latest_by_book[latest_by_book["game_id"] == game_id].copy()
    if group.empty:
        return result
    scoped = group[np.isclose(group["line"], selected_line, equal_nan=False)].copy()
    if scoped.empty:
        scoped = group
    selected_book_key = _book_key(current.get("book"))
    differs_any = False
    for side in ("over", "under"):
        price_col = f"{side}_moneyline"
        priced = scoped.dropna(subset=[price_col]).copy()
        if priced.empty:
            continue
        best = priced.sort_values([price_col, "book_rank", "book"], ascending=[False, True, True])
        row = best.iloc[0]
        result[f"best_{side}_book"] = row["book"]
        result[f"best_{side}_moneyline"] = float(row[price_col])
        differs_any = differs_any or _book_key(row["book"]) != selected_book_key
    result["best_book_differs_any_side"] = bool(differs_any)
    return result


def _line_matches(values: pd.Series, target: Any, tolerance: float) -> pd.Series:
    if pd.isna(target):
        return pd.Series(False, index=values.index)
    return (pd.to_numeric(values, errors="coerce") - float(target)).abs() <= tolerance


def _summarize_close_candidates(
    pregame_pairs: pd.DataFrame,
    *,
    game_id: str,
    total_close: Any,
    selected_book: Any,
    current_snapshot_time: Any,
    tolerance: float,
) -> dict[str, Any]:
    empty = {
        "close_candidate_count": 0,
        "close_candidate_books": [],
        "close_candidate_sources": [],
        "latest_close_candidate_snapshot_time_utc": None,
        "latest_close_candidate_hours_before_start": None,
        "selected_book_matches_total_close": False,
        "close_candidate_older_than_current": False,
        "selected_book_not_close_candidate": False,
    }
    if pd.isna(total_close) or pregame_pairs.empty:
        return empty
    group = pregame_pairs[pregame_pairs["game_id"] == game_id].copy()
    if group.empty:
        return empty
    candidates = group[_line_matches(group["line"], total_close, tolerance)].copy()
    if candidates.empty:
        return empty | {"selected_book_not_close_candidate": bool(pd.notna(selected_book))}

    candidates = _with_timing(candidates)
    latest = candidates.sort_values(
        ["snapshot_time_utc", "book_rank", "book"],
        ascending=[False, True, True],
    ).iloc[0]
    candidate_book_keys = {_book_key(book) for book in candidates["book"].dropna()}
    selected_key = _book_key(selected_book)
    latest_snapshot = latest["snapshot_time_utc"]
    current_snapshot = pd.Timestamp(current_snapshot_time) if pd.notna(current_snapshot_time) else None
    return {
        "close_candidate_count": int(len(candidates)),
        "close_candidate_books": sorted(str(book) for book in candidates["book"].dropna().unique()),
        "close_candidate_sources": sorted(
            str(source) for source in candidates["snapshot_source"].dropna().unique()
        ),
        "latest_close_candidate_snapshot_time_utc": _iso(latest_snapshot),
        "latest_close_candidate_hours_before_start": _float_or_none(
            latest.get("hours_before_start")
        ),
        "selected_book_matches_total_close": bool(selected_key in candidate_book_keys),
        "close_candidate_older_than_current": bool(
            current_snapshot is not None and latest_snapshot < current_snapshot
        ),
        "selected_book_not_close_candidate": bool(
            selected_key and selected_key not in candidate_book_keys
        ),
    }


def _row_issues(row: dict[str, Any]) -> list[str]:
    issues = []
    if not row["has_pregame_totals_pair"]:
        issues.append("no_pregame_totals_pair")
    if not row["has_total_close"]:
        issues.append("missing_total_close")
    if row["has_total_close"] and not row["has_persisted_total_close_provenance"]:
        issues.append("close_provenance_not_stored")
    if row["has_total_close"] and not row["total_close_observed_in_pregame_odds"]:
        issues.append("total_close_not_observed_in_pregame_odds")
    if row["is_stale_current_snapshot"]:
        issues.append("stale_current_snapshot")
    if row["close_candidate_older_than_current"]:
        issues.append("close_candidate_older_than_current")
    if row["selected_book_not_close_candidate"]:
        issues.append("selected_book_not_close_candidate")
    return issues


def _lineage_rows(
    settled: pd.DataFrame,
    pairs: pd.DataFrame,
    *,
    max_hours_before_start: Optional[float],
    line_match_tolerance: float,
) -> list[dict[str, Any]]:
    if settled.empty:
        return []
    pregame_pairs = pairs[
        pairs["snapshot_time_utc"].notna()
        & pairs["start_time_utc"].notna()
        & (pairs["snapshot_time_utc"] <= pairs["start_time_utc"])
    ].copy()
    current = _select_current(pregame_pairs)
    latest_books = _latest_by_book(pregame_pairs)
    current_by_game = {row["game_id"]: row for _, row in current.iterrows()}

    rows = []
    for _, game in settled.sort_values(["league", "start_time_utc", "game_id"]).iterrows():
        game_id = str(game["game_id"])
        current_row = current_by_game.get(game_id)
        has_pregame = not pregame_pairs[pregame_pairs["game_id"] == game_id].empty
        best = _best_side_books(latest_books, current_row)
        total_close = game.get("total_close")
        persisted_close_book = game.get("total_close_book")
        persisted_close_time = game.get("total_close_snapshot_time_utc")
        persisted_close_snapshot = game.get("total_close_snapshot_id")
        persisted_close_source = game.get("total_close_source")
        has_persisted_provenance = bool(
            pd.notna(persisted_close_snapshot)
            and pd.notna(persisted_close_time)
            and pd.notna(persisted_close_book)
        )
        close = _summarize_close_candidates(
            pregame_pairs,
            game_id=game_id,
            total_close=total_close,
            selected_book=None if current_row is None else current_row.get("book"),
            current_snapshot_time=None
            if current_row is None
            else current_row.get("snapshot_time_utc"),
            tolerance=line_match_tolerance,
        )
        current_hours = None if current_row is None else _float_or_none(current_row.get("hours_before_start"))
        selected_line = None if current_row is None else _float_or_none(current_row.get("line"))
        total_close_float = _float_or_none(total_close)
        line_delta = (
            None
            if selected_line is None or total_close_float is None
            else float(total_close_float - selected_line)
        )
        if current_hours is None or max_hours_before_start is None:
            stale = False
        else:
            stale = current_hours > max_hours_before_start
        row = {
            "game_id": game_id,
            "league": game.get("league"),
            "start_time_utc": _iso(game.get("start_time_utc")),
            "home_team": game.get("home_team"),
            "away_team": game.get("away_team"),
            "actual_total": (
                None
                if pd.isna(game.get("home_score")) or pd.isna(game.get("away_score"))
                else float(game["home_score"] + game["away_score"])
            ),
            "has_pregame_totals_pair": bool(has_pregame),
            "has_current_totals_pair": current_row is not None,
            "selected_snapshot_id": None if current_row is None else current_row.get("snapshot_id"),
            "selected_snapshot_time_utc": None
            if current_row is None
            else _iso(current_row.get("snapshot_time_utc")),
            "selected_snapshot_source": None
            if current_row is None
            else current_row.get("snapshot_source"),
            "selected_book": None if current_row is None else current_row.get("book"),
            "selected_line": selected_line,
            "selected_over_moneyline": None
            if current_row is None
            else _float_or_none(current_row.get("over_moneyline")),
            "selected_under_moneyline": None
            if current_row is None
            else _float_or_none(current_row.get("under_moneyline")),
            "hours_before_start": current_hours,
            "is_stale_current_snapshot": bool(stale),
            "total_close": total_close_float,
            "has_total_close": total_close_float is not None,
            "total_close_snapshot_id": None
            if pd.isna(persisted_close_snapshot)
            else persisted_close_snapshot,
            "total_close_snapshot_time_utc": _iso(persisted_close_time),
            "total_close_book_id": None
            if pd.isna(game.get("total_close_book_id"))
            else int(game.get("total_close_book_id")),
            "total_close_book": None if pd.isna(persisted_close_book) else persisted_close_book,
            "total_close_source": None
            if pd.isna(persisted_close_source)
            else persisted_close_source,
            "has_persisted_total_close_provenance": has_persisted_provenance,
            "persisted_close_book_matches_selected_book": bool(
                has_persisted_provenance
                and current_row is not None
                and _book_key(persisted_close_book) == _book_key(current_row.get("book"))
            ),
            "total_close_source_version": game.get("source_version"),
            "line_delta_to_total_close": line_delta,
            "over_clv_from_total_close": line_delta,
            "under_clv_from_total_close": None if line_delta is None else -line_delta,
            "total_close_observed_in_pregame_odds": close["close_candidate_count"] > 0,
            **close,
            **best,
        }
        row["issues"] = _row_issues(row)
        rows.append(row)
    return rows


def _issue_counts(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts: Counter[str] = Counter()
    for row in rows:
        counts.update(row.get("issues", []))
    return [
        {"issue": issue, "game_count": count}
        for issue, count in sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    ]


def _clv_distribution(rows: list[dict[str, Any]]) -> dict[str, Any]:
    values = [row["over_clv_from_total_close"] for row in rows if row["over_clv_from_total_close"] is not None]
    if not values:
        return {
            "count": 0,
            "mean_over_clv": None,
            "median_over_clv": None,
            "mean_absolute_clv": None,
            "zero_clv_games": 0,
        }
    series = pd.Series(values, dtype=float)
    return {
        "count": int(len(series)),
        "mean_over_clv": float(series.mean()),
        "median_over_clv": float(series.median()),
        "mean_absolute_clv": float(series.abs().mean()),
        "zero_clv_games": int((series.abs() <= DEFAULT_LINE_TOLERANCE).sum()),
    }


def _recommendation(rows: list[dict[str, Any]]) -> str:
    odds_backed_rows = [row for row in rows if row["has_pregame_totals_pair"]]
    issue_names = {issue["issue"] for issue in _issue_counts(odds_backed_rows)}
    blocking = {
        "total_close_not_observed_in_pregame_odds",
        "stale_current_snapshot",
        "close_candidate_older_than_current",
    }
    if issue_names & blocking:
        return "fix_close_line_lineage_before_trusting_clv"
    if "close_provenance_not_stored" in issue_names:
        return "lineage_usable_but_store_close_book_and_snapshot_before_launch"
    if any(not row["has_pregame_totals_pair"] for row in rows):
        return "lineage_usable_for_odds_backed_rows_historical_gaps_remain"
    return "lineage_acceptable"


def _summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    settled_games = len(rows)
    odds_backed_rows = [row for row in rows if row["has_pregame_totals_pair"]]
    historical_gap_rows = [row for row in rows if not row["has_pregame_totals_pair"]]
    return {
        "settled_games": settled_games,
        "games_with_pregame_totals_pair": sum(row["has_pregame_totals_pair"] for row in rows),
        "games_with_current_totals_pair": sum(row["has_current_totals_pair"] for row in rows),
        "games_with_total_close": sum(row["has_total_close"] for row in rows),
        "games_with_persisted_total_close_provenance": sum(
            row["has_persisted_total_close_provenance"] for row in rows
        ),
        "games_with_total_close_observed_in_pregame_odds": sum(
            row["total_close_observed_in_pregame_odds"] for row in rows
        ),
        "games_where_persisted_close_book_matches_selected_book": sum(
            row["persisted_close_book_matches_selected_book"] for row in rows
        ),
        "games_where_selected_book_matches_total_close": sum(
            row["selected_book_matches_total_close"] for row in rows
        ),
        "games_where_selected_book_not_close_candidate": sum(
            row["selected_book_not_close_candidate"] for row in rows
        ),
        "stale_current_snapshot_games": sum(row["is_stale_current_snapshot"] for row in rows),
        "close_candidate_older_than_current_games": sum(
            row["close_candidate_older_than_current"] for row in rows
        ),
        "selected_vs_best_book_differs_any_side_games": sum(
            row["best_book_differs_any_side"] for row in rows
        ),
        "issue_counts": _issue_counts(rows),
        "odds_backed_settled_games": len(odds_backed_rows),
        "odds_backed_games_with_persisted_total_close_provenance": sum(
            row["has_persisted_total_close_provenance"] for row in odds_backed_rows
        ),
        "odds_backed_issue_counts": _issue_counts(odds_backed_rows),
        "historical_gap_games_without_pregame_totals_pair": len(historical_gap_rows),
        "historical_gap_issue_counts": _issue_counts(historical_gap_rows),
        "selected_line_clv_distribution": _clv_distribution(rows),
    }


def _problem_examples(rows: list[dict[str, Any]], top_n: int) -> list[dict[str, Any]]:
    problem_rows = [row for row in rows if row.get("issues")]
    problem_rows.sort(
        key=lambda row: (
            len(row.get("issues", [])),
            row.get("is_stale_current_snapshot"),
            row.get("close_candidate_older_than_current"),
            row.get("game_id"),
        ),
        reverse=True,
    )
    keys = [
        "game_id",
        "league",
        "start_time_utc",
        "selected_book",
        "selected_snapshot_time_utc",
        "hours_before_start",
        "selected_line",
        "total_close",
        "total_close_book",
        "total_close_snapshot_time_utc",
        "latest_close_candidate_snapshot_time_utc",
        "latest_close_candidate_hours_before_start",
        "close_candidate_books",
        "line_delta_to_total_close",
        "issues",
    ]
    return [{key: row.get(key) for key in keys} for row in problem_rows[:top_n]]


def build_clv_lineage_report(
    db_path: Path = DB_PATH,
    leagues: Optional[Iterable[str]] = DEFAULT_LEAGUES,
    market: str = DEFAULT_MARKET,
    max_hours_before_start: Optional[float] = DEFAULT_MAX_HOURS_BEFORE_START,
    line_match_tolerance: float = DEFAULT_LINE_TOLERANCE,
    top_n: int = DEFAULT_TOP_N,
) -> dict[str, Any]:
    """Build a totals CLV lineage report from settled games and odds snapshots."""
    if market.lower() != "totals":
        raise ValueError("CLV lineage audit currently supports market='totals' only")
    db_path = Path(db_path)
    normalized_leagues = _split_csv(leagues, DEFAULT_LEAGUES)
    if not db_path.exists():
        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "db_path": str(db_path),
            "leagues": normalized_leagues,
            "market": "totals",
            "max_hours_before_start": max_hours_before_start,
            "line_match_tolerance": line_match_tolerance,
            "error": f"database not found: {db_path}",
            "summary": _summary([]),
            "game_lineage": [],
            "problem_examples": [],
            "recommendation": "fix_close_line_lineage_before_trusting_clv",
        }

    _ensure_total_close_lineage_columns(db_path)
    settled = _read_settled_games(db_path, normalized_leagues)
    pairs = _read_totals_pairs(db_path, normalized_leagues)
    rows = _lineage_rows(
        settled,
        pairs,
        max_hours_before_start=max_hours_before_start,
        line_match_tolerance=line_match_tolerance,
    )
    source_limitations = []
    if any(
        row["has_total_close"] and not row["has_persisted_total_close_provenance"]
        for row in rows
    ):
        source_limitations.append(
            (
                "Some game_results.total_close rows still have no close book, close "
                "snapshot id, or close snapshot timestamp, so exact same-book CLV "
                "cannot be proven for those rows."
            )
        )
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "db_path": str(db_path),
        "leagues": normalized_leagues,
        "market": "totals",
        "max_hours_before_start": max_hours_before_start,
        "line_match_tolerance": line_match_tolerance,
        "expected_close_lineage_fields": list(EXPECTED_CLOSE_LINEAGE_FIELDS),
        "source_limitations": source_limitations,
        "summary": _summary(rows),
        "game_lineage": rows,
        "problem_examples": _problem_examples(rows, top_n),
        "recommendation": _recommendation(rows),
    }


def _format_issue_counts(issue_counts: list[dict[str, Any]]) -> str:
    if not issue_counts:
        return "none"
    return ", ".join(f"{item['issue']}={item['game_count']}" for item in issue_counts)


def format_console_summary(report: dict[str, Any]) -> str:
    summary = report.get("summary", {})
    lines = [
        "CLV lineage audit",
        f"DB: {report['db_path']}",
        f"Leagues: {','.join(report.get('leagues', []))}",
        f"Market: {report.get('market')}",
        f"Settled games: {summary.get('settled_games', 0)}",
        (
            "Pregame/current/close observed: "
            f"{summary.get('games_with_pregame_totals_pair', 0)}/"
            f"{summary.get('games_with_current_totals_pair', 0)}/"
            f"{summary.get('games_with_total_close_observed_in_pregame_odds', 0)}"
        ),
        (
            "Persisted close provenance: "
            f"{summary.get('games_with_persisted_total_close_provenance', 0)}"
        ),
        f"Stale current snapshots: {summary.get('stale_current_snapshot_games', 0)}",
        f"Issues: {_format_issue_counts(summary.get('issue_counts', []))}",
        f"Odds-backed issues: {_format_issue_counts(summary.get('odds_backed_issue_counts', []))}",
        f"Recommendation: {report.get('recommendation')}",
    ]
    if report.get("error"):
        lines.append(f"Error: {report['error']}")
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


def _default_output_path() -> Path:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return Path("reports/data_quality") / f"clv_lineage_{timestamp}.json"


def _parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Audit totals CLV line/source lineage.")
    parser.add_argument("--db", type=Path, default=DB_PATH, help="SQLite database path.")
    parser.add_argument(
        "--league",
        "--leagues",
        dest="leagues",
        default=",".join(DEFAULT_LEAGUES),
        help="Comma-separated leagues to audit.",
    )
    parser.add_argument("--market", default=DEFAULT_MARKET, choices=["totals"])
    parser.add_argument(
        "--max-hours-before-start",
        type=float,
        default=DEFAULT_MAX_HOURS_BEFORE_START,
        help="Current selected odds older than this are flagged as stale.",
    )
    parser.add_argument(
        "--line-match-tolerance",
        type=float,
        default=DEFAULT_LINE_TOLERANCE,
        help="Tolerance for matching total_close to odds snapshot lines.",
    )
    parser.add_argument("--top-n", type=int, default=DEFAULT_TOP_N)
    parser.add_argument("--output", type=Path, default=None, help="Optional JSON output path.")
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
    )
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = _parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level))
    report = build_clv_lineage_report(
        db_path=args.db,
        leagues=_split_csv(args.leagues, DEFAULT_LEAGUES),
        market=args.market,
        max_hours_before_start=args.max_hours_before_start,
        line_match_tolerance=args.line_match_tolerance,
        top_n=args.top_n,
    )
    print(format_console_summary(report))
    output_path = args.output or _default_output_path()
    write_report(report, output_path)
    print(f"JSON written to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
