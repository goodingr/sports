"""Data quality checks for the launch prediction pipeline."""

from __future__ import annotations

import argparse
import logging
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional

from src.db.core import DB_PATH
from src.predict.config import SUPPORTED_LEAGUES

LOGGER = logging.getLogger(__name__)


@dataclass
class CheckResult:
    name: str
    passed: bool
    count: int
    detail: str

    @property
    def status(self) -> str:
        return "PASS" if self.passed else "FAIL"


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _league_clause(alias: str, leagues: Optional[Iterable[str]], params: List[object]) -> str:
    if not leagues:
        return ""
    normalized = sorted({league.upper() for league in leagues})
    placeholders = ",".join("?" for _ in normalized)
    params.extend(normalized)
    return f" AND {alias}.league IN ({placeholders})"


def check_orphan_results(conn: sqlite3.Connection) -> CheckResult:
    count = conn.execute(
        """
        SELECT COUNT(*)
        FROM game_results gr
        LEFT JOIN games g ON g.game_id = gr.game_id
        WHERE g.game_id IS NULL
        """
    ).fetchone()[0]
    return CheckResult("orphan_results", count == 0, count, "game_results rows without a games row")


def check_orphan_predictions(conn: sqlite3.Connection) -> CheckResult:
    count = conn.execute(
        """
        SELECT COUNT(*)
        FROM predictions p
        LEFT JOIN games g ON g.game_id = p.game_id
        WHERE g.game_id IS NULL
        """
    ).fetchone()[0]
    return CheckResult("orphan_predictions", count == 0, count, "predictions rows without a games row")


def check_duplicate_games(conn: sqlite3.Connection, leagues: Optional[Iterable[str]]) -> CheckResult:
    params: List[object] = []
    league_filter = _league_clause("s", leagues, params)
    count = conn.execute(
        f"""
        SELECT COUNT(*)
        FROM (
            SELECT
                g.sport_id,
                date(g.start_time_utc) AS game_date,
                g.home_team_id,
                g.away_team_id,
                COUNT(*) AS duplicate_count
            FROM games g
            JOIN sports s ON s.sport_id = g.sport_id
            WHERE g.start_time_utc IS NOT NULL
              {league_filter}
            GROUP BY g.sport_id, game_date, g.home_team_id, g.away_team_id
            HAVING COUNT(*) > 1
        )
        """,
        params,
    ).fetchone()[0]
    return CheckResult("duplicate_games", count == 0, count, "same league/date/home/away appears more than once")


def check_stale_games(conn: sqlite3.Connection, leagues: Optional[Iterable[str]], stale_hours: int) -> CheckResult:
    params: List[object] = [f"-{stale_hours} hours"]
    league_filter = _league_clause("s", leagues, params)
    count = conn.execute(
        f"""
        SELECT COUNT(*)
        FROM games g
        JOIN sports s ON s.sport_id = g.sport_id
        WHERE g.start_time_utc IS NOT NULL
          AND julianday(g.start_time_utc) < julianday('now', ?)
          AND COALESCE(g.status, 'scheduled') != 'final'
          {league_filter}
        """,
        params,
    ).fetchone()[0]
    return CheckResult("stale_games", count == 0, count, f"games older than {stale_hours}h not marked final")


def check_missing_scores(conn: sqlite3.Connection, leagues: Optional[Iterable[str]], stale_hours: int) -> CheckResult:
    params: List[object] = [f"-{stale_hours} hours"]
    league_filter = _league_clause("s", leagues, params)
    count = conn.execute(
        f"""
        SELECT COUNT(*)
        FROM games g
        JOIN sports s ON s.sport_id = g.sport_id
        LEFT JOIN game_results gr ON gr.game_id = g.game_id
        WHERE g.start_time_utc IS NOT NULL
          AND julianday(g.start_time_utc) < julianday('now', ?)
          AND (gr.home_score IS NULL OR gr.away_score IS NULL)
          {league_filter}
        """,
        params,
    ).fetchone()[0]
    return CheckResult("missing_scores", count == 0, count, f"past games older than {stale_hours}h missing final scores")


def check_odds_freshness(
    conn: sqlite3.Connection,
    leagues: Optional[Iterable[str]],
    max_age_hours: int,
) -> CheckResult:
    target_leagues = sorted({league.upper() for league in leagues}) if leagues else SUPPORTED_LEAGUES
    stale = []
    for league in target_leagues:
        row = conn.execute(
            """
            SELECT
                MAX(os.fetched_at_utc) AS latest_fetched,
                COUNT(DISTINCT CASE WHEN julianday(g.start_time_utc) >= julianday('now') THEN g.game_id END) AS future_games
            FROM sports s
            LEFT JOIN odds_snapshots os ON os.sport_id = s.sport_id
            LEFT JOIN games g ON g.sport_id = s.sport_id
            WHERE s.league = ?
            """,
            (league,),
        ).fetchone()
        if not row or row["future_games"] == 0:
            continue
        latest = row["latest_fetched"]
        if latest is None:
            stale.append(f"{league}: no odds snapshot")
            continue
        age = conn.execute(
            "SELECT (julianday('now') - julianday(?)) * 24.0",
            (latest,),
        ).fetchone()[0]
        if age is None or age > max_age_hours:
            stale.append(f"{league}: {age:.1f}h old" if age is not None else f"{league}: unknown age")
    return CheckResult(
        "odds_freshness",
        not stale,
        len(stale),
        f"leagues with future games and odds older than {max_age_hours}h" + (f" ({'; '.join(stale[:5])})" if stale else ""),
    )


def check_future_games_without_odds(
    conn: sqlite3.Connection,
    leagues: Optional[Iterable[str]],
    window_days: int,
) -> CheckResult:
    params: List[object] = [f"+{window_days} days"]
    league_filter = _league_clause("s", leagues, params)
    count = conn.execute(
        f"""
        SELECT COUNT(*)
        FROM games g
        JOIN sports s ON s.sport_id = g.sport_id
        WHERE g.start_time_utc IS NOT NULL
          AND julianday(g.start_time_utc) >= julianday('now')
          AND julianday(g.start_time_utc) <= julianday('now', ?)
          AND COALESCE(g.status, 'scheduled') != 'final'
          AND NOT EXISTS (
              SELECT 1
              FROM odds o
              WHERE o.game_id = g.game_id
                AND o.market IN ('h2h', 'totals', 'spreads')
                AND o.price_american IS NOT NULL
          )
          {league_filter}
        """,
        params,
    ).fetchone()[0]
    return CheckResult("future_games_without_odds", count == 0, count, f"future games in next {window_days}d without usable odds")


def run_checks(
    db_path: Path = DB_PATH,
    *,
    leagues: Optional[Iterable[str]] = None,
    stale_hours: int = 6,
    odds_max_age_hours: int = 12,
    future_window_days: int = 14,
) -> List[CheckResult]:
    with _connect(db_path) as conn:
        return [
            check_orphan_results(conn),
            check_orphan_predictions(conn),
            check_duplicate_games(conn, leagues),
            check_stale_games(conn, leagues, stale_hours),
            check_missing_scores(conn, leagues, stale_hours),
            check_odds_freshness(conn, leagues, odds_max_age_hours),
            check_future_games_without_odds(conn, leagues, future_window_days),
        ]


def format_summary(results: List[CheckResult]) -> str:
    lines = ["DATA QUALITY SUMMARY"]
    for result in results:
        lines.append(f"{result.status:<4} {result.name:<28} count={result.count:<5} {result.detail}")
    failed = [result for result in results if not result.passed]
    lines.append(f"OVERALL {'PASS' if not failed else 'FAIL'} ({len(failed)} failing checks)")
    return "\n".join(lines)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run data-quality checks for games, odds, scores, and predictions.")
    parser.add_argument("--db-path", type=Path, default=DB_PATH)
    parser.add_argument("--leagues", nargs="+", default=None)
    parser.add_argument("--stale-hours", type=int, default=6)
    parser.add_argument("--odds-max-age-hours", type=int, default=12)
    parser.add_argument("--future-window-days", type=int, default=14)
    parser.add_argument("--warn-only", action="store_true", help="Always exit 0 after printing the summary")
    parser.add_argument("--log-level", default="WARNING", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"])
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level))
    leagues = args.leagues
    if leagues and len(leagues) == 1 and "," in leagues[0]:
        leagues = [league.strip() for league in leagues[0].split(",") if league.strip()]
    results = run_checks(
        args.db_path,
        leagues=leagues,
        stale_hours=args.stale_hours,
        odds_max_age_hours=args.odds_max_age_hours,
        future_window_days=args.future_window_days,
    )
    print(format_summary(results))
    if not args.warn_only and any(not result.passed for result in results):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
