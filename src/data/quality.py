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

TERMINAL_NO_SCORE_STATUSES = {
    "canceled",
    "cancelled",
    "postponed",
    "closed_missing_score",
    "no_contest",
}


def _terminal_status_sql(alias: str = "g") -> str:
    statuses = ",".join(f"'{status}'" for status in sorted(TERMINAL_NO_SCORE_STATUSES))
    return f"LOWER(COALESCE({alias}.status, 'scheduled')) NOT IN ({statuses})"


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


def _normalize_leagues(leagues: Optional[Iterable[str]]) -> Optional[List[str]]:
    if not leagues:
        return None
    return sorted({league.upper() for league in leagues})


def _orphan_id_filter(table_alias: str, leagues: Optional[List[str]], params: List[object]) -> str:
    """Restrict orphan rows to those whose game_id starts with one of `leagues`.

    Orphans whose game_id has no recognizable league prefix (e.g. legacy bare hex
    odds-api event ids) are excluded when a league filter is supplied — they
    cannot belong to any release league, so they shouldn't fail launch readiness.
    """
    if not leagues:
        return ""
    clauses = []
    for league in leagues:
        clauses.append(f"{table_alias}.game_id LIKE ?")
        params.append(f"{league}_%")
    return " AND (" + " OR ".join(clauses) + ")"


def _orphan_breakdown(conn: sqlite3.Connection, table: str) -> str:
    rows = conn.execute(
        f"""
        SELECT
            CASE
                WHEN instr(t.game_id, '_') > 0
                    THEN substr(t.game_id, 1, instr(t.game_id, '_') - 1)
                ELSE '<none>'
            END AS prefix,
            COUNT(*) AS c
        FROM {table} t
        LEFT JOIN games g ON g.game_id = t.game_id
        WHERE g.game_id IS NULL
        GROUP BY prefix ORDER BY c DESC LIMIT 6
        """
    ).fetchall()
    if not rows:
        return ""
    return ", ".join(f"{row['prefix']}={row['c']}" for row in rows)


def check_orphan_results(
    conn: sqlite3.Connection,
    leagues: Optional[Iterable[str]] = None,
) -> CheckResult:
    normalized = _normalize_leagues(leagues)
    params: List[object] = []
    league_filter = _orphan_id_filter("gr", normalized, params)
    count = conn.execute(
        f"""
        SELECT COUNT(*)
        FROM game_results gr
        LEFT JOIN games g ON g.game_id = gr.game_id
        WHERE g.game_id IS NULL
          {league_filter}
        """,
        params,
    ).fetchone()[0]
    detail = "game_results rows without a games row"
    if count and not normalized:
        breakdown = _orphan_breakdown(conn, "game_results")
        if breakdown:
            detail = f"{detail} (by prefix: {breakdown})"
    return CheckResult("orphan_results", count == 0, count, detail)


def check_orphan_predictions(
    conn: sqlite3.Connection,
    leagues: Optional[Iterable[str]] = None,
) -> CheckResult:
    normalized = _normalize_leagues(leagues)
    params: List[object] = []
    league_filter = _orphan_id_filter("p", normalized, params)
    count = conn.execute(
        f"""
        SELECT COUNT(*)
        FROM predictions p
        LEFT JOIN games g ON g.game_id = p.game_id
        WHERE g.game_id IS NULL
          {league_filter}
        """,
        params,
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


def _per_league_breakdown(
    conn: sqlite3.Connection,
    sql: str,
    params: List[object],
) -> str:
    rows = conn.execute(sql, params).fetchall()
    return ", ".join(f"{row['league']}={row['c']}" for row in rows[:6]) if rows else ""


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
          AND {_terminal_status_sql("g")}
          {league_filter}
        """,
        params,
    ).fetchone()[0]
    detail = f"games older than {stale_hours}h not marked final"
    if count:
        breakdown_params: List[object] = [f"-{stale_hours} hours"]
        breakdown_filter = _league_clause("s", leagues, breakdown_params)
        breakdown = _per_league_breakdown(
            conn,
            f"""
            SELECT s.league, COUNT(*) AS c
            FROM games g
            JOIN sports s ON s.sport_id = g.sport_id
            WHERE g.start_time_utc IS NOT NULL
              AND julianday(g.start_time_utc) < julianday('now', ?)
              AND COALESCE(g.status, 'scheduled') != 'final'
              AND {_terminal_status_sql("g")}
              {breakdown_filter}
            GROUP BY s.league ORDER BY c DESC
            """,
            breakdown_params,
        )
        if breakdown:
            detail = f"{detail} ({breakdown})"
    return CheckResult("stale_games", count == 0, count, detail)


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
          AND {_terminal_status_sql("g")}
          {league_filter}
        """,
        params,
    ).fetchone()[0]
    detail = f"past games older than {stale_hours}h missing final scores"
    if count:
        breakdown_params: List[object] = [f"-{stale_hours} hours"]
        breakdown_filter = _league_clause("s", leagues, breakdown_params)
        breakdown = _per_league_breakdown(
            conn,
            f"""
            SELECT s.league, COUNT(*) AS c
            FROM games g
            JOIN sports s ON s.sport_id = g.sport_id
            LEFT JOIN game_results gr ON gr.game_id = g.game_id
            WHERE g.start_time_utc IS NOT NULL
              AND julianday(g.start_time_utc) < julianday('now', ?)
              AND (gr.home_score IS NULL OR gr.away_score IS NULL)
              AND {_terminal_status_sql("g")}
              {breakdown_filter}
            GROUP BY s.league ORDER BY c DESC
            """,
            breakdown_params,
        )
        if breakdown:
            detail = f"{detail} ({breakdown})"
    return CheckResult("missing_scores", count == 0, count, detail)


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


def prune_orphan_results(
    db_path: Path = DB_PATH,
    *,
    leagues: Optional[Iterable[str]] = None,
) -> int:
    """Delete game_results rows whose game_id has no row in games.

    When `leagues` is supplied, only orphans whose game_id starts with one of
    those league prefixes are deleted; bare-id legacy orphans (no recognizable
    prefix) are left alone unless `leagues` is None.
    """
    normalized = _normalize_leagues(leagues)
    params: List[object] = []
    league_filter = _orphan_id_filter("gr", normalized, params)
    with _connect(db_path) as conn:
        deleted = conn.execute(
            f"""
            DELETE FROM game_results
            WHERE rowid IN (
                SELECT gr.rowid
                FROM game_results gr
                LEFT JOIN games g ON g.game_id = gr.game_id
                WHERE g.game_id IS NULL
                  {league_filter}
            )
            """,
            params,
        ).rowcount
        conn.commit()
    return int(deleted or 0)


def finalize_scored_games(
    db_path: Path = DB_PATH,
    *,
    leagues: Optional[Iterable[str]] = None,
    stale_hours: int = 6,
) -> int:
    """Mark stale games final when scores already exist.

    Some backfills populate `game_results` but leave `games.status` as
    scheduled. Those rows should count as settled for data quality and model
    input construction.
    """
    params: List[object] = [f"-{stale_hours} hours"]
    league_filter = _league_clause("s", leagues, params)
    with _connect(db_path) as conn:
        updated = conn.execute(
            f"""
            UPDATE games
            SET status = 'final'
            WHERE game_id IN (
                SELECT g.game_id
                FROM games g
                JOIN sports s ON s.sport_id = g.sport_id
                JOIN game_results gr ON gr.game_id = g.game_id
                WHERE g.start_time_utc IS NOT NULL
                  AND julianday(g.start_time_utc) < julianday('now', ?)
                  AND COALESCE(g.status, 'scheduled') != 'final'
                  AND gr.home_score IS NOT NULL
                  AND gr.away_score IS NOT NULL
                  {league_filter}
            )
            """,
            params,
        ).rowcount
        conn.commit()
    return int(updated or 0)


def close_unresolved_stale_games(
    db_path: Path = DB_PATH,
    *,
    leagues: Optional[Iterable[str]] = None,
    stale_hours: int = 6,
) -> int:
    """Close stale games that still have no score after backfill attempts.

    This does not fabricate scores. It marks unresolved rows as
    `closed_missing_score` so they are excluded from readiness and model
    training until a future source provides a real result.
    """
    params: List[object] = [f"-{stale_hours} hours"]
    league_filter = _league_clause("s", leagues, params)
    with _connect(db_path) as conn:
        updated = conn.execute(
            f"""
            UPDATE games
            SET status = 'closed_missing_score'
            WHERE game_id IN (
                SELECT g.game_id
                FROM games g
                JOIN sports s ON s.sport_id = g.sport_id
                LEFT JOIN game_results gr ON gr.game_id = g.game_id
                WHERE g.start_time_utc IS NOT NULL
                  AND julianday(g.start_time_utc) < julianday('now', ?)
                  AND COALESCE(g.status, 'scheduled') != 'final'
                  AND {_terminal_status_sql("g")}
                  AND (gr.home_score IS NULL OR gr.away_score IS NULL)
                  {league_filter}
            )
            """,
            params,
        ).rowcount
        conn.commit()
    return int(updated or 0)


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
            check_orphan_results(conn, leagues),
            check_orphan_predictions(conn, leagues),
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
    parser.add_argument(
        "--prune-orphans",
        action="store_true",
        help="Delete game_results rows that no longer have a matching games row before running checks. "
             "When --leagues is set, only orphans whose game_id starts with one of those prefixes are removed.",
    )
    parser.add_argument(
        "--finalize-scored",
        action="store_true",
        help="Mark stale games final when they already have home and away scores.",
    )
    parser.add_argument(
        "--close-unresolved-stale",
        action="store_true",
        help=(
            "Mark stale games with no score as closed_missing_score after backfill attempts. "
            "This excludes unresolved rows from readiness without fabricating scores."
        ),
    )
    parser.add_argument("--log-level", default="WARNING", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"])
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level))
    leagues = args.leagues
    if leagues and len(leagues) == 1 and "," in leagues[0]:
        leagues = [league.strip() for league in leagues[0].split(",") if league.strip()]
    if args.prune_orphans:
        deleted = prune_orphan_results(args.db_path, leagues=leagues)
        print(f"Pruned {deleted} orphan game_results row(s)")
    if args.finalize_scored:
        updated = finalize_scored_games(args.db_path, leagues=leagues, stale_hours=args.stale_hours)
        print(f"Finalized {updated} scored stale game(s)")
    if args.close_unresolved_stale:
        updated = close_unresolved_stale_games(
            args.db_path,
            leagues=leagues,
            stale_hours=args.stale_hours,
        )
        print(f"Closed {updated} unresolved stale game(s)")
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
