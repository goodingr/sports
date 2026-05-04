"""Backfill stored totals close-line provenance where it can be inferred safely."""

from __future__ import annotations

import argparse
import json
import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

import pandas as pd

from src.data.clv_lineage_audit import _ensure_total_close_lineage_columns
from src.db.core import DB_PATH

LOGGER = logging.getLogger(__name__)
DEFAULT_LEAGUES = ("NBA",)
DEFAULT_TOLERANCE = 0.01
STRATEGY_MATCH_EXISTING = "match_existing"
STRATEGY_LATEST_PREGAME = "latest_pregame"
STRATEGIES = (STRATEGY_MATCH_EXISTING, STRATEGY_LATEST_PREGAME)
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


def _book_rank(book_name: Any) -> int:
    normalized = str(book_name or "").strip().lower()
    try:
        return BOOK_PRIORITY.index(normalized)
    except ValueError:
        return len(BOOK_PRIORITY)


def _read_targets(db_path: Path, leagues: list[str]) -> pd.DataFrame:
    league_sql, params = _league_filter_sql(leagues)
    query = f"""
        SELECT
            gr.game_id,
            UPPER(s.league) AS league,
            g.start_time_utc,
            gr.total_close
        FROM game_results gr
        JOIN games g ON gr.game_id = g.game_id
        JOIN sports s ON g.sport_id = s.sport_id
        WHERE gr.total_close IS NOT NULL
          AND (
              gr.total_close_snapshot_id IS NULL
              OR gr.total_close_snapshot_time_utc IS NULL
              OR gr.total_close_book IS NULL
          )
          {league_sql}
    """
    rows = _read_sql(db_path, query, params)
    if rows.empty:
        return rows
    rows["start_time_utc"] = pd.to_datetime(
        rows["start_time_utc"], utc=True, errors="coerce", format="mixed"
    )
    rows["total_close"] = pd.to_numeric(rows["total_close"], errors="coerce")
    return rows.dropna(subset=["start_time_utc", "total_close"])


def _read_rebuild_targets(db_path: Path, leagues: list[str]) -> pd.DataFrame:
    league_sql, params = _league_filter_sql(leagues)
    query = f"""
        SELECT
            gr.game_id,
            UPPER(s.league) AS league,
            g.start_time_utc,
            gr.home_score,
            gr.away_score,
            gr.total_close,
            gr.total_close_snapshot_id,
            gr.total_close_snapshot_time_utc,
            gr.total_close_book
        FROM game_results gr
        JOIN games g ON gr.game_id = g.game_id
        JOIN sports s ON g.sport_id = s.sport_id
        WHERE gr.home_score IS NOT NULL
          AND gr.away_score IS NOT NULL
          {league_sql}
    """
    rows = _read_sql(db_path, query, params)
    if rows.empty:
        return rows
    rows["start_time_utc"] = pd.to_datetime(
        rows["start_time_utc"], utc=True, errors="coerce", format="mixed"
    )
    rows["total_close_snapshot_time_utc"] = pd.to_datetime(
        rows["total_close_snapshot_time_utc"], utc=True, errors="coerce", format="mixed"
    )
    rows["total_close"] = pd.to_numeric(rows["total_close"], errors="coerce")
    return rows.dropna(subset=["start_time_utc"])


def _read_totals_candidates(db_path: Path, leagues: list[str]) -> pd.DataFrame:
    league_sql, params = _league_filter_sql(leagues)
    query = f"""
        SELECT
            g.game_id,
            os.snapshot_id,
            os.fetched_at_utc AS snapshot_time_utc,
            os.source AS source,
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
        WHERE LOWER(o.market) = 'totals'
          AND o.line IS NOT NULL
          AND o.price_american IS NOT NULL
          {league_sql}
        GROUP BY g.game_id, os.snapshot_id, os.fetched_at_utc, os.source, o.book_id, b.name, o.line
        HAVING over_moneyline IS NOT NULL AND under_moneyline IS NOT NULL
    """
    rows = _read_sql(db_path, query, params)
    if rows.empty:
        return rows
    rows["snapshot_time_utc"] = pd.to_datetime(
        rows["snapshot_time_utc"], utc=True, errors="coerce", format="mixed"
    )
    rows["line"] = pd.to_numeric(rows["line"], errors="coerce")
    return rows.dropna(subset=["snapshot_time_utc", "line"])


def _candidate_payload(row: pd.Series) -> dict[str, Any]:
    return {
        "snapshot_id": row["snapshot_id"],
        "snapshot_time_utc": pd.Timestamp(row["snapshot_time_utc"]).isoformat(),
        "book_id": int(row["book_id"]),
        "book": row["book"],
        "source": row.get("source"),
        "line": float(row["line"]),
    }


def _resolve_target(
    target: pd.Series,
    candidates: pd.DataFrame,
    *,
    tolerance: float,
) -> tuple[str, dict[str, Any] | None, list[dict[str, Any]]]:
    game_candidates = candidates[candidates["game_id"] == target["game_id"]].copy()
    game_candidates = game_candidates[
        game_candidates["snapshot_time_utc"] <= target["start_time_utc"]
    ].copy()
    game_candidates = game_candidates[
        (game_candidates["line"] - float(target["total_close"])).abs() <= tolerance
    ].copy()
    if game_candidates.empty:
        return "unresolved", None, []

    latest_time = game_candidates["snapshot_time_utc"].max()
    latest = game_candidates[game_candidates["snapshot_time_utc"] == latest_time].copy()
    payloads = [_candidate_payload(row) for _, row in latest.iterrows()]
    if len(latest) != 1:
        return "ambiguous", None, payloads
    return "resolved", payloads[0], payloads


def _resolve_latest_pregame_target(
    target: pd.Series,
    candidates: pd.DataFrame,
) -> tuple[str, dict[str, Any] | None, list[dict[str, Any]]]:
    game_candidates = candidates[candidates["game_id"] == target["game_id"]].copy()
    game_candidates = game_candidates[
        game_candidates["snapshot_time_utc"] <= target["start_time_utc"]
    ].copy()
    if game_candidates.empty:
        return "unresolved", None, []

    latest_time = game_candidates["snapshot_time_utc"].max()
    latest = game_candidates[game_candidates["snapshot_time_utc"] == latest_time].copy()
    latest["_book_rank"] = latest["book"].map(_book_rank)
    latest = latest.sort_values(["_book_rank", "book", "line"], ascending=[True, True, True])
    selected = latest.iloc[0]
    payloads = [_candidate_payload(row) for _, row in latest.drop(columns=["_book_rank"]).iterrows()]
    return "resolved", _candidate_payload(selected), payloads


def _apply_updates(db_path: Path, resolved: list[dict[str, Any]]) -> int:
    if not resolved:
        return 0
    with sqlite3.connect(str(db_path)) as conn:
        updated = 0
        for row in resolved:
            payload = row["candidate"]
            updated += conn.execute(
                """
                UPDATE game_results
                SET total_close = COALESCE(?, total_close),
                    total_close_snapshot_id = ?,
                    total_close_snapshot_time_utc = ?,
                    total_close_book_id = ?,
                    total_close_book = ?,
                    total_close_source = ?
                WHERE game_id = ?
                """,
                (
                    payload.get("line"),
                    payload["snapshot_id"],
                    payload["snapshot_time_utc"],
                    payload["book_id"],
                    payload["book"],
                    payload["source"],
                    row["game_id"],
                ),
            ).rowcount
        conn.commit()
    return updated


def build_backfill_report(
    db_path: Path = DB_PATH,
    leagues: Optional[Iterable[str]] = DEFAULT_LEAGUES,
    *,
    tolerance: float = DEFAULT_TOLERANCE,
    strategy: str = STRATEGY_MATCH_EXISTING,
    write: bool = False,
) -> dict[str, Any]:
    db_path = Path(db_path)
    normalized_leagues = _split_csv(leagues, DEFAULT_LEAGUES)
    normalized_strategy = strategy.replace("-", "_").strip().lower()
    if normalized_strategy not in STRATEGIES:
        raise ValueError(f"Unknown backfill strategy: {strategy}")
    if not db_path.exists():
        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "db_path": str(db_path),
            "leagues": normalized_leagues,
            "dry_run": not write,
            "strategy": normalized_strategy,
            "error": f"database not found: {db_path}",
            "resolved": [],
            "ambiguous": [],
            "unresolved": [],
            "updated_rows": 0,
        }

    _ensure_total_close_lineage_columns(db_path)
    targets = (
        _read_rebuild_targets(db_path, normalized_leagues)
        if normalized_strategy == STRATEGY_LATEST_PREGAME
        else _read_targets(db_path, normalized_leagues)
    )
    candidates = _read_totals_candidates(db_path, normalized_leagues)
    resolved: list[dict[str, Any]] = []
    ambiguous: list[dict[str, Any]] = []
    unresolved: list[dict[str, Any]] = []
    for _, target in targets.iterrows():
        if normalized_strategy == STRATEGY_LATEST_PREGAME:
            status, candidate, candidate_options = _resolve_latest_pregame_target(
                target,
                candidates,
            )
        else:
            status, candidate, candidate_options = _resolve_target(
                target,
                candidates,
                tolerance=tolerance,
            )
        base = {
            "game_id": target["game_id"],
            "league": target["league"],
            "existing_total_close": None
            if pd.isna(target.get("total_close"))
            else float(target["total_close"]),
            "existing_total_close_snapshot_id": None
            if pd.isna(target.get("total_close_snapshot_id"))
            else target.get("total_close_snapshot_id"),
            "existing_total_close_book": None
            if pd.isna(target.get("total_close_book"))
            else target.get("total_close_book"),
        }
        if status == "resolved" and candidate is not None:
            resolved.append(
                {
                    **base,
                    "candidate": candidate,
                    "candidate_count_at_close_time": len(candidate_options),
                    "would_change_total_close": bool(
                        base["existing_total_close"] is None
                        or abs(base["existing_total_close"] - candidate["line"]) > tolerance
                    ),
                }
            )
        elif status == "ambiguous":
            ambiguous.append({**base, "candidates": candidate_options})
        else:
            unresolved.append(base)

    updated_rows = _apply_updates(db_path, resolved) if write else 0
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "db_path": str(db_path),
        "leagues": normalized_leagues,
        "dry_run": not write,
        "strategy": normalized_strategy,
        "line_match_tolerance": tolerance,
        "target_count": int(len(targets)),
        "resolved_count": len(resolved),
        "ambiguous_count": len(ambiguous),
        "unresolved_count": len(unresolved),
        "would_change_total_close_count": sum(
            1 for row in resolved if row.get("would_change_total_close")
        ),
        "updated_rows": updated_rows,
        "resolved": resolved[:100],
        "ambiguous": ambiguous[:100],
        "unresolved": unresolved[:100],
    }


def write_report(report: dict[str, Any], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    return output_path


def _default_output_path(write: bool) -> Path:
    mode = "write" if write else "dry_run"
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return Path("reports/data_quality") / f"total_close_provenance_backfill_{mode}_{timestamp}.json"


def _parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, default=DB_PATH)
    parser.add_argument("--league", "--leagues", dest="leagues", default=",".join(DEFAULT_LEAGUES))
    parser.add_argument("--tolerance", type=float, default=DEFAULT_TOLERANCE)
    parser.add_argument(
        "--strategy",
        choices=("match-existing", "latest-pregame"),
        default="match-existing",
        help=(
            "match-existing only fills provenance when the stored total_close can be "
            "matched uniquely; latest-pregame rebuilds total_close from the latest "
            "pregame odds snapshot using book priority."
        ),
    )
    parser.add_argument("--dry-run", action="store_true", help="Preview without writing.")
    parser.add_argument("--write", action="store_true", help="Apply high-confidence updates.")
    parser.add_argument("--output", type=Path, default=None)
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
    )
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = _parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level))
    write = bool(args.write and not args.dry_run)
    report = build_backfill_report(
        db_path=args.db,
        leagues=_split_csv(args.leagues, DEFAULT_LEAGUES),
        tolerance=args.tolerance,
        strategy=args.strategy,
        write=write,
    )
    output_path = args.output or _default_output_path(write)
    write_report(report, output_path)
    print("Total close provenance backfill")
    print(f"strategy={report.get('strategy')}")
    print(f"target_count={report.get('target_count', 0)}")
    print(f"resolved_count={report.get('resolved_count', 0)}")
    print(f"ambiguous_count={report.get('ambiguous_count', 0)}")
    print(f"unresolved_count={report.get('unresolved_count', 0)}")
    print(f"would_change_total_close_count={report.get('would_change_total_close_count', 0)}")
    print(f"updated_rows={report.get('updated_rows', 0)}")
    print(f"output={output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
