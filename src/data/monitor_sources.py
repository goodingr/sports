"""Monitor data source ingestion health and alert on failures."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timedelta, timezone
from typing import List

from src.db.core import connect
from pathlib import Path


def cmd_health(args: argparse.Namespace) -> None:
    """Show health summary for all sources."""
    hours = args.hours
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()

    db_path = Path(args.db_path) if getattr(args, "db_path", None) else None
    with connect(db_path) as conn:
        query = """
            SELECT
                ds.source_key,
                ds.name,
                ds.league,
                ds.category,
                ds.enabled,
                ds.default_frequency,
                COUNT(sr.run_id) AS total_runs,
                SUM(CASE WHEN sr.status = 'success' THEN 1 ELSE 0 END) AS success_count,
                SUM(CASE WHEN sr.status = 'failed' THEN 1 ELSE 0 END) AS failed_count,
                MAX(sr.started_at) AS last_run_at,
                MAX(CASE WHEN sr.status = 'success' THEN sr.started_at END) AS last_success_at
            FROM data_sources ds
            LEFT JOIN source_runs sr ON ds.source_id = sr.source_id
                AND sr.started_at >= ?
            WHERE ds.enabled = 1
            GROUP BY ds.source_id, ds.source_key, ds.name, ds.league, ds.category, ds.default_frequency
            ORDER BY ds.league, ds.source_key
        """
        rows = conn.execute(query, (cutoff,)).fetchall()

    summary = []
    for row in rows:
        total = row["total_runs"] or 0
        success = row["success_count"] or 0
        failed = row["failed_count"] or 0
        success_rate = (success / total * 100) if total > 0 else None

        summary.append(
            {
                "source_key": row["source_key"],
                "name": row["name"],
                "league": row["league"],
                "category": row["category"],
                "frequency": row["default_frequency"],
                "total_runs": total,
                "success": success,
                "failed": failed,
                "success_rate": round(success_rate, 1) if success_rate is not None else None,
                "last_run": row["last_run_at"],
                "last_success": row["last_success_at"],
            }
        )

    print(json.dumps(summary, indent=2, default=str))
    return summary


def cmd_failures(args: argparse.Namespace) -> None:
    """List recent failed runs."""
    hours = args.hours
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()

    db_path = Path(args.db_path) if getattr(args, "db_path", None) else None
    with connect(db_path) as conn:
        query = """
            SELECT
                ds.source_key,
                ds.name,
                ds.league,
                sr.run_id,
                sr.started_at,
                sr.finished_at,
                sr.message,
                sr.records_ingested
            FROM source_runs sr
            JOIN data_sources ds ON sr.source_id = ds.source_id
            WHERE sr.status = 'failed'
                AND sr.started_at >= ?
            ORDER BY sr.started_at DESC
            LIMIT ?
        """
        rows = conn.execute(query, (cutoff, args.limit)).fetchall()

    failures = [dict(row) for row in rows]
    print(json.dumps(failures, indent=2, default=str))
    return failures


def cmd_stale(args: argparse.Namespace) -> None:
    """Find sources that haven't run recently."""
    hours = args.threshold
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()

    db_path = Path(args.db_path) if getattr(args, "db_path", None) else None
    with connect(db_path) as conn:
        query = """
            SELECT
                ds.source_key,
                ds.name,
                ds.league,
                ds.default_frequency,
                MAX(sr.started_at) AS last_run_at
            FROM data_sources ds
            LEFT JOIN source_runs sr ON ds.source_id = sr.source_id
            WHERE ds.enabled = 1
            GROUP BY ds.source_id, ds.source_key, ds.name, ds.league, ds.default_frequency
            HAVING MAX(sr.started_at) IS NULL OR MAX(sr.started_at) < ?
            ORDER BY ds.league, ds.source_key
        """
        rows = conn.execute(query, (cutoff,)).fetchall()

    stale = [dict(row) for row in rows]
    print(json.dumps(stale, indent=2, default=str))
    return stale


def cmd_check(args: argparse.Namespace) -> None:
    """Exit with non-zero if any sources are unhealthy."""
    db_path = getattr(args, "db_path", None)
    health_args = argparse.Namespace(hours=args.hours, db_path=db_path)
    failures_args = argparse.Namespace(hours=args.hours, limit=100, db_path=db_path)
    summary = cmd_health(health_args)
    failures = cmd_failures(failures_args)

    exit_code = 0
    if failures:
        print(f"\n⚠️  Found {len(failures)} failed runs in the last {args.hours} hours", file=sys.stderr)
        exit_code = 1

    for source in summary:
        if source["total_runs"] == 0:
            continue
        success_rate = source["success_rate"]
        if success_rate is not None and success_rate < args.min_success_rate:
            print(
                f"\n⚠️  {source['source_key']} has low success rate: {success_rate}%",
                file=sys.stderr,
            )
            exit_code = 1

    if exit_code == 0:
        print("\n✅ All sources healthy", file=sys.stderr)

    sys.exit(exit_code)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Monitor data source ingestion health")
    parser.add_argument(
        "--db-path",
        type=str,
        default=None,
        help="Path to SQLite database (defaults to data/betting.db)",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    health_parser = subparsers.add_parser("health", help="Show health summary for all sources")
    health_parser.add_argument("--hours", type=int, default=24, help="Look back N hours")
    health_parser.set_defaults(func=cmd_health)

    failures_parser = subparsers.add_parser("failures", help="List recent failed runs")
    failures_parser.add_argument("--hours", type=int, default=24, help="Look back N hours")
    failures_parser.add_argument("--limit", type=int, default=20, help="Max results")
    failures_parser.set_defaults(func=cmd_failures)

    stale_parser = subparsers.add_parser("stale", help="Find sources that haven't run recently")
    stale_parser.add_argument("--threshold", type=int, default=48, help="Hours since last run")
    stale_parser.set_defaults(func=cmd_stale)

    check_parser = subparsers.add_parser("check", help="Exit non-zero if unhealthy")
    check_parser.add_argument("--hours", type=int, default=24, help="Look back N hours")
    check_parser.add_argument("--min-success-rate", type=float, default=80.0, help="Minimum success rate %")
    check_parser.set_defaults(func=cmd_check)

    return parser


def main(argv: List[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()

