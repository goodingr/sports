"""Simple CLI for inspecting the SQLite warehouse."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import List

from .core import connect


def _rows_to_json(rows) -> str:
    return json.dumps([dict(row) for row in rows], indent=2, default=str)


def cmd_summary(args: argparse.Namespace) -> None:
    tables = [
        "sports",
        "teams",
        "games",
        "odds_snapshots",
        "odds",
        "game_results",
        "models",
        "model_predictions",
        "recommendations",
        "data_sources",
        "source_runs",
        "source_files",
        "injury_reports",
    ]
    summary = {}
    db_path = Path(args.db_path) if args.db_path else None
    with connect(db_path) as conn:
        for table in tables:
            count = conn.execute(f"SELECT COUNT(*) AS c FROM {table}").fetchone()["c"]
            summary[table] = count
    print(json.dumps(summary, indent=2))


def cmd_models(args: argparse.Namespace) -> None:
    db_path = Path(args.db_path) if args.db_path else None
    with connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT model_id, trained_at, model_type, calibration, league, seasons_start, seasons_end
            FROM models
            ORDER BY trained_at DESC
            LIMIT ?
            """,
            (args.limit,),
        ).fetchall()
    print(_rows_to_json(rows))


def cmd_recommendations(args: argparse.Namespace) -> None:
    query = """
        SELECT r.recommendation_id, r.model_id, r.game_id, t.code AS team_code,
               r.edge, r.kelly_fraction, r.stake, r.recommended_at
        FROM recommendations r
        JOIN teams t ON r.team_id = t.team_id
        ORDER BY r.recommended_at DESC
        LIMIT ?
    """
    db_path = Path(args.db_path) if args.db_path else None
    with connect(db_path) as conn:
        rows = conn.execute(query, (args.limit,)).fetchall()
    print(_rows_to_json(rows))


def cmd_source_runs(args: argparse.Namespace) -> None:
    """Show recent source ingestion runs."""
    query = """
        SELECT
            ds.source_key,
            ds.name,
            ds.league,
            sr.run_id,
            sr.started_at,
            sr.finished_at,
            sr.status,
            sr.message,
            sr.records_ingested
        FROM source_runs sr
        JOIN data_sources ds ON sr.source_id = ds.source_id
        ORDER BY sr.started_at DESC
        LIMIT ?
    """
    db_path = Path(args.db_path) if args.db_path else None
    with connect(db_path) as conn:
        rows = conn.execute(query, (args.limit,)).fetchall()
    print(_rows_to_json(rows))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Inspect betting analytics database")
    parser.add_argument(
        "--db-path",
        type=str,
        default=None,
        help="Path to SQLite database (defaults to data/betting.db)",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    summary_parser = subparsers.add_parser("summary", help="Show row counts for key tables")
    summary_parser.set_defaults(func=cmd_summary)

    models_parser = subparsers.add_parser("models", help="List latest trained models")
    models_parser.add_argument("--limit", type=int, default=5)
    models_parser.set_defaults(func=cmd_models)

    rec_parser = subparsers.add_parser("recommendations", help="Show recent recommendations")
    rec_parser.add_argument("--limit", type=int, default=20)
    rec_parser.set_defaults(func=cmd_recommendations)

    runs_parser = subparsers.add_parser("source-runs", help="Show recent source ingestion runs")
    runs_parser.add_argument("--limit", type=int, default=20)
    runs_parser.set_defaults(func=cmd_source_runs)

    return parser


def main(argv: List[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()

