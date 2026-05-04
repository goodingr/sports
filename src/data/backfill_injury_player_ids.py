"""Backfill missing injury report player identifiers from local high-confidence sources."""

from __future__ import annotations

import argparse
import json
import logging
import re
import sqlite3
import unicodedata
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

from src.data.team_mappings import normalize_team_code
from src.db.core import DB_PATH

LOGGER = logging.getLogger(__name__)
CandidateIndex = dict[tuple[str, str], dict[str, dict[str, int]]]


@dataclass(frozen=True)
class CandidateEvidence:
    player_id: str
    source: str
    evidence_count: int


@dataclass(frozen=True)
class MissingInjuryRow:
    injury_id: int
    league: str
    team_id: Optional[int]
    team_code: Optional[str]
    player_name: str
    source_key: str
    report_date: Optional[str]


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type IN ('table', 'view') AND name = ?",
        (table_name,),
    ).fetchone()
    return row is not None


def _column_names(conn: sqlite3.Connection, table_name: str) -> set[str]:
    if not _table_exists(conn, table_name):
        return set()
    return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()}


def _normalize_player_name(value: Any) -> str:
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = text.encode("ascii", "ignore").decode("ascii")
    text = text.lower()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _normalize_team(league: str, value: Any) -> Optional[str]:
    raw = str(value or "").strip()
    if not raw:
        return None
    normalized = normalize_team_code(league, raw) or raw
    normalized = str(normalized).strip().upper()
    return normalized or None


def _team_lookup(conn: sqlite3.Connection, league: str) -> dict[int, str]:
    if not all(_table_exists(conn, table) for table in ("sports", "teams")):
        return {}
    rows = conn.execute(
        """
        SELECT t.team_id, UPPER(t.code) AS team_code
        FROM teams t
        JOIN sports s ON s.sport_id = t.sport_id
        WHERE UPPER(s.league) = ?
        """,
        (league.upper(),),
    ).fetchall()
    return {
        int(row["team_id"]): normalized
        for row in rows
        if (normalized := _normalize_team(league, row["team_code"]))
    }


def _team_key(
    row: sqlite3.Row | MissingInjuryRow,
    *,
    league: str,
    teams_by_id: dict[int, str],
) -> Optional[str]:
    team_id = row.team_id if isinstance(row, MissingInjuryRow) else row["team_id"]
    team_code = row.team_code if isinstance(row, MissingInjuryRow) else row["team_code"]
    if team_id is not None:
        try:
            mapped = teams_by_id.get(int(team_id))
        except (TypeError, ValueError):
            mapped = None
        if mapped:
            return mapped
    return _normalize_team(league, team_code)


def _candidate_key(name: Any, team_code: Optional[str]) -> Optional[tuple[str, str]]:
    name_key = _normalize_player_name(name)
    if not name_key or not team_code:
        return None
    return name_key, team_code


def _ensure_player_id_column(conn: sqlite3.Connection) -> None:
    if "player_id" not in _column_names(conn, "injury_reports"):
        conn.execute("ALTER TABLE injury_reports ADD COLUMN player_id TEXT")


def _load_missing_rows(
    conn: sqlite3.Connection,
    league: str,
    *,
    player_id_column_present: bool,
) -> list[MissingInjuryRow]:
    if not _table_exists(conn, "injury_reports"):
        return []
    player_id_predicate = (
        "AND TRIM(COALESCE(player_id, '')) = ''" if player_id_column_present else ""
    )
    rows = conn.execute(
        f"""
        SELECT injury_id, UPPER(league) AS league, team_id, team_code, player_name, source_key, report_date
        FROM injury_reports
        WHERE UPPER(league) = ?
          {player_id_predicate}
        ORDER BY report_date DESC, injury_id
        """,
        (league.upper(),),
    ).fetchall()
    return [
        MissingInjuryRow(
            injury_id=int(row["injury_id"]),
            league=str(row["league"]),
            team_id=row["team_id"],
            team_code=row["team_code"],
            player_name=str(row["player_name"] or ""),
            source_key=str(row["source_key"] or ""),
            report_date=row["report_date"],
        )
        for row in rows
    ]


def _add_candidate(
    index: CandidateIndex,
    *,
    key: Optional[tuple[str, str]],
    player_id: Any,
    source: str,
    evidence_count: int = 1,
) -> None:
    if key is None:
        return
    clean_id = str(player_id or "").strip()
    if not clean_id:
        return
    index[key][clean_id][source] += max(int(evidence_count), 1)


def _candidate_index_from_rows(
    rows: Iterable[dict[str, Any]],
    *,
    league: str,
    source: str,
) -> CandidateIndex:
    index: CandidateIndex = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    for row in rows:
        team_code = _normalize_team(league, row.get("team_code") or row.get("team"))
        key = _candidate_key(row.get("player_name"), team_code)
        _add_candidate(
            index,
            key=key,
            player_id=row.get("player_id"),
            source=source,
            evidence_count=int(row.get("evidence_count") or 1),
        )
    return index


def _load_existing_injury_candidates(
    conn: sqlite3.Connection,
    *,
    league: str,
    teams_by_id: dict[int, str],
) -> CandidateIndex:
    index: CandidateIndex = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    if not _table_exists(conn, "injury_reports") or "player_id" not in _column_names(
        conn, "injury_reports"
    ):
        return index
    rows = conn.execute(
        """
        SELECT team_id, team_code, player_name, player_id, source_key
        FROM injury_reports
        WHERE UPPER(league) = ?
          AND TRIM(COALESCE(player_id, '')) != ''
        """,
        (league.upper(),),
    ).fetchall()
    for row in rows:
        team_code = _team_key(row, league=league, teams_by_id=teams_by_id)
        key = _candidate_key(row["player_name"], team_code)
        _add_candidate(
            index,
            key=key,
            player_id=row["player_id"],
            source=f"injury_reports:{row['source_key'] or 'unknown'}",
        )
    return index


def _load_player_stats_candidates(
    conn: sqlite3.Connection,
    *,
    league: str,
) -> CandidateIndex:
    index: CandidateIndex = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    if not all(_table_exists(conn, table) for table in ("player_stats", "teams", "sports")):
        return index
    rows = conn.execute(
        """
        SELECT
            ps.player_name,
            ps.player_id,
            UPPER(t.code) AS team_code,
            COUNT(*) AS evidence_count
        FROM player_stats ps
        JOIN teams t ON t.team_id = ps.team_id
        JOIN sports s ON s.sport_id = t.sport_id
        WHERE UPPER(s.league) = ?
          AND TRIM(COALESCE(ps.player_name, '')) != ''
          AND ps.player_id IS NOT NULL
        GROUP BY ps.player_name, ps.player_id, t.code
        """,
        (league.upper(),),
    ).fetchall()
    for row in rows:
        team_code = _normalize_team(league, row["team_code"])
        key = _candidate_key(row["player_name"], team_code)
        _add_candidate(
            index,
            key=key,
            player_id=row["player_id"],
            source="player_stats:name_team",
            evidence_count=int(row["evidence_count"] or 1),
        )
    return index


def _load_espn_active_injury_candidates(
    *,
    league: str,
    timeout: int,
) -> tuple[CandidateIndex, dict[str, Any]]:
    if league.upper() != "NBA":
        return {}, {"enabled": False, "source_rows": 0, "error": None}

    try:
        from src.data.sources import nba_injuries_espn

        teams = nba_injuries_espn._fetch_team_payloads(timeout=timeout)
        athlete_cache: dict[str, dict[str, Any]] = {}
        rows: list[dict[str, Any]] = []
        for team in teams:
            rows.extend(
                nba_injuries_espn._fetch_team_injuries(
                    team,
                    timeout=timeout,
                    athlete_cache=athlete_cache,
                )
            )
    except Exception as exc:  # noqa: BLE001 - candidate source should not break local audit.
        LOGGER.warning("Unable to load ESPN active NBA injury candidates: %s", exc)
        return {}, {"enabled": True, "source_rows": 0, "error": str(exc)}

    return (
        _candidate_index_from_rows(rows, league=league, source="espn_active_injuries"),
        {"enabled": True, "source_rows": len(rows), "error": None},
    )


def _merge_candidate_indexes(
    *indexes: CandidateIndex,
) -> CandidateIndex:
    merged: CandidateIndex = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    for index in indexes:
        for key, player_ids in index.items():
            for player_id, sources in player_ids.items():
                for source, count in sources.items():
                    merged[key][player_id][source] += int(count)
    return merged


def _candidate_payload(candidates: dict[str, dict[str, int]]) -> list[dict[str, Any]]:
    rows = []
    for player_id, sources in candidates.items():
        rows.append(
            {
                "player_id": player_id,
                "sources": [
                    {"source": source, "evidence_count": int(count)}
                    for source, count in sorted(sources.items())
                ],
                "evidence_count": int(sum(sources.values())),
            }
        )
    rows.sort(key=lambda row: (-row["evidence_count"], row["player_id"]))
    return rows


def _resolve_rows(
    missing_rows: list[MissingInjuryRow],
    *,
    league: str,
    teams_by_id: dict[int, str],
    candidates: dict[tuple[str, str], dict[str, dict[str, int]]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    resolved: list[dict[str, Any]] = []
    ambiguous: list[dict[str, Any]] = []
    unresolved: list[dict[str, Any]] = []

    for row in missing_rows:
        team_code = _team_key(row, league=league, teams_by_id=teams_by_id)
        key = _candidate_key(row.player_name, team_code)
        base_payload = {
            "injury_id": row.injury_id,
            "league": row.league,
            "team_code": team_code,
            "player_name": row.player_name,
            "source_key": row.source_key,
            "report_date": row.report_date,
        }
        if key is None:
            unresolved.append({**base_payload, "reason": "missing_name_or_team"})
            continue
        row_candidates = candidates.get(key, {})
        if not row_candidates:
            unresolved.append({**base_payload, "reason": "no_candidate"})
            continue
        candidate_rows = _candidate_payload(row_candidates)
        if len(candidate_rows) == 1:
            resolved.append(
                {
                    **base_payload,
                    "player_id": candidate_rows[0]["player_id"],
                    "match_key": {"player_name": key[0], "team_code": key[1]},
                    "evidence": candidate_rows[0]["sources"],
                    "evidence_count": candidate_rows[0]["evidence_count"],
                }
            )
        else:
            ambiguous.append(
                {
                    **base_payload,
                    "match_key": {"player_name": key[0], "team_code": key[1]},
                    "candidates": candidate_rows,
                    "reason": "multiple_candidate_player_ids",
                }
            )
    return resolved, ambiguous, unresolved


def _apply_resolutions(conn: sqlite3.Connection, resolved: Iterable[dict[str, Any]]) -> int:
    updates = [(row["player_id"], row["injury_id"]) for row in resolved]
    if not updates:
        return 0
    cursor = conn.executemany(
        """
        UPDATE injury_reports
        SET player_id = ?
        WHERE injury_id = ?
          AND TRIM(COALESCE(player_id, '')) = ''
        """,
        updates,
    )
    return int(cursor.rowcount if cursor.rowcount is not None else 0)


def build_backfill_report(
    *,
    db_path: Path = DB_PATH,
    league: str = "NBA",
    write: bool = False,
    fetch_espn: bool = True,
    timeout: int = 30,
) -> dict[str, Any]:
    """Resolve and optionally backfill missing injury player ids."""
    db_path = Path(db_path)
    league = league.upper()
    if not db_path.exists():
        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "db_path": str(db_path),
            "league": league,
            "dry_run": not write,
            "error": f"database not found: {db_path}",
            "missing_rows": 0,
            "resolvable_rows": 0,
            "ambiguous_rows": 0,
            "unresolved_rows": 0,
            "updated_rows": 0,
            "resolutions": [],
            "ambiguous": [],
            "unresolved": [],
        }

    with _connect(db_path) as conn:
        player_id_column_present_before = "player_id" in _column_names(conn, "injury_reports")
        schema_migration_applied = False
        if write and _table_exists(conn, "injury_reports") and not player_id_column_present_before:
            _ensure_player_id_column(conn)
            schema_migration_applied = True
        player_id_column_present = "player_id" in _column_names(conn, "injury_reports")
        teams_by_id = _team_lookup(conn, league)
        missing_rows = _load_missing_rows(
            conn,
            league,
            player_id_column_present=player_id_column_present,
        )
        injury_candidates = _load_existing_injury_candidates(
            conn,
            league=league,
            teams_by_id=teams_by_id,
        )
        player_stats_candidates = _load_player_stats_candidates(conn, league=league)
        if fetch_espn:
            espn_candidates, espn_candidate_status = _load_espn_active_injury_candidates(
                league=league,
                timeout=timeout,
            )
        else:
            espn_candidates = {}
            espn_candidate_status = {"enabled": False, "source_rows": 0, "error": None}
        candidates = _merge_candidate_indexes(
            injury_candidates,
            player_stats_candidates,
            espn_candidates,
        )
        resolved, ambiguous, unresolved = _resolve_rows(
            missing_rows,
            league=league,
            teams_by_id=teams_by_id,
            candidates=candidates,
        )
        updated_rows = _apply_resolutions(conn, resolved) if write else 0
        if write:
            conn.commit()

    source_counts: dict[str, int] = defaultdict(int)
    for row in resolved:
        for evidence in row["evidence"]:
            source_counts[evidence["source"]] += 1

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "db_path": str(db_path),
        "league": league,
        "dry_run": not write,
        "player_id_column_present_before": player_id_column_present_before,
        "schema_migration_applied": schema_migration_applied,
        "espn_candidate_source": espn_candidate_status,
        "missing_rows": len(missing_rows),
        "resolvable_rows": len(resolved),
        "ambiguous_rows": len(ambiguous),
        "unresolved_rows": len(unresolved),
        "updated_rows": updated_rows,
        "candidate_sources": dict(sorted(source_counts.items())),
        "resolutions": resolved,
        "ambiguous": ambiguous,
        "unresolved": unresolved,
    }


def format_summary(report: dict[str, Any]) -> str:
    lines = [
        "INJURY PLAYER ID BACKFILL SUMMARY",
        f"league={report['league']} dry_run={report['dry_run']} db={report['db_path']}",
    ]
    if report.get("error"):
        lines.append(f"ERROR {report['error']}")
    lines.extend(
        [
            f"player_id_column_present_before={report.get('player_id_column_present_before')}",
            f"schema_migration_applied={report.get('schema_migration_applied')}",
            f"espn_candidate_source={report.get('espn_candidate_source')}",
            f"missing_rows={report['missing_rows']}",
            f"resolvable_rows={report['resolvable_rows']}",
            f"ambiguous_rows={report['ambiguous_rows']}",
            f"unresolved_rows={report['unresolved_rows']}",
            f"updated_rows={report['updated_rows']}",
        ]
    )
    if report.get("candidate_sources"):
        source_text = ", ".join(
            f"{source}:{count}" for source, count in report["candidate_sources"].items()
        )
        lines.append(f"candidate_sources={source_text}")
    for row in report.get("ambiguous", [])[:5]:
        candidate_ids = ", ".join(item["player_id"] for item in row["candidates"])
        lines.append(
            "AMBIGUOUS "
            f"injury_id={row['injury_id']} player={row['player_name']} "
            f"team={row['team_code']} candidates={candidate_ids}"
        )
    for row in report.get("unresolved", [])[:5]:
        lines.append(
            "UNRESOLVED "
            f"injury_id={row['injury_id']} player={row['player_name']} "
            f"team={row['team_code']} reason={row['reason']}"
        )
    return "\n".join(lines)


def write_report(report: dict[str, Any], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    return output_path


def _parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill missing injury_reports.player_id values from local evidence."
    )
    parser.add_argument("--db-path", "--db", dest="db_path", type=Path, default=DB_PATH)
    parser.add_argument("--league", default="NBA")
    parser.add_argument("--timeout", type=int, default=30)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview matches without writing. This is the default unless --write is passed.",
    )
    mode.add_argument(
        "--write",
        action="store_true",
        help="Apply high-confidence non-ambiguous player_id matches.",
    )
    parser.add_argument(
        "--skip-espn-fetch",
        action="store_true",
        help="Use only local DB evidence and do not fetch ESPN active injury references.",
    )
    parser.add_argument("--output", type=Path, default=None)
    parser.add_argument(
        "--log-level",
        default="WARNING",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
    )
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = _parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level))
    report = build_backfill_report(
        db_path=args.db_path,
        league=args.league,
        write=bool(args.write),
        fetch_espn=not args.skip_espn_fetch,
        timeout=args.timeout,
    )
    print(format_summary(report))
    if args.output:
        write_report(report, args.output)
        print(f"JSON written to {args.output}")
    return 1 if report.get("error") else 0


if __name__ == "__main__":
    raise SystemExit(main())
