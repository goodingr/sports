"""Availability coverage checks for injury/player availability data."""

from __future__ import annotations

import argparse
import json
import logging
import os
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

from src.data.config import PROJECT_ROOT
from src.data.team_mappings import normalize_team_code
from src.db.core import DB_PATH

LOGGER = logging.getLogger(__name__)

DEFAULT_LOOKAHEAD_DAYS = 7
DEFAULT_MAX_STALE_DAYS = 7
DEFAULT_MIN_COVERAGE = 0.80
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "reports" / "data_quality"
TERMINAL_GAME_STATUSES = {
    "canceled",
    "cancelled",
    "closed_missing_score",
    "final",
    "no_contest",
    "postponed",
}


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
    return {row[1] for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()}


def _parse_datetime(value: Any) -> Optional[datetime]:
    if value in (None, ""):
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        try:
            parsed = datetime.fromisoformat(f"{text}T00:00:00+00:00")
        except ValueError:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _iso(value: Optional[datetime]) -> Optional[str]:
    return value.astimezone(timezone.utc).isoformat() if value else None


def _age_days(as_of: datetime, report_at: Optional[datetime]) -> Optional[float]:
    if report_at is None:
        return None
    return round((as_of - report_at).total_seconds() / 86400.0, 2)


def _coerce_coverage_threshold(value: float) -> float:
    if value < 0 or value > 1:
        raise ValueError("min_coverage must be between 0 and 1")
    return value


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw in (None, ""):
        return default
    try:
        return float(raw)
    except ValueError:
        LOGGER.warning("Ignoring invalid %s=%r", name, raw)
        return default


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw in (None, ""):
        return default
    try:
        return int(raw)
    except ValueError:
        LOGGER.warning("Ignoring invalid %s=%r", name, raw)
        return default


def _normalize_leagues(leagues: Optional[Iterable[str]]) -> list[str]:
    return sorted({league.upper() for league in leagues or []})


def _code_candidates(league: str, value: Any) -> set[str]:
    raw = str(value or "").strip()
    if not raw:
        return set()
    candidates = {raw.upper()}
    normalized = normalize_team_code(league, raw)
    if normalized:
        candidates.add(normalized.upper())
    return candidates


def _load_upcoming_games(
    conn: sqlite3.Connection,
    *,
    league: str,
    as_of: datetime,
    lookahead_days: int,
) -> list[dict[str, Any]]:
    if not all(_table_exists(conn, table) for table in ("sports", "teams", "games")):
        return []

    rows = conn.execute(
        """
        SELECT
            g.game_id,
            UPPER(s.league) AS league,
            g.start_time_utc,
            COALESCE(g.status, 'scheduled') AS status,
            ht.team_id AS home_team_id,
            UPPER(ht.code) AS home_team_code,
            ht.name AS home_team_name,
            at.team_id AS away_team_id,
            UPPER(at.code) AS away_team_code,
            at.name AS away_team_name
        FROM games g
        JOIN sports s ON s.sport_id = g.sport_id
        JOIN teams ht ON ht.team_id = g.home_team_id
        JOIN teams at ON at.team_id = g.away_team_id
        WHERE UPPER(s.league) = ?
          AND g.start_time_utc IS NOT NULL
        """,
        (league.upper(),),
    ).fetchall()

    window_end = as_of + timedelta(days=lookahead_days)
    games: list[dict[str, Any]] = []
    for row in rows:
        start_time = _parse_datetime(row["start_time_utc"])
        if start_time is None or start_time < as_of or start_time > window_end:
            continue
        if str(row["status"] or "").strip().lower() in TERMINAL_GAME_STATUSES:
            continue
        games.append(
            {
                "game_id": row["game_id"],
                "league": row["league"],
                "start_time_utc": start_time,
                "status": row["status"],
                "home_team_id": row["home_team_id"],
                "home_team_code": row["home_team_code"],
                "home_team_name": row["home_team_name"],
                "away_team_id": row["away_team_id"],
                "away_team_code": row["away_team_code"],
                "away_team_name": row["away_team_name"],
            }
        )
    return sorted(games, key=lambda item: (item["start_time_utc"], item["game_id"]))


def _load_teams(conn: sqlite3.Connection, *, league: str) -> dict[str, Any]:
    if not all(_table_exists(conn, table) for table in ("sports", "teams")):
        return {"ids": set(), "codes": set(), "by_id": {}, "by_code": {}}
    rows = conn.execute(
        """
        SELECT t.team_id, UPPER(t.code) AS code, t.name
        FROM teams t
        JOIN sports s ON s.sport_id = t.sport_id
        WHERE UPPER(s.league) = ?
        """,
        (league.upper(),),
    ).fetchall()
    by_id = {row["team_id"]: dict(row) for row in rows}
    by_code = {row["code"]: dict(row) for row in rows if row["code"]}
    return {
        "ids": set(by_id),
        "codes": set(by_code),
        "by_id": by_id,
        "by_code": by_code,
    }


def _load_injury_rows(conn: sqlite3.Connection, *, league: str) -> tuple[list[dict[str, Any]], bool]:
    if not _table_exists(conn, "injury_reports"):
        return [], False

    columns = _column_names(conn, "injury_reports")
    player_id_expr = "player_id" if "player_id" in columns else "NULL AS player_id"
    query = f"""
        SELECT
            UPPER(league) AS league,
            team_id,
            team_code,
            player_name,
            {player_id_expr},
            report_date,
            game_date,
            source_key,
            created_at
        FROM injury_reports
        WHERE UPPER(league) = ?
    """
    rows = []
    for row in conn.execute(query, (league.upper(),)).fetchall():
        normalized_codes = _code_candidates(league, row["team_code"])
        rows.append(
            {
                "league": row["league"],
                "team_id": row["team_id"],
                "team_code": str(row["team_code"] or "").strip().upper() or None,
                "normalized_team_codes": normalized_codes,
                "player_name": str(row["player_name"] or "").strip(),
                "player_id": str(row["player_id"] or "").strip() or None,
                "report_date": _parse_datetime(row["report_date"]),
                "game_date": _parse_datetime(row["game_date"]),
                "source_key": row["source_key"],
                "created_at": _parse_datetime(row["created_at"]),
            }
        )
    return rows, "player_id" in columns


def _team_row_matches(row: dict[str, Any], *, team_id: Any, team_code: str) -> bool:
    if row.get("team_id") is not None and team_id is not None:
        try:
            if int(row["team_id"]) == int(team_id):
                return True
        except (TypeError, ValueError):
            pass
    return team_code.upper() in row.get("normalized_team_codes", set())


def _availability_rows_for_team(
    rows: list[dict[str, Any]],
    *,
    team_id: Any,
    team_code: str,
    as_of: datetime,
    stale_after: datetime,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    matching = [
        row
        for row in rows
        if row["report_date"] is not None
        and row["report_date"] <= as_of
        and _team_row_matches(row, team_id=team_id, team_code=team_code)
    ]
    recent = [row for row in matching if row["report_date"] >= stale_after]
    matching.sort(key=lambda item: item["report_date"] or datetime.min.replace(tzinfo=timezone.utc))
    recent.sort(key=lambda item: item["report_date"] or datetime.min.replace(tzinfo=timezone.utc))
    return matching, recent


def _row_excerpt(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "team_id": row.get("team_id"),
        "team_code": row.get("team_code"),
        "player_name": row.get("player_name"),
        "player_id": row.get("player_id"),
        "report_date": _iso(row.get("report_date")),
        "source_key": row.get("source_key"),
    }


def _mapping_report(
    rows: list[dict[str, Any]],
    *,
    league: str,
    teams: dict[str, Any],
    player_id_column_present: bool,
) -> dict[str, Any]:
    missing_team_rows: list[dict[str, Any]] = []
    missing_player_rows: list[dict[str, Any]] = []
    for row in rows:
        team_id = row.get("team_id")
        codes = row.get("normalized_team_codes", set())
        has_team_id = team_id in teams["ids"]
        has_team_code = bool(codes & teams["codes"])
        if not has_team_id or not has_team_code:
            item = _row_excerpt(row)
            item["reason"] = (
                "missing_team_id_and_code"
                if not has_team_id and not has_team_code
                else "missing_team_id"
                if not has_team_id
                else "missing_team_code"
            )
            missing_team_rows.append(item)

        has_player_id = bool(row.get("player_id"))
        has_player_name = bool(str(row.get("player_name") or "").strip())
        if not has_player_id or not has_player_name:
            item = _row_excerpt(row)
            item["reason"] = (
                "missing_player_id_column"
                if not player_id_column_present
                else "missing_player_id_and_name"
                if not has_player_id and not has_player_name
                else "missing_player_id"
                if not has_player_id
                else "missing_player_name"
            )
            missing_player_rows.append(item)

    return {
        "league": league.upper(),
        "player_id_column_present": player_id_column_present,
        "injury_rows_total": len(rows),
        "team_rows_missing_mapping": len(missing_team_rows),
        "player_rows_missing_mapping": len(missing_player_rows),
        "team_mapping_coverage_percentage": (
            round(1.0 - len(missing_team_rows) / len(rows), 4) if rows else None
        ),
        "player_mapping_coverage_percentage": (
            round(1.0 - len(missing_player_rows) / len(rows), 4) if rows else None
        ),
        "sample_team_rows_missing_mapping": missing_team_rows[:25],
        "sample_player_rows_missing_mapping": missing_player_rows[:25],
    }


def build_availability_report(
    *,
    db_path: Path = DB_PATH,
    league: str = "NBA",
    lookahead_days: int = DEFAULT_LOOKAHEAD_DAYS,
    max_stale_days: int = DEFAULT_MAX_STALE_DAYS,
    min_coverage: float = DEFAULT_MIN_COVERAGE,
    as_of: Optional[datetime] = None,
) -> dict[str, Any]:
    """Build a serializable availability coverage report."""

    league = league.upper()
    min_coverage = _coerce_coverage_threshold(min_coverage)
    as_of = (as_of or datetime.now(timezone.utc)).astimezone(timezone.utc)
    stale_after = as_of - timedelta(days=max_stale_days)

    if not db_path.exists():
        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "db_path": str(db_path),
            "league": league,
            "as_of": as_of.isoformat(),
            "lookahead_days": lookahead_days,
            "max_stale_days": max_stale_days,
            "min_coverage": min_coverage,
            "upcoming_games": {
                "total": 0,
                "with_any_availability_rows": 0,
                "with_full_availability_rows": 0,
                "team_slots": 0,
                "covered_team_slots": 0,
                "coverage_percentage": None,
            },
            "coverage_by_date": [],
            "coverage_by_date_team": [],
            "coverage_by_team": [],
            "recent_teams": [],
            "stale_availability_rows": [],
            "missing_mappings": {
                "league": league,
                "player_id_column_present": False,
                "injury_rows_total": 0,
                "team_rows_missing_mapping": 0,
                "player_rows_missing_mapping": 0,
                "team_mapping_coverage_percentage": None,
                "player_mapping_coverage_percentage": None,
                "sample_team_rows_missing_mapping": [],
                "sample_player_rows_missing_mapping": [],
            },
            "passes_min_coverage": False,
            "warnings": [f"Database not found: {db_path}"],
        }

    with _connect(db_path) as conn:
        teams = _load_teams(conn, league=league)
        injury_rows, player_id_column_present = _load_injury_rows(conn, league=league)
        games = _load_upcoming_games(
            conn,
            league=league,
            as_of=as_of,
            lookahead_days=lookahead_days,
        )

    mapping = _mapping_report(
        injury_rows,
        league=league,
        teams=teams,
        player_id_column_present=player_id_column_present,
    )

    team_slots: list[dict[str, Any]] = []
    stale_rows: list[dict[str, Any]] = []
    game_details: list[dict[str, Any]] = []

    for game in games:
        game_slot_records = []
        for side in ("home", "away"):
            team_id = game[f"{side}_team_id"]
            team_code = str(game[f"{side}_team_code"] or "").upper()
            team_name = game[f"{side}_team_name"]
            matching, recent = _availability_rows_for_team(
                injury_rows,
                team_id=team_id,
                team_code=team_code,
                as_of=as_of,
                stale_after=stale_after,
            )
            latest = matching[-1] if matching else None
            latest_report_at = latest["report_date"] if latest else None
            covered = bool(recent)
            distinct_players = {
                row["player_id"] or row["player_name"]
                for row in recent
                if row.get("player_id") or row.get("player_name")
            }
            record = {
                "game_id": game["game_id"],
                "game_date": game["start_time_utc"].date().isoformat(),
                "start_time_utc": game["start_time_utc"].isoformat(),
                "side": side,
                "team_id": team_id,
                "team_code": team_code,
                "team_name": team_name,
                "has_availability_rows": covered,
                "recent_availability_rows": len(recent),
                "recent_distinct_players": len(distinct_players),
                "latest_report_at": _iso(latest_report_at),
                "latest_report_age_days": _age_days(as_of, latest_report_at),
            }
            team_slots.append(record)
            game_slot_records.append(record)
            if matching and not covered:
                stale_rows.append(
                    {
                        "game_id": game["game_id"],
                        "game_date": game["start_time_utc"].date().isoformat(),
                        "team_id": team_id,
                        "team_code": team_code,
                        "team_name": team_name,
                        "latest_report_at": _iso(latest_report_at),
                        "latest_report_age_days": _age_days(as_of, latest_report_at),
                    }
                )

        game_details.append(
            {
                "game_id": game["game_id"],
                "start_time_utc": game["start_time_utc"].isoformat(),
                "home_team_code": game["home_team_code"],
                "away_team_code": game["away_team_code"],
                "has_any_availability_rows": any(
                    item["has_availability_rows"] for item in game_slot_records
                ),
                "has_full_availability_rows": all(
                    item["has_availability_rows"] for item in game_slot_records
                ),
                "teams": game_slot_records,
            }
        )

    total_slots = len(team_slots)
    covered_slots = sum(1 for row in team_slots if row["has_availability_rows"])
    coverage_percentage = round(covered_slots / total_slots, 4) if total_slots else None

    by_date: dict[str, dict[str, Any]] = {}
    for row in team_slots:
        bucket = by_date.setdefault(
            row["game_date"],
            {"date": row["game_date"], "team_slots": 0, "covered_team_slots": 0},
        )
        bucket["team_slots"] += 1
        bucket["covered_team_slots"] += int(row["has_availability_rows"])
    coverage_by_date = []
    for value in by_date.values():
        value["coverage_percentage"] = round(
            value["covered_team_slots"] / value["team_slots"], 4
        )
        coverage_by_date.append(value)

    by_team: dict[tuple[Any, str], dict[str, Any]] = {}
    for row in team_slots:
        key = (row["team_id"], row["team_code"])
        bucket = by_team.setdefault(
            key,
            {
                "team_id": row["team_id"],
                "team_code": row["team_code"],
                "team_name": row["team_name"],
                "team_slots": 0,
                "covered_team_slots": 0,
            },
        )
        bucket["team_slots"] += 1
        bucket["covered_team_slots"] += int(row["has_availability_rows"])
    coverage_by_team = []
    for value in by_team.values():
        value["coverage_percentage"] = round(
            value["covered_team_slots"] / value["team_slots"], 4
        )
        coverage_by_team.append(value)

    recent_team_rows: dict[tuple[Any, str], dict[str, Any]] = {}
    for row in injury_rows:
        report_at = row.get("report_date")
        if report_at is None or report_at < stale_after or report_at > as_of:
            continue
        code = next(iter(row.get("normalized_team_codes") or []), row.get("team_code") or "")
        key = (row.get("team_id"), code)
        bucket = recent_team_rows.setdefault(
            key,
            {
                "team_id": row.get("team_id"),
                "team_code": code,
                "latest_report_at": None,
                "row_count": 0,
                "player_keys": set(),
            },
        )
        bucket["row_count"] += 1
        bucket["player_keys"].add(row.get("player_id") or row.get("player_name"))
        latest = bucket["latest_report_at"]
        if latest is None or report_at > latest:
            bucket["latest_report_at"] = report_at
    recent_teams = []
    for value in recent_team_rows.values():
        player_keys = {item for item in value.pop("player_keys") if item}
        value["player_count"] = len(player_keys)
        value["latest_report_at"] = _iso(value["latest_report_at"])
        recent_teams.append(value)

    upcoming_summary = {
        "total": len(games),
        "with_any_availability_rows": sum(
            1 for game in game_details if game["has_any_availability_rows"]
        ),
        "with_full_availability_rows": sum(
            1 for game in game_details if game["has_full_availability_rows"]
        ),
        "team_slots": total_slots,
        "covered_team_slots": covered_slots,
        "coverage_percentage": coverage_percentage,
        "games": game_details,
    }

    warnings: list[str] = []
    if total_slots == 0:
        warnings.append(f"No upcoming {league} games found in the next {lookahead_days} day(s)")
        passes_min_coverage = True
    else:
        passes_min_coverage = bool(
            coverage_percentage is not None and coverage_percentage >= min_coverage
        )
        if not passes_min_coverage:
            warnings.append(
                f"{league} availability coverage {coverage_percentage:.1%} is below "
                f"configured minimum {min_coverage:.1%}"
            )
    if stale_rows:
        warnings.append(
            f"{len(stale_rows)} upcoming {league} team slot(s) only have stale availability rows"
        )
    if mapping["team_rows_missing_mapping"]:
        warnings.append(
            f"{mapping['team_rows_missing_mapping']} {league} availability row(s) have missing team mappings"
        )
    if mapping["player_rows_missing_mapping"]:
        warnings.append(
            f"{mapping['player_rows_missing_mapping']} {league} availability row(s) have missing player mappings"
        )

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "db_path": str(db_path),
        "league": league,
        "as_of": as_of.isoformat(),
        "lookahead_days": lookahead_days,
        "max_stale_days": max_stale_days,
        "min_coverage": min_coverage,
        "upcoming_games": upcoming_summary,
        "coverage_by_date": sorted(coverage_by_date, key=lambda item: item["date"]),
        "coverage_by_date_team": team_slots,
        "coverage_by_team": sorted(
            coverage_by_team,
            key=lambda item: (str(item["team_code"]), str(item["team_id"])),
        ),
        "recent_teams": sorted(
            recent_teams,
            key=lambda item: (str(item.get("team_code") or ""), str(item.get("team_id") or "")),
        ),
        "stale_availability_rows": stale_rows,
        "missing_mappings": mapping,
        "passes_min_coverage": passes_min_coverage,
        "warnings": warnings,
    }


def write_availability_report(
    report: dict[str, Any],
    *,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    output_path: Optional[Path] = None,
) -> Path:
    if output_path is None:
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        output_path = output_dir / f"availability_{report['league'].lower()}_{timestamp}.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    return output_path


def format_summary(report: dict[str, Any], report_path: Optional[Path] = None) -> str:
    upcoming = report["upcoming_games"]
    coverage = upcoming["coverage_percentage"]
    coverage_text = "n/a" if coverage is None else f"{coverage:.1%}"
    lines = [
        "AVAILABILITY QUALITY SUMMARY",
        f"league={report['league']} as_of={report['as_of']} lookahead_days={report['lookahead_days']}",
        (
            f"upcoming_games={upcoming['total']} "
            f"full_games={upcoming['with_full_availability_rows']} "
            f"team_slot_coverage={upcoming['covered_team_slots']}/{upcoming['team_slots']} "
            f"({coverage_text})"
        ),
        (
            "mapping_gaps="
            f"team:{report['missing_mappings']['team_rows_missing_mapping']} "
            f"player:{report['missing_mappings']['player_rows_missing_mapping']}"
        ),
        f"stale_team_slots={len(report['stale_availability_rows'])}",
        f"overall={'PASS' if report['passes_min_coverage'] else 'WARN'}",
    ]
    if report_path is not None:
        lines.append(f"report={report_path}")
    for warning in report["warnings"]:
        lines.append(f"WARNING {warning}")
    return "\n".join(lines)


def warn_if_low_availability_coverage(
    *,
    db_path: Path = DB_PATH,
    leagues: Optional[Iterable[str]] = None,
    min_coverage: Optional[float] = None,
    lookahead_days: Optional[int] = None,
    max_stale_days: Optional[int] = None,
    output_dir: Optional[Path] = None,
) -> Optional[dict[str, Any]]:
    """Write an NBA availability report and log warnings without failing callers."""

    normalized = _normalize_leagues(leagues)
    if normalized and "NBA" not in normalized:
        return None

    try:
        threshold = _coerce_coverage_threshold(
            min_coverage
            if min_coverage is not None
            else _env_float("NBA_AVAILABILITY_MIN_COVERAGE", DEFAULT_MIN_COVERAGE)
        )
        lookahead = (
            lookahead_days
            if lookahead_days is not None
            else _env_int("NBA_AVAILABILITY_LOOKAHEAD_DAYS", DEFAULT_LOOKAHEAD_DAYS)
        )
        stale_days = (
            max_stale_days
            if max_stale_days is not None
            else _env_int("NBA_AVAILABILITY_MAX_STALE_DAYS", DEFAULT_MAX_STALE_DAYS)
        )
        destination = output_dir or Path(
            os.getenv("NBA_AVAILABILITY_REPORT_DIR", str(DEFAULT_OUTPUT_DIR))
        )

        report = build_availability_report(
            db_path=db_path,
            league="NBA",
            lookahead_days=lookahead,
            max_stale_days=stale_days,
            min_coverage=threshold,
        )
        report_path = write_availability_report(report, output_dir=destination)
    except Exception as exc:  # noqa: BLE001 - this is warning-only pipeline observability.
        LOGGER.warning("NBA availability coverage check could not run: %s", exc)
        return None

    actionable_warnings = [
        warning
        for warning in report["warnings"]
        if not warning.startswith("No upcoming NBA games")
    ]
    if actionable_warnings:
        LOGGER.warning(
            "NBA availability coverage check found %d warning(s); report=%s; coverage=%s",
            len(actionable_warnings),
            report_path,
            report["upcoming_games"].get("coverage_percentage"),
        )
        for warning in actionable_warnings:
            LOGGER.warning("NBA availability: %s", warning)
    else:
        LOGGER.info("NBA availability coverage check passed; report=%s", report_path)
    return report


def _parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Report NBA availability coverage quality.")
    parser.add_argument("--db-path", "--db", dest="db_path", type=Path, default=DB_PATH)
    parser.add_argument("--league", default="NBA")
    parser.add_argument("--lookahead-days", type=int, default=DEFAULT_LOOKAHEAD_DAYS)
    parser.add_argument("--max-stale-days", type=int, default=DEFAULT_MAX_STALE_DAYS)
    parser.add_argument("--min-coverage", type=float, default=DEFAULT_MIN_COVERAGE)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--output", type=Path, default=None)
    parser.add_argument("--as-of", default=None, help="UTC timestamp used as the report clock.")
    parser.add_argument(
        "--enforce",
        action="store_true",
        help="Exit nonzero when coverage is below --min-coverage.",
    )
    parser.add_argument(
        "--log-level",
        default="WARNING",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
    )
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = _parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level))
    as_of = _parse_datetime(args.as_of) if args.as_of else None
    report = build_availability_report(
        db_path=args.db_path,
        league=args.league,
        lookahead_days=args.lookahead_days,
        max_stale_days=args.max_stale_days,
        min_coverage=args.min_coverage,
        as_of=as_of,
    )
    report_path = write_availability_report(
        report,
        output_dir=args.output_dir,
        output_path=args.output,
    )
    print(format_summary(report, report_path))
    if args.enforce and not report["passes_min_coverage"]:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
