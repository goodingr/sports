"""Discover upcoming NBA fixtures and provider IDs from the free events endpoint."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sqlite3
from collections import Counter, defaultdict
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from pathlib import Path
from uuid import uuid4
from zoneinfo import ZoneInfo

import requests

from src.data.config import RAW_DATA_DIR, OddsAPIKeyManager, load_env
from src.data.ingest_odds import _request_metadata, _request_odds, write_snapshot
from src.data.team_mappings import NBA_ALIASES
from src.db.core import connect, resolve_db_path
from src.db.loaders import (
    _advance_odds_api_schedule_clock,
    _ensure_odds_api_schedule_clock,
    _generate_internal_id,
)

_SPORT = "basketball_nba"
_URL = f"https://api.the-odds-api.com/v4/sports/{_SPORT}/events"
_SOURCE = "the-odds-api-nba-events"
_ENTRY_PROBE_SOURCE = "the-odds-api-nba-entry-probe"
_INTERVAL = timedelta(hours=1)
_HORIZON = timedelta(hours=48)
_MATCH_TOLERANCE = timedelta(minutes=10)
_ALIAS_TOLERANCE = timedelta(hours=2)
_EASTERN = ZoneInfo("America/New_York")
_EVENT_ID = re.compile(r"[a-zA-Z0-9]{32}")


@dataclass(frozen=True)
class _Event:
    event_id: str
    home: str
    away: str
    home_code: str
    away_code: str
    start: datetime


def _utc(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
        return parsed.astimezone(timezone.utc) if parsed.utcoffset() is not None else None
    except (ValueError, OverflowError):
        return None


def _recent_attempt(conn: sqlite3.Connection, now: datetime) -> bool:
    latest = conn.execute(
        """
        SELECT r.started_at FROM source_runs r JOIN data_sources s ON s.source_id = r.source_id
        WHERE s.source_key = ? ORDER BY r.started_at DESC LIMIT 1
        """,
        (_SOURCE,),
    ).fetchone()
    if latest is None:
        return False
    started = _utc(latest["started_at"])
    if started is None:
        raise RuntimeError("Invalid NBA event discovery attempt timestamp")
    return now - started < _INTERVAL


def _reserve(path: Path, *, source: str = _SOURCE) -> tuple[str, int, datetime] | None:
    with connect(path) as conn:
        conn.execute("BEGIN IMMEDIATE")
        now = datetime.now(timezone.utc)
        if source == _SOURCE and _recent_attempt(conn, now):
            return None
        frequency = "hourly" if source == _SOURCE else "manual"
        name = (
            "The Odds API NBA events" if source == _SOURCE else "The Odds API NBA entry quota probe"
        )
        conn.execute(
            """
            INSERT INTO data_sources (source_key, name, league, category, uri, default_frequency)
            VALUES (?, ?, 'NBA', 'schedule', ?, ?)
            ON CONFLICT (source_key) DO NOTHING
            """,
            (source, name, _URL, frequency),
        )
        source_id = conn.execute(
            "SELECT source_id FROM data_sources WHERE source_key = ?", (source,)
        ).fetchone()["source_id"]
        run_id = uuid4().hex
        conn.execute(
            "INSERT INTO source_runs (run_id, source_id, started_at, status) VALUES (?, ?, ?, 'running')",
            (run_id, source_id, now.isoformat()),
        )
        return run_id, source_id, now


def _eligible_events(payload: list, fetched: datetime, cutoff: datetime) -> list[_Event]:
    ids = Counter(
        row["id"] for row in payload if isinstance(row, dict) and isinstance(row.get("id"), str)
    )
    pairings: dict[frozenset[str], list[_Event]] = defaultdict(list)
    for row in payload:
        if not isinstance(row, dict) or row.get("sport_key") != _SPORT:
            continue
        event_id, home, away = row.get("id"), row.get("home_team"), row.get("away_team")
        start = _utc(row.get("commence_time"))
        if (
            not isinstance(event_id, str)
            or not _EVENT_ID.fullmatch(event_id)
            or not isinstance(home, str)
            or not isinstance(away, str)
            or start is None
        ):
            continue
        home_code = NBA_ALIASES.get(home.strip().lower())
        away_code = NBA_ALIASES.get(away.strip().lower())
        if home_code is None or away_code is None or home_code == away_code:
            continue
        pairings[frozenset((home_code, away_code))].append(
            _Event(event_id, home, away, home_code, away_code, start)
        )
    return [
        event
        for events in pairings.values()
        for event in events
        if fetched < event.start <= cutoff
        and ids[event.event_id] == 1
        and not any(
            other is not event
            and (
                abs(event.start - other.start) <= _ALIAS_TOLERANCE
                or event.start.astimezone(_EASTERN).date()
                == other.start.astimezone(_EASTERN).date()
            )
            for other in events
        )
    ]


def _existing_pairing(
    conn: sqlite3.Connection, sport_id: int, event: _Event, home_id: int, away_id: int
) -> list[sqlite3.Row]:
    eastern_date = event.start.astimezone(_EASTERN).date()
    day_start = datetime.combine(eastern_date, time(), _EASTERN)
    day_end = day_start + timedelta(days=1)
    return conn.execute(
        """
        SELECT g.game_id, g.home_team_id, g.away_team_id, g.start_time_utc, g.status, g.odds_api_id,
               r.home_score, r.away_score
        FROM games g LEFT JOIN game_results r ON r.game_id = g.game_id
        WHERE g.sport_id = ? AND (
            (g.home_team_id = ? AND g.away_team_id = ?) OR (g.home_team_id = ? AND g.away_team_id = ?)
        ) AND (
            julianday(g.start_time_utc) >= julianday(?) AND julianday(g.start_time_utc) < julianday(?)
            OR abs(unixepoch(g.start_time_utc) - unixepoch(?)) <= ?
            OR (substr(g.start_time_utc, 1, 10) = ? AND substr(g.start_time_utc, 12, 8) = '00:00:00'
                AND trim(COALESCE(g.odds_api_id, '')) = '')
        )
        LIMIT 2
        """,
        (
            sport_id,
            home_id,
            away_id,
            away_id,
            home_id,
            day_start.isoformat(),
            day_end.isoformat(),
            event.start.isoformat(),
            _ALIAS_TOLERANCE.total_seconds(),
            eastern_date.isoformat(),
        ),
    ).fetchall()


def _link_event(
    conn: sqlite3.Connection,
    run_id: str,
    event: _Event,
    sport_id: int,
    teams: dict[str, int],
    now: datetime,
    fetched: datetime,
    window: tuple[datetime, datetime] | None,
) -> str:
    home_id, away_id = teams.get(event.home_code), teams.get(event.away_code)
    if home_id is None or away_id is None or event.start <= now:
        return "skipped"
    if conn.execute("SELECT 1 FROM games WHERE odds_api_id = ?", (event.event_id,)).fetchone():
        return "skipped"
    matches = _existing_pairing(conn, sport_id, event, home_id, away_id)
    if len(matches) > 1:
        return "skipped"
    if matches:
        game = matches[0]
        start = _utc(game["start_time_utc"])
        if (
            (game["status"] or "").lower() != "scheduled"
            or game["home_score"] is not None
            or game["away_score"] is not None
            or (game["home_team_id"], game["away_team_id"]) != (home_id, away_id)
            or (game["odds_api_id"] or "").strip()
            or start is None
            or start <= now
            or abs(start - event.start) > _MATCH_TOLERANCE
        ):
            return "skipped"
        game_id, fixture_start, action = game["game_id"], start, "linked"
    else:
        game_id = _generate_internal_id("NBA", event.start.isoformat(), event.home, event.away)
        if conn.execute("SELECT 1 FROM games WHERE game_id = ?", (game_id,)).fetchone():
            return "skipped"
        fixture_start, action = event.start, "created"

    if window is not None and not (
        window[0] <= min(event.start, fixture_start) <= max(event.start, fixture_start) <= window[1]
    ):
        return "skipped"

    inserted = conn.execute(
        """
        INSERT INTO nba_event_discovery_links (
            provider_event_id, game_id, run_id, provider_start_utc, fixture_start_utc,
            home_team_id, away_team_id, action
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING
        """,
        (
            event.event_id,
            game_id,
            run_id,
            event.start.isoformat(),
            fixture_start.isoformat(),
            home_id,
            away_id,
            action,
        ),
    ).rowcount
    if not inserted:
        return "skipped"
    if action == "linked":
        conn.execute(
            "UPDATE games SET odds_api_id = ? WHERE game_id = ?", (event.event_id, game_id)
        )
    else:
        conn.execute(
            """
            INSERT INTO games (game_id, sport_id, season, game_type, start_time_utc, home_team_id,
                               away_team_id, status, odds_api_id)
            VALUES (?, ?, ?, 'NBA', ?, ?, ?, 'scheduled', ?)
            """,
            (
                game_id,
                sport_id,
                event.start.year,
                event.start.isoformat(),
                home_id,
                away_id,
                event.event_id,
            ),
        )
        _ensure_odds_api_schedule_clock(conn)
        _advance_odds_api_schedule_clock(conn, game_id, _SPORT, fetched)
    return action


def _store(
    path: Path,
    run_id: str,
    source_id: int,
    payload: list,
    fetched: datetime,
    cutoff: datetime,
    raw_path: Path,
    *,
    window: tuple[datetime, datetime] | None = None,
) -> dict[str, int]:
    events = _eligible_events(payload, fetched, cutoff)
    digest = hashlib.sha256(raw_path.read_bytes()).hexdigest()
    counts = {"linked": 0, "created": 0, "skipped": len(payload) - len(events)}
    with connect(path) as conn:
        conn.execute("BEGIN IMMEDIATE")
        sport_id = conn.execute("SELECT sport_id FROM sports WHERE league = 'NBA'").fetchone()
        if sport_id is None:
            raise RuntimeError("NBA reference teams are unavailable in the warehouse")
        teams = dict(
            conn.execute("SELECT code, team_id FROM teams WHERE sport_id = ?", (sport_id[0],))
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS nba_event_discovery_links (
                provider_event_id TEXT PRIMARY KEY,
                game_id TEXT NOT NULL UNIQUE,
                run_id TEXT NOT NULL REFERENCES source_runs(run_id),
                provider_start_utc TEXT NOT NULL,
                fixture_start_utc TEXT NOT NULL,
                home_team_id INTEGER NOT NULL,
                away_team_id INTEGER NOT NULL,
                action TEXT NOT NULL CHECK (action IN ('linked', 'created'))
            )
            """
        )
        now = datetime.now(timezone.utc)
        for event in events:
            counts[_link_event(conn, run_id, event, sport_id[0], teams, now, fetched, window)] += 1
        conn.execute(
            """
            INSERT INTO source_files (source_id, captured_at, storage_path, hash)
            VALUES (?, ?, ?, ?)
            """,
            (source_id, fetched.isoformat(), str(raw_path), digest),
        )
        conn.execute(
            """
            UPDATE source_runs SET status = 'success', finished_at = ?, raw_path = ?,
                                   records_ingested = ?, message = ?
            WHERE run_id = ?
            """,
            (
                now.isoformat(),
                str(raw_path),
                counts["linked"] + counts["created"],
                json.dumps(counts),
                run_id,
            ),
        )
    return counts


def _archive_events(
    payload: list,
    response: requests.Response,
    fetched: datetime,
    params: dict[str, str],
    source: str,
) -> Path:
    folder = "events" if source == _SOURCE else "entry_probe"
    return write_snapshot(
        {
            "source": source,
            "fetched_at": fetched.isoformat(),
            "request": _request_metadata(_URL, params),
            "results": payload,
            "rate_limit_remaining": response.headers.get("x-requests-remaining"),
            "request_credits": 0,
        },
        RAW_DATA_DIR / "odds" / _SPORT / folder,
    )


def _mark_failed(path: Path, run_id: str, raw_path: Path | None) -> None:
    with connect(path) as conn:
        conn.execute(
            "UPDATE source_runs SET status = 'failed', finished_at = ?, raw_path = ? WHERE run_id = ?",
            (datetime.now(timezone.utc).isoformat(), str(raw_path) if raw_path else None, run_id),
        )


def retain_entry_probe(
    path: Path,
    response: requests.Response,
    fetched: datetime,
    window: tuple[datetime, datetime],
) -> dict[str, int] | None:
    """Retain an already received zero-credit response without spending the discovery slot."""
    if response.status_code != 200 or response.headers.get("x-requests-last") != "0":
        return None
    try:
        payload = response.json()
    except ValueError:
        return None
    if not isinstance(payload, list) or not any(
        event.start >= window[0] for event in _eligible_events(payload, fetched, window[1])
    ):
        return None

    reservation = _reserve(path, source=_ENTRY_PROBE_SOURCE)
    assert reservation is not None
    run_id, source_id, _ = reservation
    raw_path = None
    try:
        raw_path = _archive_events(payload, response, fetched, {}, _ENTRY_PROBE_SOURCE)
        return _store(path, run_id, source_id, payload, fetched, window[1], raw_path, window=window)
    except (RuntimeError, OSError, sqlite3.Error):
        _mark_failed(path, run_id, raw_path)
        raise


def discover_events(*, collect: bool = False) -> dict[str, str | int]:
    """Inspect the discovery clock, or fetch one free NBA schedule batch when due."""
    load_env()
    path = resolve_db_path().resolve()
    if not path.is_file():
        return {"status": "warehouse_unavailable"}
    with closing(sqlite3.connect(f"{path.as_uri()}?mode=ro", uri=True)) as conn:
        conn.row_factory = sqlite3.Row
        if conn.execute("SELECT 1 FROM sports WHERE league = 'NBA'").fetchone() is None:
            return {"status": "nba_unavailable"}
        if _recent_attempt(conn, datetime.now(timezone.utc)):
            return {"status": "cooldown"}
    if not collect:
        return {"status": "due"}

    keys = OddsAPIKeyManager.get_available_keys()
    index = OddsAPIKeyManager.get_current_index()
    if not 0 <= index < len(keys):
        raise RuntimeError("No active Odds API key is configured")
    reservation = _reserve(path)
    if reservation is None:
        return {"status": "cooldown"}
    run_id, source_id, started = reservation
    cutoff = started + _HORIZON
    params = {
        "apiKey": keys[index],
        "dateFormat": "iso",
        "commenceTimeFrom": started.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "commenceTimeTo": cutoff.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    raw_path = None
    try:
        response = _request_odds(_URL, params)
        fetched = datetime.now(timezone.utc)
        if response.status_code != 200:
            raise RuntimeError(f"Odds API events returned HTTP {response.status_code}")
        if response.headers.get("x-requests-last") != "0":
            raise RuntimeError("Odds API events response did not confirm zero credits")
        try:
            payload = response.json()
        except ValueError:
            raise RuntimeError("Odds API events returned invalid JSON") from None
        if not isinstance(payload, list):
            raise RuntimeError("Odds API events returned an unexpected response")
        raw_path = _archive_events(payload, response, fetched, params, _SOURCE)
        counts = _store(path, run_id, source_id, payload, fetched, cutoff, raw_path)
    except (RuntimeError, OSError, sqlite3.Error) as exc:
        _mark_failed(path, run_id, raw_path)
        if isinstance(exc, RuntimeError):
            raise
        raise RuntimeError("Could not persist the NBA event discovery batch") from exc
    return {"status": "discovered", **counts}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--collect", action="store_true", help="Allow one free events request when due"
    )
    parser.add_argument(
        "--observe-paper",
        action="store_true",
        help="Retain known schedules for the configured paper trial",
    )
    args = parser.parse_args()
    if args.observe_paper and not args.collect:
        parser.error("--observe-paper requires --collect")
    try:
        result = discover_events(collect=args.collect)
        if args.observe_paper and result["status"] in {"discovered", "cooldown"}:
            from src.predict.paper_ledger import load_trial, register_trial

            register_trial(load_trial())
    except (RuntimeError, sqlite3.Error, ValueError) as exc:
        parser.exit(1, f"{exc}\n")
    print(json.dumps(result))


if __name__ == "__main__":
    main()
