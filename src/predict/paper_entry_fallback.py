"""Plan and bind an optional NBA entry window when every provider ID is missing."""

from __future__ import annotations

import hashlib
import sqlite3
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import requests

from src.data.audit_nba_schedule import _NBA_GAMES, _NBA_HEADERS, _Fixture, _official_fixtures
from src.data.ingest_nba_events import (
    _eligible_events,
    _Event,
    _existing_pairing,
    _parse_event,
    _utc,
)
from src.db.loaders import _generate_internal_id
from src.predict.paper_close_poll import _ClosingFixture
from src.predict.paper_entry_poll import (
    _DAILY_CAPTURE_ET,
    _EASTERN,
    _MIN_TRIGGER_LEAD,
    _already_handled,
)
from src.predict.paper_ledger import PaperTrial, _eligible_game
from src.predict.paper_settlement import _START_TOLERANCE

_MAX_OFFICIAL_AGE = timedelta(minutes=30)
_POLL_INTERVAL = timedelta(minutes=15)


@dataclass(frozen=True)
class _Target:
    official: _Fixture
    home_team_id: int
    away_team_id: int
    game_id: str | None
    canonical_start: datetime | None


@dataclass(frozen=True)
class FallbackPlan:
    eastern_date: date
    planned_at: datetime
    official_observed_at: datetime
    official_sha256: str
    trial_specification: str
    sport_id: int
    request_from: datetime
    request_to: datetime
    targets: tuple[_Target, ...]

    def odds_window(self) -> dict[str, str]:
        return {
            "dateFormat": "iso",
            "commenceTimeFrom": _iso_parameter(self.request_from),
            "commenceTimeTo": _iso_parameter(self.request_to),
        }


@dataclass(frozen=True)
class WindowFixture:
    fixture: _ClosingFixture
    provider: _Event
    action: str | None


def fetch_official_slate(day: date) -> tuple[bytes, datetime]:
    url = f"{_NBA_GAMES}?date={day.isoformat()}"
    try:
        response = requests.get(url, headers=_NBA_HEADERS, timeout=15, allow_redirects=False)
    except requests.RequestException:
        raise RuntimeError("Could not fetch the official NBA games feed") from None
    if response.status_code != 200:
        raise RuntimeError(f"Official NBA games feed returned HTTP {response.status_code}")
    return response.content, datetime.now(timezone.utc)


def archive_official_slate(directory: Path, raw: bytes, plan: FallbackPlan) -> dict[str, object]:
    if hashlib.sha256(raw).hexdigest() != plan.official_sha256:
        raise RuntimeError("Official NBA slate changed before archiving")
    directory.mkdir(parents=True, exist_ok=True)
    name = "official_schedule.html"
    with (directory / name).open("xb") as archive:
        archive.write(raw)
    return {
        "url": f"{_NBA_GAMES}?date={plan.eastern_date.isoformat()}",
        "observed_at": plan.official_observed_at.isoformat(),
        "sha256": plan.official_sha256,
        "filename": name,
        "nba_game_ids": [target.official.game_id for target in plan.targets],
    }


def _iso_parameter(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _existing_target(
    conn: sqlite3.Connection, fixture: _Fixture, sport_id: int, home_id: int, away_id: int
) -> list[sqlite3.Row]:
    # The discovery pairing query uses the supplied team IDs and start. This is
    # the official NBA ID, which remains distinct from an Odds API event ID.
    official = _Event(
        fixture.game_id, fixture.home, fixture.away, fixture.home, fixture.away, fixture.start
    )
    return _existing_pairing(conn, sport_id, official, home_id, away_id)


def _safe_canonical(
    rows: list[sqlite3.Row], home_id: int, away_id: int, official_start: datetime
) -> tuple[sqlite3.Row, datetime] | None:
    if len(rows) != 1:
        return None
    row = rows[0]
    start = _utc(row["start_time_utc"])
    if (
        start is None
        or abs(start - official_start) > _START_TOLERANCE
        or (row["home_team_id"], row["away_team_id"]) != (home_id, away_id)
        or (row["status"] or "").lower() != "scheduled"
        or row["home_score"] is not None
        or row["away_score"] is not None
    ):
        return None
    return row, start


def plan_missing_ids(
    conn: sqlite3.Connection,
    trial: PaperTrial,
    official_raw: bytes,
    official_observed_at: datetime,
    now: datetime,
) -> tuple[str, FallbackPlan | None]:
    """Plan a window after the ordinary collector finds no batch and no reservation."""
    if now.utcoffset() is None or official_observed_at.utcoffset() is None:
        raise ValueError("Use timezone-aware observation times")
    if not timedelta(0) <= now - official_observed_at <= _MAX_OFFICIAL_AGE:
        return "stale_official_observation", None
    day = now.astimezone(_EASTERN).date()
    _, fixtures = _official_fixtures(official_raw, day)
    official_teams = [code for fixture in fixtures for code in (fixture.home, fixture.away)]
    official_ids = [
        team_id for fixture in fixtures for team_id in (fixture.home_id, fixture.away_id)
    ]
    if len(set(official_teams)) != len(official_teams) or len(set(official_ids)) != len(
        official_ids
    ):
        return "fixture_conflict", None
    sport = conn.execute("SELECT sport_id FROM sports WHERE league = 'NBA'").fetchone()
    if sport is None:
        return "nba_unavailable", None
    sport_id = sport["sport_id"]
    teams = dict(conn.execute("SELECT code, team_id FROM teams WHERE sport_id = ?", (sport_id,)))
    evening = datetime.combine(day, _DAILY_CAPTURE_ET, _EASTERN)
    lead = timedelta(minutes=trial.min_minutes_before_start)
    max_lead = timedelta(hours=trial.max_hours_before_start)
    targets = []
    earliest = []
    for fixture in fixtures:
        if not (lead <= fixture.start - now <= max_lead and fixture.start - lead < evening):
            continue
        home, away = teams.get(fixture.home), teams.get(fixture.away)
        if home is None or away is None:
            return "fixture_conflict", None
        rows = _existing_target(conn, fixture, sport_id, home, away)
        canonical = _safe_canonical(rows, home, away, fixture.start)
        if rows and canonical is None:
            return "fixture_conflict", None
        if canonical is not None:
            game, start = canonical
            if game["odds_api_id"]:
                return "provider_ids_present", None
            if _already_handled(conn, trial.trial_id, game):
                continue
            if (
                min(start, fixture.start) - now < lead
                or max(start, fixture.start) - lead >= evening
            ):
                return "fixture_conflict", None
            game_id = game["game_id"]
        else:
            game_id, start = None, None
        targets.append(_Target(fixture, home, away, game_id, start))
        earliest.append(min(fixture.start, start) if start is not None else fixture.start)

    if not targets:
        return "no_official_early_fixture", None
    trigger_min = max(lead, _MIN_TRIGGER_LEAD)
    triggers = [start - now for start in earliest if start - now >= trigger_min]
    if not triggers:
        return "outside_last_window", None
    if min(triggers) >= trigger_min + _POLL_INTERVAL:
        return "waiting_for_last_window", None
    starts = [target.official.start for target in targets]
    return "window_due", FallbackPlan(
        day,
        now.astimezone(timezone.utc),
        official_observed_at.astimezone(timezone.utc),
        hashlib.sha256(official_raw).hexdigest(),
        trial.specification,
        sport_id,
        min(starts) - _START_TOLERANCE,
        max(starts) + _START_TOLERANCE,
        tuple(targets),
    )


def classify_probe(payload: list, plan: FallbackPlan) -> str:
    """Separate genuinely absent target IDs from usable or conflicting free evidence."""
    targets = {
        frozenset((target.official.home, target.official.away)): target for target in plan.targets
    }
    events = [_parse_event(row) for row in payload]
    counts = Counter(event.event_id for event in events if event is not None)
    matched: set[frozenset[str]] = set()
    for event in events:
        if event is None:
            return "conflict"
        pair = frozenset((event.home_code, event.away_code))
        target = targets.get(pair)
        if target is None or event.start.astimezone(_EASTERN).date() != plan.eastern_date:
            continue
        official = target.official
        starts: tuple[datetime, ...] = (official.start, event.start)
        if target.canonical_start is not None:
            starts += (target.canonical_start,)
        if (
            pair in matched
            or counts[event.event_id] != 1
            or (event.home_code, event.away_code) != (official.home, official.away)
            or max(starts) - min(starts) > _START_TOLERANCE
        ):
            return "conflict"
        matched.add(pair)
    return "available" if matched else "absent"


def match_window_fixtures(
    conn: sqlite3.Connection,
    trial: PaperTrial,
    plan: FallbackPlan,
    payload: list,
    fetched: datetime,
    book_id: int,
) -> dict[str, WindowFixture]:
    """Resolve raw provider identities to the reviewed official targets without writing."""
    if (
        fetched.utcoffset() is None
        or fetched < plan.planned_at
        or fetched.astimezone(_EASTERN).date() != plan.eastern_date
        or fetched - plan.official_observed_at > _MAX_OFFICIAL_AGE
        or trial.specification != plan.trial_specification
    ):
        return {}
    events = _eligible_events(payload, fetched, plan.request_to)
    teams = dict(
        conn.execute("SELECT code, team_id FROM teams WHERE sport_id = ?", (plan.sport_id,))
    )
    fixtures = {}
    for target in plan.targets:
        fixture = target.official
        matches = [
            event
            for event in events
            if (event.home_code, event.away_code) == (fixture.home, fixture.away)
            and event.start >= plan.request_from
            and abs(event.start - fixture.start) <= _START_TOLERANCE
        ]
        if len(matches) != 1:
            continue
        event = matches[0]
        home, away = target.home_team_id, target.away_team_id
        if (teams.get(fixture.home), teams.get(fixture.away)) != (home, away):
            continue
        rows = _existing_pairing(conn, plan.sport_id, event, home, away)
        canonical = _safe_canonical(rows, home, away, fixture.start)
        if rows and canonical is None:
            continue
        if canonical is None:
            if target.game_id is not None:
                continue
            game_id = _generate_internal_id("NBA", event.start.isoformat(), event.home, event.away)
            if conn.execute("SELECT 1 FROM games WHERE game_id = ?", (game_id,)).fetchone():
                continue
            canonical_start = event.start
            action: str | None = "created"
        else:
            game, canonical_start = canonical
            game_id = game["game_id"]
            if (
                (target.game_id is not None and target.game_id != game_id)
                or (game["odds_api_id"] and game["odds_api_id"] != event.event_id)
                or _already_handled(conn, trial.trial_id, game)
                or _eligible_game(conn, game_id, trial.league) is None
            ):
                continue
            action = None if game["odds_api_id"] else "linked"
        owners = conn.execute(
            "SELECT game_id FROM games WHERE odds_api_id = ?", (event.event_id,)
        ).fetchall()
        if owners and [owner["game_id"] for owner in owners] != [game_id]:
            continue
        starts: tuple[datetime, ...] = (fixture.start, canonical_start, event.start)
        if target.canonical_start is not None:
            starts += (target.canonical_start,)
        earliest, latest = min(starts), max(starts)
        if latest - earliest > _START_TOLERANCE:
            continue
        fixtures[event.event_id] = WindowFixture(
            _ClosingFixture(game_id, plan.sport_id, home, away, book_id, earliest, latest),
            event,
            action,
        )
    return fixtures
