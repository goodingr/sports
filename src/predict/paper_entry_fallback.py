"""Plan and bind an optional NBA entry window when every provider ID is missing."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import requests

from src.data.audit_nba_schedule import (
    _NBA_GAMES,
    _NBA_HEADERS,
    AUDIT_REPORTS_DIR,
    _Fixture,
    _official_fixtures,
    read_official_schedule_evidence,
)
from src.data.ingest_nba_events import (
    _EVENT_ID,
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
_MAX_PLANNING_AGE = timedelta(days=2)
_POLL_INTERVAL = timedelta(minutes=15)


@dataclass(frozen=True)
class _Target:
    official: _Fixture
    home_team_id: int
    away_team_id: int
    game_id: str | None
    canonical_start: datetime | None
    provider_id: str | None

    @property
    def earliest_start(self) -> datetime:
        return min(self.official.start, self.canonical_start or self.official.start)


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
    ordinary_due_ids: frozenset[str]
    probe_starts: tuple[tuple[str, datetime], ...] = ()

    def odds_window(self) -> dict[str, str]:
        return {
            "dateFormat": "iso",
            "commenceTimeFrom": _iso_parameter(self.request_from),
            "commenceTimeTo": _iso_parameter(self.request_to),
        }

    def dispatch_status(self, now: datetime, minimum_lead: timedelta) -> str:
        if now < self.planned_at:
            return "entry_clock_reversed"
        if not timedelta(0) <= now - self.official_observed_at <= _MAX_OFFICIAL_AGE:
            return "stale_official_observation"
        if self.ordinary_due_ids:
            starts = dict(self.probe_starts)
            actionable = any(
                min(target.earliest_start, starts[provider_id]) - now >= minimum_lead
                for target in self.targets
                if (provider_id := target.provider_id) is not None
                and provider_id in self.ordinary_due_ids
            )
            return "window_due" if actionable else "entry_lead_elapsed"
        return _last_window_status(
            (target for target in self.targets if target.provider_id is None), now, minimum_lead
        )


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


def archive_official_slate(
    directory: Path,
    raw: bytes,
    day: date,
    observed_at: datetime,
    plan: FallbackPlan | None = None,
) -> dict[str, object]:
    digest = hashlib.sha256(raw).hexdigest()
    if plan is not None and digest != plan.official_sha256:
        raise RuntimeError("Official NBA slate changed before archiving")
    directory.mkdir(parents=True, exist_ok=True)
    name = "official_schedule.html"
    with (directory / name).open("xb") as archive:
        archive.write(raw)
    return {
        "url": f"{_NBA_GAMES}?date={day.isoformat()}",
        "observed_at": observed_at.isoformat(),
        "sha256": digest,
        "filename": name,
        "nba_game_ids": [target.official.game_id for target in plan.targets] if plan else [],
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


def _last_window_status(targets: Iterable[_Target], now: datetime, minimum_lead: timedelta) -> str:
    trigger_min = max(minimum_lead, _MIN_TRIGGER_LEAD)
    leads = (target.earliest_start - now for target in targets)
    trigger = min((lead for lead in leads if lead >= trigger_min), default=None)
    if trigger is None:
        return "outside_last_window"
    if trigger >= trigger_min + _POLL_INTERVAL:
        return "waiting_for_last_window"
    return "window_due"


def read_official_fixtures(
    raw: bytes, observed_at: datetime, now: datetime
) -> tuple[str, list[_Fixture]]:
    if now.utcoffset() is None or observed_at.utcoffset() is None:
        raise ValueError("Use timezone-aware observation times")
    if not timedelta(0) <= now - observed_at <= _MAX_OFFICIAL_AGE:
        return "stale_official_observation", []
    return _unambiguous_fixtures(raw, now.astimezone(_EASTERN).date())


def _unambiguous_fixtures(raw: bytes, day: date) -> tuple[str, list[_Fixture]]:
    _, fixtures = _official_fixtures(raw, day)
    codes = {code for fixture in fixtures for code in (fixture.home, fixture.away)}
    ids = {team_id for fixture in fixtures for team_id in (fixture.home_id, fixture.away_id)}
    if len(codes) != 2 * len(fixtures) or len(ids) != 2 * len(fixtures):
        return "fixture_conflict", []
    return "verified", fixtures


def anchor_ordinary_clocks(
    conn: sqlite3.Connection,
    due: dict[str, _ClosingFixture],
    official: Iterable[_Fixture] = (),
    *,
    free_events: list | None = None,
) -> dict[str, _ClosingFixture]:
    """Use compatible earlier clocks and omit fixtures with conflicting ones."""
    if not due:
        return due
    sport_id = next(iter(due.values())).sport_id
    codes = dict(conn.execute("SELECT team_id, code FROM teams WHERE sport_id = ?", (sport_id,)))
    official_starts = {(fixture.home, fixture.away): fixture.start for fixture in official}
    events = [_parse_event(row) for row in free_events] if free_events is not None else []
    by_id = {event.event_id: event for event in events if event is not None}
    counts = Counter(
        row["id"]
        for row in free_events or []
        if isinstance(row, dict) and isinstance(row.get("id"), str)
    )
    projected = {}
    for provider_id, fixture in due.items():
        home = codes.get(fixture.home_team_id)
        away = codes.get(fixture.away_team_id)
        if home is None or away is None:
            projected[provider_id] = fixture
            continue
        pair = (home, away)
        starts = [fixture.start]
        official_start = official_starts.get(pair)
        if official_start is not None:
            starts.append(official_start)
        event = by_id.get(provider_id)
        if (
            event is not None
            and counts[provider_id] == 1
            and (event.home_code, event.away_code) == pair
        ):
            starts.append(event.start)
        earliest = min(starts)
        if fixture.latest_start - earliest > _START_TOLERANCE:
            continue
        projected[provider_id] = replace(fixture, start=earliest)
    return projected


def plan_missing_ids(
    conn: sqlite3.Connection,
    trial: PaperTrial,
    official_raw: bytes,
    official_observed_at: datetime,
    now: datetime,
    ordinary_due: dict[str, _ClosingFixture],
    fixtures: list[_Fixture],
) -> tuple[str, FallbackPlan | None]:
    """Plan a window for unresolved official fixtures and cover every ordinary due row."""
    day = now.astimezone(_EASTERN).date()
    sport = conn.execute("SELECT sport_id FROM sports WHERE league = 'NBA'").fetchone()
    if sport is None:
        return "nba_unavailable", None
    sport_id = sport["sport_id"]
    teams = dict(conn.execute("SELECT code, team_id FROM teams WHERE sport_id = ?", (sport_id,)))
    evening = datetime.combine(day, _DAILY_CAPTURE_ET, _EASTERN)
    lead = timedelta(minutes=trial.min_minutes_before_start)
    max_lead = timedelta(hours=trial.max_hours_before_start)
    targets = []
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
            if _already_handled(conn, trial.trial_id, game):
                continue
            if (
                min(start, fixture.start) - now < lead
                or max(start, fixture.start) - lead >= evening
            ):
                return "fixture_conflict", None
            game_id = game["game_id"]
            provider_id = game["odds_api_id"] or None
            if provider_id is not None:
                if not _EVENT_ID.fullmatch(provider_id) or [
                    row["game_id"]
                    for row in conn.execute(
                        "SELECT game_id FROM games WHERE odds_api_id = ?", (provider_id,)
                    )
                ] != [game_id]:
                    return "fixture_conflict", None
        else:
            game_id, start, provider_id = None, None, None
        targets.append(_Target(fixture, home, away, game_id, start, provider_id))
    unresolved = [target for target in targets if target.provider_id is None]
    if not targets:
        return "no_official_early_fixture", None
    if not unresolved:
        return "no_official_missing_fixture", None
    covered = {target.provider_id: target.game_id for target in targets if target.provider_id}
    if any(
        covered.get(provider_id) != fixture.game_id for provider_id, fixture in ordinary_due.items()
    ):
        return "ordinary_not_covered", None
    if not ordinary_due:
        status = _last_window_status(unresolved, now, lead)
        if status != "window_due":
            return status, None
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
        frozenset(ordinary_due),
    )


def cached_official_window_due(
    conn: sqlite3.Connection,
    trial: PaperTrial,
    now: datetime,
    ordinary_due: dict[str, _ClosingFixture],
    data_dir: Path,
) -> bool:
    """Use a saved slate only to decide whether a fresh official fetch is useful."""
    day = now.astimezone(_EASTERN).date()
    try:
        path = max((AUDIT_REPORTS_DIR / day.isoformat()).glob("*.json"), default=None)
        if path is None:
            return False
        report = json.loads(path.read_bytes())
        source = report["official_source"]
        observed = _utc(source["observed_at_utc"])
        if (
            report["report_version"] != 1 or report["eastern_date"] != day.isoformat()
            or report["trial_id"] != trial.trial_id or observed is None
            or not timedelta() <= now - observed <= _MAX_PLANNING_AGE
        ):
            return False
        raw = read_official_schedule_evidence(day, source, data_dir)
        verified, fixtures = _unambiguous_fixtures(raw, day)
        if verified != "verified":
            return False
    except (AttributeError, KeyError, OSError, TypeError, ValueError):
        return False
    status, _ = plan_missing_ids(conn, trial, raw, observed, now, ordinary_due, fixtures)
    return status == "window_due"


def classify_probe(payload: list, plan: FallbackPlan) -> tuple[str, dict[str, datetime]]:
    """Separate genuinely absent target IDs from usable or conflicting free evidence."""
    targets = {
        frozenset((target.official.home, target.official.away)): target for target in plan.targets
    }
    events = [_parse_event(row) for row in payload]
    counts = Counter(event.event_id for event in events if event is not None)
    matched: set[frozenset[str]] = set()
    starts_by_id: dict[str, datetime] = {}
    missing_observed = False
    for event in events:
        if event is None:
            return "conflict", {}
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
            or (target.provider_id is not None and target.provider_id != event.event_id)
            or max(starts) - min(starts) > _START_TOLERANCE
        ):
            return "conflict", {}
        matched.add(pair)
        starts_by_id[event.event_id] = event.start
        if target.provider_id is None:
            missing_observed = True
    return ("available" if missing_observed else "absent"), starts_by_id


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
    probe_starts = dict(plan.probe_starts)
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
        if target.provider_id is not None and event.event_id != target.provider_id:
            continue
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
                or (target.provider_id is not None and game["odds_api_id"] != target.provider_id)
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
        if event.event_id in probe_starts:
            starts += (probe_starts[event.event_id],)
        earliest, latest = min(starts), max(starts)
        if latest - earliest > _START_TOLERANCE:
            continue
        fixtures[event.event_id] = WindowFixture(
            _ClosingFixture(game_id, plan.sport_id, home, away, book_id, earliest, latest),
            event,
            action,
        )
    return fixtures
