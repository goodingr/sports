"""Compare the official NBA slate with warehouse fixtures and retained provider IDs."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sqlite3
from collections import Counter, defaultdict
from contextlib import closing
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any
from uuid import uuid4
from zoneinfo import ZoneInfo

import requests

from scripts.backup_evidence import read_raw_evidence
from src.data.config import PROJECT_ROOT, load_env
from src.data.ingest_nba_events import _EVENT_ID, _SOURCE, _SPORT, _URL, _utc
from src.data.team_mappings import NBA_ALIASES
from src.db.core import resolve_db_path
from src.predict.paper_entry_poll import _DAILY_CAPTURE_ET
from src.predict.paper_ledger import DEFAULT_RULES, load_trial
from src.predict.paper_settlement import _START_TOLERANCE

_EASTERN = ZoneInfo("America/New_York")
_NBA_GAMES = "https://www.nba.com/games"
AUDIT_REPORTS_DIR = PROJECT_ROOT / "reports" / "paper_schedule_audit"
_ODDS_URL = f"https://api.the-odds-api.com/v4/sports/{_SPORT}/odds"
_NBA_GAME_ID = re.compile(r"[0-9]{10}")
_NEXT_DATA = re.compile(
    rb'<script\b[^>]*\bid=["\']__NEXT_DATA__["\'][^>]*>(.*?)</script>', re.DOTALL
)
_NBA_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/json;q=0.9,*/*;q=0.8",
    "Referer": "https://www.nba.com/",
    "Origin": "https://www.nba.com",
}


@dataclass(frozen=True)
class _Fixture:
    game_id: str
    start: datetime
    home: str
    away: str
    home_id: int
    away_id: int


@dataclass(frozen=True)
class _ProviderEvent:
    event_id: str
    start: datetime
    observed_at: datetime
    source: str
    raw_path: str


def _official_fixtures(raw: bytes, day: date) -> tuple[int, list[_Fixture]]:
    matches = _NEXT_DATA.findall(raw)
    try:
        payload = json.loads(matches[0] if len(matches) == 1 else raw)
        page = payload.get("props", payload)["pageProps"]
        iso_day = day.isoformat()
        count = page["allGamesInCurrentYear"][str(day.year)][iso_day]
        modules = page["gameCardFeed"]["modules"]
        if (
            page["selectedDate"] != iso_day
            or page["hasError"] is not False
            or type(count) is not int
            or count < 0
            or not isinstance(modules, list)
            or any(not isinstance(module.get("cards"), list) for module in modules)
        ):
            raise ValueError
        cards = [
            card["cardData"]
            for module in modules
            for card in module["cards"]
            if card["cardType"] == "game"
        ]
        if len(cards) != count:
            raise ValueError
        ids = set()
        fixtures = []
        for card in cards:
            game_id = card["gameId"]
            status = card["gameStatus"]
            if (
                not _NBA_GAME_ID.fullmatch(game_id)
                or game_id in ids
                or type(status) is not int
                or status not in (1, 2, 3)
            ):
                raise ValueError
            ids.add(game_id)
            if (card["leagueId"], card["seasonType"], status) != ("00", "Regular Season", 1):
                continue
            start = _utc(card["gameTimeUtc"])
            home, away = card["homeTeam"], card["awayTeam"]
            home_code, away_code = home["teamTricode"], away["teamTricode"]
            home_id, away_id = home["teamId"], away["teamId"]
            if (
                start is None
                or start.astimezone(_EASTERN).date() != day
                or home_code not in NBA_ALIASES.values()
                or away_code not in NBA_ALIASES.values()
                or home_code == away_code
                or type(home_id) is not int
                or type(away_id) is not int
                or min(home_id, away_id) <= 0
                or home_id == away_id
            ):
                raise ValueError
            fixtures.append(_Fixture(game_id, start, home_code, away_code, home_id, away_id))
        return count, fixtures
    except (AttributeError, KeyError, TypeError, ValueError, UnicodeError):
        raise ValueError(
            "Invalid or incomplete official NBA games feed for the selected date"
        ) from None


def _provider_evidence(
    conn: sqlite3.Connection, data_dir: Path, since: datetime, now: datetime
) -> tuple[dict[tuple[str, str], list[_ProviderEvent]], dict[str, int]]:
    captures = conn.execute(
        """
        SELECT f.captured_at AS observed_at, f.storage_path AS raw_path, f.hash, 'events' AS source
        FROM source_files f JOIN data_sources d ON d.source_id = f.source_id
        WHERE d.source_key = ? AND julianday(f.captured_at) BETWEEN julianday(?) AND julianday(?)
        UNION
        SELECT o.fetched_at_utc, o.raw_path, NULL, 'odds'
        FROM odds_snapshots o JOIN sports s ON s.sport_id = o.sport_id
        WHERE s.league = 'NBA' AND o.source = 'the-odds-api'
          AND o.raw_path IS NOT NULL AND julianday(o.fetched_at_utc) BETWEEN julianday(?) AND julianday(?)
        """,
        (_SOURCE, since.isoformat(), now.isoformat(), since.isoformat(), now.isoformat()),
    )
    events: dict[tuple[str, str], list[_ProviderEvent]] = defaultdict(list)
    coverage = {"archives_checked": 0, "archives_unavailable_or_invalid": 0}
    for row in captures:
        coverage["archives_checked"] += 1
        try:
            raw = read_raw_evidence(row["raw_path"], data_dir)
            payload = json.loads(raw)
            observed = _utc(row["observed_at"])
            is_events = row["source"] == "events"
            if (
                observed is None
                or not since <= observed <= now
                or _utc(payload.get("fetched_at")) != observed
                or payload["request"]["url"] != (_URL if is_events else _ODDS_URL)
                or not isinstance(payload["results"], list)
                or (
                    is_events
                    and (
                        payload.get("source") != _SOURCE
                        or payload.get("request_credits") != 0
                        or hashlib.sha256(raw).hexdigest() != row["hash"]
                    )
                )
            ):
                raise ValueError
        except (AttributeError, KeyError, OSError, TypeError, ValueError):
            coverage["archives_unavailable_or_invalid"] += 1
            continue
        ids = Counter(
            game["id"]
            for game in payload["results"]
            if isinstance(game, dict) and isinstance(game.get("id"), str)
        )
        for game in payload["results"]:
            if not isinstance(game, dict) or game.get("sport_key") != _SPORT:
                continue
            event_id = game.get("id")
            home, away = game.get("home_team"), game.get("away_team")
            start = _utc(game.get("commence_time"))
            if (
                not isinstance(event_id, str)
                or not _EVENT_ID.fullmatch(event_id)
                or ids[event_id] != 1
                or not isinstance(home, str)
                or not isinstance(away, str)
                or start is None
            ):
                continue
            home_code = NBA_ALIASES.get(home.strip().lower())
            away_code = NBA_ALIASES.get(away.strip().lower())
            if home_code and away_code and home_code != away_code:
                events[(home_code, away_code)].append(
                    _ProviderEvent(event_id, start, observed, row["source"], row["raw_path"])
                )
    return events, coverage


def _audit_rows(
    conn: sqlite3.Connection,
    day: date,
    fixtures: list[_Fixture],
    provider: dict[tuple[str, str], list[_ProviderEvent]],
    now: datetime,
    minimum_lead: timedelta,
) -> list[dict[str, Any]]:
    beginning = datetime.combine(day, time(), _EASTERN)
    end = beginning + timedelta(days=1)
    evening = datetime.combine(day, _DAILY_CAPTURE_ET, _EASTERN)
    games = conn.execute(
        """
        SELECT g.game_id, g.start_time_utc, g.odds_api_id, h.code AS home, a.code AS away
        FROM games g JOIN sports s ON s.sport_id = g.sport_id
        JOIN teams h ON h.team_id = g.home_team_id JOIN teams a ON a.team_id = g.away_team_id
        WHERE s.league = 'NBA' AND lower(g.status) != 'voided'
          AND julianday(g.start_time_utc) BETWEEN julianday(?) AND julianday(?)
        """,
        ((beginning - _START_TOLERANCE).isoformat(), (end + _START_TOLERANCE).isoformat()),
    ).fetchall()
    provider_pairs = Counter(
        event_id
        for events in provider.values()
        for event_id in {event.event_id for event in events}
    )
    rows = []
    for fixture in fixtures:
        cutoff = fixture.start - minimum_lead
        relevant = []
        for game in games:
            start = _utc(game["start_time_utc"])
            if (
                {game["home"], game["away"]} == {fixture.home, fixture.away}
                and start is not None
                and (
                    start.astimezone(_EASTERN).date() == day
                    or abs(start - fixture.start) <= _START_TOLERANCE
                )
            ):
                relevant.append((game, start))
        matched = None
        matched_start = None
        if len(relevant) == 1:
            candidate, start = relevant[0]
            if (candidate["home"], candidate["away"]) == (fixture.home, fixture.away) and abs(
                start - fixture.start
            ) <= _START_TOLERANCE:
                matched = candidate
                matched_start = start
        earliest: dict[str, _ProviderEvent] = {}
        compatible_ids = set()
        for event in provider.get((fixture.home, fixture.away), []):
            if abs(event.start - fixture.start) > _START_TOLERANCE:
                continue
            prior = earliest.get(event.event_id)
            if prior is None or event.observed_at < prior.observed_at:
                earliest[event.event_id] = event
            if matched_start is not None and abs(event.start - matched_start) <= _START_TOLERANCE:
                compatible_ids.add(event.event_id)
        provider_ambiguous = len(earliest) > 1 or any(
            provider_pairs[event_id] > 1 for event_id in earliest
        )
        observed = next(iter(earliest.values())) if earliest and not provider_ambiguous else None
        stored_id = matched["odds_api_id"] if matched is not None else None
        duplicate_id = (
            stored_id
            and conn.execute(
                "SELECT COUNT(*) FROM games WHERE odds_api_id = ?", (stored_id,)
            ).fetchone()[0]
            != 1
        )
        ambiguous = bool(
            (relevant and matched is None)
            or provider_ambiguous
            or duplicate_id
            or (
                observed is not None
                and observed.event_id == stored_id
                and stored_id not in compatible_ids
            )
        )
        linked = bool(not ambiguous and observed is not None and observed.event_id == stored_id)
        if ambiguous:
            status = "ambiguous"
        elif matched is None:
            status = "missing_warehouse_fixture"
        elif observed is None:
            status = "unverified_provider_id" if stored_id else "missing_provider_id"
        elif not linked:
            status = "conflicting_provider_id" if stored_id else "unlinked_provider_id"
        else:
            status = "late" if observed.observed_at > cutoff else "linked"
        rows.append(
            {
                "nba_game_id": fixture.game_id,
                "home": fixture.home,
                "away": fixture.away,
                "nba_home_team_id": fixture.home_id,
                "nba_away_team_id": fixture.away_id,
                "official_start_utc": fixture.start.isoformat(),
                "decision_deadline_utc": cutoff.isoformat(),
                "early_entry": cutoff < evening,
                "deadline_passed": now > cutoff,
                "warehouse_matched": matched is not None,
                "warehouse_candidates": [game["game_id"] for game, _ in relevant],
                "warehouse_provider_id": stored_id,
                "verified_provider_ids": sorted(earliest),
                "first_retained_provider_observation_utc": observed.observed_at.isoformat()
                if observed
                else None,
                "first_retained_provider_observation_source": observed.source if observed else None,
                "first_retained_provider_observation_path": observed.raw_path if observed else None,
                "provider_observed_by_deadline": observed.observed_at <= cutoff
                if observed
                else None,
                "provider_linked": linked,
                "status": status,
            }
        )
    return rows


def audit_schedule(
    day: date,
    *,
    fixture_path: Path | None = None,
    fixture_observed_at: datetime | None = None,
    fetch: bool = False,
    db_path: Path | None = None,
) -> dict[str, Any]:
    """Read the current warehouse with an explicitly sourced official schedule observation."""
    if fetch == (fixture_path is not None):
        raise ValueError("Select exactly one official schedule input")
    source_url = f"{_NBA_GAMES}?date={day.isoformat()}"
    if fetch:
        if fixture_observed_at is not None:
            raise ValueError("Fixture observation times only apply to local inputs")
        try:
            response = requests.get(
                source_url, headers=_NBA_HEADERS, timeout=15, allow_redirects=False
            )
        except requests.RequestException:
            raise RuntimeError("Could not fetch the official NBA games feed") from None
        if response.status_code != 200:
            raise RuntimeError(f"Official NBA games feed returned HTTP {response.status_code}")
        raw = response.content
        observed_at = datetime.now(timezone.utc)
        provenance = {"input": "live", "etag": response.headers.get("ETag")}
    else:
        if fixture_observed_at is None or fixture_observed_at.utcoffset() is None:
            raise ValueError("A local fixture requires a timezone-aware observation time")
        assert fixture_path is not None
        raw = fixture_path.read_bytes()
        observed_at = fixture_observed_at.astimezone(timezone.utc)
        provenance = {"input": "local_fixture", "fixture_path": str(fixture_path.resolve())}

    count, fixtures = _official_fixtures(raw, day)
    path = resolve_db_path(db_path).resolve()
    trial = load_trial(DEFAULT_RULES)
    with closing(sqlite3.connect(f"{path.as_uri()}?mode=ro", uri=True)) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("BEGIN")
        now = datetime.now(timezone.utc)
        if observed_at > now:
            raise ValueError("Official schedule observation cannot be in the future")
        since = datetime.combine(day, time(), _EASTERN) - timedelta(days=14)
        provider, evidence_coverage = _provider_evidence(conn, path.parent, since, now)
        rows = _audit_rows(
            conn, day, fixtures, provider, now, timedelta(minutes=trial.min_minutes_before_start)
        )
    return {
        "report_version": 1,
        "trial_id": trial.trial_id,
        "eastern_date": day.isoformat(),
        "warehouse_audited_at_utc": now.isoformat(),
        "official_source": {
            **provenance,
            "url": source_url,
            "observed_at_utc": observed_at.isoformat(),
            "sha256": hashlib.sha256(raw).hexdigest(),
        },
        "official_calendar_games": count,
        "scheduled_regular_season_games": len(rows),
        "early_games": sum(row["early_entry"] for row in rows),
        "warehouse_matched": sum(row["warehouse_matched"] for row in rows),
        "warehouse_missing": sum(not row["warehouse_candidates"] for row in rows),
        "provider_evidence": {
            **evidence_coverage,
            "searched_since_utc": since.astimezone(timezone.utc).isoformat(),
        },
        "provider_observed": sum(
            row["first_retained_provider_observation_utc"] is not None for row in rows
        ),
        "provider_linked": sum(row["provider_linked"] for row in rows),
        "provider_observed_late": sum(
            row["provider_observed_by_deadline"] is False for row in rows
        ),
        "provider_unverified_after_deadline": sum(
            row["deadline_passed"] and row["provider_observed_by_deadline"] is None for row in rows
        ),
        "by_status": dict(sorted(Counter(row["status"] for row in rows).items())),
        "fixtures": rows,
    }


def save_schedule_audit(report: dict[str, Any], directory: Path = AUDIT_REPORTS_DIR) -> Path:
    """Atomically retain a dated audit without overwriting other observations."""
    day = date.fromisoformat(report["eastern_date"])
    audited_at = _utc(report["warehouse_audited_at_utc"])
    if audited_at is None:
        raise ValueError("Audit time must include a time zone")
    day_dir = directory / day.isoformat()
    day_dir.mkdir(parents=True, exist_ok=True)
    path = day_dir / f"{audited_at:%Y%m%dT%H%M%S%fZ}-{uuid4().hex}.json"
    temporary = None
    try:
        with NamedTemporaryFile(mode="w", encoding="utf-8", dir=day_dir, delete=False) as stream:
            temporary = Path(stream.name)
            json.dump(report, stream, indent=2, allow_nan=False)
            stream.write("\n")
        temporary.replace(path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
    return path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--date", required=True, type=date.fromisoformat, help="NBA Eastern slate date"
    )
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--fetch", action="store_true", help="Read the public NBA games page")
    source.add_argument("--fixture", type=Path, help="Use saved official HTML or Next.js JSON")
    parser.add_argument(
        "--fixture-observed-at", help="Original ISO 8601 observation time for a local fixture"
    )
    parser.add_argument("--save", action="store_true", help="Save this audit for the paper dashboard")
    args = parser.parse_args()
    load_env()
    observed_at = _utc(args.fixture_observed_at)
    if args.fixture_observed_at is not None and observed_at is None:
        parser.error("--fixture-observed-at requires a timezone-aware ISO 8601 timestamp")
    try:
        result = audit_schedule(
            args.date,
            fetch=args.fetch,
            fixture_path=args.fixture,
            fixture_observed_at=observed_at,
        )
        if args.save:
            save_schedule_audit(result)
    except (OSError, RuntimeError, ValueError, sqlite3.Error) as exc:
        parser.exit(1, f"{exc}\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
