"""Collect closing odds for pending paper decisions within a fixed quota."""

from __future__ import annotations

import argparse
import json
import sqlite3
from collections import Counter
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

import requests

from src.data.config import RAW_DATA_DIR, OddsAPIKeyManager, load_env
from src.data.ingest_odds import _request_metadata, write_snapshot
from src.data.team_mappings import NBA_ALIASES
from src.db.core import connect, resolve_db_path
from src.db.loaders import _american_to_decimal, _implied_probability
from src.predict.paper_settlement import (
    _MIN_LEAD,
    _START_TOLERANCE,
    _close_for_decision,
    _utc_timestamp,
    _valid_american_odds,
    _valid_total_line,
)

_SPORT = "basketball_nba"
_SOURCE = "the-odds-api"
_BOOK = "draftkings"
_BASE_URL = f"https://api.the-odds-api.com/v4/sports/{_SPORT}"
_MIN_TO_START = timedelta(minutes=10)
_MAX_TO_START = timedelta(minutes=60)
# Wait one 15-minute scheduled interval plus up to three minutes of delay.
_BATCH_LEAD = _MIN_TO_START + timedelta(minutes=18)
_COOLDOWN = timedelta(minutes=15)
_MAX_DAILY_REQUESTS = 3
_CREDIT_RESERVE = 200


@dataclass(frozen=True)
class _ClosingFixture:
    game_id: str
    sport_id: int
    home_team_id: int
    away_team_id: int
    book_id: int
    start: datetime
    latest_start: datetime


def _has_table(conn: sqlite3.Connection, name: str) -> bool:
    return (
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (name,)
        ).fetchone()
        is not None
    )


def _due_events(conn: sqlite3.Connection, now: datetime) -> dict[str, _ClosingFixture]:
    pending = conn.execute(
        """
        SELECT d.*, g.start_time_utc AS current_start, g.sport_id, g.odds_api_id
        FROM paper_decisions d
        JOIN games g ON g.game_id = d.game_id
          AND g.home_team_id = d.home_team_id AND g.away_team_id = d.away_team_id
        JOIN sports s ON s.sport_id = g.sport_id AND s.league = 'NBA'
        JOIN books b ON b.book_id = d.book_id AND LOWER(TRIM(b.name)) = ?
        LEFT JOIN game_results r ON r.game_id = g.game_id
        LEFT JOIN paper_settlements p ON p.trial_id = d.trial_id AND p.game_id = d.game_id
        WHERE p.trial_id IS NULL AND LOWER(g.status) = 'scheduled' AND g.odds_api_id <> ''
          AND r.home_score IS NULL AND r.away_score IS NULL
          AND d.odds_source = ? AND LOWER(TRIM(d.book)) = ?
          AND julianday(d.start_time_utc) > julianday(?)
          AND NOT EXISTS (
              SELECT 1 FROM games other
              WHERE other.sport_id = g.sport_id AND other.odds_api_id = g.odds_api_id
                AND other.game_id <> g.game_id
          )
        """,
        (_BOOK, _SOURCE, _BOOK, now.isoformat()),
    )
    events: dict[str, _ClosingFixture] = {}
    ambiguous: set[str] = set()
    for decision in pending:
        captured_at = _utc_timestamp(decision["captured_at"])
        captured_start = _utc_timestamp(decision["start_time_utc"])
        current_start = _utc_timestamp(decision["current_start"])
        if captured_at is None or captured_start is None or current_start is None:
            continue
        start = min(captured_start, current_start)
        if (
            not (_MIN_TO_START <= start - now <= _MAX_TO_START)
            or abs(current_start - captured_start) > _START_TOLERANCE
            or start - captured_at < _MIN_LEAD
            or captured_at >= now
        ):
            continue
        if _close_for_decision(conn, decision, captured_at, start) is not None:
            continue
        event_id = decision["odds_api_id"]
        previous = events.get(event_id)
        if previous and previous.book_id != decision["book_id"]:
            ambiguous.add(event_id)
        if previous is None or start < previous.start:
            events[event_id] = _ClosingFixture(
                game_id=decision["game_id"],
                sport_id=decision["sport_id"],
                home_team_id=decision["home_team_id"],
                away_team_id=decision["away_team_id"],
                book_id=decision["book_id"],
                start=start,
                latest_start=max(captured_start, current_start),
            )
    return {event_id: fixture for event_id, fixture in events.items() if event_id not in ambiguous}


def _plan(
    conn: sqlite3.Connection, now: datetime, *, batch_scheduled: bool = False
) -> tuple[str, dict[str, _ClosingFixture]]:
    if not _has_table(conn, "paper_decisions"):
        return "ledger_unavailable", {}
    events = _due_events(conn, now)
    if not events:
        return "no_close_due", events
    if _has_table(conn, "paper_close_requests"):
        midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
        count, latest = conn.execute(
            """
            SELECT COUNT(CASE WHEN requested_at >= ? THEN 1 END), MAX(requested_at)
            FROM paper_close_requests
            """,
            (midnight.isoformat(),),
        ).fetchone()
        if count >= _MAX_DAILY_REQUESTS:
            return "daily_cap", events
        latest_request = _utc_timestamp(latest)
        if latest_request is not None and now - latest_request < _COOLDOWN:
            return "cooldown", events
    recent = conn.execute(
        """
        SELECT 1
        FROM odds_snapshots o JOIN sports s ON s.sport_id = o.sport_id
        WHERE s.league = 'NBA' AND o.source = ?
          AND julianday(o.fetched_at_utc) > julianday(?)
          AND julianday(o.fetched_at_utc) <= julianday(?)
        LIMIT 1
        """,
        (_SOURCE, (now - _COOLDOWN).isoformat(), now.isoformat()),
    ).fetchone()
    if recent:
        return "cooldown", events
    if batch_scheduled and min(fixture.start for fixture in events.values()) - now > _BATCH_LEAD:
        return "waiting_for_batch", events
    return "due", events


def _provider_get(
    endpoint: str, params: dict[str, str], *, allow_key_rejection: bool = False
) -> requests.Response:
    try:
        response = requests.get(
            f"{_BASE_URL}/{endpoint}", params=params, timeout=10, allow_redirects=False
        )
    except requests.RequestException:
        raise RuntimeError(f"Odds API {endpoint} request failed") from None
    if response.status_code == 401 and allow_key_rejection:
        try:
            body = response.json()
        except ValueError:
            body = None
        if isinstance(body, dict) and body.get("error_code") in {
            "INVALID_KEY",
            "OUT_OF_USAGE_CREDITS",
        }:
            return response
    if response.status_code != 200:
        raise RuntimeError(f"Odds API {endpoint} returned HTTP {response.status_code}")
    return response


def _budgeted_key() -> str | None:
    keys = OddsAPIKeyManager.get_available_keys()
    index = OddsAPIKeyManager.get_current_index()
    if not 0 <= index < len(keys):
        raise RuntimeError("No active Odds API key is configured")
    for key in dict.fromkeys([keys[index], *keys]):
        response = _provider_get("events", {"apiKey": key}, allow_key_rejection=True)
        if response.status_code == 401:
            continue
        quota = response.headers.get("x-requests-remaining")
        try:
            remaining = int(quota) if quota is not None else -1
        except ValueError:
            return None
        if remaining < 0:
            return None
        if remaining >= _CREDIT_RESERVE + 1:
            return key
    return None


def _reserve_attempt(
    db_path: Path, *, batch_scheduled: bool
) -> tuple[str, dict[str, _ClosingFixture]]:
    with connect(db_path) as conn:
        conn.execute("BEGIN IMMEDIATE")
        now = datetime.now(timezone.utc)
        status, events = _plan(conn, now, batch_scheduled=batch_scheduled)
        if status == "due":
            conn.execute(
                "CREATE TABLE IF NOT EXISTS paper_close_requests (requested_at TEXT PRIMARY KEY)"
            )
            conn.execute("INSERT INTO paper_close_requests VALUES (?)", (now.isoformat(),))
        return status, events


def _totals_pair(event: dict) -> tuple[float, float, float] | None:
    bookmakers = event.get("bookmakers")
    if not isinstance(bookmakers, list):
        return None
    books = [book for book in bookmakers if isinstance(book, dict) and book.get("key") == _BOOK]
    if len(books) != 1 or not isinstance(books[0].get("markets"), list):
        return None
    totals = [
        market
        for market in books[0]["markets"]
        if isinstance(market, dict) and market.get("key") == "totals"
    ]
    if len(totals) != 1:
        return None
    outcomes = totals[0].get("outcomes")
    if not isinstance(outcomes, list) or len(outcomes) != 2:
        return None
    quotes: dict[str, tuple[float, float]] = {}
    for outcome in outcomes:
        if not isinstance(outcome, dict):
            return None
        side, point, price = outcome.get("name"), outcome.get("point"), outcome.get("price")
        if (
            side not in ("Over", "Under")
            or side in quotes
            or not isinstance(point, (int, float))
            or isinstance(point, bool)
            or not isinstance(price, (int, float))
            or not _valid_total_line(point)
            or not _valid_american_odds(price)
        ):
            return None
        quotes[side] = (point, price)
    line, over = quotes["Over"]
    under_line, under = quotes["Under"]
    return (line, over, under) if line == under_line else None


def _matches_fixture(
    conn: sqlite3.Connection, event: dict, fixture: _ClosingFixture, fetched_at: datetime
) -> bool:
    commence = _utc_timestamp(event.get("commence_time"))
    home, away = event.get("home_team"), event.get("away_team")
    if (
        event.get("sport_key") != _SPORT
        or not isinstance(home, str)
        or not isinstance(away, str)
        or commence is None
    ):
        return False
    matches = conn.execute(
        """
        SELECT g.game_id, g.start_time_utc, g.status, g.home_team_id, g.away_team_id,
               r.home_score, r.away_score,
               h.code AS home_code, a.code AS away_code, b.name AS book_name
        FROM games g
        JOIN teams h ON h.team_id = g.home_team_id
        JOIN teams a ON a.team_id = g.away_team_id
        JOIN books b ON b.book_id = ?
        LEFT JOIN game_results r ON r.game_id = g.game_id
        WHERE g.sport_id = ? AND g.odds_api_id = ?
        LIMIT 2
        """,
        (fixture.book_id, fixture.sport_id, event["id"]),
    ).fetchall()
    if len(matches) != 1:
        return False
    game = matches[0]
    current_start = _utc_timestamp(game["start_time_utc"])
    if current_start is None:
        return False
    starts = (fixture.start, fixture.latest_start, current_start, commence)
    return (
        game["game_id"] == fixture.game_id
        and (game["status"] or "").lower() == "scheduled"
        and game["home_score"] is None
        and game["away_score"] is None
        and (game["home_team_id"], game["away_team_id"])
        == (fixture.home_team_id, fixture.away_team_id)
        and game["book_name"].strip().lower() == _BOOK
        and (NBA_ALIASES.get(home.strip().lower()), NBA_ALIASES.get(away.strip().lower()))
        == (game["home_code"], game["away_code"])
        and max(starts) - min(starts) <= _START_TOLERANCE
        and fetched_at < min(starts)
    )


def _append_closing_quotes(
    db_path: Path,
    payload: list,
    events: dict[str, _ClosingFixture],
    fetched_at: datetime,
    raw_path: Path,
    snapshot_id: str,
) -> int:
    requested = [
        event
        for event in payload
        if isinstance(event, dict) and isinstance(event.get("id"), str) and event["id"] in events
    ]
    counts = Counter(event["id"] for event in requested)
    rows = []
    sport_id = None
    with connect(db_path) as conn:
        conn.execute("BEGIN IMMEDIATE")
        for event in requested:
            if counts[event["id"]] != 1:
                continue
            fixture = events[event["id"]]
            pair = _totals_pair(event)
            if pair is None or not _matches_fixture(conn, event, fixture, fetched_at):
                continue
            sport_id = fixture.sport_id
            line, over, under = pair
            for side, price in (("Over", over), ("Under", under)):
                rows.append(
                    (
                        snapshot_id,
                        fixture.game_id,
                        fixture.book_id,
                        side,
                        price,
                        _american_to_decimal(price),
                        _implied_probability(price),
                        line,
                    )
                )
        if rows:
            conn.execute(
                """
                INSERT INTO odds_snapshots (snapshot_id, fetched_at_utc, sport_id, source, raw_path)
                VALUES (?, ?, ?, ?, ?)
                """,
                (snapshot_id, fetched_at.isoformat(), sport_id, _SOURCE, str(raw_path)),
            )
            conn.executemany(
                """
                INSERT INTO odds (snapshot_id, game_id, book_id, market, outcome, price_american,
                                  price_decimal, implied_prob, line)
                VALUES (?, ?, ?, 'totals', ?, ?, ?, ?, ?)
                """,
                rows,
            )
    return len(rows) // 2


def poll_closes(*, collect: bool = False, batch_scheduled: bool = False) -> dict[str, str | int]:
    """Inspect local decisions by default, or collect one eligible odds batch."""
    path = resolve_db_path().resolve()
    if not path.is_file():
        return {"status": "ledger_unavailable", "due_events": 0}
    with closing(sqlite3.connect(f"{path.as_uri()}?mode=ro", uri=True)) as conn:
        conn.row_factory = sqlite3.Row
        status, events = _plan(conn, datetime.now(timezone.utc), batch_scheduled=batch_scheduled)
    result: dict[str, str | int] = {"status": status, "due_events": len(events)}
    if status != "due" or not collect:
        return result

    load_env()
    key = _budgeted_key()
    if key is None:
        return {**result, "status": "quota_reserve"}

    status, events = _reserve_attempt(path, batch_scheduled=batch_scheduled)
    result.update(status=status, due_events=len(events))
    if status != "due":
        return result

    params = {
        "apiKey": key,
        "regions": "us",
        "markets": "totals",
        "bookmakers": _BOOK,
        "eventIds": ",".join(sorted(events)),
        "oddsFormat": "american",
    }
    response = _provider_get("odds", params)
    fetched_at = datetime.now(timezone.utc)
    try:
        payload = response.json()
    except ValueError:
        raise RuntimeError("Odds API returned invalid closing odds JSON") from None
    if not isinstance(payload, list):
        raise RuntimeError("Odds API returned an unexpected closing odds response")

    snapshot_id = str(uuid4())
    try:
        raw_path = write_snapshot(
            {
                "source": _SOURCE,
                "fetched_at": fetched_at.isoformat(),
                "request": _request_metadata(f"{_BASE_URL}/odds", params),
                "results": payload,
                "rate_limit_remaining": response.headers.get("x-requests-remaining"),
                "rate_limit_reset": response.headers.get("x-requests-reset"),
            },
            RAW_DATA_DIR / "odds" / _SPORT / "paper_closing" / snapshot_id,
        )
    except OSError as exc:
        raise RuntimeError("Could not archive the closing odds response") from exc
    events_loaded = _append_closing_quotes(path, payload, events, fetched_at, raw_path, snapshot_id)
    return {
        **result,
        "status": "collected" if events_loaded else "no_closing_quotes",
        "events_loaded": events_loaded,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--collect", action="store_true", help="Allow one paid odds request if due")
    parser.add_argument(
        "--batch-scheduled", action="store_true", help="Batch due events on a 15-minute schedule"
    )
    args = parser.parse_args()
    try:
        result = poll_closes(collect=args.collect, batch_scheduled=args.batch_scheduled)
    except RuntimeError as exc:
        parser.exit(1, f"{exc}\n")
    print(json.dumps(result))


if __name__ == "__main__":
    main()
