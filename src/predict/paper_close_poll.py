"""Collect closing odds for pending paper decisions within a fixed quota."""

from __future__ import annotations

import argparse
import json
import sqlite3
from contextlib import closing
from datetime import datetime, timedelta, timezone

import requests

from src.data.config import OddsAPIKeyManager, load_env
from src.db.core import connect, resolve_db_path
from src.db.loaders import load_odds_snapshot
from src.predict.paper_settlement import (
    _MIN_LEAD,
    _START_TOLERANCE,
    _close_for_decision,
    _utc_timestamp,
)

_SPORT = "basketball_nba"
_SOURCE = "the-odds-api"
_BOOK = "draftkings"
_BASE_URL = f"https://api.the-odds-api.com/v4/sports/{_SPORT}"
_MIN_TO_START = timedelta(minutes=10)
_MAX_TO_START = timedelta(minutes=60)
_COOLDOWN = timedelta(minutes=15)
_MAX_DAILY_REQUESTS = 3
_CREDIT_RESERVE = 200


def _has_table(conn: sqlite3.Connection, name: str) -> bool:
    return (
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (name,)
        ).fetchone()
        is not None
    )


def _due_events(conn: sqlite3.Connection, now: datetime) -> dict[str, datetime]:
    pending = conn.execute(
        """
        SELECT d.*, g.start_time_utc AS current_start, g.sport_id, g.odds_api_id
        FROM paper_decisions d
        JOIN games g ON g.game_id = d.game_id
          AND g.home_team_id = d.home_team_id AND g.away_team_id = d.away_team_id
        JOIN sports s ON s.sport_id = g.sport_id AND s.league = 'NBA'
        LEFT JOIN paper_settlements p ON p.trial_id = d.trial_id AND p.game_id = d.game_id
        WHERE p.trial_id IS NULL AND LOWER(g.status) = 'scheduled' AND g.odds_api_id <> ''
          AND d.odds_source = ? AND LOWER(TRIM(d.book)) = ?
          AND julianday(d.start_time_utc) > julianday(?)
        """,
        (_SOURCE, _BOOK, now.isoformat()),
    )
    events: dict[str, datetime] = {}
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
        events[event_id] = min(start, events.get(event_id, start))
    return events


def _plan(conn: sqlite3.Connection, now: datetime) -> tuple[str, dict[str, datetime]]:
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
    return ("cooldown" if recent else "due"), events


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


def _reserve_attempt() -> tuple[str, dict[str, datetime]]:
    with connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        now = datetime.now(timezone.utc)
        status, events = _plan(conn, now)
        if status == "due":
            conn.execute(
                "CREATE TABLE IF NOT EXISTS paper_close_requests (requested_at TEXT PRIMARY KEY)"
            )
            conn.execute("INSERT INTO paper_close_requests VALUES (?)", (now.isoformat(),))
        return status, events


def poll_closes(*, collect: bool = False) -> dict[str, str | int]:
    """Inspect local decisions by default, or collect one eligible odds batch."""
    path = resolve_db_path().resolve()
    if not path.is_file():
        return {"status": "ledger_unavailable", "due_events": 0}
    with closing(sqlite3.connect(f"{path.as_uri()}?mode=ro", uri=True)) as conn:
        conn.row_factory = sqlite3.Row
        status, events = _plan(conn, datetime.now(timezone.utc))
    result: dict[str, str | int] = {"status": status, "due_events": len(events)}
    if status != "due" or not collect:
        return result

    load_env()
    key = _budgeted_key()
    if key is None:
        return {**result, "status": "quota_reserve"}

    status, events = _reserve_attempt()
    result.update(status=status, due_events=len(events))
    if status != "due":
        return result

    response = _provider_get(
        "odds",
        {
            "apiKey": key,
            "regions": "us",
            "markets": "totals",
            "bookmakers": _BOOK,
            "eventIds": ",".join(sorted(events)),
            "oddsFormat": "american",
        },
    )
    fetched_at = datetime.now(timezone.utc)
    try:
        payload = response.json()
    except ValueError:
        raise RuntimeError("Odds API returned invalid closing odds JSON") from None
    if not isinstance(payload, list):
        raise RuntimeError("Odds API returned an unexpected closing odds response")

    results = []
    for event in payload:
        if not isinstance(event, dict):
            continue
        event_id = event.get("id")
        commence_value = event.get("commence_time")
        if (
            not isinstance(event_id, str)
            or event_id not in events
            or not isinstance(commence_value, str)
        ):
            continue
        commence = _utc_timestamp(commence_value)
        if commence is None or fetched_at >= min(commence, events[event_id]):
            continue
        books = []
        for book in event.get("bookmakers") or []:
            if isinstance(book, dict) and book.get("key") == _BOOK:
                markets = [
                    market
                    for market in (book.get("markets") or [])
                    if market.get("key") == "totals"
                ]
                if markets:
                    books.append({**book, "markets": markets})
        if books:
            results.append({**event, "bookmakers": books})
    if results:
        load_odds_snapshot(
            {"source": _SOURCE, "fetched_at": fetched_at.isoformat(), "results": results},
            region="us",
            sport_key=_SPORT,
        )
    return {
        **result,
        "status": "collected" if results else "no_closing_quotes",
        "events_loaded": len(results),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--collect", action="store_true", help="Allow one paid odds request if due")
    args = parser.parse_args()
    try:
        result = poll_closes(collect=args.collect)
    except RuntimeError as exc:
        parser.exit(1, f"{exc}\n")
    print(json.dumps(result))


if __name__ == "__main__":
    main()
