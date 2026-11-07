"""Collect one targeted morning quote batch for NBA paper fixtures the daily run would miss."""

from __future__ import annotations

import argparse
import json
import logging
import sqlite3
from collections import Counter
from contextlib import closing
from dataclasses import replace
from datetime import datetime, time, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import uuid4
from zoneinfo import ZoneInfo

import pandas as pd
import requests

from src.data.config import RAW_DATA_DIR, load_env
from src.data.ingest_nba_events import _write_game_mapping, retain_entry_probe
from src.data.ingest_odds import _request_metadata, write_snapshot
from src.data.team_mappings import NBA_ALIASES
from src.db.core import connect, resolve_db_path, store_odds_snapshot_game_starts
from src.db.loaders import _american_to_decimal, _implied_probability
from src.models.train import CalibratedModel, EnsembleModel, ProbabilityCalibrator  # noqa: F401
from src.predict.engine import PredictionEngine
from src.predict.paper_close_poll import (
    _BASE_URL,
    _SPORT,
    _budgeted_key,
    _ClosingFixture,
    _has_table,
    _matches_fixture,
    _provider_get,
    _totals_pair,
)
from src.predict.paper_ledger import (
    DEFAULT_RULES,
    PaperTrial,
    _decision_quote,
    _eligible_game,
    _model_inputs_match,
    capture_predictions,
    load_trial,
    record_capture_failure,
)
from src.predict.paper_settlement import (
    _START_TOLERANCE,
    _quote_source_start,
    _utc_timestamp,
    _valid_american_odds,
    _valid_total_line,
)
from src.predict.storage import _apply_paper_odds

if TYPE_CHECKING:
    from src.predict.paper_entry_fallback import FallbackPlan

LOGGER = logging.getLogger(__name__)
_EASTERN = ZoneInfo("America/New_York")
_DAILY_CAPTURE_ET = time(18, 15)
_MIN_TRIGGER_LEAD = timedelta(minutes=30)
_MAX_TRIGGER_LEAD = timedelta(minutes=75)
_ENTRY_MARKETS = "h2h,totals"
_REQUEST_CREDITS = 2
_FreshQuotes = dict[str, tuple[str, datetime]]


def _already_handled(conn: sqlite3.Connection, trial_id: str, game: sqlite3.Row) -> bool:
    if (
        _has_table(conn, "paper_decisions")
        and conn.execute(
            """
        SELECT 1 FROM paper_decisions
        WHERE trial_id = ? AND (game_id = ? OR (
            home_team_id = ? AND away_team_id = ?
            AND abs(julianday(start_time_utc) - julianday(?)) < 2.0 / 24
        )) LIMIT 1
        """,
            (
                trial_id,
                game["game_id"],
                game["home_team_id"],
                game["away_team_id"],
                game["start_time_utc"],
            ),
        ).fetchone()
    ):
        return True
    return (
        _has_table(conn, "paper_fixture_observations")
        and conn.execute(
            """
        SELECT 1 FROM paper_fixture_observations o
        JOIN paper_capture_runs r ON r.run_id = o.run_id
        WHERE r.trial_id = ? AND o.game_id = ?
          AND o.outcome IN ('recorded', 'already_recorded', 'below_edge_threshold')
          AND abs(unixepoch(o.start_time_utc) - unixepoch(?)) <= ?
        LIMIT 1
        """,
            (trial_id, game["game_id"], game["start_time_utc"], _START_TOLERANCE.total_seconds()),
        ).fetchone()
        is not None
    )


def _plan(
    conn: sqlite3.Connection, trial: PaperTrial, now: datetime
) -> tuple[str, dict[str, _ClosingFixture], _FreshQuotes]:
    if not all(
        _has_table(conn, table)
        for table in (
            "paper_trials",
            "paper_decisions",
            "paper_capture_runs",
            "paper_fixture_observations",
        )
    ):
        return "ledger_unavailable", {}, {}
    registered = conn.execute(
        "SELECT registered_at, specification FROM paper_trials WHERE trial_id = ?",
        (trial.trial_id,),
    ).fetchone()
    if registered is None:
        return "trial_unregistered", {}, {}
    if registered["specification"] != trial.specification:
        raise RuntimeError(f"Paper trial {trial.trial_id} differs from its registered rules")
    registered_at = _utc_timestamp(registered["registered_at"])
    if registered_at is None or not registered_at <= now < datetime.fromisoformat(trial.review_at):
        return "trial_inactive", {}, {}
    local_day = now.astimezone(_EASTERN).date()
    daily_capture = datetime.combine(local_day, _DAILY_CAPTURE_ET, tzinfo=_EASTERN)
    if now >= daily_capture:
        return "outside_entry_window", {}, {}

    books = conn.execute(
        "SELECT book_id FROM books WHERE lower(name) = ? LIMIT 2", (trial.book,)
    ).fetchall()
    if len(books) != 1:
        return "book_unavailable", {}, {}
    minimum_lead = timedelta(minutes=trial.min_minutes_before_start)
    first = now + minimum_lead
    last = min(now + timedelta(hours=trial.max_hours_before_start), daily_capture + minimum_lead)
    candidates = conn.execute(
        """
        SELECT g.game_id, g.sport_id, g.odds_api_id, g.start_time_utc, g.home_team_id, g.away_team_id
        FROM games g JOIN sports s ON s.sport_id = g.sport_id
        WHERE s.league = ? AND lower(g.status) = 'scheduled' AND g.odds_api_id <> ''
          AND julianday(g.start_time_utc) BETWEEN julianday(?) AND julianday(?)
          AND NOT EXISTS (
              SELECT 1 FROM games other
              WHERE other.sport_id = g.sport_id AND other.odds_api_id = g.odds_api_id
                AND other.game_id <> g.game_id
          )
        """,
        (trial.league, first.isoformat(), last.isoformat()),
    )
    due = {}
    for game in candidates:
        start = _utc_timestamp(game["start_time_utc"])
        if (
            start is not None
            and first <= start <= last
            and start - minimum_lead < daily_capture
            and "," not in game["odds_api_id"]
            and _eligible_game(conn, game["game_id"], trial.league) is not None
            and not _already_handled(conn, trial.trial_id, game)
        ):
            due[game["odds_api_id"]] = _ClosingFixture(
                game_id=game["game_id"],
                sport_id=game["sport_id"],
                home_team_id=game["home_team_id"],
                away_team_id=game["away_team_id"],
                book_id=books[0]["book_id"],
                start=start,
                latest_start=start,
            )
    if not any(
        _MIN_TRIGGER_LEAD <= fixture.start - now <= _MAX_TRIGGER_LEAD for fixture in due.values()
    ):
        return "no_entry_due", {}, {}
    fresh = _fresh_quotes(conn, trial, due, now)
    if len(fresh) == len(due):
        return "reusable", due, fresh
    if _entry_slot_taken(conn, now):
        return "daily_cap", due, {}
    return "due", due, {}


def _fresh_quotes(
    conn: sqlite3.Connection, trial: PaperTrial, due: dict[str, _ClosingFixture], now: datetime
) -> _FreshQuotes:
    earliest = now - timedelta(minutes=trial.max_quote_age_minutes)
    fixture_starts = {fixture.game_id: fixture.start for fixture in due.values()}
    game_ids = list(fixture_starts)
    markers = ", ".join("?" for _ in game_ids)
    games = pd.read_sql_query(
        f"""
        SELECT g.game_id, h.name AS home_team, a.name AS away_team
        FROM games g JOIN teams h ON h.team_id = g.home_team_id
        JOIN teams a ON a.team_id = g.away_team_id WHERE g.game_id IN ({markers})
        """,
        conn,
        params=game_ids,
    )
    stored_starts = {}
    if _has_table(conn, "paper_entry_quote_starts"):
        stored_starts = {
            (row["snapshot_id"], row["game_id"]): _utc_timestamp(row["earliest_start_utc"])
            for row in conn.execute(
                f"""
                SELECT snapshot_id, game_id, earliest_start_utc FROM paper_entry_quote_starts
                WHERE game_id IN ({markers})
                """,
                game_ids,
            )
        }
    fresh: _FreshQuotes = {}
    sport_id = next(iter(due.values())).sport_id
    for row in _apply_paper_odds(conn, games, sport_id).itertuples(index=False):
        game = _eligible_game(conn, row.game_id, trial.league)
        if game is None or pd.isna(row.selected_totals_book_id):
            continue
        quote = _decision_quote(conn, trial, row, game)
        if quote is None or not (
            _valid_total_line(quote["line"])
            and _valid_american_odds(quote["over_price"])
            and _valid_american_odds(quote["under_price"])
            and _model_inputs_match(conn, row, quote)
        ):
            continue
        fetched = _utc_timestamp(quote["fetched_at_utc"])
        if fetched is None or not earliest <= fetched <= now:
            continue
        snapshot_id = quote["snapshot_id"]
        start = fixture_starts[row.game_id]
        provider_start = _quote_source_start(
            conn, quote, fetched, moneylines=(row.home_moneyline, row.away_moneyline)
        )
        if provider_start is None or abs(start - provider_start) > _START_TOLERANCE:
            continue
        start = min(start, provider_start)
        evidence_key = (snapshot_id, row.game_id)
        if evidence_key in stored_starts:
            verified_start = stored_starts[evidence_key]
            if verified_start is None or abs(start - verified_start) > _START_TOLERANCE:
                continue
            start = min(start, verified_start)
        else:
            raw_path = conn.execute(
                "SELECT raw_path FROM odds_snapshots WHERE snapshot_id = ?", (snapshot_id,)
            ).fetchone()[0]
            if isinstance(raw_path, str) and "paper_entry" in raw_path.replace(
                "\\", "/"
            ).lower().split("/"):
                continue
        fresh[row.game_id] = (snapshot_id, start)
    return fresh


def _entry_slot_taken(conn: sqlite3.Connection, now: datetime) -> bool:
    day = now.astimezone(_EASTERN).date().isoformat()
    return (
        _has_table(conn, "paper_entry_requests")
        and conn.execute(
            "SELECT 1 FROM paper_entry_requests WHERE entry_date_et = ?", (day,)
        ).fetchone()
        is not None
    )


def _reuse_within_observed_clocks(
    due: dict[str, _ClosingFixture], fresh: _FreshQuotes
) -> tuple[str, dict[str, _ClosingFixture], _FreshQuotes, None]:
    # Reuse only quotes whose stored clocks already cover newly observed limits.
    due = {
        provider_id: fixture
        for provider_id, fixture in due.items()
        if fresh[fixture.game_id][1] <= fixture.start
    }
    fresh = {fixture.game_id: fresh[fixture.game_id] for fixture in due.values()}
    return ("reusable" if due else "fixture_clock_conflict"), due, fresh, None


def _plan_entry_path(
    conn: sqlite3.Connection,
    trial: PaperTrial,
    now: datetime,
    official: tuple[bytes, datetime] | None,
) -> tuple[str, dict[str, _ClosingFixture], _FreshQuotes, FallbackPlan | None]:
    status, due, fresh = _plan(conn, trial, now)
    if official is not None and status in {"due", "no_entry_due", "reusable"}:
        from src.predict.paper_entry_fallback import (
            anchor_ordinary_clocks,
            plan_missing_ids,
            read_official_fixtures,
        )

        if status != "reusable" and _entry_slot_taken(conn, now):
            return "daily_cap", due, fresh, None
        raw, observed_at = official
        window_status, fixtures = read_official_fixtures(raw, observed_at, now)
        window = None
        if window_status == "verified":
            anchored = anchor_ordinary_clocks(conn, due, fixtures)
            if status == "reusable":
                return _reuse_within_observed_clocks(anchored, fresh)
            if anchored.keys() != due.keys():
                return ("due" if anchored else "fixture_clock_conflict"), anchored, fresh, None
            due = anchored
            window_status, window = plan_missing_ids(
                conn, trial, raw, observed_at, now, due, fixtures
            )
        if window is not None or status == "no_entry_due":
            return window_status, due, fresh, window
    return status, due, fresh, None


def _reserve(
    db_path: Path,
    trial: PaperTrial,
    official: tuple[bytes, datetime] | None = None,
    free_events: list | None = None,
    *,
    probe_safe: bool = True,
) -> tuple[str, dict[str, _ClosingFixture], _FreshQuotes, FallbackPlan | None]:
    with connect(db_path) as conn:
        conn.execute("BEGIN IMMEDIATE")
        now = datetime.now(timezone.utc)
        status, due, fresh, window = _plan_entry_path(conn, trial, now, official)
        if window is not None:
            from src.predict.paper_entry_fallback import classify_probe

            probe_status, starts = (
                classify_probe(free_events, window) if free_events is not None else ("conflict", {})
            )
            known_ids = {target.provider_id for target in window.targets if target.provider_id}
            lead = timedelta(minutes=trial.min_minutes_before_start)
            evening = datetime.combine(window.eastern_date, _DAILY_CAPTURE_ET, _EASTERN)
            if (
                not probe_safe
                or probe_status != "absent"
                or not known_ids <= starts.keys()
                or any(not now + lead <= start < evening + lead for start in starts.values())
            ):
                if not due:
                    return "provider_probe_conflict", {}, {}, None
                status, window = "due", None
            else:
                window = replace(window, probe_starts=tuple(starts.items()))
                due = {}
        if status in {"due", "reusable"} and free_events is not None:
            from src.predict.paper_entry_fallback import anchor_ordinary_clocks

            due = anchor_ordinary_clocks(conn, due, free_events=free_events)
            if status == "reusable":
                return _reuse_within_observed_clocks(due, fresh)
            if not due:
                return "fixture_clock_conflict", {}, {}, None
        if status in {"due", "window_due"}:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS paper_entry_requests (
                    entry_date_et TEXT PRIMARY KEY, requested_at_utc TEXT NOT NULL
                )
                """
            )
            conn.execute(
                "INSERT INTO paper_entry_requests VALUES (?, ?)",
                (now.astimezone(_EASTERN).date().isoformat(), now.isoformat()),
            )
        return status, due, fresh, window


def _moneyline_pair(event: dict, book_key: str) -> tuple[float, float] | None:
    # _totals_pair has already checked that there is exactly one matching book.
    book = next(
        book
        for book in event["bookmakers"]
        if isinstance(book, dict) and book.get("key") == book_key
    )
    h2h = [
        market
        for market in book["markets"]
        if isinstance(market, dict) and market.get("key") == "h2h"
    ]
    if len(h2h) != 1:
        return None
    outcomes = h2h[0].get("outcomes")
    if not isinstance(outcomes, list) or len(outcomes) != 2:
        return None
    prices: dict[str, float] = {}
    for outcome in outcomes:
        if not isinstance(outcome, dict):
            return None
        name, price = outcome.get("name"), outcome.get("price")
        code = NBA_ALIASES.get(name.strip().lower()) if isinstance(name, str) else None
        if code is None or not isinstance(price, (int, float)) or not _valid_american_odds(price):
            return None
        prices[code] = price
    home = NBA_ALIASES[event["home_team"].strip().lower()]
    away = NBA_ALIASES[event["away_team"].strip().lower()]
    if set(prices) != {home, away} or home == away:
        return None
    return prices[home], prices[away]


def _valid_window_market_clocks(event: dict, book_key: str, fetched: datetime) -> bool:
    book = next(
        book
        for book in event["bookmakers"]
        if isinstance(book, dict) and book.get("key") == book_key
    )
    for market in book["markets"]:
        if (
            isinstance(market, dict)
            and market.get("key") in ("h2h", "totals")
            and "last_update" in market
        ):
            updated = _utc_timestamp(market["last_update"])
            if updated is None or updated > fetched:
                return False
    return True


def _append_entry_quotes(
    db_path: Path,
    trial: PaperTrial,
    payload: list,
    due: dict[str, _ClosingFixture],
    fetched_at: datetime,
    snapshot_id: str,
    raw_path: Path,
    window: FallbackPlan | None = None,
) -> tuple[dict[str, _ClosingFixture], dict[str, datetime]]:
    if window is not None and due:
        raise RuntimeError("An official NBA entry window cannot be combined with a targeted batch")
    quoted = {}
    rows = []
    provider_starts = {}
    minimum_lead = timedelta(minutes=trial.min_minutes_before_start)
    local_day = fetched_at.astimezone(_EASTERN).date()
    daily_capture = datetime.combine(local_day, _DAILY_CAPTURE_ET, tzinfo=_EASTERN)
    with connect(db_path) as conn:
        conn.execute("BEGIN IMMEDIATE")
        matches = {}
        if window is not None:
            from src.predict.paper_entry_fallback import match_window_fixtures

            books = conn.execute(
                "SELECT book_id FROM books WHERE lower(name) = ? LIMIT 2", (trial.book,)
            ).fetchall()
            if len(books) != 1:
                return {}, {}
            matches = match_window_fixtures(
                conn, trial, window, payload, fetched_at, books[0]["book_id"]
            )
        candidates = (
            {event_id: match.fixture for event_id, match in matches.items()} if window else due
        )
        requested = [
            event
            for event in payload
            if isinstance(event, dict)
            and isinstance(event.get("id"), str)
            and event["id"] in candidates
        ]
        counts = Counter(event["id"] for event in requested)
        for event in requested:
            event_id = event["id"]
            if counts[event_id] != 1:
                continue
            fixture = candidates[event_id]
            match = matches.get(event_id)
            totals = _totals_pair(event)
            provider_start = _utc_timestamp(event.get("commence_time"))
            if totals is None or provider_start is None:
                continue
            if match is None:
                if not _matches_fixture(conn, event, fixture, fetched_at):
                    continue
                game = _eligible_game(conn, fixture.game_id, trial.league)
                current_start = _utc_timestamp(game["start_time_utc"]) if game is not None else None
                if current_start is None:
                    continue
                earliest_start = min(fixture.start, provider_start, current_start)
                latest_start = max(provider_start, current_start)
            else:
                earliest_start, latest_start = fixture.start, fixture.latest_start
            moneyline = _moneyline_pair(event, trial.book)
            if (
                moneyline is None
                or earliest_start - fetched_at < minimum_lead
                or latest_start - minimum_lead >= daily_capture
            ):
                continue
            if match is not None:
                if not _valid_window_market_clocks(event, trial.book, fetched_at):
                    continue
                if match.action is not None:
                    _write_game_mapping(
                        conn,
                        match.provider,
                        fixture.sport_id,
                        fixture.game_id,
                        (fixture.home_team_id, fixture.away_team_id),
                        fetched_at,
                        create=match.action == "created",
                    )
                due[event_id] = fixture
            line, over, under = totals
            home, away = moneyline
            quoted[fixture.game_id] = earliest_start
            provider_starts[fixture.game_id] = provider_start.isoformat()
            for market, outcome, price, point in (
                ("h2h", "home", home, None),
                ("h2h", "away", away, None),
                ("totals", "Over", over, line),
                ("totals", "Under", under, line),
            ):
                rows.append(
                    (
                        snapshot_id,
                        fixture.game_id,
                        fixture.book_id,
                        market,
                        outcome,
                        price,
                        _american_to_decimal(price),
                        _implied_probability(price),
                        point,
                    )
                )
        if rows:
            conn.execute(
                """
                INSERT INTO odds_snapshots (snapshot_id, fetched_at_utc, sport_id, source, raw_path)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    snapshot_id,
                    fetched_at.isoformat(),
                    next(iter(due.values())).sport_id,
                    trial.odds_source,
                    str(raw_path),
                ),
            )
            store_odds_snapshot_game_starts(conn, snapshot_id, provider_starts)
            conn.executemany(
                """
                INSERT INTO odds (snapshot_id, game_id, book_id, market, outcome, price_american,
                                  price_decimal, implied_prob, line)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS paper_entry_quote_starts (
                    snapshot_id TEXT NOT NULL REFERENCES odds_snapshots(snapshot_id) ON DELETE CASCADE,
                    game_id TEXT NOT NULL REFERENCES games(game_id) ON DELETE CASCADE,
                    earliest_start_utc TEXT NOT NULL,
                    PRIMARY KEY (snapshot_id, game_id)
                )
                """
            )
            conn.executemany(
                """
                INSERT INTO paper_entry_quote_starts (snapshot_id, game_id, earliest_start_utc)
                VALUES (?, ?, ?)
                """,
                [(snapshot_id, game_id, start.isoformat()) for game_id, start in quoted.items()],
            )
    return due, quoted


def _capture(
    db_path: Path,
    trial: PaperTrial,
    engine: PredictionEngine,
    due: dict[str, _ClosingFixture],
    quoted: dict[str, datetime],
    snapshots: dict[str, str],
) -> int:
    fixture_ids = {fixture.game_id for fixture in due.values()}
    minimum_lead = timedelta(minutes=trial.min_minutes_before_start)
    predictions = None
    engine_returned_predictions = False
    try:
        now = datetime.now(timezone.utc)
        eligible = {game_id for game_id, start in quoted.items() if start - now >= minimum_lead}
        if eligible:
            predictions = engine.predict_pinned_totals(
                trial.league, trial.model_sha256, days_ahead=1, fixture_ids=eligible
            )
        if predictions is not None and not predictions.empty:
            engine_returned_predictions = True
            now = datetime.now(timezone.utc)
            allowed = {game_id for game_id in eligible if quoted[game_id] - now >= minimum_lead}
            expected = predictions["game_id"].map(snapshots)
            predictions = predictions[
                predictions["game_id"].isin(allowed)
                & predictions["selected_totals_snapshot_id"].eq(expected).fillna(False)
            ]
        counts = capture_predictions(
            trial,
            predictions,
            engine.totals_model_sha256,
            db_path,
            fixture_ids=fixture_ids,
            fixture_start_limits=quoted,
        )
    except Exception:
        try:
            record_capture_failure(trial, db_path, fixture_ids=fixture_ids)
        except Exception:
            LOGGER.exception("Could not record the failed early paper capture")
        raise
    if eligible and not engine_returned_predictions and counts.get("prediction_missing"):
        raise RuntimeError(
            "The early paper entry run produced no predictions for the collected quotes"
        )
    return counts.get("recorded", 0)


def poll_entry(
    *,
    collect: bool = False,
    rules: Path | None = None,
    official_window_fallback: bool = False,
    scheduled_official_fallback: bool = False,
) -> dict[str, str | int]:
    load_env()
    trial = load_trial(rules if rules is not None else DEFAULT_RULES)
    path = resolve_db_path().resolve()
    if not path.is_file():
        return {"status": "warehouse_unavailable", "due_events": 0}
    with closing(sqlite3.connect(f"{path.as_uri()}?mode=ro", uri=True)) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("BEGIN")
        now = datetime.now(timezone.utc)
        status, due, fresh = _plan(conn, trial, now)
        if (
            collect
            and (official_window_fallback or scheduled_official_fallback)
            and status == "no_entry_due"
            and _entry_slot_taken(conn, now)
        ):
            status = "daily_cap"
        if (
            collect and scheduled_official_fallback and not official_window_fallback
            and status in {"due", "no_entry_due"}
        ):
            from src.predict.paper_entry_fallback import cached_official_window_due

            official_window_fallback = cached_official_window_due(conn, trial, now, due, path.parent)
    official = None
    window = None
    if collect and official_window_fallback and status in {"due", "no_entry_due"}:
        from src.predict.paper_entry_fallback import fetch_official_slate

        try:
            official = fetch_official_slate(now.astimezone(_EASTERN).date())
            with closing(sqlite3.connect(f"{path.as_uri()}?mode=ro", uri=True)) as conn:
                conn.row_factory = sqlite3.Row
                conn.execute("BEGIN")
                status, due, fresh, window = _plan_entry_path(
                    conn, trial, datetime.now(timezone.utc), official
                )
        except (RuntimeError, ValueError):
            if status != "due":
                raise
            official = None
    result: dict[str, str | int] = {
        "status": status,
        "due_events": len(window.targets) if window else len(due),
    }
    if not collect or status not in {"due", "reusable", "window_due"}:
        return result

    engine = PredictionEngine(model_type=trial.model_type)
    engine.load_pinned_totals(trial.league, trial.model_sha256)
    key = None
    probe_evidence = None
    probe_payload = None
    probe_safe = True
    if status in {"due", "window_due"}:
        probe: list[requests.Response] = []
        key = _budgeted_key(request_credits=_REQUEST_CREDITS, on_free_events=probe.append)
        if key is None:
            return {**result, "status": "quota_reserve"}
        if window is not None and not probe:
            if not due:
                return {**result, "status": "unverified_zero_credit_probe"}
        if probe:
            fetched = datetime.now(timezone.utc)
            if official_window_fallback and (window is not None or due):
                from src.predict.paper_entry_fallback import classify_probe

                try:
                    probe_payload = probe[0].json()
                except ValueError:
                    probe_payload = None
                if isinstance(probe_payload, list):
                    probe_status = None
                    if (
                        window is not None
                        and classify_probe(probe_payload, window)[0] == "conflict"
                    ):
                        probe_status = "provider_probe_conflict"
                    probe_evidence = {
                        "url": f"{_BASE_URL}/events",
                        "observed_at": fetched.isoformat(),
                        "request_credits": 0,
                        "results": probe_payload,
                    }
                else:
                    probe_payload = None
                    probe_status = "invalid_zero_credit_probe"
                if probe_status is not None:
                    if not due:
                        return {**result, "status": probe_status}
                    probe_safe = False
            local_day = fetched.astimezone(_EASTERN).date()
            evening = datetime.combine(local_day, _DAILY_CAPTURE_ET, tzinfo=_EASTERN)
            minimum_lead = timedelta(minutes=trial.min_minutes_before_start)
            probe_window = (
                fetched + minimum_lead,
                min(
                    fetched + timedelta(hours=trial.max_hours_before_start),
                    evening + minimum_lead - timedelta(microseconds=1),
                ),
            )
            if probe_safe and fetched < evening:
                try:
                    counts = retain_entry_probe(path, probe[0], fetched, probe_window)
                    if counts and (counts["linked"] or counts["created"]):
                        LOGGER.info("NBA entry quota probe discovered fixtures: %s", counts)
                except (RuntimeError, OSError, sqlite3.Error):
                    LOGGER.warning("Could not retain the optional NBA entry event response")
                    if window is not None and not due:
                        return {**result, "status": "zero_credit_probe_retention_failed"}
                    probe_safe = False
        status, due, fresh, window = _reserve(
            path, trial, official, probe_payload, probe_safe=probe_safe
        )
        result.update(status=status, due_events=len(window.targets) if window else len(due))
    if status == "reusable":
        quoted = {game_id: start for game_id, (_, start) in fresh.items()}
        snapshots = {game_id: snapshot_id for game_id, (snapshot_id, _) in fresh.items()}
        recorded = _capture(path, trial, engine, due, quoted, snapshots)
        return {**result, "status": "reused", "decisions_recorded": recorded}
    if status not in {"due", "window_due"}:
        return result
    assert key is not None
    entry_day = (
        window.eastern_date
        if window
        else next(iter(due.values())).start.astimezone(_EASTERN).date()
    )
    params = {
        "apiKey": key,
        "regions": "us",
        "markets": _ENTRY_MARKETS,
        "bookmakers": trial.book,
        "oddsFormat": "american",
    }
    if window is not None:
        params.update(window.odds_window())
    else:
        params["eventIds"] = ",".join(sorted(due))

    # The date slot is already committed. Recheck the clocks before spending it.
    dispatch_at = datetime.now(timezone.utc)
    if dispatch_at.astimezone(_EASTERN).date() != entry_day:
        return {**result, "status": "entry_date_changed"}
    if dispatch_at >= datetime.fromisoformat(trial.review_at):
        return {**result, "status": "trial_inactive"}
    if dispatch_at >= datetime.combine(entry_day, _DAILY_CAPTURE_ET, tzinfo=_EASTERN):
        return {**result, "status": "outside_entry_window"}
    minimum_lead = timedelta(minutes=trial.min_minutes_before_start)
    if window is not None:
        dispatch_status = window.dispatch_status(dispatch_at, minimum_lead)
        if dispatch_status != "window_due":
            return {**result, "status": dispatch_status}
        result["mode"] = "official_window"
    elif not any(fixture.start - dispatch_at >= minimum_lead for fixture in due.values()):
        return {**result, "status": "entry_lead_elapsed"}

    response = _provider_get("odds", params)
    fetched_at = datetime.now(timezone.utc)
    try:
        payload = response.json()
    except ValueError:
        raise RuntimeError("Odds API returned invalid entry odds JSON") from None
    if not isinstance(payload, list):
        raise RuntimeError("Odds API returned an unexpected entry odds response")

    snapshot_id = str(uuid4())
    directory = RAW_DATA_DIR / "odds" / _SPORT / "paper_entry" / snapshot_id
    archive = {
        "source": trial.odds_source,
        "fetched_at": fetched_at.isoformat(),
        "request": _request_metadata(f"{_BASE_URL}/odds", params),
        "results": payload,
        "rate_limit_remaining": response.headers.get("x-requests-remaining"),
        "rate_limit_reset": response.headers.get("x-requests-reset"),
    }
    try:
        if window is not None or any(
            fixture.start < fixture.latest_start for fixture in due.values()
        ):
            if official is not None:
                from src.predict.paper_entry_fallback import archive_official_slate

                archive["official_schedule"] = archive_official_slate(
                    directory, official[0], entry_day, official[1], window
                )
            if probe_evidence is not None:
                archive["provider_events_probe"] = probe_evidence
        raw_path = write_snapshot(archive, directory)
    except OSError as exc:
        raise RuntimeError("Could not archive the entry odds response") from exc
    due, quoted = _append_entry_quotes(
        path, trial, payload, due, fetched_at, snapshot_id, raw_path, window
    )

    recorded = (
        _capture(path, trial, engine, due, quoted, dict.fromkeys(quoted, snapshot_id)) if due else 0
    )
    return {
        **result,
        "status": "collected" if quoted else "no_entry_quotes",
        "events_loaded": len(quoted),
        "decisions_recorded": recorded,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--collect", action="store_true", help="Allow one paid odds request if due")
    parser.add_argument("--paper-rules", type=Path, default=DEFAULT_RULES)
    fallback = parser.add_mutually_exclusive_group()
    fallback.add_argument(
        "--official-window-fallback",
        action="store_true",
        help="Use the same entry slot for a verified official slate when provider IDs are missing",
    )
    fallback.add_argument(
        "--scheduled-official-fallback",
        action="store_true",
        help="Check saved slate evidence before fetching a fresh official schedule",
    )
    args = parser.parse_args()
    if (args.official_window_fallback or args.scheduled_official_fallback) and not args.collect:
        parser.error("The official fallback options require --collect")
    logging.basicConfig(level=logging.INFO)
    try:
        result = poll_entry(
            collect=args.collect,
            rules=args.paper_rules,
            official_window_fallback=args.official_window_fallback,
            scheduled_official_fallback=args.scheduled_official_fallback,
        )
    except (RuntimeError, ValueError) as exc:
        parser.exit(1, f"{exc}\n")
    print(json.dumps(result))


if __name__ == "__main__":
    main()
