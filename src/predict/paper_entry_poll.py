"""Collect one targeted morning quote batch for NBA paper fixtures the daily run would miss."""

from __future__ import annotations

import argparse
import json
import logging
import sqlite3
from collections import Counter
from contextlib import closing
from datetime import datetime, time, timedelta, timezone
from pathlib import Path
from uuid import uuid4
from zoneinfo import ZoneInfo

import pandas as pd
import requests

from src.data.config import RAW_DATA_DIR, load_env
from src.data.ingest_nba_events import retain_entry_probe
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
    _utc_timestamp,
    _valid_american_odds,
    _valid_total_line,
)
from src.predict.storage import _apply_deterministic_odds

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
    if (
        _has_table(conn, "paper_entry_requests")
        and conn.execute(
            "SELECT 1 FROM paper_entry_requests WHERE entry_date_et = ?", (local_day.isoformat(),)
        ).fetchone()
    ):
        return "daily_cap", due, {}
    return "due", due, {}


def _fresh_quotes(
    conn: sqlite3.Connection, trial: PaperTrial, due: dict[str, _ClosingFixture], now: datetime
) -> _FreshQuotes:
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
    odds = pd.read_sql_query(
        f"""
        SELECT o.game_id, o.market, o.outcome, o.price_american, o.line, o.snapshot_id,
               o.book_id, b.name AS book_name, s.fetched_at_utc, s.raw_path
        FROM odds o JOIN odds_snapshots s ON s.snapshot_id = o.snapshot_id
        JOIN books b ON b.book_id = o.book_id
        WHERE o.game_id IN ({markers}) AND o.market IN ('h2h', 'totals')
          AND o.price_american IS NOT NULL
          AND julianday(s.fetched_at_utc) BETWEEN julianday(?) AND julianday(?)
        """,
        conn,
        params=[
            *game_ids,
            (now - timedelta(minutes=trial.max_quote_age_minutes)).isoformat(),
            now.isoformat(),
        ],
    )
    raw_paths = dict(zip(odds["snapshot_id"], odds["raw_path"], strict=True))
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
    for row in _apply_deterministic_odds(games, odds).itertuples(index=False):
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
        snapshot_id = quote["snapshot_id"]
        start = fixture_starts[row.game_id]
        evidence_key = (snapshot_id, row.game_id)
        if evidence_key in stored_starts:
            verified_start = stored_starts[evidence_key]
            if verified_start is None or abs(start - verified_start) > _START_TOLERANCE:
                continue
            start = min(start, verified_start)
        else:
            raw_path = raw_paths.get(snapshot_id)
            if isinstance(raw_path, str) and "paper_entry" in raw_path.replace(
                "\\", "/"
            ).lower().split("/"):
                continue
        fresh[row.game_id] = (snapshot_id, start)
    return fresh


def _reserve(
    db_path: Path, trial: PaperTrial
) -> tuple[str, dict[str, _ClosingFixture], _FreshQuotes]:
    with connect(db_path) as conn:
        conn.execute("BEGIN IMMEDIATE")
        now = datetime.now(timezone.utc)
        status, due, fresh = _plan(conn, trial, now)
        if status == "due":
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
        return status, due, fresh


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


def _append_entry_quotes(
    db_path: Path,
    trial: PaperTrial,
    payload: list,
    due: dict[str, _ClosingFixture],
    fetched_at: datetime,
    snapshot_id: str,
    raw_path: Path,
) -> dict[str, datetime]:
    requested = [
        event
        for event in payload
        if isinstance(event, dict) and isinstance(event.get("id"), str) and event["id"] in due
    ]
    counts = Counter(event["id"] for event in requested)
    quoted = {}
    rows = []
    provider_starts = {}
    minimum_lead = timedelta(minutes=trial.min_minutes_before_start)
    local_day = fetched_at.astimezone(_EASTERN).date()
    daily_capture = datetime.combine(local_day, _DAILY_CAPTURE_ET, tzinfo=_EASTERN)
    with connect(db_path) as conn:
        conn.execute("BEGIN IMMEDIATE")
        for event in requested:
            if counts[event["id"]] != 1:
                continue
            fixture = due[event["id"]]
            totals = _totals_pair(event)
            if totals is None or not _matches_fixture(conn, event, fixture, fetched_at):
                continue
            game = _eligible_game(conn, fixture.game_id, trial.league)
            if game is None:
                continue
            moneyline = _moneyline_pair(event, trial.book)
            provider_start = _utc_timestamp(event.get("commence_time"))
            current_start = _utc_timestamp(game["start_time_utc"])
            if moneyline is None or provider_start is None or current_start is None:
                continue
            earliest_start = min(fixture.start, provider_start, current_start)
            latest_start = max(provider_start, current_start)
            if (
                earliest_start - fetched_at < minimum_lead
                or latest_start - minimum_lead >= daily_capture
            ):
                continue
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
    return quoted


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


def poll_entry(*, collect: bool = False, rules: Path | None = None) -> dict[str, str | int]:
    load_env()
    trial = load_trial(rules if rules is not None else DEFAULT_RULES)
    path = resolve_db_path().resolve()
    if not path.is_file():
        return {"status": "warehouse_unavailable", "due_events": 0}
    with closing(sqlite3.connect(f"{path.as_uri()}?mode=ro", uri=True)) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("BEGIN")
        status, due, fresh = _plan(conn, trial, datetime.now(timezone.utc))
    result: dict[str, str | int] = {"status": status, "due_events": len(due)}
    if not collect or status not in {"due", "reusable"}:
        return result

    engine = PredictionEngine(model_type=trial.model_type)
    engine.load_pinned_totals(trial.league, trial.model_sha256)
    key = None
    if status == "due":
        probe: list[requests.Response] = []
        key = _budgeted_key(request_credits=_REQUEST_CREDITS, on_free_events=probe.append)
        if key is None:
            return {**result, "status": "quota_reserve"}
        if probe:
            fetched = datetime.now(timezone.utc)
            local_day = fetched.astimezone(_EASTERN).date()
            evening = datetime.combine(local_day, _DAILY_CAPTURE_ET, tzinfo=_EASTERN)
            minimum_lead = timedelta(minutes=trial.min_minutes_before_start)
            window = (
                fetched + minimum_lead,
                min(
                    fetched + timedelta(hours=trial.max_hours_before_start),
                    evening + minimum_lead - timedelta(microseconds=1),
                ),
            )
            if fetched < evening:
                try:
                    counts = retain_entry_probe(path, probe[0], fetched, window)
                    if counts and (counts["linked"] or counts["created"]):
                        LOGGER.info("NBA entry quota probe discovered fixtures: %s", counts)
                except (RuntimeError, OSError, sqlite3.Error):
                    LOGGER.warning("Could not retain the optional NBA entry event response")
        status, due, fresh = _reserve(path, trial)
        result.update(status=status, due_events=len(due))
    if status == "reusable":
        quoted = {game_id: start for game_id, (_, start) in fresh.items()}
        snapshots = {game_id: snapshot_id for game_id, (snapshot_id, _) in fresh.items()}
        recorded = _capture(path, trial, engine, due, quoted, snapshots)
        return {**result, "status": "reused", "decisions_recorded": recorded}
    if status != "due":
        return result
    assert key is not None
    if (
        datetime.now(timezone.utc).astimezone(_EASTERN).date()
        != next(iter(due.values())).start.astimezone(_EASTERN).date()
    ):
        return {**result, "status": "entry_date_changed"}

    params = {
        "apiKey": key,
        "regions": "us",
        "markets": _ENTRY_MARKETS,
        "bookmakers": trial.book,
        "eventIds": ",".join(sorted(due)),
        "oddsFormat": "american",
    }
    response = _provider_get("odds", params)
    fetched_at = datetime.now(timezone.utc)
    try:
        payload = response.json()
    except ValueError:
        raise RuntimeError("Odds API returned invalid entry odds JSON") from None
    if not isinstance(payload, list):
        raise RuntimeError("Odds API returned an unexpected entry odds response")

    snapshot_id = str(uuid4())
    try:
        raw_path = write_snapshot(
            {
                "source": trial.odds_source,
                "fetched_at": fetched_at.isoformat(),
                "request": _request_metadata(f"{_BASE_URL}/odds", params),
                "results": payload,
                "rate_limit_remaining": response.headers.get("x-requests-remaining"),
                "rate_limit_reset": response.headers.get("x-requests-reset"),
            },
            RAW_DATA_DIR / "odds" / _SPORT / "paper_entry" / snapshot_id,
        )
    except OSError as exc:
        raise RuntimeError("Could not archive the entry odds response") from exc
    quoted = _append_entry_quotes(path, trial, payload, due, fetched_at, snapshot_id, raw_path)

    recorded = _capture(path, trial, engine, due, quoted, dict.fromkeys(quoted, snapshot_id))
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
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    try:
        result = poll_entry(collect=args.collect, rules=args.paper_rules)
    except (RuntimeError, ValueError) as exc:
        parser.exit(1, f"{exc}\n")
    print(json.dumps(result))


if __name__ == "__main__":
    main()
