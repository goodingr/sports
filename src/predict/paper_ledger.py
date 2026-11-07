"""Record and review a prospective NBA totals paper trial in the warehouse."""

from __future__ import annotations

import argparse
import json
import math
import re
import sqlite3
from collections import Counter
from contextlib import closing
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from src.db.core import connect, resolve_db_path
from src.models.prediction_quality import expected_value, no_vig_pair
from src.predict.paper_report import build_paper_report
from src.predict.paper_schedule import record_known_fixtures
from src.predict.paper_settlement import settle_open_trials

DEFAULT_RULES = Path("config/paper_trial.yml")
SCHEMA_PATH = Path(__file__).with_name("paper_schema.sql")


def _utc(value: Any) -> datetime:
    if isinstance(value, str):
        value = datetime.fromisoformat(value)
    if not isinstance(value, datetime) or pd.isna(value) or value.utcoffset() is None:
        raise ValueError("expected a timezone-aware timestamp")
    return value.astimezone(timezone.utc)


@dataclass(frozen=True)
class PaperTrial:
    trial_id: str
    league: str
    market: str
    model_type: str
    model_sha256: str
    book: str
    odds_source: str
    min_edge: float
    max_quote_age_minutes: int
    min_minutes_before_start: int
    max_hours_before_start: int
    min_decisions: int
    min_roi: float
    min_clv_coverage: float
    review_at: str

    def __post_init__(self) -> None:
        if (self.league, self.market, self.book, self.odds_source) != (
            "NBA",
            "totals",
            "draftkings",
            "the-odds-api",
        ):
            raise ValueError("paper settlement supports NBA DraftKings totals from The Odds API")
        if not re.fullmatch(r"[a-z0-9_]+", self.trial_id):
            raise ValueError("invalid paper trial ID")
        if not re.fullmatch(r"[0-9a-f]{64}", self.model_sha256):
            raise ValueError("paper trial requires a lowercase SHA-256 model digest")
        if not (
            0 < self.min_edge <= 1
            and 0 < self.max_quote_age_minutes <= 15
            and 0 < self.min_minutes_before_start < self.max_hours_before_start * 60
            and self.min_decisions >= 150
            and 0.05 <= self.min_roi <= 1
            and 0.8 <= self.min_clv_coverage <= 1
        ):
            raise ValueError("invalid paper trial thresholds")
        _utc(self.review_at)

    @property
    def specification(self) -> str:
        return json.dumps(asdict(self), sort_keys=True, allow_nan=False)


def load_trial(path: Path = DEFAULT_RULES) -> PaperTrial:
    return PaperTrial(**yaml.safe_load(path.read_text(encoding="utf-8")))


def _register(conn: sqlite3.Connection, trial: PaperTrial, now: datetime) -> datetime:
    existing = conn.execute(
        "SELECT registered_at, specification FROM paper_trials WHERE trial_id = ?",
        (trial.trial_id,),
    ).fetchone()
    if existing is not None:
        if existing["specification"] != trial.specification:
            raise ValueError(f"paper trial {trial.trial_id} changed; use a new trial ID")
        return _utc(existing["registered_at"])
    if now >= _utc(trial.review_at):
        raise ValueError(f"paper trial {trial.trial_id} is already past its review deadline")
    conn.execute(
        "INSERT INTO paper_trials VALUES (?, ?, ?)",
        (trial.trial_id, now.isoformat(), trial.specification),
    )
    return now


def register_trial(trial: PaperTrial, db_path: Path | None = None) -> None:
    with connect(db_path) as conn:
        conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
        conn.execute("BEGIN IMMEDIATE")
        now = datetime.now(timezone.utc)
        _register(conn, trial, now)
        record_known_fixtures(
            conn,
            trial.trial_id,
            trial.league,
            now,
            _utc(trial.review_at),
            max_lead_hours=trial.max_hours_before_start,
        )


def _eligible_game(conn: sqlite3.Connection, game_id: str, league: str) -> sqlite3.Row | None:
    return conn.execute(
        """
        SELECT g.sport_id, g.home_team_id, g.away_team_id, g.start_time_utc
        FROM games g
        JOIN sports s ON s.sport_id = g.sport_id
        LEFT JOIN game_results r ON r.game_id = g.game_id
        WHERE g.game_id = ? AND s.league = ? AND lower(g.status) = 'scheduled'
          AND r.home_score IS NULL AND r.away_score IS NULL
          AND NOT EXISTS (
            SELECT 1 FROM games other
            LEFT JOIN game_results result ON result.game_id = other.game_id
            WHERE other.game_id <> g.game_id AND other.sport_id = g.sport_id
              AND other.home_team_id = g.home_team_id AND other.away_team_id = g.away_team_id
              AND abs(julianday(other.start_time_utc) - julianday(g.start_time_utc)) < 2.0 / 24
              AND (lower(other.status) IN ('live', 'in_progress', 'final', 'completed', 'closed')
                   OR result.home_score IS NOT NULL OR result.away_score IS NOT NULL)
          )
        """,
        (game_id, league),
    ).fetchone()


def _decision_quote(
    conn: sqlite3.Connection, trial: PaperTrial, row: Any, game: sqlite3.Row
) -> sqlite3.Row | None:
    return conn.execute(
        """
        SELECT sn.snapshot_id, sn.fetched_at_utc, b.book_id, b.name AS book,
               over.line, over.price_american AS over_price, under.price_american AS under_price
        FROM odds_snapshots sn
        JOIN odds over ON over.snapshot_id = sn.snapshot_id
        JOIN odds under ON under.snapshot_id = over.snapshot_id
          AND under.game_id = over.game_id AND under.book_id = over.book_id
          AND under.market = over.market AND under.line = over.line
        JOIN books b ON b.book_id = over.book_id
        WHERE sn.snapshot_id = ? AND over.game_id = ? AND b.book_id = ?
          AND sn.sport_id = ? AND sn.source = ? AND lower(b.name) = ?
          AND over.market = 'totals' AND lower(over.outcome) = 'over'
          AND lower(under.outcome) = 'under'
          AND sn.snapshot_id = (
            SELECT newest.snapshot_id FROM odds_snapshots newest
            WHERE newest.sport_id = sn.sport_id AND newest.source = sn.source
              AND EXISTS (
                SELECT 1 FROM odds recent
                WHERE recent.snapshot_id = newest.snapshot_id AND recent.game_id = over.game_id
                  AND recent.market = 'totals'
              )
            ORDER BY julianday(newest.fetched_at_utc) DESC, newest.snapshot_id DESC
            LIMIT 1
          )
        """,
        (
            row.selected_totals_snapshot_id,
            row.game_id,
            int(row.selected_totals_book_id),
            game["sport_id"],
            trial.odds_source,
            trial.book,
        ),
    ).fetchone()


def _model_inputs_match(conn: sqlite3.Connection, row: Any, quote: sqlite3.Row) -> bool:
    """Check the odds inputs the model saw against the same decision snapshot."""
    moneylines = {
        odd["outcome"]: odd["price_american"]
        for odd in conn.execute(
            """
            SELECT lower(outcome) AS outcome, price_american
            FROM odds
            WHERE snapshot_id = ? AND game_id = ? AND book_id = ?
              AND market = 'h2h' AND lower(outcome) IN ('home', 'away')
            """,
            (quote["snapshot_id"], row.game_id, quote["book_id"]),
        )
    }
    home_price, away_price = moneylines.get("home"), moneylines.get("away")
    # The SHA-pinned model never splits on its spread input.
    return all(
        price is not None and math.isfinite(price) and abs(price) >= 100
        for price in (home_price, away_price)
    ) and (home_price, away_price) == (row.home_moneyline, row.away_moneyline)


def _capture_one(
    conn: sqlite3.Connection, trial: PaperTrial, row: Any, registered: datetime, now: datetime
) -> str:
    game = _eligible_game(conn, row.game_id, trial.league)
    if game is None:
        return "game_unavailable"
    try:
        start = _utc(game["start_time_utc"])
        model_start = _utc(row.commence_time)
        predicted_at = _utc(row.predicted_at)
    except ValueError:
        return "invalid_time"
    lead = start - now
    if (
        start != model_start
        or not registered <= predicted_at <= now
        or not timedelta(minutes=trial.min_minutes_before_start)
        <= lead
        <= timedelta(hours=trial.max_hours_before_start)
    ):
        return "outside_decision_window"
    if pd.isna(row.selected_totals_snapshot_id) or pd.isna(row.selected_totals_book_id):
        return "no_paired_quote"
    quote = _decision_quote(conn, trial, row, game)
    if quote is None:
        return "quote_not_current_at_trial_book"
    try:
        fetched_at = _utc(quote["fetched_at_utc"])
    except ValueError:
        return "invalid_time"
    if not fetched_at <= predicted_at or now - fetched_at > timedelta(
        minutes=trial.max_quote_age_minutes
    ):
        return "stale_quote"
    if not _model_inputs_match(conn, row, quote):
        return "unverified_model_inputs"

    line, over_price, under_price = quote["line"], quote["over_price"], quote["under_price"]
    over_prob, under_prob = row.over_predicted_prob, row.under_predicted_prob
    if (
        not all(
            value is not None and math.isfinite(value)
            for value in (line, over_price, under_price, over_prob, under_prob)
        )
        or (line, over_price, under_price)
        != (row.total_line, row.over_moneyline, row.under_moneyline)
        or line <= 0
        or not float(line * 2).is_integer()
        or abs(over_price) < 100
        or abs(under_price) < 100
        or not 0 <= over_prob <= 1
        or not 0 <= under_prob <= 1
        or not math.isclose(over_prob + under_prob, 1, abs_tol=1e-9)
    ):
        return "invalid_quote_or_prediction"
    over_market, under_market = no_vig_pair(over_price, under_price)
    if over_prob - over_market >= under_prob - under_market:
        side, price, opposite, model, market = (
            "over",
            over_price,
            under_price,
            over_prob,
            over_market,
        )
    else:
        side, price, opposite, model, market = (
            "under",
            under_price,
            over_price,
            under_prob,
            under_market,
        )
    edge = model - market
    if edge < trial.min_edge or expected_value(model, price, stake=1) <= 0:
        return "below_edge_threshold"

    cursor = conn.execute(
        """
        INSERT INTO paper_decisions (
            trial_id, game_id, home_team_id, away_team_id, start_time_utc, captured_at, predicted_at,
            odds_source, snapshot_id, odds_fetched_at, book_id, book, side, line, price,
            opposite_price, model_probability, market_probability, edge
        )
        SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        WHERE NOT EXISTS (
            SELECT 1 FROM paper_decisions
            WHERE trial_id = ? AND home_team_id = ? AND away_team_id = ?
              AND abs(julianday(start_time_utc) - julianday(?)) < 2.0 / 24
        )
        ON CONFLICT(trial_id, game_id) DO NOTHING
        """,
        (
            trial.trial_id,
            row.game_id,
            game["home_team_id"],
            game["away_team_id"],
            start.isoformat(),
            now.isoformat(),
            predicted_at.isoformat(),
            trial.odds_source,
            quote["snapshot_id"],
            fetched_at.isoformat(),
            quote["book_id"],
            quote["book"],
            side,
            line,
            price,
            opposite,
            model,
            market,
            edge,
            trial.trial_id,
            game["home_team_id"],
            game["away_team_id"],
            start.isoformat(),
        ),
    )
    return "recorded" if cursor.rowcount else "already_recorded"


def _record_observations(
    conn: sqlite3.Connection,
    trial: PaperTrial,
    now: datetime,
    status: str,
    outcomes: dict[str, tuple[str, str | None]],
) -> int:
    cursor = conn.execute(
        "INSERT INTO paper_capture_runs (trial_id, observed_at, status) VALUES (?, ?, ?)",
        (trial.trial_id, now.isoformat(), status),
    )
    fixtures = conn.execute(
        """
        SELECT g.game_id, g.start_time_utc
        FROM games g JOIN sports s ON s.sport_id = g.sport_id
        WHERE s.league = ? AND lower(g.status) = 'scheduled'
          AND julianday(g.start_time_utc) BETWEEN julianday(?) AND julianday(?)
        ORDER BY g.game_id
        """,
        (
            trial.league,
            (now + timedelta(minutes=trial.min_minutes_before_start)).isoformat(),
            (now + timedelta(hours=trial.max_hours_before_start)).isoformat(),
        ),
    ).fetchall()
    conn.executemany(
        """
        INSERT INTO paper_fixture_observations
            (run_id, game_id, start_time_utc, outcome, snapshot_id)
        VALUES (?, ?, ?, ?, ?)
        """,
        [
            (
                cursor.lastrowid,
                fixture["game_id"],
                fixture["start_time_utc"],
                *outcomes.get(fixture["game_id"], ("prediction_missing", None)),
            )
            for fixture in fixtures
        ],
    )
    return sum(fixture["game_id"] not in outcomes for fixture in fixtures)


def record_capture_failure(trial: PaperTrial, db_path: Path | None = None) -> None:
    with connect(db_path) as conn:
        conn.execute("BEGIN IMMEDIATE")
        now = datetime.now(timezone.utc)
        _register(conn, trial, now)
        if now < _utc(trial.review_at):
            record_known_fixtures(
                conn,
                trial.trial_id,
                trial.league,
                now,
                _utc(trial.review_at),
                max_lead_hours=trial.max_hours_before_start,
            )
            _record_observations(conn, trial, now, "failed", {})


def capture_predictions(
    trial: PaperTrial,
    predictions: pd.DataFrame | None,
    model_sha256: str | None,
    db_path: Path | None = None,
) -> dict[str, int]:
    if predictions is not None and not predictions.empty and model_sha256 != trial.model_sha256:
        raise ValueError(
            f"loaded totals model does not match the pinned digest for {trial.trial_id}"
        )
    counts: Counter[str] = Counter()
    with connect(db_path) as conn:
        conn.execute("BEGIN IMMEDIATE")
        now = datetime.now(timezone.utc)
        registered = _register(conn, trial, now)
        if now >= _utc(trial.review_at):
            return {"trial_closed": 1}
        record_known_fixtures(
            conn,
            trial.trial_id,
            trial.league,
            now,
            _utc(trial.review_at),
            max_lead_hours=trial.max_hours_before_start,
        )
        outcomes: dict[str, tuple[str, str | None]] = {}
        if predictions is not None:
            for row in predictions.itertuples(index=False):
                outcome = _capture_one(conn, trial, row, registered, now)
                counts[outcome] += 1
                snapshot = row.selected_totals_snapshot_id
                outcomes[row.game_id] = (outcome, None if pd.isna(snapshot) else str(snapshot))
        status = "no_predictions" if predictions is None or predictions.empty else "evaluated"
        missing = _record_observations(conn, trial, now, status, outcomes)
        if missing:
            counts["prediction_missing"] += missing
    return dict(counts)


def update(trial: PaperTrial, db_path: Path | None = None) -> dict[str, Any]:
    with connect(db_path) as conn:
        conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
        conn.execute("BEGIN IMMEDIATE")
        now = datetime.now(timezone.utc)
        _register(conn, trial, now)
        record_known_fixtures(
            conn,
            trial.trial_id,
            trial.league,
            now,
            _utc(trial.review_at),
            max_lead_hours=trial.max_hours_before_start,
        )
        newly_settled = settle_open_trials(conn, now)
        return {**build_paper_report(conn, now), "newly_settled": newly_settled}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["update", "report"])
    parser.add_argument("--db", type=Path)
    parser.add_argument("--rules", type=Path, default=DEFAULT_RULES)
    args = parser.parse_args()
    db_path = resolve_db_path(args.db)
    if args.command == "update":
        result = update(load_trial(args.rules), db_path)
    else:
        with closing(sqlite3.connect(f"{db_path.resolve().as_uri()}?mode=ro", uri=True)) as conn:
            conn.row_factory = sqlite3.Row
            conn.execute("BEGIN")
            result = build_paper_report(conn, datetime.now(timezone.utc))
    print(json.dumps(result, indent=2, allow_nan=False))


if __name__ == "__main__":
    main()
