"""Exercise the frozen archive and model boundaries without production outcomes."""

import json
import sqlite3
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pytest

from reports.research.ncaab_imbalance_offset import experiment, review
from scripts import ncaab_quote_coverage

FETCHED = datetime(2026, 11, 5, 22, tzinfo=timezone.utc)
TIP = FETCHED + timedelta(hours=2)
EVENT_ID = "a" * 32
TEAMS = {"home_team": "Kentucky Wildcats", "away_team": "Duke Blue Devils"}


@pytest.fixture
def protocol():
    return json.loads(experiment.PROTOCOL.read_text(encoding="utf-8"))


def test_zero_and_lower_bounds_are_exact():
    offsets = np.zeros(12)
    imbalance = np.full(12, 2.0)
    assert experiment._fit_beta(offsets, imbalance, np.ones(12), -1, 0, 5) == 0
    assert experiment._fit_beta(offsets, imbalance, np.zeros(12), -1, 0, 0.1) == -1


def test_interior_optimum_penalizes_summed_loss():
    offsets = np.zeros(12)
    imbalance = np.full(12, 2.0)
    labels = np.r_[np.ones(4), np.zeros(8)]
    beta = experiment._fit_beta(offsets, imbalance, labels, -1, 0, 5)
    assert -1 < beta < 0
    p = 1 / (1 + np.exp(-2 * beta))
    assert abs((p * 12 - 4) * 2 + 5 * beta) < 1e-10


@pytest.mark.parametrize("age", [timedelta(seconds=-1), None, timedelta(minutes=10, seconds=1)])
def test_book_timestamp_cannot_substitute_for_market_timestamp(age):
    book = {
        "last_update": FETCHED.isoformat(),
        "markets": [{
            "key": "totals",
            "last_update": (FETCHED - age).isoformat() if age is not None else None,
            "outcomes": [
                {"name": "Over", "point": 142.5, "price": -110},
                {"name": "Under", "point": 142.5, "price": -110},
            ],
        }],
    }
    assert experiment._market(book, "totals", ("over", "under"), FETCHED, timedelta(minutes=10)) is None
    book["markets"][0]["last_update"] = (FETCHED - timedelta(minutes=10)).isoformat()
    assert experiment._market(book, "totals", ("over", "under"), FETCHED, timedelta(minutes=10)) == (0.5, 142.5)


def _final(**changes):
    return {
        "id": EVENT_ID,
        "sport_key": "basketball_ncaab",
        "commence_time": TIP.isoformat(),
        "completed": True,
        "scores": [
            {"name": TEAMS["home_team"], "score": "78"},
            {"name": TEAMS["away_team"], "score": "68"},
        ],
        **TEAMS,
        **changes,
    }


def _archive(tmp_path: Path, observed: datetime, event: dict):
    name = observed.strftime("scores_%Y-%m-%dT%H-%M-%SZ.json")
    path = tmp_path / name
    path.write_text(json.dumps([event]), encoding="utf-8")
    return path


def _cohort():
    teams = experiment._teams(TEAMS, "NCAAB")
    assert teams is not None
    quote = experiment.Quote(EVENT_ID, teams, TIP, FETCHED, 142.5, 0.5, 0.75)
    return {quote.event_id: quote}


def test_later_scores_file_is_never_opened(tmp_path, monkeypatch, protocol):
    cutoff = TIP + timedelta(hours=8)
    _archive(tmp_path, TIP + timedelta(hours=4), _final())
    forbidden = _archive(tmp_path, cutoff, _final())
    read_bytes = Path.read_bytes

    def read_before_cutoff(path):
        assert path != forbidden
        return read_bytes(path)

    monkeypatch.setattr(Path, "read_bytes", read_before_cutoff)
    grades, counts = experiment._score_evidence(tmp_path, _cohort(), protocol, FETCHED, cutoff)
    assert grades == {EVENT_ID: 146}
    assert counts["development_score_receipts"] == 1


def test_conflicting_and_wrong_identity_grades_stay_unresolved(tmp_path, protocol):
    _archive(tmp_path, TIP + timedelta(hours=3), _final(home_team=TEAMS["away_team"], away_team=TEAMS["home_team"]))
    grades, counts = experiment._score_evidence(
        tmp_path, _cohort(), protocol, FETCHED, TIP + timedelta(hours=8)
    )
    assert not grades
    assert counts["missing_score_fixtures"] == 1
    _archive(tmp_path, TIP + timedelta(hours=4), _final())
    _archive(tmp_path, TIP + timedelta(hours=5), _final(scores=[
        {"name": TEAMS["home_team"], "score": "80"},
        {"name": TEAMS["away_team"], "score": "68"},
    ]))
    grades, counts = experiment._score_evidence(
        tmp_path, _cohort(), protocol, FETCHED, TIP + timedelta(hours=8)
    )
    assert not grades
    assert counts["ambiguous_score_fixtures"] == 1


def _quote_event(fetched):
    return {
        "id": EVENT_ID, "sport_key": "basketball_ncaab", "commence_time": TIP.isoformat(), **TEAMS,
        "bookmakers": [{"key": "draftkings", "markets": [
            {"key": "h2h", "last_update": fetched.isoformat(), "outcomes": [
                {"name": TEAMS["home_team"], "price": -200},
                {"name": TEAMS["away_team"], "price": 160},
            ]},
            {"key": "totals", "last_update": fetched.isoformat(), "outcomes": [
                {"name": "Over", "point": 142.5, "price": -110},
                {"name": "Under", "point": 142.5, "price": -110},
            ]},
        ]}],
    }


def test_prospective_quote_selection_checks_capture_clock_and_live_source(tmp_path, monkeypatch, protocol):
    conn = sqlite3.connect(":memory:")
    conn.executescript("""
        CREATE TABLE sports(sport_id, league);
        CREATE TABLE odds_snapshots(snapshot_id, fetched_at_utc, raw_path, sport_id, source);
        INSERT INTO sports VALUES (1, 'NCAAB');
    """)
    captures = [
        ("before", FETCHED - timedelta(microseconds=1), "the-odds-api"),
        ("historical", FETCHED, "the-odds-api-history"),
        ("live", FETCHED, "the-odds-api"),
        ("live_duplicate", FETCHED, "the-odds-api"),
    ]
    for name, fetched, source in captures:
        path = tmp_path / f"{name}.json"
        path.write_text(json.dumps({
            "fetched_at": fetched.isoformat(),
            "request": {"url": "https://api.the-odds-api.com/v4/sports/basketball_ncaab/odds"},
            "results": [_quote_event(fetched)],
        }), encoding="utf-8")
        conn.execute(
            "INSERT INTO odds_snapshots VALUES (?, ?, ?, 1, ?)", (name, fetched.isoformat(), str(path), source)
        )

    original = experiment.read_raw_evidence

    def live_only(path, data_dir):
        assert Path(path).stem in {"live", "live_duplicate"}
        return original(path, data_dir)

    monkeypatch.setattr(experiment, "read_raw_evidence", live_only)
    with conn:
        quotes, counts = experiment._quotes(
            conn, tmp_path, protocol, FETCHED, TIP + timedelta(days=1), first_capture=FETCHED
        )
    assert len(quotes) == 1
    assert quotes[EVENT_ID].fetched == FETCHED
    assert counts["verified_odds_receipts"] == counts["duplicate_odds_registrations"] == 1


def test_holdout_clock_precedes_warehouse_and_score_access(tmp_path, monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("The closed holdout accessed the warehouse or scores")

    monkeypatch.setattr(review.sqlite3, "connect", forbidden)
    monkeypatch.setattr(review.experiment, "_score_evidence", forbidden)
    result = review.review(tmp_path / "db", tmp_path, now=FETCHED)
    assert result["status"] == "review_not_yet_open"


def test_paired_interval_uses_eastern_fixture_dates():
    quote = _cohort()[EVENT_ID]
    # A 23:30 ET tip and a 00:30 ET tip belong to different ET clusters.
    first = experiment.Quote(EVENT_ID, quote.teams, FETCHED.replace(hour=4, minute=30), quote.fetched,
                             quote.line, quote.over, quote.home)
    second = experiment.Quote(EVENT_ID, quote.teams, FETCHED.replace(hour=5, minute=30), quote.fetched,
                              quote.line, quote.over, quote.home)
    metrics = review._paired_metrics(
        [(first, False), (second, False)], -0.2, bootstrap_resamples=20, bootstrap_seed=42
    )
    assert metrics["eastern_dates"] == 2
    assert metrics["paired_log_loss_delta_ci95"][1] < 0


def test_earlier_schedule_only_conflict_is_ambiguous(tmp_path, protocol):
    conn = sqlite3.connect(":memory:")
    conn.executescript("""
        CREATE TABLE sports(sport_id, league);
        CREATE TABLE odds_snapshots(snapshot_id, fetched_at_utc, raw_path, sport_id, source);
        INSERT INTO sports VALUES (1, 'NCAAB');
    """)
    earlier = FETCHED - timedelta(minutes=30)
    no_markets = _quote_event(earlier)
    no_markets["bookmakers"] = []
    no_markets["commence_time"] = (TIP + timedelta(minutes=30)).isoformat()
    for name, fetched, event in (("schedule", earlier, no_markets), ("quote", FETCHED, _quote_event(FETCHED))):
        path = tmp_path / f"{name}.json"
        path.write_text(json.dumps({
            "fetched_at": fetched.isoformat(),
            "request": {"url": "https://api.the-odds-api.com/v4/sports/basketball_ncaab/odds"},
            "results": [event],
        }), encoding="utf-8")
        conn.execute("INSERT INTO odds_snapshots VALUES (?, ?, ?, 1, 'the-odds-api')",
                     (name, fetched.isoformat(), str(path)))
    with conn:
        quotes, counts = experiment._quotes(conn, tmp_path, protocol, FETCHED, TIP + timedelta(days=1))
    assert not quotes
    assert counts["ambiguous_quote_fixtures"] == 1


def test_explicit_post_final_reopening_remains_unresolved(tmp_path, protocol):
    _archive(tmp_path, TIP + timedelta(hours=4), _final())
    _archive(tmp_path, TIP + timedelta(hours=5), _final(completed=False, scores=None))
    grades, counts = experiment._score_evidence(
        tmp_path, _cohort(), protocol, FETCHED, TIP + timedelta(hours=8)
    )
    assert not grades
    assert counts["ambiguous_score_fixtures"] == 1


def test_quote_coverage_is_empty_before_first_capture(tmp_path, monkeypatch, protocol):
    start = datetime.fromisoformat(
        protocol["prospective_holdout"]["first_quote_and_kickoff_not_before_utc"]
    )

    def forbidden(*args, **kwargs):
        pytest.fail("Preview opened evidence before the first eligible capture")

    monkeypatch.setattr(ncaab_quote_coverage.sqlite3, "connect", forbidden)
    monkeypatch.setattr(experiment, "_score_evidence", forbidden)
    assert ncaab_quote_coverage.preview(
        tmp_path / "warehouse.db", now=start - timedelta(microseconds=1)
    ) == {
        "verified_odds_receipts": 0,
        "unverified_odds_registrations": 0,
        "duplicate_odds_registrations": 0,
        "eligible_fixtures": 0,
        "eastern_fixture_dates": 0,
        "ambiguous_quote_fixtures": 0,
        "earliest_eligible_capture_utc": None,
        "latest_eligible_capture_utc": None,
    }


def test_quote_coverage_counts_a_later_fixture_without_reading_scores(tmp_path, monkeypatch):
    fetched = datetime(2026, 11, 8, 23, 8, tzinfo=timezone.utc)
    event = _quote_event(fetched)
    event["commence_time"] = (fetched + timedelta(hours=2)).isoformat()
    receipt = tmp_path / "odds.json"
    receipt.write_text(json.dumps({
        "fetched_at": fetched.isoformat(),
        "request": {"url": "https://api.the-odds-api.com/v4/sports/basketball_ncaab/odds"},
        "results": [event],
    }), encoding="utf-8")

    db = tmp_path / "warehouse.db"
    with closing(sqlite3.connect(db)) as conn, conn:
        conn.executescript("""
            CREATE TABLE sports(sport_id, league);
            CREATE TABLE odds_snapshots(snapshot_id, fetched_at_utc, raw_path, sport_id, source);
            INSERT INTO sports VALUES (1, 'NCAAB');
        """)
        conn.executemany(
            "INSERT INTO odds_snapshots VALUES (?, ?, ?, 1, 'the-odds-api')",
            [
                ("original", fetched.isoformat(), str(receipt)),
                ("duplicate", fetched.isoformat(), str(receipt)),
                ("missing", fetched.isoformat(), str(tmp_path / "missing.json")),
            ],
        )

    score_dir = tmp_path / "raw" / "scores" / "NCAAB"
    score_dir.mkdir(parents=True)
    forbidden_score = _archive(score_dir, fetched + timedelta(minutes=1), _final())
    original_open = Path.open

    def open_without_scores(path, *args, **kwargs):
        assert path != forbidden_score
        return original_open(path, *args, **kwargs)

    def forbidden(*args, **kwargs):
        pytest.fail("Preview called the score reader")

    monkeypatch.setattr(Path, "open", open_without_scores)
    monkeypatch.setattr(experiment, "_score_evidence", forbidden)
    assert ncaab_quote_coverage.preview(db, now=fetched + timedelta(minutes=1)) == {
        "verified_odds_receipts": 1,
        "unverified_odds_registrations": 1,
        "duplicate_odds_registrations": 1,
        "eligible_fixtures": 1,
        "eastern_fixture_dates": 1,
        "ambiguous_quote_fixtures": 0,
        "earliest_eligible_capture_utc": fetched.isoformat(),
        "latest_eligible_capture_utc": fetched.isoformat(),
    }
