"""Exercise price diagnostics with synthetic grades and original quote pairs."""

import json
import sys
from datetime import datetime, timedelta, timezone

import pytest

from src.db.core import connect, initialize, store_odds_snapshot_game_starts
from src.predict import paper_ledger
from src.predict.paper_ledger import SCHEMA_PATH, load_trial
from src.predict.paper_price_diagnostics import closing_price_diagnostics
from src.predict.paper_settlement import settle_open_trials

START = datetime(2026, 11, 8, 23, tzinfo=timezone.utc)
CAPTURE = START - timedelta(hours=2)
NOW = START + timedelta(hours=4)
TEAMS = ("San Antonio Spurs", "Sacramento Kings")


@pytest.fixture
def ledger(tmp_path):
    db = tmp_path / "data" / "betting.db"
    initialize(db)
    trial = load_trial()
    with connect(db) as conn:
        conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
        conn.execute(
            "INSERT INTO sports VALUES (1, 'Basketball', 'NBA', 'totals', ?)",
            (START.isoformat(),),
        )
        conn.execute("INSERT INTO books (book_id, name) VALUES (1, 'DraftKings')")
        conn.executemany(
            "INSERT INTO teams (team_id, sport_id, code, name) VALUES (?, 1, ?, ?)",
            [(1, "SAS", TEAMS[0]), (2, "SAC", TEAMS[1])],
        )
        conn.execute(
            """
            INSERT INTO games (game_id, sport_id, start_time_utc, home_team_id, away_team_id,
                               status, odds_api_id)
            VALUES ('game', 1, ?, 1, 2, 'final', ?)
            """,
            (START.isoformat(), "a" * 32),
        )
        conn.execute("INSERT INTO game_results (game_id, home_score, away_score) VALUES ('game', 112, 108)")
        conn.execute(
            "INSERT INTO paper_trials VALUES (?, ?, ?)",
            (trial.trial_id, (CAPTURE - timedelta(days=1)).isoformat(), trial.specification),
        )
        yield conn, trial, db.parent / "raw" / "odds" / "basketball_nba"


def _quote(conn, raw_dir, name, fetched, line, prices):
    raw_dir.mkdir(parents=True, exist_ok=True)
    receipt = raw_dir / f"{name}.json"
    sides = tuple(zip(("Over", "Under"), prices))
    receipt.write_text(json.dumps({
        "fetched_at": fetched.isoformat(),
        "source": "the-odds-api",
        "request": {"url": "https://api.the-odds-api.com/v4/sports/basketball_nba/odds"},
        "results": [{
            "id": "a" * 32,
            "sport_key": "basketball_nba",
            "home_team": TEAMS[0],
            "away_team": TEAMS[1],
            "commence_time": START.isoformat(),
            "bookmakers": [{"key": "draftkings", "markets": [{
                "key": "totals",
                "outcomes": [
                    {"name": side, "price": price, "point": line} for side, price in sides
                ],
            }]}],
        }],
    }), encoding="utf-8")
    conn.execute(
        "INSERT INTO odds_snapshots VALUES (?, ?, 1, 'the-odds-api', ?)",
        (name, fetched.isoformat(), str(receipt)),
    )
    conn.executemany(
        """
        INSERT INTO odds (snapshot_id, game_id, book_id, market, outcome, price_american, line)
        VALUES (?, 'game', 1, 'totals', ?, ?, ?)
        """,
        [(name, side, price, line) for side, price in sides],
    )
    store_odds_snapshot_game_starts(conn, name, {"game": START.isoformat()})
    return receipt


def _grade(ledger, *, side="over", line=220.5, close_line=None, close_prices=(-130, 110)):
    conn, trial, raw_dir = ledger
    entry_at = CAPTURE - timedelta(minutes=1)
    _quote(conn, raw_dir, "entry", entry_at, line, (-110, -110))
    close_file = _quote(
        conn, raw_dir, "close", START - timedelta(minutes=30),
        line if close_line is None else close_line, close_prices,
    )
    conn.execute(
        """
        INSERT INTO paper_decisions VALUES (
            ?, 'game', 1, 2, ?, ?, ?, 'the-odds-api', 'entry', ?, 1, 'DraftKings',
            ?, ?, -110, -110, .75, .5, .25
        )
        """,
        (
            trial.trial_id, START.isoformat(), CAPTURE.isoformat(), CAPTURE.isoformat(),
            entry_at.isoformat(), side, line,
        ),
    )
    assert settle_open_trials(conn, NOW) == 1
    return close_file


def _audit(ledger, now=NOW):
    conn, trial, _ = ledger
    return closing_price_diagnostics(
        conn, trial.trial_id, datetime.fromisoformat(trial.review_at), NOW, now,
    )


@pytest.mark.parametrize(
    ("side", "prices", "close_decimal", "other_decimal"),
    [
        ("over", (-130, 110), 1 + 100 / 130, 2.1),
        ("under", (-120, 105), 2.05, 1 + 100 / 120),
        ("over", (-120, 100), 1 + 100 / 120, 2.0),
    ],
)
def test_same_line_price_clv_and_close_ev_use_both_exact_prices(
    ledger, side, prices, close_decimal, other_decimal
):
    _grade(ledger, side=side, close_prices=prices)
    result = _audit(ledger)
    entry_decimal = 1 + 100 / 110
    fair = other_decimal / (close_decimal + other_decimal)
    assert result["verified_same_line_closes"] == result["same_line_coverage"] == 1
    assert result["mean_same_line_price_clv"] == pytest.approx(entry_decimal / close_decimal - 1)
    assert result["mean_same_line_no_vig_close_ev_conditional_non_push"] == pytest.approx(
        fair * entry_decimal - 1
    )


def test_verified_changed_line_does_not_compare_prices(ledger):
    _grade(ledger, close_line=221.5)
    result = _audit(ledger)
    assert result["verified_changed_line_closes"] == 1
    assert result["verified_same_line_closes"] == result["same_line_coverage"] == 0
    assert result["mean_same_line_price_clv"] is None
    assert result["mean_same_line_no_vig_close_ev_conditional_non_push"] is None


def test_integer_total_with_observed_push_has_conditional_close_ev(ledger):
    _grade(ledger, line=220)
    result = _audit(ledger)
    assert result["same_line_integer_totals"] == result["same_line_observed_pushes"] == 1
    assert result["mean_same_line_no_vig_close_ev_conditional_non_push"] > 0


@pytest.mark.parametrize(
    "defect",
    ["missing_close_archive", "duplicate_outcome", "opposite_price_changed", "score_corrected",
     "newer_close", "tied_close", "clock_disagrees"],
)
def test_invalid_or_corrected_lineage_is_excluded(ledger, defect):
    conn, _, raw_dir = ledger
    close_file = _grade(ledger)
    if defect == "missing_close_archive":
        close_file.unlink()
    elif defect == "duplicate_outcome":
        conn.execute(
            """
            INSERT INTO odds (snapshot_id, game_id, book_id, market, outcome, price_american, line)
            VALUES ('close', 'game', 1, 'totals', 'over', -130, 220.5)
            """
        )
    elif defect == "opposite_price_changed":
        conn.execute(
            "UPDATE odds SET price_american = 125 WHERE snapshot_id = 'close' AND outcome = 'Under'"
        )
    elif defect == "clock_disagrees":
        conn.execute(
            "UPDATE odds_snapshot_game_starts SET observed_start_utc = ? WHERE snapshot_id = 'close'",
            ((START + timedelta(minutes=1)).isoformat(),),
        )
    elif defect == "score_corrected":
        conn.execute("UPDATE game_results SET home_score = 113")
    else:
        minutes = 20 if defect == "newer_close" else 30
        _quote(conn, raw_dir, "conflict", START - timedelta(minutes=minutes), 220.5, (-120, 100))

    result = _audit(ledger)
    assert result["graded_decisions"] == result["unverified_grade_linked_closes"] == 1
    assert result["verified_same_line_closes"] == result["verified_changed_line_closes"] == 0
    assert result["mean_same_line_no_vig_close_ev_conditional_non_push"] is None


def test_legacy_close_uses_original_pair_when_provider_clock_row_is_missing(ledger):
    _grade(ledger)
    conn, _, _ = ledger
    conn.execute("DELETE FROM odds_snapshot_game_starts")
    assert _audit(ledger)["verified_same_line_closes"] == 1


def test_no_grade_linked_close_is_reported_separately(ledger):
    _grade(ledger)
    conn, _, _ = ledger
    conn.execute("UPDATE paper_settlement_history SET close_snapshot_id = NULL")
    result = _audit(ledger)
    assert result["graded_decisions"] == result["missing_grade_linked_closes"] == 1
    assert result["unverified_grade_linked_closes"] == result["verified_same_line_closes"] == 0


def test_revised_grade_cannot_replace_original_cutoff_evidence(ledger):
    conn, _, raw_dir = ledger
    _grade(ledger)
    _quote(conn, raw_dir, "later", START - timedelta(minutes=20), 220.5, (-120, 100))
    observed_at = NOW + timedelta(hours=1)
    assert settle_open_trials(conn, observed_at) == 1
    assert _audit(ledger, now=observed_at)["unverified_grade_linked_closes"] == 1


def test_read_only_cli_exposes_optional_diagnostics(ledger, monkeypatch, capsys):
    conn, _, _ = ledger
    _grade(ledger)
    conn.commit()
    db = conn.execute("PRAGMA database_list").fetchone()["file"]

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return NOW.astimezone(tz)

    monkeypatch.setattr(paper_ledger, "datetime", Clock)
    args = ["paper_ledger", "report", "--db", db]
    monkeypatch.setattr(sys, "argv", args)
    paper_ledger.main()
    default = json.loads(capsys.readouterr().out)["trials"][0]

    monkeypatch.setattr(sys, "argv", [*args, "--price-diagnostics"])
    paper_ledger.main()
    augmented = json.loads(capsys.readouterr().out)["trials"][0]
    assert augmented.pop("closing_price_diagnostics")["verified_same_line_closes"] == 1
    assert augmented == default
