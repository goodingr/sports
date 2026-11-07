import sqlite3
import sys
from datetime import datetime, timedelta, timezone

from scripts.nba_quote_timing import timing_report
from src.data import ingest_odds


def test_timing_links_only_the_exact_receipt_and_bounded_release():
    fetched = datetime(2026, 11, 7, 23, tzinfo=timezone.utc)

    def stamp(seconds):
        return (fetched + timedelta(seconds=seconds)).isoformat()

    with sqlite3.connect(":memory:") as conn:
        conn.executescript("""
            CREATE TABLE sports (sport_id, league);
            CREATE TABLE odds_snapshots (snapshot_id, fetched_at_utc, sport_id, source, raw_path);
            CREATE TABLE paper_capture_runs (run_id, observed_at);
            CREATE TABLE paper_fixture_observations (run_id, snapshot_id, outcome);
            CREATE TABLE paper_decisions (snapshot_id, odds_fetched_at, captured_at);
            CREATE TABLE published_releases (release_id, published_at, payload);
            CREATE TABLE published_picks (first_release_id, payload);
            INSERT INTO sports VALUES (1, 'NBA');
            INSERT INTO paper_fixture_observations VALUES
                (1, 'fresh', 'recorded'), (1, 'fresh', 'below_edge_threshold'),
                (2, 'unrelated', 'recorded');
            INSERT INTO published_picks VALUES
                (1, '{"quote_snapshot_id":"fresh"}'), (2, '{"quote_snapshot_id":"unrelated"}');
        """)
        conn.executemany(
            "INSERT INTO odds_snapshots VALUES (?, ?, 1, 'the-odds-api', ?)",
            [("fresh", stamp(0), "receipt.json"), ("unrelated", stamp(20), "other.json")],
        )
        conn.executemany("INSERT INTO paper_capture_runs VALUES (?, ?)", [(1, stamp(42)), (2, stamp(44))])
        conn.executemany(
            "INSERT INTO paper_decisions VALUES (?, ?, ?)",
            [("fresh", stamp(0), stamp(45)), ("unrelated", stamp(20), stamp(46))],
        )
        conn.executemany(
            "INSERT INTO published_releases VALUES (?, ?, json_object('generated_at', ?))",
            [(1, stamp(604), stamp(599)), (2, stamp(605.25), stamp(603))],
        )
        gate_window = {
            "paid_started": fetched + timedelta(seconds=602),
            "paid_finished": fetched + timedelta(seconds=606),
        }

        report = timing_report(conn, "receipt.json", **gate_window)
        assert report == {
            "status": "available",
            "fetched_at_utc": stamp(0),
            "paper_attempt_ages_seconds": [42.0],
            "paper_decision_ages_seconds": [45.0],
            "paper_fixture_outcomes": {"recorded": 1, "below_edge_threshold": 1},
            "paid": {
                "status": "release_observed",
                "gate_started_age_seconds": 602.0,
                "gate_finished_age_seconds": 606.0,
                "release_age_seconds": 605.25,
                "new_picks_from_receipt": 0,
            },
        }
        conn.execute("INSERT INTO published_picks VALUES (2, '{\"quote_snapshot_id\":\"fresh\"}')")
        assert timing_report(conn, "receipt.json", **gate_window)["paid"]["new_picks_from_receipt"] == 1
        conn.execute(
            "INSERT INTO published_releases VALUES (3, ?, json_object('generated_at', ?))",
            (stamp(605.5), stamp(604)),
        )
        assert timing_report(conn, "receipt.json", **gate_window)["paid"]["status"] == "ambiguous_release"

        assert timing_report(conn, "missing.json")["reason"] == "receipt_not_loaded"
        conn.execute(
            "INSERT INTO odds_snapshots VALUES ('duplicate', ?, 1, 'the-odds-api', 'receipt.json')",
            (stamp(0),),
        )
        assert timing_report(conn, "receipt.json")["reason"] == "ambiguous_receipt"


def test_ingest_receipt_pointer_is_opt_in_and_write_failure_is_diagnostic(
    tmp_path, monkeypatch, caplog
):
    receipt = tmp_path / "exact fresh receipt.json"
    pointer = tmp_path / "receipt-pointer"

    def run(_dotenv, **kwargs):
        assert kwargs["sport_key"] == "basketball_nba"
        assert kwargs["require_fresh"] is True
        return receipt

    monkeypatch.setattr(ingest_odds, "run", run)
    args = ["ingest_odds", "--league", "NBA", "--require-fresh"]
    monkeypatch.setattr(sys, "argv", args)
    ingest_odds.main()
    assert not pointer.exists()

    monkeypatch.setattr(sys, "argv", [*args, "--receipt-output", str(pointer)])
    ingest_odds.main()
    assert pointer.read_text(encoding="utf-8") == str(receipt)

    monkeypatch.setattr(sys, "argv", [*args, "--receipt-output", str(tmp_path / "absent" / "pointer")])
    ingest_odds.main()
    assert "Could not write the fresh odds receipt timing pointer" in caplog.text
    assert str(tmp_path) not in caplog.text
