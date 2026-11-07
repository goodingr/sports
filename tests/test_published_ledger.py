from __future__ import annotations

import sqlite3
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

import src.predict.published_ledger as ledger
from src.db.core import initialize
from src.predict.published_ledger import (
    checkout_has_recent_delivery,
    load_active_release,
    publish_release,
    published_record,
)


@pytest.fixture
def warehouse(tmp_path: Path):
    path = tmp_path / "published.db"
    initialize(path)
    start = datetime.now(timezone.utc) + timedelta(hours=2)
    with sqlite3.connect(path) as conn:
        conn.execute("INSERT INTO sports (sport_id, name, league, default_market) VALUES (1,'NBA','NBA','totals')")
        conn.execute("INSERT INTO teams (team_id,sport_id,code,name) VALUES (1,1,'H','Home'),(2,1,'A','Away')")
        conn.execute(
            "INSERT INTO games (game_id,sport_id,home_team_id,away_team_id,start_time_utc) VALUES ('g',1,1,2,?)",
            (start.isoformat(),),
        )
        conn.execute("INSERT INTO books (book_id,name) VALUES (1,'DraftKings')")
        conn.execute(
            "INSERT INTO odds_snapshots VALUES ('quote',?,1,'the-odds-api',NULL)",
            ((start - timedelta(hours=2, minutes=1)).isoformat(),),
        )
        conn.executemany(
            """
            INSERT INTO odds (snapshot_id,game_id,book_id,market,outcome,price_american,line)
            VALUES ('quote','g',1,'totals',?,100,220)
            """,
            [("Over",), ("Under",)],
        )
    return path, start


def release(start):
    return {
        "generated_at": (start - timedelta(hours=2)).isoformat(),
        "passing_approved_rule_ids": ["validated"],
        "publishable_profitable_list_exists": True,
        "bets": [{
            "rule_id": "validated", "game_id": "g", "league": "NBA", "market": "totals",
            "home_team": "Home", "away_team": "Away", "start_time_utc": start.isoformat(),
            "side": "over", "total_line": 220.0, "moneyline": 100, "book": "DraftKings",
            "edge": 0.1, "quality_summary": {"passes_launch_gate": True},
            "model_probability": 0.6, "market_probability": 0.5,
            "quote_snapshot_id": "quote", "quote_book_id": 1,
            "predicted_at": (start - timedelta(hours=2)).isoformat(),
        }],
    }


def publish_at_evaluation(payload, path):
    return publish_release(payload, path, commit_at=datetime.fromisoformat(payload["generated_at"]))


@pytest.mark.parametrize(
    ("release_change", "hours_after_refresh", "game_status", "expected"),
    [
        ({}, 0, "scheduled", True),
        ({}, 31, "scheduled", False),
        ({}, 0, "voided", False),
        ({"reason": "data_quality_failed"}, 0, "scheduled", False),
        ({"passing_approved_rule_ids": []}, 0, "scheduled", False),
        ({"skipped_passing_rule_ids": ["validated"]}, 0, "scheduled", False),
        ({"passing_approved_rule_ids": ["other"], "approved_rule_ids": ["other"]}, 0, "scheduled", False),
    ],
)
def test_checkout_requires_recent_delivery_and_current_passing_release(
    warehouse, release_change, hours_after_refresh, game_status, expected
):
    path, start = warehouse
    original = release(start)
    original["approved_rule_ids"] = ["validated"]
    original["bets"][0]["quality_summary"]["evidence_source"] = "prospective_paper"
    issued = publish_at_evaluation(original, path)
    assert len(issued["bets"]) == 1
    assert checkout_has_recent_delivery(path, now=datetime.fromisoformat(issued["published_at"]))
    refresh_at = start + timedelta(hours=1)
    refresh = {
        "generated_at": refresh_at.isoformat(),
        "approved_rule_ids": ["validated"],
        "passing_approved_rule_ids": ["validated"],
        "reason": "no_current_bets_matching_passing_rules",
        "bets": [],
        **release_change,
    }
    publish_at_evaluation(refresh, path)
    assert load_active_release(path)["bets"] == []
    with sqlite3.connect(path) as conn:
        conn.execute("UPDATE games SET status = ?", (game_status,))
    assert checkout_has_recent_delivery(path, now=refresh_at + timedelta(hours=hours_after_refresh)) is expected
    later = refresh_at + timedelta(days=8)
    publish_at_evaluation({**refresh, "generated_at": later.isoformat()}, path)
    assert not checkout_has_recent_delivery(path, now=later)


def test_checkout_cannot_open_with_only_trial_approval_and_no_issued_pick(warehouse):
    path, start = warehouse
    observed = start - timedelta(hours=1)
    publish_at_evaluation({
        "generated_at": observed.isoformat(),
        "approved_rule_ids": ["validated"],
        "passing_approved_rule_ids": ["validated"],
        "reason": "no_current_bets_matching_passing_rules",
        "bets": [],
    }, path)
    assert not checkout_has_recent_delivery(path, now=observed)


def test_published_quote_is_immutable_and_retries_do_not_add_bets(warehouse):
    path, start = warehouse
    original = release(start)
    assert len(publish_release(original, path)["bets"]) == 1
    assert len(publish_release(original, path)["bets"]) == 1
    revised = deepcopy(original)
    revised["bets"][0]["total_line"] = 218.5
    assert publish_release(revised, path)["bets"] == []
    with sqlite3.connect(path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM published_picks").fetchone() == (1,)
    assert load_active_release(path)["bets"] == []
    assert published_record(path)["data"] == []


@pytest.mark.parametrize(
    ("home", "away", "status", "outcome", "profit"),
    [(115, 110, "final", "win", 1.0), (100, 110, "final", "loss", -1.0),
     (112, 108, "final", "push", 0.0), (None, None, "voided", "void", 0.0)],
)
def test_grading_uses_the_original_quote_and_includes_all_outcomes(
    warehouse, home, away, status, outcome, profit
):
    path, start = warehouse
    publish_release(release(start), path)
    with sqlite3.connect(path) as conn:
        conn.execute("UPDATE games SET status = ?", (status,))
        conn.execute(
            "INSERT INTO game_results (game_id,home_score,away_score) VALUES ('g',?,?)",
            (home, away),
        )
    closed = {"generated_at": (start + timedelta(hours=6)).isoformat(), "bets": []}
    publish_at_evaluation(closed, path)
    publish_at_evaluation(closed, path)
    record = published_record(path)
    assert record["total"] == 1
    assert record["data"][0]["total_line"] == 220.0
    assert record["data"][0]["outcome"] == outcome
    assert record["profit_units"] == profit
    assert record["graded_picks"] == int(outcome != "void")
    assert load_active_release(path)["bets"] == []


def test_concurrent_publications_record_one_pick(warehouse):
    path, start = warehouse
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: publish_release(release(start), path), range(2)))
    assert all(len(result["bets"]) == 1 for result in results)
    with sqlite3.connect(path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM published_picks").fetchone() == (1,)


def test_voided_games_disappear_immediately_before_the_next_publish(warehouse):
    path, start = warehouse
    publish_release(release(start), path)
    assert len(load_active_release(path)["bets"]) == 1
    with sqlite3.connect(path) as conn:
        conn.execute("UPDATE games SET status = 'voided'")
    assert load_active_release(path)["bets"] == []


def test_score_corrections_update_returns_and_keep_prior_grading(warehouse):
    path, start = warehouse
    publish_release(release(start), path)
    closed = {"generated_at": (start + timedelta(hours=6)).isoformat(), "bets": []}
    with sqlite3.connect(path) as conn:
        conn.execute("UPDATE games SET status = 'final'")
        conn.execute("INSERT INTO game_results (game_id,home_score,away_score) VALUES ('g',115,110)")
    publish_at_evaluation(closed, path)
    with sqlite3.connect(path) as conn:
        conn.execute("UPDATE game_results SET home_score = 100")
    publish_at_evaluation(closed, path)
    publish_at_evaluation(closed, path)

    record = published_record(path)
    assert record["data"][0]["outcome"] == "loss"
    assert record["profit_units"] == -1.0
    assert record["grading_corrections"] == 1
    with sqlite3.connect(path) as conn:
        assert conn.execute(
            "SELECT outcome FROM published_settlement_history ORDER BY revision_id"
        ).fetchall() == [("win",), ("loss",)]


def test_withdrawn_final_results_return_to_pending_with_the_prior_grade_retained(warehouse):
    path, start = warehouse
    publish_release(release(start), path)
    closed = {"generated_at": (start + timedelta(hours=6)).isoformat(), "bets": []}
    with sqlite3.connect(path) as conn:
        conn.execute("UPDATE games SET status = 'final'")
        conn.execute("INSERT INTO game_results (game_id,home_score,away_score) VALUES ('g',115,110)")
    publish_at_evaluation(closed, path)
    with sqlite3.connect(path) as conn:
        conn.execute("UPDATE games SET status = 'in_progress'")
        conn.execute("UPDATE game_results SET home_score = NULL, away_score = NULL")
    publish_at_evaluation(closed, path)
    publish_at_evaluation(closed, path)

    record = published_record(path)
    assert record["total"] == record["wins"] == record["profit_units"] == 0
    assert record["pending_picks"] == 1
    assert record["grading_corrections"] == 1
    with sqlite3.connect(path) as conn:
        assert conn.execute(
            "SELECT outcome FROM published_settlement_history ORDER BY revision_id"
        ).fetchall() == [("win",), ("pending",)]


def test_quote_provenance_and_half_point_line_required(warehouse):
    path, start = warehouse
    missing = release(start)
    missing["bets"][0].pop("quote_snapshot_id")
    assert publish_release(missing, path)["bets"] == []
    quarter_line = release(start)
    quarter_line["bets"][0]["total_line"] = 220.25
    assert publish_release(quarter_line, path)["bets"] == []
    with sqlite3.connect(path) as conn:
        conn.execute("DELETE FROM odds")
    assert publish_release(release(start), path)["bets"] == []
    assert published_record(path)["published_picks"] == 0


def test_alias_fixture_is_not_published_twice(warehouse):
    path, start = warehouse
    publish_release(release(start), path)
    with sqlite3.connect(path) as conn:
        conn.execute(
            "INSERT INTO games (game_id,sport_id,home_team_id,away_team_id,start_time_utc) VALUES ('alias',1,1,2,?)",
            (start.isoformat(),),
        )
        conn.execute(
            """
            INSERT INTO odds (snapshot_id,game_id,book_id,market,outcome,price_american,line)
            SELECT snapshot_id,'alias',book_id,market,outcome,price_american,line FROM odds
            """
        )
    alias = release(start)
    alias["bets"][0]["game_id"] = "alias"
    assert publish_release(alias, path)["bets"] == []
    assert published_record(path)["published_picks"] == 1


def test_invalid_nba_final_and_future_corrected_start_do_not_grade(warehouse):
    path, start = warehouse
    publish_release(release(start), path)
    with sqlite3.connect(path) as conn:
        conn.execute("UPDATE games SET status='final'")
        conn.execute("INSERT INTO game_results (game_id,home_score,away_score) VALUES ('g',0,0)")
    publish_at_evaluation(
        {"generated_at": (start + timedelta(minutes=1)).isoformat(), "bets": []}, path
    )
    assert published_record(path)["pending_picks"] == 1
    with sqlite3.connect(path) as conn:
        conn.execute("UPDATE game_results SET home_score=115, away_score=110")
        conn.execute("UPDATE games SET start_time_utc=?", ((start + timedelta(minutes=5)).isoformat(),))
    publish_at_evaluation(
        {"generated_at": (start + timedelta(minutes=2)).isoformat(), "bets": []}, path
    )
    assert published_record(path)["pending_picks"] == 1


def test_two_way_moneyline_tie_is_a_push(warehouse):
    path, start = warehouse
    with sqlite3.connect(path) as conn:
        conn.execute("UPDATE sports SET league='NFL'")
        conn.executemany(
            """
            INSERT INTO odds (snapshot_id,game_id,book_id,market,outcome,price_american)
            VALUES ('quote','g',1,'h2h',?,100)
            """,
            [("home",), ("away",)],
        )
    moneyline = release(start)
    moneyline["bets"][0].update(market="moneyline", side="home", total_line=None)
    assert len(publish_release(moneyline, path)["bets"]) == 1
    with sqlite3.connect(path) as conn:
        conn.execute("UPDATE games SET status='final'")
        conn.execute("INSERT INTO game_results (game_id,home_score,away_score) VALUES ('g',21,21)")
    publish_at_evaluation(
        {"generated_at": (start + timedelta(hours=6)).isoformat(), "bets": []}, path
    )
    assert published_record(path)["data"][0]["outcome"] == "push"


@pytest.mark.parametrize("revocation_delay_seconds", [0, 30])
def test_earlier_or_tied_evaluation_cannot_restore_a_revocation(warehouse, revocation_delay_seconds):
    path, start = warehouse
    candidate = release(start)
    evaluation = datetime.fromisoformat(candidate["generated_at"])
    revoked_at = evaluation + timedelta(seconds=revocation_delay_seconds)
    revoked = publish_at_evaluation(
        {
            "generated_at": revoked_at.isoformat(), "bets": [],
            "passing_approved_rule_ids": [], "reason": "data_quality_failed",
        },
        path,
    )
    result = publish_release(candidate, path, commit_at=revoked_at + timedelta(seconds=1))
    assert result["superseded"] is True
    assert result["release_id"] == revoked["release_id"]
    assert result["reason"] == "data_quality_failed"
    assert load_active_release(path)["bets"] == []
    with sqlite3.connect(path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM published_releases").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM published_picks").fetchone()[0] == 0


@pytest.mark.parametrize("delay", ["fresh", "expired", "started"])
def test_production_publication_checks_and_records_transaction_time(warehouse, monkeypatch, delay):
    path, start = warehouse
    candidate = release(start)
    evaluated_at = start - timedelta(minutes=1) if delay == "started" else start - timedelta(hours=2)
    candidate["generated_at"] = evaluated_at.isoformat()
    candidate["bets"][0]["predicted_at"] = evaluated_at.isoformat()
    with sqlite3.connect(path) as conn:
        conn.execute(
            "UPDATE odds_snapshots SET fetched_at_utc = ?",
            ((evaluated_at - timedelta(minutes=1)).isoformat(),),
        )
    committed_at = evaluated_at + (
        timedelta(minutes=16) if delay == "expired" else timedelta(minutes=2)
    )

    class PublicationClock(datetime):
        @classmethod
        def now(cls, tz=None):
            return committed_at.astimezone(tz)

    monkeypatch.setattr(ledger, "datetime", PublicationClock)
    result = publish_release(candidate, path)
    assert result["generated_at"] == evaluated_at.isoformat()
    assert result["published_at"] == committed_at.isoformat()
    if delay == "fresh":
        assert result["bets"][0]["published_at"] == committed_at.isoformat()
    else:
        assert result["bets"] == []
        assert published_record(path)["published_picks"] == 0
