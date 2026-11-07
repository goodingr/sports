import hashlib
import json
import sqlite3
from pathlib import Path

import pytest

from src.data.replay_local_odds import replay_plan


@pytest.fixture
def archive_plan(tmp_path):
    db_path = tmp_path / "sports.db"
    with sqlite3.connect(db_path) as conn:
        conn.executescript(Path("src/db/schema.sql").read_text(encoding="utf-8"))
        conn.execute("INSERT INTO sports (sport_id, name, league, default_market) VALUES (1, 'Hockey', 'NHL', 'h2h')")
        conn.execute("INSERT INTO teams (team_id, sport_id, name, code) VALUES (1, 1, 'Nashville Predators', 'NSH')")
        conn.execute("INSERT INTO teams (team_id, sport_id, name, code) VALUES (2, 1, 'Florida Panthers', 'FLA')")
        conn.execute(
            "INSERT INTO games (game_id, sport_id, start_time_utc, status, home_team_id, away_team_id) "
            "VALUES ('fixture', 1, '2025-11-25T01:09:00+00:00', 'final', 1, 2)"
        )
        conn.execute("INSERT INTO game_results (game_id, home_score, away_score) VALUES ('fixture', 3, 2)")
    raw_path = tmp_path / "odds.json"
    raw_path.write_text(json.dumps({
        "fetched_at": "2025-11-25T00:30:00+00:00",
        "results": [{
            "id": "provider-id", "commence_time": "2025-11-25T01:00:00Z",
            "home_team": "Nashville Predators", "away_team": "Florida Panthers",
            "bookmakers": [{"title": "DraftKings", "markets": [{
                "key": "totals", "outcomes": [
                    {"name": "Over", "point": 5.5, "price": -110},
                    {"name": "Under", "point": 5.5, "price": -110},
                ],
            }]}],
        }],
    }), encoding="utf-8")
    plan = {
        "fixtures": [{
            "game_id": "fixture", "sport_id": 1, "league": "NHL", "status": "final",
            "start_time_utc": "2025-11-25T01:09:00+00:00", "home": "Nashville Predators",
            "away": "Florida Panthers", "home_score": 3, "away_score": 2,
        }],
        "snapshots": [{
            "league": "NHL", "sport_id": 1, "source": "the-odds-api", "region": "us",
            "fetched_at_utc": "2025-11-25T00:30:00+00:00", "raw_path": str(raw_path),
            "raw_sha256": hashlib.sha256(raw_path.read_bytes()).hexdigest(), "snapshot_id": None,
            "rows": [{
                "game_id": "fixture", "book": "DraftKings", "market": "totals",
                "outcome": side, "line": 5.5, "price_american": -110,
            } for side in ("Over", "Under")],
        }],
    }
    plan_path = tmp_path / "plan.json"
    plan_path.write_text(json.dumps(plan), encoding="utf-8")
    return db_path, plan_path, raw_path, plan


def test_replay_is_additive_idempotent_and_preserves_canonical_results(archive_plan, tmp_path):
    db_path, plan_path, raw_path, _ = archive_plan
    preview = replay_plan(plan_path, db_path=db_path)
    assert preview["new_quote_rows"] == 2
    with sqlite3.connect(db_path) as conn:
        assert conn.execute("SELECT count(*) FROM odds").fetchone()[0] == 0
    result = replay_plan(plan_path, db_path=db_path, write=True, manifest_path=tmp_path / "first.json")
    assert result["protected_hashes_before"] == result["protected_hashes_after"]
    with sqlite3.connect(db_path) as conn:
        assert conn.execute("SELECT source, fetched_at_utc, raw_path FROM odds_snapshots").fetchone() == (
            "the-odds-api", "2025-11-25T00:30:00+00:00", str(raw_path)
        )
        assert conn.execute("SELECT count(*) FROM odds").fetchone()[0] == 2
    replay = replay_plan(plan_path, db_path=db_path, write=True, manifest_path=tmp_path / "second.json")
    assert replay["new_quote_rows"] == 0
    assert replay["new_snapshots"] == []


def test_replay_rejects_changed_source_file(archive_plan, tmp_path):
    db_path, plan_path, raw_path, _ = archive_plan
    raw_path.write_text("{}", encoding="utf-8")
    with pytest.raises(ValueError, match="Archive file changed"):
        replay_plan(plan_path, db_path=db_path, write=True, manifest_path=tmp_path / "manifest.json")
    with sqlite3.connect(db_path) as conn:
        assert conn.execute("SELECT count(*) FROM odds_snapshots").fetchone()[0] == 0


def test_replay_preserves_conflicting_existing_quote(archive_plan, tmp_path):
    db_path, plan_path, raw_path, plan = archive_plan
    with sqlite3.connect(db_path) as conn:
        conn.execute("INSERT INTO books (book_id, name) VALUES (1, 'DraftKings')")
        conn.execute(
            "INSERT INTO odds_snapshots (snapshot_id, sport_id, source, fetched_at_utc, raw_path) "
            "VALUES ('existing', 1, 'the-odds-api', '2025-11-25T00:30:00+00:00', ?)", (str(raw_path),)
        )
        conn.execute(
            "INSERT INTO odds (snapshot_id, game_id, book_id, market, outcome, price_american, line) "
            "VALUES ('existing', 'fixture', 1, 'totals', 'Over', -105, 5.5)"
        )
    plan["snapshots"][0]["snapshot_id"] = "existing"
    plan_path.write_text(json.dumps(plan), encoding="utf-8")
    with pytest.raises(ValueError, match="existing quote conflicts"):
        replay_plan(plan_path, db_path=db_path, write=True, manifest_path=tmp_path / "manifest.json")
    with sqlite3.connect(db_path) as conn:
        assert conn.execute("SELECT outcome, price_american FROM odds").fetchall() == [("Over", -105)]


def test_replay_rejects_ambiguous_settled_fixture(archive_plan):
    db_path, plan_path, _, _ = archive_plan
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO games (game_id, sport_id, start_time_utc, status, home_team_id, away_team_id) "
            "VALUES ('duplicate', 1, '2025-11-25T01:00:00+00:00', 'final', 1, 2)"
        )
        conn.execute("INSERT INTO game_results (game_id, home_score, away_score) VALUES ('duplicate', 3, 2)")
    with pytest.raises(ValueError, match="ambiguous"):
        replay_plan(plan_path, db_path=db_path)


def test_replay_rejects_poststart_or_forged_quote(archive_plan):
    db_path, plan_path, raw_path, plan = archive_plan
    plan["snapshots"][0]["rows"][0]["price_american"] = 200
    plan_path.write_text(json.dumps(plan), encoding="utf-8")
    with pytest.raises(ValueError, match="Planned price"):
        replay_plan(plan_path, db_path=db_path)

    data = json.loads(raw_path.read_text())
    data["fetched_at"] = "2025-11-25T01:01:00+00:00"
    raw_path.write_text(json.dumps(data), encoding="utf-8")
    plan["snapshots"][0].update({
        "fetched_at_utc": data["fetched_at"], "raw_sha256": hashlib.sha256(raw_path.read_bytes()).hexdigest()
    })
    plan_path.write_text(json.dumps(plan), encoding="utf-8")
    with pytest.raises(ValueError, match="not observed before"):
        replay_plan(plan_path, db_path=db_path)
