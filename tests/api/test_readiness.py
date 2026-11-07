from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from src.api import readiness
from src.api.settings import APISettings

NOW = datetime(2026, 11, 7, 10, 0, 0, 123456, tzinfo=timezone.utc)


class FrozenDateTime(datetime):
    @classmethod
    def now(cls, tz=None):
        return NOW.astimezone(tz)


def _create_ready_db(db_path, *, timestamp=None):
    timestamp = timestamp or datetime.now(timezone.utc).isoformat()
    with sqlite3.connect(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE sports (sport_id INTEGER PRIMARY KEY, league TEXT NOT NULL);
            CREATE TABLE teams (team_id INTEGER PRIMARY KEY);
            CREATE TABLE games (game_id TEXT PRIMARY KEY, sport_id INTEGER NOT NULL);
            CREATE TABLE books (book_id INTEGER PRIMARY KEY);
            CREATE TABLE odds_snapshots (
                snapshot_id TEXT PRIMARY KEY,
                fetched_at_utc TEXT NOT NULL,
                sport_id INTEGER NOT NULL
            );
            CREATE TABLE odds (
                snapshot_id TEXT, game_id TEXT, book_id INTEGER, market TEXT, outcome TEXT,
                price_american REAL
            );
            CREATE TABLE predictions (
                prediction_id INTEGER PRIMARY KEY,
                game_id TEXT NOT NULL,
                model_type TEXT NOT NULL,
                predicted_at TEXT NOT NULL,
                total_line REAL,
                over_prob REAL,
                under_prob REAL,
                over_edge REAL,
                under_edge REAL,
                predicted_total_points REAL
            );
            CREATE TABLE models (
                model_id TEXT PRIMARY KEY,
                league TEXT,
                artifact_path TEXT
            );
            """
        )
        conn.executescript(
            """
            INSERT INTO sports (sport_id, league) VALUES (1, 'NBA');
            INSERT INTO games (game_id, sport_id) VALUES ('game-1', 1);
            INSERT INTO books (book_id) VALUES (1);
            INSERT INTO models (model_id, league, artifact_path) VALUES ('model-1', 'NBA', '');
            """
        )
        conn.execute(
            """
            INSERT INTO odds_snapshots (snapshot_id, fetched_at_utc, sport_id)
            VALUES ('snapshot-1', ?, 1);
            """,
            (timestamp,),
        )
        conn.execute("INSERT INTO odds VALUES ('snapshot-1', 'game-1', 1, 'totals', 'Over', -110)")
        conn.execute(
            """
            INSERT INTO predictions (
                game_id, model_type, predicted_at, total_line, over_prob, under_prob,
                over_edge, under_edge, predicted_total_points
            )
            VALUES ('game-1', 'ensemble', ?, 220.5, 0.55, 0.45, 0.04, -0.04, 224.0);
            """,
            (timestamp,),
        )


@pytest.fixture
def ready_backend(monkeypatch, tmp_path):
    db_path = tmp_path / "betting.db"
    models_dir = tmp_path / "models"
    models_dir.mkdir()
    (models_dir / "nba_ensemble_calibrated_moneyline.pkl").write_bytes(b"model")
    _create_ready_db(db_path, timestamp=NOW.isoformat())

    settings = APISettings(
        db_path=db_path,
        models_dir=models_dir,
        release_leagues=("NBA",),
        odds_freshness_minutes=60,
        predictions_freshness_minutes=60,
        min_disk_free_mb=0,
        cors_origins=(),
    )
    monkeypatch.setattr(readiness, "get_settings", lambda: settings)
    monkeypatch.setattr(readiness, "datetime", FrozenDateTime)
    return settings


def test_ready_endpoint_reports_ready_when_backend_dependencies_are_present(
    api_client, ready_backend
):
    response = api_client.get("/ready")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ready"
    assert body["checks"]["db"]["ok"] is True
    assert body["checks"]["schema"]["ok"] is True
    assert body["checks"]["models"]["leagues"]["NBA"]["ok"] is True


def test_ready_rejects_recent_header_without_any_odds(api_client, ready_backend):
    with sqlite3.connect(ready_backend.db_path) as conn:
        conn.execute("DELETE FROM odds")

    response = api_client.get("/ready")

    assert response.status_code == 503
    checks = response.json()["checks"]
    assert checks["odds_freshness"]["leagues"]["NBA"] == {
        "ok": False,
        "latest": None,
        "age_minutes": None,
    }
    assert checks["odds_freshness"]["missing_leagues"] == []
    assert checks["predictions_freshness"]["ok"] is True


@pytest.mark.parametrize("threshold", [60, 0])
def test_ready_rejects_future_only_evidence(api_client, ready_backend, threshold):
    ready_backend.odds_freshness_minutes = threshold
    ready_backend.predictions_freshness_minutes = threshold
    future = (NOW + timedelta(microseconds=1)).isoformat()
    with sqlite3.connect(ready_backend.db_path) as conn:
        conn.execute("UPDATE odds_snapshots SET fetched_at_utc = ?", (future,))
        conn.execute("UPDATE predictions SET predicted_at = ?", (future,))

    response = api_client.get("/ready")

    assert response.status_code == 503
    checks = response.json()["checks"]
    for name in ("odds_freshness", "predictions_freshness"):
        assert checks[name]["leagues"]["NBA"] == {"ok": False, "latest": None, "age_minutes": None}


def test_ready_uses_latest_eligible_activity_when_newer_headers_do_not_qualify(
    api_client, ready_backend
):
    latest = NOW - timedelta(minutes=2, microseconds=1)
    offset_form = latest.astimezone(timezone(timedelta(hours=-7))).isoformat()
    earlier = (NOW - timedelta(minutes=8)).replace(tzinfo=None).isoformat(sep=" ")
    future = (NOW + timedelta(microseconds=1)).isoformat()
    newer = (NOW - timedelta(minutes=1)).isoformat()
    ineligible_times = [
        ("future", future),
        ("corrupt", "not a timestamp"),
        ("date-only", "2026-11-07"),
        ("overflow", "0001-01-01T00:00:00+01:00"),
    ]
    with sqlite3.connect(ready_backend.db_path) as conn:
        conn.execute("UPDATE odds_snapshots SET fetched_at_utc = ?", (earlier,))
        conn.execute("UPDATE predictions SET predicted_at = ?", (earlier,))
        conn.executemany(
            "INSERT INTO odds_snapshots VALUES (?, ?, 1)",
            [("valid", offset_form), ("empty", newer), ("unpriced", newer), *ineligible_times],
        )
        conn.executemany(
            "INSERT INTO odds VALUES (?, 'game-1', 1, 'totals', 'Over', ?)",
            [("valid", 100), ("unpriced", None), *((key, -110) for key, _ in ineligible_times)],
        )
        conn.executemany(
            "INSERT INTO odds VALUES ('unpriced', 'game-1', 1, 'totals', ?, ?)",
            [("Under", 99), ("bad", "not a price"), ("inf", float("inf"))],
        )
        conn.executemany(
            "INSERT INTO predictions (game_id, model_type, predicted_at) VALUES ('game-1', ?, ?)",
            [("valid", offset_form), *ineligible_times],
        )

    response = api_client.get("/ready")

    assert response.status_code == 200
    checks = response.json()["checks"]
    for name in ("odds_freshness", "predictions_freshness"):
        assert checks[name]["leagues"]["NBA"] == {
            "ok": True,
            "latest": latest.isoformat(),
            "age_minutes": 2.0,
        }

    with sqlite3.connect(ready_backend.db_path) as conn:
        conn.execute("DELETE FROM odds WHERE snapshot_id = 'valid'")
        conn.execute("DELETE FROM predictions WHERE model_type = 'valid'")
    response = api_client.get("/ready")

    assert response.status_code == 200
    checks = response.json()["checks"]
    naive_utc = datetime.fromisoformat(earlier).replace(tzinfo=timezone.utc).isoformat()
    for name in ("odds_freshness", "predictions_freshness"):
        assert checks[name]["leagues"]["NBA"]["latest"] == naive_utc


def test_health_endpoint_is_dependency_free(api_client, monkeypatch):
    """`/health` is the liveness probe — it must not depend on the DB.

    docker-compose's deploy gate runs `/ready` (which exercises the DB and
    schema), but external orchestrators may want a cheap probe that stays
    green during transient DB unavailability so the container is not
    restart-flapped while the worker is mid-vacuum. This test guards that
    contract by pointing the readiness settings at a missing DB and proving
    `/health` still returns 200 even though `/ready` would not.
    """
    settings = APISettings(
        db_path=Path("/tmp/this/path/should/never/exist/betting.db"),
        models_dir=Path("/tmp/no-models"),
        release_leagues=("NBA",),
        cors_origins=(),
    )
    monkeypatch.setattr(readiness, "get_settings", lambda: settings)

    response = api_client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "healthy"}


def test_readiness_rejects_an_interrupted_restore(tmp_path):
    db_path = tmp_path / "betting.db"
    _create_ready_db(db_path)
    models_dir = tmp_path / "models"
    models_dir.mkdir()
    (models_dir / "nba_ensemble.pkl").write_bytes(b"model")
    marker = models_dir / ".restore-interrupted"
    marker.mkdir()
    settings = APISettings(
        db_path=db_path,
        models_dir=models_dir,
        release_leagues=("NBA",),
        min_disk_free_mb=0,
        cors_origins=(),
    )
    result = readiness.run_readiness_checks(settings)
    assert result["status"] == "not_ready"
    assert result["checks"]["restore"]["ok"] is False
    marker.rmdir()
    assert readiness.run_readiness_checks(settings)["status"] == "ready"


def test_ready_returns_503_when_database_unreachable(api_client, monkeypatch, tmp_path):
    """`/ready` is the staging gate — it must reject when the DB is gone."""
    settings = APISettings(
        db_path=tmp_path / "missing.db",
        models_dir=tmp_path / "models",
        release_leagues=("NBA",),
        cors_origins=(),
    )
    monkeypatch.setattr(readiness, "get_settings", lambda: settings)

    response = api_client.get("/ready")
    assert response.status_code == 503
    body = response.json()
    assert body["status"] == "not_ready"
    assert body["checks"]["db"]["ok"] is False
