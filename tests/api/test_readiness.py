from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from src.api import readiness
from src.api.settings import APISettings


def _create_ready_db(db_path):
    timestamp = datetime.now(timezone.utc).isoformat()
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
            CREATE TABLE odds (snapshot_id TEXT, game_id TEXT, book_id INTEGER, market TEXT, outcome TEXT);
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


def test_ready_endpoint_reports_ready_when_backend_dependencies_are_present(
    api_client, monkeypatch, tmp_path
):
    db_path = tmp_path / "betting.db"
    models_dir = tmp_path / "models"
    models_dir.mkdir()
    (models_dir / "nba_ensemble_calibrated_moneyline.pkl").write_bytes(b"model")
    _create_ready_db(db_path)

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

    response = api_client.get("/ready")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ready"
    assert body["checks"]["db"]["ok"] is True
    assert body["checks"]["schema"]["ok"] is True
    assert body["checks"]["models"]["leagues"]["NBA"]["ok"] is True


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
