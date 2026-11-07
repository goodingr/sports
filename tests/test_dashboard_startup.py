from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
from contextlib import closing
from pathlib import Path
from types import ModuleType

import pandas as pd
import pytest

from src.dashboard import data as dashboard_data
from src.db.core import initialize


def _run_python(source: str, db_path: Path) -> None:
    result = subprocess.run(
        [sys.executable, "-c", source],
        cwd=Path(__file__).resolve().parents[1],
        env={**os.environ, "SPORTS_DB_PATH": str(db_path)},
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_data_and_api_imports_do_not_start_dashboard_and_serve_an_empty_warehouse(tmp_path):
    _run_python(
        """
import os
import sqlite3
import sys
from contextlib import closing
from pathlib import Path
from fastapi.testclient import TestClient

import src.dashboard.data
from src.api.main import app

assert 'src.dashboard.app' not in sys.modules
path = Path(os.environ['SPORTS_DB_PATH'])
assert not path.exists()
with TestClient(app) as client:
    assert client.get('/health').status_code == 200
    stats = client.get('/api/bets/stats')
    assert stats.status_code == 200, stats.text
    assert stats.json()['total_bets'] == 0
    history = client.get('/api/bets/history')
    assert history.status_code == 200, history.text
    assert history.json()['data'] == []
    assert client.get('/ready').status_code == 503
with closing(sqlite3.connect(path)) as conn:
    assert conn.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table'").fetchone()[0] == 0
from src.dashboard import app as dashboard
assert dashboard.initial_df.empty
assert dashboard.app.server.test_client().get('/').status_code == 200
""",
        tmp_path / "fresh" / "warehouse.db",
    )


def test_forward_loader_surfaces_database_corruption(tmp_path, monkeypatch):
    path = tmp_path / "corrupt.db"
    path.write_bytes(b"This is not a SQLite database")
    monkeypatch.setenv("SPORTS_DB_PATH", str(path))

    with pytest.raises(sqlite3.DatabaseError, match="not a database"):
        dashboard_data.load_forward_test_data()


def test_forward_loader_surfaces_incomplete_existing_prediction_schema(tmp_path, monkeypatch):
    path = tmp_path / "incomplete.db"
    with closing(sqlite3.connect(path)) as conn:
        conn.execute("CREATE TABLE predictions (game_id TEXT)")
        conn.commit()
    monkeypatch.setenv("SPORTS_DB_PATH", str(path))

    with pytest.raises(pd.errors.DatabaseError, match="no such table|no such column"):
        dashboard_data.load_forward_test_data()


def test_dashboard_startup_preserves_full_history_and_date_picker_bounds(tmp_path):
    path = tmp_path / "populated.db"
    initialize(path)
    with closing(sqlite3.connect(path)) as conn:
        conn.execute("INSERT INTO sports (sport_id,name,league,default_market) VALUES (1,'NBA','NBA','totals')")
        conn.execute("INSERT INTO teams (team_id,sport_id,code,name) VALUES (1,1,'H','Home'),(2,1,'A','Away')")
        conn.executemany(
            """
            INSERT INTO games (game_id,sport_id,home_team_id,away_team_id,start_time_utc)
            VALUES (?,1,1,2,?)
            """,
            [("old", "2020-01-01T18:00:00+00:00"), ("new", "2026-11-07T18:00:00+00:00")],
        )
        conn.executemany(
            "INSERT INTO predictions (game_id,model_type,predicted_at) VALUES (?,'ensemble',?)",
            [("old", "2019-12-31T18:00:00+00:00"), ("new", "2026-11-06T18:00:00+00:00")],
        )
        conn.commit()
    _run_python(
        """
from datetime import date
from src.dashboard import app as dashboard
from src.dashboard.data import load_forward_test_data

assert set(dashboard.initial_df['game_id']) == {'old', 'new'}
assert dashboard.min_date == date(2020, 1, 1)
assert dashboard.max_date == date(2026, 11, 7)
bounds = dashboard.sync_date_range_controls(dashboard._df_to_json(dashboard.initial_df))
assert bounds == ('2020-01-01', '2026-11-07', '2020-01-01', '2026-11-07') * 3
filtered = load_forward_test_data(start_date='2021-01-01')
assert filtered['game_id'].tolist() == ['new']
""",
        path,
    )


def test_dashboard_run_preserves_the_public_server_arguments(monkeypatch):
    import src.dashboard

    calls = []
    application = ModuleType("src.dashboard.app")
    application.run = lambda **kwargs: calls.append(kwargs)
    monkeypatch.setitem(sys.modules, "src.dashboard.app", application)

    src.dashboard.run(True, 9123, "127.0.0.1")

    assert calls == [{"debug": True, "port": 9123, "host": "127.0.0.1"}]
