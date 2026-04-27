import pandas as pd
import sqlite3
from contextlib import contextmanager

from src.models import forward_test
from src.db.core import initialize
from src.predict import storage


def test_save_predictions_overwrites_existing(monkeypatch, tmp_path):
    forward_dir = tmp_path / 'forward_test'
    forward_dir.mkdir()
    monkeypatch.setattr(forward_test, 'FORWARD_TEST_DIR', forward_dir)

    base = {
        'game_id': ['NBA_GAME_1'],
        'commence_time': [pd.Timestamp('2025-11-10T00:00:00Z')],
        'predicted_at': [pd.Timestamp('2025-11-08T12:00:00Z')],
        'home_team': ['LAL'],
        'away_team': ['BOS'],
        'home_moneyline': [-140],
        'away_moneyline': [120],
        'home_predicted_prob': [0.6],
        'away_predicted_prob': [0.4],
        'home_implied_prob': [0.58],
        'away_implied_prob': [0.42],
        'home_edge': [0.02],
        'away_edge': [-0.01],
    }

    df1 = pd.DataFrame(base)
    forward_test.save_predictions(df1, timestamp='20250101_000000')

    df2 = pd.DataFrame({
        **base,
        'home_predicted_prob': [0.7],
        'away_predicted_prob': [0.3],
        'predicted_at': [pd.Timestamp('2025-11-08T15:00:00Z')],
    })
    forward_test.save_predictions(df2, timestamp='20250101_010000')

    master = pd.read_parquet(forward_dir / 'ensemble' / 'predictions_master.parquet')
    assert len(master) == 1
    assert master.iloc[0]['home_predicted_prob'] == 0.7
    assert master.iloc[0]['predicted_at'] == pd.Timestamp('2025-11-08T15:00:00Z', tz='UTC')


def test_sqlite_predictions_store_current_totals(monkeypatch, tmp_path):
    db_path = tmp_path / "predictions.db"
    initialize(db_path)

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO sports (sport_id, name, league, default_market) VALUES (1, 'Basketball', 'NBA', 'moneyline')"
        )
        conn.execute("INSERT INTO teams (team_id, sport_id, code, name) VALUES (1, 1, 'LAL', 'Lakers')")
        conn.execute("INSERT INTO teams (team_id, sport_id, code, name) VALUES (2, 1, 'BOS', 'Celtics')")
        conn.execute(
            """
            INSERT INTO games (
                game_id, sport_id, season, start_time_utc, home_team_id, away_team_id, status
            ) VALUES ('NBA_GAME_1', 1, 2025, '2025-11-10T00:00:00+00:00', 1, 2, 'scheduled')
            """
        )

    @contextmanager
    def temp_connect(path=None):
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    monkeypatch.setattr(storage, "connect", temp_connect)

    first = pd.DataFrame(
        {
            "game_id": ["NBA_GAME_1"],
            "home_predicted_prob": [0.55],
            "away_predicted_prob": [0.45],
            "total_line": [220.5],
            "predicted_total_points": [224.0],
            "over_predicted_prob": [0.57],
            "under_predicted_prob": [0.43],
        }
    )
    storage.save_predictions(first, "ensemble", pd.Timestamp("2025-11-08T12:00:00Z").to_pydatetime())

    second = first.copy()
    second["predicted_total_points"] = [226.5]
    storage.save_predictions(second, "ensemble", pd.Timestamp("2025-11-08T13:00:00Z").to_pydatetime())

    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            "SELECT model_type, predicted_total_points FROM predictions WHERE game_id = 'NBA_GAME_1'"
        ).fetchall()

    assert rows == [("ensemble", 226.5)]
