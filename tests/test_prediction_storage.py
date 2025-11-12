import pandas as pd

from src.models import forward_test


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

    master = pd.read_parquet(forward_dir / 'predictions_master.parquet')
    assert len(master) == 1
    assert master.iloc[0]['home_predicted_prob'] == 0.7
    assert master.iloc[0]['predicted_at'] == pd.Timestamp('2025-11-08T15:00:00Z', tz='UTC')
