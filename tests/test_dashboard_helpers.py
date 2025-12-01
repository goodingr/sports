import pandas as pd
import pytest
import sqlite3
from pathlib import Path

from src.dashboard import app as dashboard_app
from src.dashboard import components as dashboard_components
from src.dashboard import data as dashboard_data


class DummyConn:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, *_, **__):
        class Cursor:
            def fetchall(self_inner):
                return []

        return Cursor()


def test_expand_predictions_marks_ties_as_losses_for_moneyline():
    df = pd.DataFrame(
        {
            'game_id': ['G1'],
            'commence_time': [pd.Timestamp('2025-11-08T12:30:00Z')],
            'predicted_at': [pd.Timestamp('2025-11-07T18:00:00Z')],
            'home_team': ['TOT'],
            'away_team': ['MUN'],
            'home_moneyline': [-110],
            'away_moneyline': [120],
            'draw_moneyline': [275],
            'home_predicted_prob': [0.55],
            'away_predicted_prob': [0.35],
            'draw_predicted_prob': [0.1],
            'home_implied_prob': [0.5238],
            'away_implied_prob': [0.4545],
            'draw_implied_prob': [0.1818],
            'home_edge': [0.03],
            'away_edge': [-0.02],
            'draw_edge': [0.05],
            'result': ['tie'],
            'home_score': [0],
            'away_score': [0],
            'result_updated_at': [pd.Timestamp('2025-11-08T15:00:00Z')],
        }
    )

    bets = dashboard_data._expand_predictions(df)

    home_row = bets.loc[bets['side'] == 'home'].iloc[0]
    away_row = bets.loc[bets['side'] == 'away'].iloc[0]
    draw_row = bets.loc[bets['side'] == 'draw'].iloc[0]

    assert home_row['won'] is False or home_row['won'] == False
    assert away_row['won'] is False or away_row['won'] == False
    assert draw_row['won'] is True or draw_row['won'] == True


def test_get_completed_bets_filters_by_edge_threshold(monkeypatch):
    df = pd.DataFrame(
        {
            'game_id': ['G1', 'G2'],
            'commence_time': [
                pd.Timestamp('2025-11-08T14:00:00Z'),
                pd.Timestamp('2025-11-08T16:00:00Z'),
            ],
            'predicted_at': [pd.Timestamp('2025-11-07T12:00:00Z')]*2,
            'home_team': ['COMO', 'HAM'],
            'away_team': ['CAG', 'DOR'],
            'home_moneyline': [-105, 180],
            'away_moneyline': [125, -200],
            'home_predicted_prob': [0.6, 0.4],
            'away_predicted_prob': [0.4, 0.6],
            'home_implied_prob': [0.512, 0.357],
            'away_implied_prob': [0.444, 0.667],
            'home_edge': [0.12, 0.03],
            'away_edge': [0.01, 0.18],
            'result': ['tie', 'away'],
            'home_score': [0, 1],
            'away_score': [0, 2],
            'result_updated_at': [pd.Timestamp('2025-11-08T18:00:00Z')]*2,
        }
    )

    monkeypatch.setattr('src.db.core.connect', lambda: DummyConn())

    completed = dashboard_data.get_completed_bets(df, edge_threshold=0.06)

    teams = set(completed['team'])
    assert 'COMO' in teams
    assert 'DOR' in teams
    assert 'HAM' not in teams
    assert not completed['won'].isna().any()


def test_get_recommended_bets_deduplicates_by_game(monkeypatch):
    df = pd.DataFrame(
        {
            'game_id': ['G1'],
            'commence_time': [pd.Timestamp('2026-11-08T12:30:00Z')],
            'predicted_at': [pd.Timestamp('2025-11-07T10:00:00Z')],
            'home_team': ['SUN'],
            'away_team': ['ARS'],
            'home_moneyline': [150],
            'away_moneyline': [-155],
            'home_predicted_prob': [0.45],
            'away_predicted_prob': [0.55],
            'home_implied_prob': [0.4],
            'away_implied_prob': [0.6],
            'home_edge': [0.08],
            'away_edge': [0.12],
            'result': [None],
            'home_score': [None],
            'away_score': [None],
        }
    )

    bets = dashboard_data.get_recommended_bets(df, edge_threshold=0.06)
    assert len(bets) == 1
    assert bets.iloc[0]['team'] == 'ARS'
    assert bets.iloc[0]['edge'] == 0.12


def test_apply_best_moneylines_uses_highest_positive_price():
    recommended = pd.DataFrame(
        {
            "game_id": ["GAME1"],
            "side": ["away"],
            "team": ["ARK"],
            "opponent": ["LSU"],
            "moneyline": [164],
            "moneyline_book": ["DraftKings"],
        }
    )
    odds = pd.DataFrame(
        [
            {"forward_game_id": "GAME1", "outcome": "away", "book": "DraftKings", "moneyline": 164},
            {"forward_game_id": "GAME1", "outcome": "away", "book": "BetMGM", "moneyline": 180},
            {"forward_game_id": "GAME1", "outcome": "away", "book": "FanDuel", "moneyline": 170},
        ]
    )

    updated = dashboard_app._apply_best_moneylines(recommended, odds)
    assert updated.iloc[0]["moneyline"] == 180
    assert updated.iloc[0]["moneyline_book"] == "BetMGM"


def test_apply_best_moneylines_uses_least_negative_price():
    recommended = pd.DataFrame(
        {
            "game_id": ["GAME2"],
            "side": ["home"],
            "team": ["LSU"],
            "opponent": ["ARK"],
            "moneyline": [-220],
            "moneyline_book": ["Default"],
        }
    )
    odds = pd.DataFrame(
        [
            {"forward_game_id": "GAME2", "outcome": "home", "book": "BetMGM", "moneyline": -220},
            {"forward_game_id": "GAME2", "outcome": "home", "book": "FanDuel", "moneyline": -196},
            {"forward_game_id": "GAME2", "outcome": "home", "book": "DraftKings", "moneyline": -205},
        ]
    )

    updated = dashboard_app._apply_best_moneylines(recommended, odds)
    assert updated.iloc[0]["moneyline"] == -196
    assert updated.iloc[0]["moneyline_book"] == "FanDuel"


def test_map_game_ids_by_odds_api_matches_existing_games(tmp_path, monkeypatch):
    db_path = Path(tmp_path) / "odds_test.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        "CREATE TABLE games (game_id TEXT PRIMARY KEY, odds_api_id TEXT)"
    )
    conn.execute("INSERT INTO games (game_id, odds_api_id) VALUES (?, ?)", ("NBA_custom123", "event123"))
    conn.commit()
    conn.close()

    class DummyCtx:
        def __enter__(self):
            self.conn = sqlite3.connect(str(db_path))
            return self.conn

        def __exit__(self, exc_type, exc, tb):
            self.conn.close()
            return False

    monkeypatch.setattr(dashboard_data, "connect", lambda: DummyCtx())

    recommended = pd.DataFrame(
        {
            "game_id": ["event123"],
            "league": ["NBA"],
            "commence_time": [pd.Timestamp("2025-11-15T01:10:00Z")],
            "team": ["HOU"],
            "opponent": ["POR"],
        }
    )

    mapping = dashboard_data._map_game_ids_by_odds_api(recommended)
    assert len(mapping) == 1
    assert mapping.iloc[0]["prediction_game_id"] == "event123"
    assert mapping.iloc[0]["db_game_id"] == "NBA_custom123"


def test_moneyline_detail_table_filters_kaggle_books():
    df = pd.DataFrame(
        [
            {"book": "Kaggle Consensus (8 books)", "outcome": "home", "moneyline": -200},
            {"book": "FanDuel", "outcome": "home", "moneyline": -180},
            {"book": "FanDuel", "outcome": "away", "moneyline": 160},
        ]
    )

    table = dashboard_components.moneyline_detail_table(df, home_team="HOU", away_team="POR")
    assert isinstance(table.data, list)
    assert all("Kaggle" not in row["book"] for row in table.data)
    assert any(row["book"] == "FanDuel" for row in table.data)


def test_assign_versions_uses_version_config(tmp_path, monkeypatch):
    config_path = tmp_path / "versions.yml"
    config_path.write_text(
        "versions:\n"
        "  - name: v0.1\n"
        "    start: 2025-01-01T00:00:00Z\n"
        "  - name: v0.2\n"
        "    start: 2025-11-14T00:00:00Z\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(dashboard_data, "VERSION_CONFIG_PATH", config_path)
    dashboard_data._load_version_config.cache_clear()

    try:
        df = pd.DataFrame(
            {
                "predicted_at": [
                    pd.Timestamp("2025-11-13T12:00:00Z"),
                    pd.Timestamp("2025-11-15T12:00:00Z"),
                ]
            }
        )

        updated = dashboard_data._assign_versions(df.copy())
        assert list(updated["version"]) == ["v0.1", "v0.2"]
    finally:
        dashboard_data._load_version_config.cache_clear()


def test_filter_by_version_returns_matching_rows():
    df = pd.DataFrame({"version": ["v0.1", "v0.2", "v0.2"], "value": [1, 2, 3]})

    filtered = dashboard_data.filter_by_version(df, "v0.2")
    assert len(filtered) == 2
    assert filtered["version"].nunique() == 1
    assert filtered["value"].tolist() == [2, 3]

    filtered_all = dashboard_data.filter_by_version(df, "all")
    assert filtered_all.equals(df)


def test_get_default_version_value_prefers_current(tmp_path, monkeypatch):
    config_path = tmp_path / "versions.yml"
    config_path.write_text(
        "current: v0.1\n"
        "versions:\n"
        "  - name: v0.1\n"
        "    start: 2025-01-01T00:00:00Z\n"
        "  - name: v0.2\n"
        "    start: 2025-11-14T00:00:00Z\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(dashboard_data, "VERSION_CONFIG_PATH", config_path)
    dashboard_data._load_version_config.cache_clear()
    try:
        assert dashboard_data.get_default_version_value() == "v0.1"
    finally:
        dashboard_data._load_version_config.cache_clear()


def test_get_default_version_value_falls_back_to_latest(tmp_path, monkeypatch):
    config_path = tmp_path / "versions.yml"
    config_path.write_text(
        "versions:\n"
        "  - name: v0.1\n"
        "    start: 2025-01-01T00:00:00Z\n"
        "  - name: v0.2\n"
        "    start: 2025-11-14T00:00:00Z\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(dashboard_data, "VERSION_CONFIG_PATH", config_path)
    dashboard_data._load_version_config.cache_clear()
    try:
        assert dashboard_data.get_default_version_value() == "v0.2"
    finally:
        dashboard_data._load_version_config.cache_clear()
