from __future__ import annotations

import pandas as pd

from src.api.auth import get_current_user
from src.api.main import app
from src.api.routes import bets


def _prediction_rows() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "game_id": "premium-game",
                "league": "NBA",
                "commence_time": pd.Timestamp.now(tz="UTC") + pd.Timedelta(hours=4),
                "home_team": "Home",
                "away_team": "Away",
                "status": "Pending",
                "edge": 0.11,
                "side": "over",
                "description": "Over 220.5",
                "total_line": 220.5,
                "moneyline": -110,
                "predicted_total_points": 227.0,
                "profit": None,
                "won": None,
                "home_score": None,
                "away_score": None,
            }
        ]
    )


def _publishable_prediction() -> dict:
    return {
        "rule_id": "nba_totals_rule",
        "market": "totals",
        "league": "NBA",
        "game_id": "premium-game",
        "start_time_utc": (pd.Timestamp.now(tz="UTC") + pd.Timedelta(hours=4)).isoformat(),
        "home_team": "Home",
        "away_team": "Away",
        "side": "over",
        "total_line": 221.5,
        "odds": 105,
        "moneyline": 105,
        "edge": 0.19,
        "predicted_total_points": 229.0,
        "recommended_bet": "Over 221.5",
    }


def _odds_rows() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "market": "totals",
                "outcome": "Over",
                "line": 220.5,
                "moneyline": -110,
                "book": "DraftKings",
                "book_url": "https://sportsbook.draftkings.com",
                "fetched_at_utc": "2026-04-27T12:00:00+00:00",
            },
            {
                "market": "totals",
                "outcome": "Under",
                "line": 220.5,
                "moneyline": -110,
                "book": "DraftKings",
                "book_url": "https://sportsbook.draftkings.com",
                "fetched_at_utc": "2026-04-27T12:00:00+00:00",
            },
        ]
    )


def setup_function() -> None:
    app.dependency_overrides.clear()


def teardown_function() -> None:
    app.dependency_overrides.clear()


def test_game_odds_fails_closed_for_non_premium(api_client, monkeypatch):
    app.dependency_overrides[get_current_user] = lambda: {"public_metadata": {"is_premium": False}}
    monkeypatch.setattr(bets, "get_game_odds", lambda game_id: _odds_rows())
    monkeypatch.setattr(bets, "get_totals_data", lambda model_type="ensemble": _prediction_rows())

    response = api_client.get("/api/bets/game/premium-game/odds")

    assert response.status_code == 403
    assert "data" not in response.json()


def test_premium_user_can_access_detailed_odds(api_client, monkeypatch):
    app.dependency_overrides[get_current_user] = lambda: {"public_metadata": {"is_premium": True}}
    monkeypatch.setattr(bets, "get_game_odds", lambda game_id: _odds_rows())
    monkeypatch.setattr(bets, "get_totals_data", lambda model_type="ensemble": _prediction_rows())
    monkeypatch.setattr(
        bets,
        "_load_gated_publishable_bets",
        lambda: {("premium-game", "totals", "over"): _publishable_prediction()},
    )

    response = api_client.get("/api/bets/game/premium-game/odds")

    assert response.status_code == 200
    body = response.json()
    assert body["count"] == 2
    assert body["data"][0]["book"] == "DraftKings"
    assert body["prediction"]["recommended_bet"] == "Over 221.5"
    assert body["prediction"]["edge"] == 0.19


def test_premium_game_odds_omit_prediction_when_no_rule_passes(api_client, monkeypatch):
    app.dependency_overrides[get_current_user] = lambda: {"public_metadata": {"is_premium": True}}
    monkeypatch.setattr(bets, "get_game_odds", lambda game_id: _odds_rows())
    monkeypatch.setattr(bets, "get_totals_data", lambda model_type="ensemble": _prediction_rows())
    monkeypatch.setattr(bets, "_load_gated_publishable_bets", lambda: {})

    response = api_client.get("/api/bets/game/premium-game/odds")

    assert response.status_code == 200
    body = response.json()
    assert body["count"] == 2
    assert "recommended_bet" not in body["prediction"]
    assert "edge" not in body["prediction"]
    assert "predicted_total_points" not in body["prediction"]


def test_premium_game_odds_omit_prediction_for_non_gated_game(api_client, monkeypatch):
    app.dependency_overrides[get_current_user] = lambda: {"public_metadata": {"is_premium": True}}
    monkeypatch.setattr(bets, "get_game_odds", lambda game_id: _odds_rows())
    monkeypatch.setattr(bets, "get_totals_data", lambda model_type="ensemble": _prediction_rows())
    monkeypatch.setattr(
        bets,
        "_load_gated_publishable_bets",
        lambda: {
            ("other-game", "totals", "over"): {
                **_publishable_prediction(),
                "game_id": "other-game",
            }
        },
    )

    response = api_client.get("/api/bets/game/premium-game/odds")

    assert response.status_code == 200
    body = response.json()
    assert "recommended_bet" not in body["prediction"]
    assert "edge" not in body["prediction"]
