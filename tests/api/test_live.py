from __future__ import annotations

import base64
import json

import pandas as pd

from src.api.auth import get_current_user
from src.api.main import app
from src.api.routes import bets

LOCKED_FIELD_NAMES = {
    "predicted_prob",
    "implied_prob",
    "edge",
    "side",
    "description",
    "total_line",
    "odds_data",
    "book",
    "book_url",
    "moneyline",
    "recommended_bet",
    "predicted_total_points",
    "prediction",
    "recommendation",
}


def _unsigned_jwt(payload: dict) -> str:
    def encode(segment: dict) -> str:
        raw = json.dumps(segment, separators=(",", ":")).encode("utf-8")
        return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")

    return f"{encode({'alg': 'none', 'typ': 'JWT'})}.{encode(payload)}."


def _sample_bets() -> pd.DataFrame:
    now = pd.Timestamp.now(tz="UTC")
    return pd.DataFrame(
        [
            {
                "game_id": "future-game",
                "league": "NBA",
                "commence_time": now + pd.Timedelta(hours=6),
                "home_team": "Home Future",
                "away_team": "Away Future",
                "status": "Pending",
                "edge": 0.12,
                "side": "over",
                "description": "Over 221.5",
                "total_line": 221.5,
                "moneyline": -110,
                "predicted_prob": 0.58,
                "implied_prob": 0.52,
                "predicted_total_points": 228.1,
                "profit": None,
                "won": None,
                "home_score": None,
                "away_score": None,
            },
            {
                "game_id": "live-game",
                "league": "NBA",
                "commence_time": now - pd.Timedelta(hours=1),
                "home_team": "Home Live",
                "away_team": "Away Live",
                "status": "Pending",
                "edge": 0.09,
                "side": "under",
                "description": "Under 218.5",
                "total_line": 218.5,
                "moneyline": -105,
                "predicted_prob": 0.57,
                "implied_prob": 0.51,
                "predicted_total_points": 211.3,
                "profit": None,
                "won": None,
                "home_score": 45,
                "away_score": 42,
            },
            {
                "game_id": "completed-game",
                "league": "NBA",
                "commence_time": now - pd.Timedelta(days=2),
                "home_team": "Home Done",
                "away_team": "Away Done",
                "status": "Completed",
                "edge": 0.08,
                "side": "over",
                "description": "Over 215.5",
                "total_line": 215.5,
                "moneyline": -110,
                "predicted_prob": 0.56,
                "implied_prob": 0.51,
                "predicted_total_points": 223.0,
                "profit": 90.91,
                "won": True,
                "home_score": 113,
                "away_score": 108,
            },
        ]
    )


def _publishable_future_bet() -> dict:
    now = pd.Timestamp.now(tz="UTC")
    return {
        "rule_id": "nba_totals_rule",
        "market": "totals",
        "league": "NBA",
        "game_id": "future-game",
        "start_time_utc": (now + pd.Timedelta(hours=6)).isoformat(),
        "home_team": "Home Future",
        "away_team": "Away Future",
        "side": "over",
        "total_line": 222.5,
        "odds": 105,
        "moneyline": 105,
        "edge": 0.21,
        "predicted_total_points": 230.1,
        "recommended_bet": "Over 222.5",
    }


def _all_keys(value):
    if isinstance(value, dict):
        keys = set(value)
        for child in value.values():
            keys |= _all_keys(child)
        return keys
    if isinstance(value, list):
        keys = set()
        for child in value:
            keys |= _all_keys(child)
        return keys
    return set()


def _assert_no_locked_fields(payload: dict) -> None:
    leaked = LOCKED_FIELD_NAMES & _all_keys(payload)
    assert not leaked, f"locked response leaked fields: {sorted(leaked)}"


def setup_function() -> None:
    app.dependency_overrides.clear()


def teardown_function() -> None:
    app.dependency_overrides.clear()


def test_unsigned_jwt_cannot_unlock_premium(api_client, monkeypatch):
    monkeypatch.setattr(bets, "get_totals_data", lambda model_type="ensemble": _sample_bets())

    token = _unsigned_jwt(
        {
            "sub": "user_123",
            "public_metadata": {"is_premium": True},
            "exp": 4_102_444_800,
        }
    )

    response = api_client.get("/api/bets/upcoming", headers={"Authorization": f"Bearer {token}"})

    assert response.status_code == 200
    body = response.json()
    assert body["is_premium"] is False
    assert body["data"][0]["is_locked"] is True
    _assert_no_locked_fields(body["data"][0])


def test_upcoming_public_response_does_not_leak_locked_fields(api_client, monkeypatch):
    app.dependency_overrides[get_current_user] = lambda: None
    monkeypatch.setattr(bets, "get_totals_data", lambda model_type="ensemble": _sample_bets())

    response = api_client.get("/api/bets/upcoming")

    assert response.status_code == 200
    body = response.json()
    assert body["is_premium"] is False
    assert body["data"]
    _assert_no_locked_fields(body["data"][0])


def test_live_history_public_response_does_not_leak_locked_fields(api_client, monkeypatch):
    app.dependency_overrides[get_current_user] = lambda: None
    monkeypatch.setattr(bets, "get_totals_data", lambda model_type="ensemble": _sample_bets())

    response = api_client.get("/api/bets/history")

    assert response.status_code == 200
    live = next(item for item in response.json()["data"] if item["game_id"] == "live-game")
    assert live["is_live"] is True
    assert live["is_locked"] is True
    _assert_no_locked_fields(live)


def test_completed_history_public_response_does_not_leak_prediction_fields(api_client, monkeypatch):
    app.dependency_overrides[get_current_user] = lambda: None
    monkeypatch.setattr(bets, "get_totals_data", lambda model_type="ensemble": _sample_bets())

    response = api_client.get("/api/bets/history")

    assert response.status_code == 200
    completed = next(
        item for item in response.json()["data"] if item["game_id"] == "completed-game"
    )
    assert completed["status"] == "Completed"
    _assert_no_locked_fields(completed)


def test_premium_upcoming_returns_empty_when_no_approved_rule_passes(api_client, monkeypatch):
    app.dependency_overrides[get_current_user] = lambda: {"public_metadata": {"is_premium": True}}
    monkeypatch.setattr(bets, "get_totals_data", lambda model_type="ensemble": _sample_bets())
    monkeypatch.setattr(bets, "_load_gated_publishable_bets", lambda: {})

    response = api_client.get("/api/bets/upcoming")

    assert response.status_code == 200
    body = response.json()
    assert body["is_premium"] is True
    assert body["count"] == 0
    assert body["data"] == []


def test_premium_upcoming_only_returns_gated_paid_picks(api_client, monkeypatch):
    app.dependency_overrides[get_current_user] = lambda: {"public_metadata": {"is_premium": True}}
    monkeypatch.setattr(bets, "get_totals_data", lambda model_type="ensemble": _sample_bets())
    monkeypatch.setattr(
        bets,
        "_load_gated_publishable_bets",
        lambda: {("future-game", "totals", "over"): _publishable_future_bet()},
    )
    monkeypatch.setattr(bets, "_build_batch_odds_map", lambda game_ids: {})
    monkeypatch.setattr(bets, "_build_sportsbook_map", lambda recommended: {})

    response = api_client.get("/api/bets/upcoming")

    assert response.status_code == 200
    body = response.json()
    assert body["is_premium"] is True
    assert body["count"] == 1
    assert len(body["data"]) == 1
    bet = body["data"][0]
    assert bet["game_id"] == "future-game"
    assert bet["side"] == "over"
    assert bet["edge"] == 0.21
    assert bet["recommended_bet"] == "Over 222.5"


def test_premium_upcoming_drops_rows_not_in_gated_set(api_client, monkeypatch):
    app.dependency_overrides[get_current_user] = lambda: {"public_metadata": {"is_premium": True}}
    monkeypatch.setattr(bets, "get_totals_data", lambda model_type="ensemble": _sample_bets())
    monkeypatch.setattr(
        bets,
        "_load_gated_publishable_bets",
        lambda: {
            ("some-other-game", "totals", "over"): {
                **_publishable_future_bet(),
                "game_id": "some-other-game",
            }
        },
    )

    response = api_client.get("/api/bets/upcoming")

    assert response.status_code == 200
    body = response.json()
    assert body["is_premium"] is True
    assert body["count"] == 1
    assert body["data"][0]["game_id"] == "some-other-game"


def test_premium_history_live_rows_filtered_by_gate(api_client, monkeypatch):
    app.dependency_overrides[get_current_user] = lambda: {"public_metadata": {"is_premium": True}}
    monkeypatch.setattr(bets, "get_totals_data", lambda model_type="ensemble": _sample_bets())
    monkeypatch.setattr(bets, "_load_gated_publishable_bets", lambda: {})
    monkeypatch.setattr(bets, "_build_batch_odds_map", lambda game_ids: {})
    monkeypatch.setattr(bets, "_build_sportsbook_map", lambda recommended: {})

    response = api_client.get("/api/bets/history")

    assert response.status_code == 200
    body = response.json()
    game_ids = [item["game_id"] for item in body["data"]]
    assert "live-game" not in game_ids
    assert "completed-game" in game_ids


def test_loader_returns_empty_when_publishable_file_absent(monkeypatch, tmp_path):
    missing_path = tmp_path / "missing.json"
    monkeypatch.setattr(bets, "PUBLISHABLE_BETS_PATH", missing_path)

    assert bets._load_gated_publishable_bets() == {}


def test_loader_returns_empty_when_gate_did_not_pass(monkeypatch, tmp_path):
    path = tmp_path / "latest_publishable_bets.json"
    path.write_text(
        json.dumps({"publishable_profitable_list_exists": False, "bets": []}),
        encoding="utf-8",
    )
    monkeypatch.setattr(bets, "PUBLISHABLE_BETS_PATH", path)

    assert bets._load_gated_publishable_bets() == {}


def test_loader_keys_bets_by_game_market_side(monkeypatch, tmp_path):
    path = tmp_path / "latest_publishable_bets.json"
    path.write_text(
        json.dumps(
            {
                "publishable_profitable_list_exists": True,
                "bets": [
                    {"game_id": "G1", "market": "totals", "side": "OVER"},
                    {"game_id": "G2", "market": "moneyline", "side": "home"},
                ],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(bets, "PUBLISHABLE_BETS_PATH", path)

    keyed = bets._load_gated_publishable_bets()
    assert ("G1", "totals", "over") in keyed
    assert ("G2", "moneyline", "home") in keyed
