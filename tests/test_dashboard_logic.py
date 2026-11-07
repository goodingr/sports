from __future__ import annotations

import pandas as pd

from src.dashboard import app as dashboard


def test_dashboard_displays_the_quote_used_for_totals_recommendations(monkeypatch):
    now = pd.Timestamp.now(tz="UTC")
    predictions = pd.DataFrame([{
        "game_id": "NBA_test",
        "league": "NBA",
        "model_type": "ensemble",
        "commence_time": now + pd.Timedelta(hours=2),
        "predicted_at": now,
        "total_line": 219.5,
        "predicted_total_points": 231.0,
        "home_team": "BOS",
        "away_team": "NYK",
        "over_moneyline": -110,
        "under_moneyline": -110,
        "over_predicted_prob": 0.75,
        "under_predicted_prob": 0.25,
        "over_implied_prob": 0.52,
        "under_implied_prob": 0.52,
        "over_edge": 0.23,
        "under_edge": -0.27,
        "home_score": None,
        "away_score": None,
        "result": None,
    }])
    quotes = pd.DataFrame([{
        "forward_game_id": "NBA_test",
        "outcome": "over",
        "book": "DraftKings",
        "moneyline": 125.0,
        "line": 221.5,
        "home_team_full": "Boston Celtics",
        "away_team_full": "New York Knicks",
    }])
    monkeypatch.setattr(dashboard, "load_forward_test_data", lambda **_: predictions)
    monkeypatch.setattr(
        dashboard, "get_totals_odds_for_recommended", lambda rows: quotes if not rows.empty else pd.DataFrame()
    )
    monkeypatch.setattr(dashboard.components, "overunder_recommended_table", lambda frame: frame)

    recommended = dashboard.update_overunder_page(None, "all", "ensemble", None, None, "all", 0.06)[7]

    assert len(recommended) == 1
    pick = recommended.iloc[0]
    assert pick["description"] == "Over 221.5"
    assert pick["moneyline"] == 125.0
    assert pick["book"] == "DraftKings"
    assert pick["home_team"] == "Boston Celtics"
