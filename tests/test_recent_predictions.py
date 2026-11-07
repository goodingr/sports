from __future__ import annotations

import pandas as pd
import pytest

from src.dashboard.data import DISPLAY_TIMEZONE, get_recent_predictions


def _predictions(timezone: str | None) -> pd.DataFrame:
    now = pd.Timestamp.now(tz="UTC")
    starts = [
        now + pd.Timedelta(days=3),
        now - pd.Timedelta(days=2),
        now + pd.Timedelta(days=2),
        now + pd.Timedelta(days=4),
    ]
    starts = [start.tz_convert(timezone) if timezone else start.tz_localize(None) for start in starts]
    return pd.DataFrame({
        "game_id": ["later", "past", "sooner", "completed"],
        "commence_time": starts,
        "predicted_at": [now] * 4,
        "home_team": ["Home"] * 4,
        "away_team": ["Away"] * 4,
        "league": ["NBA"] * 4,
        "home_moneyline": [-110] * 4,
        "away_moneyline": [100] * 4,
        "home_predicted_prob": [0.7] * 4,
        "away_predicted_prob": [0.3] * 4,
        "home_implied_prob": [0.52] * 4,
        "away_implied_prob": [0.5] * 4,
        "home_edge": [0.18] * 4,
        "away_edge": [-0.2] * 4,
        "result": [None, None, None, "home"],
        "home_score": [None, None, None, 110],
        "away_score": [None, None, None, 100],
    })


@pytest.mark.parametrize("timezone", ["UTC", "America/Los_Angeles", None])
def test_recent_predictions_filters_orders_and_localizes_upcoming_bets(timezone):
    predictions = _predictions(timezone)

    recent = get_recent_predictions(predictions, edge_threshold=0.1, limit=2)

    assert recent["game_id"].tolist() == ["sooner", "later"]
    assert recent["side"].tolist() == ["home", "home"]
    assert recent["won"].isna().all()
    assert recent["commence_time"].dt.tz == DISPLAY_TIMEZONE
    expected = pd.Timestamp(predictions.loc[predictions["game_id"] == "sooner", "commence_time"].iloc[0])
    if expected.tzinfo is None:
        expected = expected.tz_localize("UTC")
    assert recent.iloc[0]["commence_time"] == expected.tz_convert(DISPLAY_TIMEZONE)
    assert get_recent_predictions(predictions, edge_threshold=0.1, limit=1)["game_id"].tolist() == ["sooner"]


def test_recent_predictions_returns_empty_when_all_prices_are_invalid():
    predictions = _predictions("UTC")
    predictions[["home_moneyline", "away_moneyline"]] = 0

    assert get_recent_predictions(predictions).empty
