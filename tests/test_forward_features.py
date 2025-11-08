"""Tests ensuring forward-test feature preparation populates key signals."""

from __future__ import annotations

from typing import Dict, Optional

import numpy as np
import pandas as pd

from src.models import forward_test


class FakeLoader:
    def __init__(self, league: str) -> None:
        self.league = league

    def load_team_metrics(self, season: Optional[int] = None) -> pd.DataFrame:
        if self.league == "NBA":
            return pd.DataFrame(
                {
                    "team": ["LAL", "BOS"],
                    "E_OFF_RATING": [115.0, 113.0],
                    "season": [season, season],
                }
            )
        return pd.DataFrame()

    def load_injuries(self, game_date: Optional[pd.Timestamp] = None) -> pd.DataFrame:
        return pd.DataFrame()

    def get_injury_count(
        self,
        team: str,
        game_date: Optional[pd.Timestamp] = None,
        status: Optional[str] = None,
        position: Optional[str] = None,
    ) -> int:
        team = team.upper()
        mapping = {"NE": 3, "DAL": 1, "LAL": 2, "BOS": 1}
        return mapping.get(team, 0)

    def get_weather_features(
        self,
        game_id: Optional[str] = None,
        game_date: Optional[pd.Timestamp] = None,
        venue: Optional[str] = None,
    ) -> Dict[str, float]:
        return {
            "game_temperature_f": 45.0,
            "game_wind_mph": 12.0,
            "is_weather_precip": 1.0,
            "is_weather_dome": 0.0,
        }

    def load_rolling_metrics(self) -> pd.DataFrame:
        return pd.DataFrame()

    def get_rolling_metric(
        self,
        team: str,
        metric_name: str,
        game_date: Optional[pd.Timestamp] = None,
        window: int = 3,
        default: float = np.nan,
    ) -> float:
        return 0.5

    def load_team_metrics_for_nba(self) -> pd.DataFrame:
        return self.load_team_metrics()


def _sample_game(home: str = "NE", away: str = "DAL") -> Dict[str, object]:
    return {
        "id": "example_game",
        "home_team": home,
        "away_team": away,
        "commence_time": "2025-09-10T01:00:00Z",
        "bookmakers": [
            {
                "markets": [
                    {
                        "key": "h2h",
                        "outcomes": [
                            {"name": home, "price": -140},
                            {"name": away, "price": 120},
                        ],
                    }
                ]
            }
        ],
    }


def test_prepare_features_populates_nfl_weather_and_rest(monkeypatch) -> None:
    monkeypatch.setattr(forward_test, "FeatureLoader", lambda league: FakeLoader(league))

    model_features = [
        "injuries_out",
        "game_temperature_f",
        "team_rest_days",
        "opponent_rest_days",
        "rest_diff",
        "is_short_week",
        "is_post_bye",
        "road_trip_length_entering",
    ]
    df = forward_test.prepare_features(_sample_game(), league="NFL", model_features=model_features)

    home = df[df["is_home"] == 1].iloc[0]
    away = df[df["is_home"] == 0].iloc[0]

    assert home["injuries_out"] == 3
    assert away["injuries_out"] == 1
    assert home["game_temperature_f"] == 45.0
    assert home["team_rest_days"] == 7.0
    assert away["team_rest_days"] == 6.0
    assert home["road_trip_length_entering"] == 0.0
    assert away["road_trip_length_entering"] == 1.0


def test_prepare_features_populates_nba_injuries(monkeypatch) -> None:
    monkeypatch.setattr(forward_test, "FeatureLoader", lambda league: FakeLoader(league))
    model_features = ["injuries_out"]
    game = _sample_game(home="LAL", away="BOS")
    df = forward_test.prepare_features(game, league="NBA", model_features=model_features)
    home = df[df["is_home"] == 1].iloc[0]
    assert home["injuries_out"] == 2
