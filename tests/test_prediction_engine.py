"""Regression coverage for quote and model provenance through live inference."""

import hashlib

import joblib
import numpy as np
import pandas as pd
import pytest

from src.predict import engine as engine_module


class MoneylineModel:
    feature_names_in_ = ["moneyline"]

    def predict_proba(self, features):
        return np.tile([0.4, 0.6], (len(features), 1))


class TotalsModel:
    def predict(self, features):
        return np.full(len(features), 224.0)


@pytest.mark.parametrize("fixture_ids", [None, {"GAME"}])
def test_inference_saves_both_quote_sources_and_loaded_model_digests(tmp_path, monkeypatch, fixture_ids):
    moneyline_path = tmp_path / "nba_ensemble_calibrated_moneyline.pkl"
    totals_path = tmp_path / "nba_totals_gradient_boosting.pkl"
    joblib.dump(MoneylineModel(), moneyline_path)
    joblib.dump(
        {"regressor": TotalsModel(), "residual_std": 10.0, "feature_names": ["total_close"]},
        totals_path,
    )
    monkeypatch.setattr(engine_module, "MODEL_REGISTRY_PATH", tmp_path)
    quote_metadata = {
        "selected_totals_snapshot_id": "totals-observed",
        "selected_totals_book_id": 2,
        "selected_totals_book": "FanDuel",
        "selected_totals_fetched_at": "2026-11-07T00:00:00+00:00",
        "selected_h2h_snapshot_id": "moneyline-observed",
        "selected_h2h_book_id": 1,
        "selected_h2h_book": "DraftKings",
        "selected_h2h_fetched_at": "2026-11-06T23:59:00+00:00",
    }
    games = pd.DataFrame(
        [
            {
                "game_id": "GAME",
                "commence_time": pd.Timestamp("2026-11-07T02:00:00Z"),
                "home_team": "Lakers",
                "away_team": "Celtics",
                **quote_metadata,
            }
        ]
    )
    features = pd.DataFrame(
        [
            {
                "game_id": "GAME",
                "commence_time": games.iloc[0]["commence_time"],
                "home_team": "LAL",
                "away_team": "BOS",
                "home_moneyline": -120,
                "away_moneyline": 110,
                "moneyline": -120,
                "implied_prob": 0.53,
                "total_line": 220.5,
                "over_moneyline": -110,
                "under_moneyline": -110,
                "over_implied_prob": 0.5,
                "under_implied_prob": 0.5,
            }
        ]
    )
    scheduled = pd.concat([games, games.assign(game_id="EXCLUDED")], ignore_index=True)
    monkeypatch.setattr(engine_module, "load_games_from_database", lambda *args: scheduled)
    prediction_engine = engine_module.PredictionEngine()

    def prepare_features(selected, league):
        expected = ["GAME", "EXCLUDED"] if fixture_ids is None else ["GAME"]
        assert selected["game_id"].tolist() == expected
        return features

    monkeypatch.setattr(prediction_engine, "prepare_features", prepare_features)
    saved = []
    monkeypatch.setattr(
        engine_module,
        "save_predictions",
        lambda frame, model_type, timestamp: saved.append((frame.copy(), model_type, timestamp)),
    )

    predictions = prediction_engine.predict("NBA", fixture_ids=fixture_ids)

    assert predictions is not None
    assert len(saved) == 1
    stored, model_type, predicted_at = saved[0]
    assert model_type == "ensemble"
    assert stored.iloc[0]["predicted_at"] == predicted_at
    for field, value in quote_metadata.items():
        assert stored.iloc[0][field] == value
    assert (
        stored.iloc[0]["moneyline_model_sha256"]
        == hashlib.sha256(moneyline_path.read_bytes()).hexdigest()
    )
    assert (
        stored.iloc[0]["totals_model_sha256"]
        == hashlib.sha256(totals_path.read_bytes()).hexdigest()
    )
    assert stored.iloc[0]["over_predicted_prob"] > 0.5
