from datetime import date
import pandas as pd

from src.models import forward_test


class DummyResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self) -> None:  # pragma: no cover - nothing to do
        return

    def json(self):
        return self._payload


def test_fetch_espn_cfb_scores_parses_completed_games(monkeypatch):
    payload = {
        "events": [
            {
                "competitions": [
                    {
                        "status": {"type": {"state": "post"}},
                        "competitors": [
                            {
                                "homeAway": "home",
                                "team": {"displayName": "Akron Zips"},
                                "score": "44",
                            },
                            {
                                "homeAway": "away",
                                "team": {"displayName": "Massachusetts Minutemen"},
                                "score": "10",
                            },
                        ],
                    }
                ]
            },
            {
                "competitions": [
                    {
                        "status": {"type": {"state": "in"}},
                        "competitors": [
                            {
                                "homeAway": "home",
                                "team": {"displayName": "Miami (OH) RedHawks"},
                                "score": "17",
                            },
                            {
                                "homeAway": "away",
                                "team": {"displayName": "Ohio Bobcats"},
                                "score": "24",
                            },
                        ],
                    }
                ]
            },
        ]
    }

    def fake_get(url, params, timeout):
        assert "scoreboard" in url
        assert params["dates"] == "20251104"
        return DummyResponse(payload)

    monkeypatch.setattr("requests.get", fake_get)

    scores = forward_test._fetch_espn_cfb_scores([date(2025, 11, 4)])

    key = ("AKR", "MASSACHUSETTS", date(2025, 11, 4))
    assert key in scores
    assert scores[key] == (44, 10)
    # in-progress game should be ignored
    assert ("M-OH", "OHIO", date(2025, 11, 4)) not in scores


def test_update_results_uses_espn_cfb_scores(monkeypatch, tmp_path):
    df = pd.DataFrame(
        {
            "game_id": ["cfb_game_1"],
            "league": ["CFB"],
            "home_team": ["AKR"],
            "away_team": ["MASSACHUSETTS"],
            "commence_time": [pd.Timestamp("2025-11-04T18:00:00Z")],
            "home_edge": [0.1],
            "away_edge": [0.05],
            "result": [pd.NA],
            "home_score": [pd.NA],
            "away_score": [pd.NA],
        }
    )

    forward_test_dir = tmp_path / "forward_test"
    forward_test_dir.mkdir()
    master_path = forward_test_dir / "predictions_master.parquet"
    df.to_parquet(master_path, index=False)

    monkeypatch.setattr(forward_test, "FORWARD_TEST_DIR", forward_test_dir)

    class StubConn:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, *args, **kwargs):
            class Cursor:
                def fetchall(self_inner):
                    return []

            return Cursor()

    monkeypatch.setattr(forward_test, "connect", lambda: StubConn())
    monkeypatch.setattr(forward_test, "_fetch_recent_scores", lambda *_, **__: {})
    monkeypatch.setattr(
        forward_test,
        "_fetch_espn_cfb_scores",
        lambda dates: {("AKR", "MASSACHUSETTS", date(2025, 11, 4)): (44, 10)},
    )

    forward_test.update_results(league="CFB")

    updated = pd.read_parquet(master_path)
    assert updated.loc[0, "result"] == "home"
    assert updated.loc[0, "home_score"] == 44
    assert updated.loc[0, "away_score"] == 10
    assert pd.notna(updated.loc[0, "result_updated_at"])
