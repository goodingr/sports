"""Exercise the historical Odds API contract without spending provider credits."""

from datetime import datetime, timezone
from unittest.mock import Mock

import pytest

from src.data.config import OddsAPISettings
from src.data.ingest_odds import fetch_odds_history


def test_historical_odds_use_the_providers_actual_snapshot_time(monkeypatch):
    response = Mock(status_code=200, headers={})
    response.json.return_value = {
        "timestamp": "2026-01-18T11:55:00Z",
        "data": [{"id": "fixture"}],
    }
    request = Mock(return_value=response)
    monkeypatch.setattr("src.data.ingest_odds.requests.get", request)
    settings = OddsAPISettings(api_key="test-key", sport="basketball_nba", market="totals")

    payload = fetch_odds_history(settings, datetime(2026, 1, 18, 12, tzinfo=timezone.utc))

    assert request.call_args.args[0].endswith("/v4/historical/sports/basketball_nba/odds")
    assert request.call_args.kwargs["params"]["date"] == "2026-01-18T12:00:00Z"
    assert payload["source"] == "the-odds-api-history"
    assert payload["fetched_at"] == "2026-01-18T11:55:00+00:00"
    assert payload["results"] == [{"id": "fixture"}]
    assert "apiKey" not in payload["request"]["params"]


@pytest.mark.parametrize("timestamp", [None, "2026-01-18T11:55:00", "2026-01-18T12:00:01Z"])
def test_historical_odds_fail_on_ambiguous_or_future_snapshot_times(monkeypatch, timestamp):
    response = Mock(status_code=200, headers={})
    response.json.return_value = {"timestamp": timestamp, "data": []}
    monkeypatch.setattr("src.data.ingest_odds.requests.get", Mock(return_value=response))
    settings = OddsAPISettings(api_key="test-key", sport="basketball_nba")

    with pytest.raises(RuntimeError, match="invalid as-of timestamp"):
        fetch_odds_history(settings, datetime(2026, 1, 18, 12, tzinfo=timezone.utc))
