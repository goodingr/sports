"""Tests for the ingestion orchestrator skip logic."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List

import pytest
import requests

from src.data import ingest_odds, ingest_sources

HANDLER_PATH = "tests.test_ingest_sources:_dummy_handler"


def _dummy_handler(**kwargs: Any) -> None:  # pragma: no cover - used via handler path
    return None


def test_odds_request_metadata_excludes_credentials(monkeypatch) -> None:
    api_key = "fake-test-key"
    settings = ingest_odds.OddsAPISettings(api_key=api_key)
    def fake_get(url, *, params, **kwargs):
        assert params["apiKey"] == api_key
        response = requests.Response()
        response.status_code = 200
        response._content = (
            b'{"timestamp":"2026-01-01T00:00:00Z","data":[]}' if "/historical/" in url else b"[]"
        )
        return response

    monkeypatch.setattr(ingest_odds.OddsAPIKeyManager, "get_available_keys", lambda: [api_key])
    monkeypatch.setattr(ingest_odds.OddsAPIKeyManager, "get_current_key", lambda: api_key)
    monkeypatch.setattr(ingest_odds.requests, "get", fake_get)
    live = ingest_odds.fetch_odds(settings)
    history = ingest_odds.fetch_odds_history(settings, datetime(2026, 1, 1, tzinfo=timezone.utc))
    for payload in (live, history):
        assert "apiKey" not in payload["request"]["params"]
        assert payload["request"]["params"]["regions"] == settings.region

    def failed_get(url, *, params, **kwargs):
        raise requests.ConnectionError(f"Connection failed at {url}?apiKey={params['apiKey']}")

    monkeypatch.setattr(ingest_odds.requests, "get", failed_get)
    with pytest.raises(RuntimeError, match="^Odds API connection failed$"):
        ingest_odds.fetch_odds(settings)


def test_required_fresh_odds_distinguishes_provider_error_from_empty_success(tmp_path, monkeypatch) -> None:
    key = "fake-test-key"
    settings = ingest_odds.OddsAPISettings(api_key=key, sport="basketball_nba")
    monkeypatch.setattr(ingest_odds.OddsAPISettings, "from_env", lambda _: settings)
    monkeypatch.setattr(ingest_odds.OddsAPIKeyManager, "get_available_keys", lambda: [key])
    monkeypatch.setattr(ingest_odds.OddsAPIKeyManager, "get_current_key", lambda: key)
    monkeypatch.setattr(ingest_odds, "ensure_directories", lambda: None)
    monkeypatch.setattr(ingest_odds, "RAW_DATA_DIR", tmp_path)
    loaded = []
    monkeypatch.setattr(ingest_odds, "load_odds_snapshot", lambda payload, **_: loaded.append(payload))

    response = requests.Response()
    response.status_code = 404
    monkeypatch.setattr(ingest_odds.requests, "get", lambda *args, **kwargs: response)
    with pytest.raises(RuntimeError, match="did not refresh basketball_nba"):
        ingest_odds.run(require_fresh=True)
    assert loaded == []
    assert list(tmp_path.rglob("odds_*.json"))

    # Standard ingestion continues to tolerate an unavailable off-season sport.
    ingest_odds.run(force_refresh=True)
    assert loaded[-1]["error"] == "404 Not Found"

    response.status_code = 200
    response._content = b"[]"
    ingest_odds.run(require_fresh=True)
    assert "error" not in loaded[-1]
    assert loaded[-1]["results"] == []


def test_entry_run_mode_defaults() -> None:
    assert ingest_sources._entry_run_mode({"category": "historical_stats"}) == "bootstrap"
    assert ingest_sources._entry_run_mode({"category": "odds"}) == "continuous"
    assert (
        ingest_sources._entry_run_mode({"category": "odds", "run_mode": "bootstrap"})
        == "bootstrap"
    )
    assert (
        ingest_sources._entry_run_mode({"category": "historical_stats", "run_mode": "continuous"})
        == "continuous"
    )


def _build_config(entries: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    return {"test": entries}


def test_run_sources_skips_bootstrap_when_already_successful(monkeypatch) -> None:
    executed: List[str] = []

    def fake_invoke(handler_path: str, *, kwargs: Dict[str, Any]) -> None:
        executed.append(kwargs["tag"])

    monkeypatch.setattr(ingest_sources, "_invoke_handler", fake_invoke)
    monkeypatch.setattr(
        ingest_sources,
        "has_successful_source_run",
        lambda key: key == "bootstrap_source",
    )

    config = _build_config(
        [
            {
                "key": "bootstrap_source",
                "category": "historical_stats",
                "handler": HANDLER_PATH,
                "enabled": True,
                "params": {"tag": "bootstrap"},
            },
            {
                "key": "continuous_source",
                "category": "odds",
                "handler": HANDLER_PATH,
                "enabled": True,
                "params": {"tag": "continuous"},
            },
        ]
    )

    ingest_sources.run_sources(
        config,
        leagues=None,
        sources=None,
        seasons=[],
        timeout=None,
        dry_run=False,
        full_refresh=False,
    )

    assert executed == ["continuous"]


def test_run_sources_full_refresh_executes_bootstrap(monkeypatch) -> None:
    executed: List[str] = []

    def fake_invoke(handler_path: str, *, kwargs: Dict[str, Any]) -> None:
        executed.append(kwargs["tag"])

    monkeypatch.setattr(ingest_sources, "_invoke_handler", fake_invoke)
    monkeypatch.setattr(
        ingest_sources,
        "has_successful_source_run",
        lambda key: key == "bootstrap_source",
    )

    config = _build_config(
        [
            {"key": "bootstrap_source", "category": "historical_stats", "handler": HANDLER_PATH, "enabled": True, "params": {"tag": "bootstrap"}}
        ]
    )

    ingest_sources.run_sources(
        config,
        leagues=None,
        sources=None,
        seasons=[],
        timeout=None,
        dry_run=False,
        full_refresh=True,
    )

    assert executed == ["bootstrap"]
