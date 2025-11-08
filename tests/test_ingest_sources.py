"""Tests for the ingestion orchestrator skip logic."""

from __future__ import annotations

from typing import Any, Dict, List

from src.data import ingest_sources

HANDLER_PATH = "tests.test_ingest_sources:_dummy_handler"


def _dummy_handler(**kwargs: Any) -> None:  # pragma: no cover - used via handler path
    return None


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
