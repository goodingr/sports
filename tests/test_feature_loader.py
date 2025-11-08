"""Tests for FeatureLoader advanced metrics and shared directories."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from src.models.feature_loader import FeatureLoader, _latest_source_directory


def _write_parquet(tmp_path: Path, parts: list[str], name: str, df: pd.DataFrame) -> Path:
    target = tmp_path.joinpath(*parts)
    target.mkdir(parents=True, exist_ok=True)
    path = target / name
    df.to_parquet(path, index=False)
    return path


def test_latest_source_directory_uses_override(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    base_raw = tmp_path / "data" / "raw" / "sources"
    soccer_dir = base_raw / "soccer" / "advanced_stats" / "2025-01-01T00-00-00Z"
    soccer_dir.mkdir(parents=True, exist_ok=True)
    (soccer_dir / "advanced_stats.parquet").write_text("placeholder")

    monkeypatch.setattr("src.models.feature_loader.RAW_SOURCES_DIR", base_raw)

    latest = _latest_source_directory("EPL", "advanced_stats")
    assert latest == soccer_dir


def test_get_advanced_metric_reads_soccer_stats(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    base_raw = tmp_path / "data" / "raw" / "sources"
    monkeypatch.setattr("src.models.feature_loader.RAW_SOURCES_DIR", base_raw)

    data = pd.DataFrame(
        [
            {"league": "EPL", "team": "Arsenal", "team_code": "ARS", "season": 2024, "xG": 75.5},
            {"league": "EPL", "team": "Chelsea", "team_code": "CHE", "season": 2024, "xG": 60.0},
        ]
    )
    _write_parquet(
        tmp_path,
        ["data", "raw", "sources", "soccer", "advanced_stats", "2025-01-01T00-00-00Z"],
        "advanced_stats.parquet",
        data,
    )

    loader = FeatureLoader("EPL")

    value = loader.get_advanced_metric("Arsenal", "xG", season=2024, default=0.0)
    assert value == pytest.approx(75.5)

    missing = loader.get_advanced_metric("Arsenal", "shots", season=2024, default=-1.0)
    assert missing == -1.0


def test_get_advanced_metric_handles_team_aliases(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    base_raw = tmp_path / "data" / "raw" / "sources"
    monkeypatch.setattr("src.models.feature_loader.RAW_SOURCES_DIR", base_raw)

    df = pd.DataFrame(
        [
            {"league": "NBA", "team": "Los Angeles Lakers", "season": 2024, "E_NET_RATING": 5.4},
        ]
    )
    _write_parquet(
        tmp_path,
        ["data", "raw", "sources", "nba", "advanced_stats", "2025-02-01T00-00-00Z"],
        "advanced_stats.parquet",
        df,
    )

    loader = FeatureLoader("NBA")
    # Passing alias "LAL" should normalize and match the row above
    value = loader.get_advanced_metric("LAL", "E_NET_RATING", season=2024, default=0.0)
    assert value == pytest.approx(5.4)


def test_load_advanced_stats_filters_league(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    base_raw = tmp_path / "data" / "raw" / "sources"
    monkeypatch.setattr("src.models.feature_loader.RAW_SOURCES_DIR", base_raw)

    df = pd.DataFrame(
        [
            {"league": "EPL", "team": "Arsenal"},
            {"league": "NBA", "team": "Lakers"},
        ]
    )
    _write_parquet(
        tmp_path,
        ["data", "raw", "sources", "soccer", "advanced_stats", "2025-03-01T00-00-00Z"],
        "advanced_stats.parquet",
        df,
    )

    loader = FeatureLoader("EPL")
    loaded = loader.load_advanced_stats()
    assert set(loaded["league"].unique()) <= {"EPL"}
