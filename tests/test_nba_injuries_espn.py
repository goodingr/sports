"""Tests for the ESPN NBA injury ingestion helpers."""

from __future__ import annotations

from unittest.mock import Mock

from src.data.sources import nba_injuries_espn


def test_format_iso_handles_none_and_parses_dates() -> None:
    assert nba_injuries_espn._format_iso(None) is None
    ts = "2025-11-08T06:02Z"
    assert nba_injuries_espn._format_iso(ts) == "2025-11-08T06:02:00Z"


def test_get_athlete_uses_cache(monkeypatch) -> None:
    fake_payload = {"displayName": "Test Player"}
    fetch = Mock(return_value=fake_payload)
    cache: dict[str, dict[str, str]] = {}
    monkeypatch.setattr(nba_injuries_espn, "_safe_fetch", fetch)

    ref = "https://example.com/athlete/1?foo=bar"
    result = nba_injuries_espn._get_athlete(ref, timeout=1, cache=cache)
    assert result == fake_payload
    fetch.assert_called_once()

    # Second call should hit cache and not invoke fetch again
    result_cached = nba_injuries_espn._get_athlete(ref, timeout=1, cache=cache)
    assert result_cached == fake_payload
    assert fetch.call_count == 1


def test_fetch_team_injuries_parses_items(monkeypatch) -> None:
    team = {
        "abbreviation": "ATL",
        "displayName": "Atlanta Hawks",
        "season": 2025,
        "injuries": {"$ref": "injury_listing"},
    }
    listing = {
        "items": [
            {
                "$ref": "detail_ref",
            }
        ]
    }
    detail = {
        "athlete": {"$ref": "athlete_ref"},
        "status": "Out",
        "details": {"fantasyStatus": {"description": "OUT"}, "type": "Illness", "returnDate": "2025-11-08T00:00Z"},
        "shortComment": "Short blurb",
        "date": "2025-11-07T12:00Z",
    }
    athlete = {"displayName": "Luke Test", "position": {"abbreviation": "G"}}

    monkeypatch.setattr(
        nba_injuries_espn,
        "_safe_fetch",
        lambda url, timeout: {"athlete_ref": athlete, "injury_listing": listing, "detail_ref": detail}[url],
    )

    cache: dict[str, dict[str, str]] = {"athlete_ref": athlete}
    rows = nba_injuries_espn._fetch_team_injuries(team, timeout=10, athlete_cache=cache)
    assert len(rows) == 1
    row = rows[0]
    assert row["team_code"] == "ATL"
    assert row["player_name"] == "Luke Test"
    assert row["status"] == "Out"
    assert row["practice_status"] == "OUT"
    assert row["season"] == 2025
