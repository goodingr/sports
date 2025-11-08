"""Fetch NBA injury data via ESPN's public core API."""
from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
from requests import RequestException

from src.db.loaders import store_injury_reports

from .utils import DEFAULT_HEADERS, SourceDefinition, source_run, write_json

LOGGER = logging.getLogger(__name__)

ESPN_CORE_BASE = "https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba"


def _request_json_with_retry(
    url: str,
    *,
    timeout: int,
    params: Optional[Dict[str, Any]] = None,
    attempts: int = 3,
    backoff: float = 1.5,
) -> Dict[str, Any]:
    delay = 1.0
    last_error: Optional[BaseException] = None
    headers = dict(DEFAULT_HEADERS)
    for attempt in range(1, attempts + 1):
        try:
            response = requests.get(url, headers=headers, timeout=timeout, params=params)
            response.raise_for_status()
            return response.json()
        except RequestException as exc:  # pragma: no cover - network failure
            last_error = exc
            LOGGER.warning("ESPN injuries request failed (attempt %s/%s): %s", attempt, attempts, exc)
            if attempt == attempts:
                raise
            time.sleep(delay)
            delay *= backoff
    assert last_error is not None
    raise last_error


def _fetch_json(url: str, *, timeout: int) -> Dict[str, Any]:
    return _request_json_with_retry(url, timeout=timeout)


def _safe_fetch(url: Optional[str], *, timeout: int) -> Optional[Dict[str, Any]]:
    if not url:
        return None
    try:
        return _fetch_json(url, timeout=timeout)
    except RequestException as exc:
        LOGGER.debug("Failed to fetch %s: %s", url, exc)
        return None


def _season_candidates() -> List[int]:
    """Return likely NBA season identifiers for the ESPN core API."""
    now = datetime.utcnow()
    primary = now.year + 1 if now.month >= 7 else now.year
    secondary = primary - 1
    if primary == secondary:
        return [primary]
    return [primary, secondary]


def _fetch_team_payloads(*, timeout: int) -> List[Dict[str, Any]]:
    """Download NBA team metadata for the active season."""
    for season in _season_candidates():
        url = f"{ESPN_CORE_BASE}/seasons/{season}/teams?limit=1000"
        try:
            listing = _fetch_json(url, timeout=timeout)
        except requests.RequestException as exc:
            LOGGER.debug("Failed to fetch NBA teams for season %s: %s", season, exc)
            continue

        teams: List[Dict[str, Any]] = []
        for item in listing.get("items", []):
            team_data = _safe_fetch(item.get("$ref"), timeout=timeout)
            if not team_data:
                continue
            team_data["season"] = season
            teams.append(team_data)

        if teams:
            LOGGER.info("Using ESPN team feed for season %s", season)
            return teams

    LOGGER.warning("Unable to fetch NBA team metadata from ESPN core API")
    return []


def _format_iso(timestamp: Optional[str]) -> Optional[str]:
    if not timestamp:
        return None
    parsed = pd.to_datetime(timestamp, utc=True, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.isoformat().replace("+00:00", "Z")


def _get_athlete(
    ref: Optional[str],
    *,
    timeout: int,
    cache: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    if not ref:
        return {}
    cache_key = ref.split("?")[0]
    if cache_key in cache:
        return cache[cache_key]
    data = _safe_fetch(ref, timeout=timeout)
    if data is None:
        return {}
    cache[cache_key] = data
    return data


def _fetch_team_injuries(
    team: Dict[str, Any],
    *,
    timeout: int,
    athlete_cache: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    injuries_url = (team.get("injuries") or {}).get("$ref")
    if not injuries_url:
        return []

    listing = _safe_fetch(injuries_url, timeout=timeout)
    if not listing:
        return []

    results: List[Dict[str, Any]] = []
    team_code = team.get("abbreviation") or team.get("shortDisplayName") or team.get("name")
    team_name = team.get("displayName") or team.get("name")
    season = team.get("season")

    for item in listing.get("items", []):
        detail = _safe_fetch(item.get("$ref"), timeout=timeout)
        if not detail:
            continue

        athlete = _get_athlete(detail.get("athlete", {}).get("$ref"), timeout=timeout, cache=athlete_cache)
        player_name = athlete.get("displayName") or athlete.get("fullName")
        if not player_name:
            continue

        position_info = athlete.get("position") or {}
        position = position_info.get("abbreviation") or position_info.get("displayName")

        detail_info = detail.get("details") or {}
        fantasy_status = detail_info.get("fantasyStatus") or {}
        status = detail.get("status") or detail.get("type", {}).get("description") or fantasy_status.get("description")
        practice_status = fantasy_status.get("description")
        detail_text = detail.get("shortComment") or detail.get("longComment")
        notes = detail.get("longComment")

        results.append(
            {
                "team_code": team_code,
                "team_name": team_name,
                "player_name": player_name,
                "position": position,
                "status": status,
                "practice_status": practice_status,
                "report_date": _format_iso(detail.get("date")),
                "game_date": _format_iso(detail_info.get("returnDate")),
                "detail": detail_text,
                "notes": notes,
                "injury_type": detail_info.get("type"),
                "season": season,
            }
        )

    return results


def ingest(*, timeout: int = 60) -> str:
    """Fetch NBA injury data from ESPN API."""
    definition = SourceDefinition(
        key="nba_injuries_espn",
        name="NBA injuries from ESPN API",
        league="NBA",
        category="injuries",
        url=ESPN_CORE_BASE,
        default_frequency="daily",
        storage_subdir="nba/injuries_espn",
    )

    output_dir = ""
    with source_run(definition) as run:
        output_dir = str(run.storage_dir)
        LOGGER.info("Fetching NBA injuries from ESPN API")

        teams = _fetch_team_payloads(timeout=timeout)
        athlete_cache: Dict[str, Dict[str, Any]] = {}
        rows: List[Dict[str, Any]] = []

        for team in teams:
            rows.extend(_fetch_team_injuries(team, timeout=timeout, athlete_cache=athlete_cache))

        if not rows:
            run.set_message("No NBA injuries found via ESPN")
            run.set_raw_path(run.storage_dir)
            return output_dir

        df = pd.DataFrame(rows)
        df["league"] = "NBA"
        df["season"] = pd.to_numeric(df["season"], errors="coerce")
        fallback_season = (
            int(df["season"].dropna().iloc[0])
            if not df["season"].dropna().empty
            else datetime.utcnow().year
        )
        df["season"] = df["season"].fillna(fallback_season).astype(int)
        df = df.drop_duplicates(subset=["team_code", "player_name", "status", "report_date"])

        json_path = run.make_path("injuries.json")
        write_json(df.to_dict(orient="records"), json_path)
        run.record_file(json_path, metadata={"rows": len(df)})

        parquet_path = run.make_path("injuries.parquet")
        df.to_parquet(parquet_path, index=False)
        run.record_file(parquet_path, metadata={"rows": len(df)}, records=len(df))

        store_injury_reports(
            df[
                [
                    col
                    for col in df.columns
                    if col
                    in {
                        "team_code",
                        "player_name",
                        "position",
                        "status",
                        "practice_status",
                        "report_date",
                        "game_date",
                        "detail",
                        "season",
                    }
                ]
            ].copy(),
            league="NBA",
            source_key="nba_injuries_espn",
        )

        run.set_records(len(df))
        run.set_message(f"Captured {len(df)} NBA injury rows from ESPN")
        run.set_raw_path(run.storage_dir)

    return output_dir


__all__ = ["ingest"]
