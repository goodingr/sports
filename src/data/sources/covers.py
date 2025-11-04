"""Scrape Covers matchup JSON payloads for NFL and NBA."""

from __future__ import annotations

import json
import logging
from typing import Dict, Optional

import requests
from bs4 import BeautifulSoup

from .utils import DEFAULT_HEADERS, SourceDefinition, source_run, write_json, write_text


LOGGER = logging.getLogger(__name__)

MATCHUP_URLS: Dict[str, str] = {
    "nfl": "https://www.covers.com/sport/football/nfl/matchups",
    "nba": "https://www.covers.com/sport/basketball/nba/matchups",
}

# Historical pages - format: https://www.covers.com/sport/basketball/nba/matchups/{date}
# Date format: YYYY-MM-DD
HISTORICAL_MATCHUP_URLS: Dict[str, str] = {
    "nfl": "https://www.covers.com/sport/football/nfl/matchups/{date}",
    "nba": "https://www.covers.com/sport/basketball/nba/matchups/{date}",
}


def _extract_next_data(html: str) -> Dict:
    soup = BeautifulSoup(html, "lxml")
    script = soup.find("script", id="__NEXT_DATA__")
    if not script:
        raise ValueError("__NEXT_DATA__ script block not found")
    return json.loads(script.text)


def _ingest_covers(league: str, date: Optional[str] = None, timeout: int = 30) -> str:
    definition = SourceDefinition(
        key=f"covers_{league}",
        name=f"Covers {league.upper()} odds",
        league=league.upper(),
        category="odds",
        url=MATCHUP_URLS[league],
        default_frequency="hourly",
        storage_subdir=f"{league}/covers",
    )

    output_dir = ""
    with source_run(definition) as run:
        output_dir = str(run.storage_dir)
        
        if date:
            url = HISTORICAL_MATCHUP_URLS[league].format(date=date)
        else:
            url = MATCHUP_URLS[league]
        
        LOGGER.info("Fetching Covers matchup payload for %s from %s", league.upper(), url)
        response = requests.get(url, timeout=timeout, headers=DEFAULT_HEADERS)
        response.raise_for_status()

        html_path = run.make_path("matchups.html")
        write_text(response.text, html_path)
        run.record_file(html_path, metadata={"url": url})

        try:
            payload = _extract_next_data(response.text)
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Unable to parse Covers JSON payload: %s", exc)
            run.set_message("Stored HTML snapshot; JSON parse failed")
            run.set_raw_path(run.storage_dir)
            return output_dir

        json_path = run.make_path("matchups.json")
        write_json(payload, json_path)

        games = payload
        # Best-effort attempt to locate events count
        total_events = None
        try:
            initial_state = payload.get("props", {}).get("pageProps", {}).get("dehydratedState", {})
            if isinstance(initial_state, dict):
                values = initial_state.get("queries", [])
                for value in values:
                    state = value.get("state", {}) if isinstance(value, dict) else {}
                    data = state.get("data") if isinstance(state, dict) else None
                    if isinstance(data, dict) and "matchups" in data:
                        games = data
                        total_events = len(data.get("matchups", []))
                        break
        except Exception as exc:  # noqa: BLE001
            LOGGER.debug("Could not infer event count from Covers payload: %s", exc)

        run.record_file(
            json_path,
            metadata={"url": url, "events": total_events},
            records=total_events,
        )
        if total_events:
            run.set_records(total_events)
            run.set_message(f"Captured {total_events} Covers matchups")
        run.set_raw_path(run.storage_dir)

    return output_dir


def ingest_nfl(*, date: Optional[str] = None, timeout: int = 30) -> str:
    return _ingest_covers("nfl", date=date, timeout=timeout)


def ingest_nba(*, date: Optional[str] = None, timeout: int = 30) -> str:
    return _ingest_covers("nba", date=date, timeout=timeout)


__all__ = ["ingest_nfl", "ingest_nba"]

