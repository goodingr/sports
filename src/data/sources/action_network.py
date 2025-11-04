"""Scrape Action Network moneyline movement for NFL and NBA."""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime
from typing import List, Optional

import pandas as pd
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By

from .selenium_utils import extract_page_source, get_selenium_driver, wait_for_element
from .utils import SourceDefinition, source_run, write_dataframe, write_text


LOGGER = logging.getLogger(__name__)

BASE_URL = "https://www.actionnetwork.com/{sport}/odds"

SPORT_PATHS = {
    "nfl": "nfl",
    "nba": "nba",
}




def _fetch_json_blob(html: str) -> Optional[dict]:
    marker = "__NEXT_DATA__"
    start = html.find(marker)
    if start == -1:
        return None

    start = html.find("{", start)
    end = html.find("</script>", start)
    if start == -1 or end == -1:
        return None

    blob = html[start:end]
    try:
        return json.loads(blob)
    except json.JSONDecodeError:
        LOGGER.warning("Failed to parse Action Network JSON blob")
        return None


def _extract_moneyline_rows(payload: dict, sport: str) -> pd.DataFrame:
    try:
        queries = payload["props"]["pageProps"]["dehydratedState"]["queries"]
    except (KeyError, TypeError):
        return pd.DataFrame()

    rows: List[dict] = []
    for query in queries:
        state = query.get("state", {})
        data = state.get("data") if isinstance(state, dict) else None
        if not isinstance(data, dict):
            continue

        events = data.get("events") or data.get("matchups")
        if not isinstance(events, list):
            continue

        for event in events:
            try:
                event_id = event.get("eventId") or event.get("id")
                teams = event["teams"]
                bookmakers = event.get("books") or []
            except (KeyError, TypeError):
                continue

            for book in bookmakers:
                book_name = book.get("name") or book.get("key")
                moneyline = book.get("moneyline") or {}
                lines = moneyline.get("lines") or []
                for line in lines:
                    rows.append(
                        {
                            "sport": sport.upper(),
                            "event_id": event_id,
                            "book": book_name,
                            "team": line.get("participant").get("shortName") if line.get("participant") else None,
                            "moneyline": line.get("price"),
                            "timestamp": line.get("timestamp") or event.get("startTime"),
                        }
                    )

    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["retrieved_at"] = datetime.utcnow().isoformat()
    return df


def _ingest(sport: str, *, timeout: int) -> str:
    definition = SourceDefinition(
        key=f"action_network_{sport}",
        name=f"Action Network {sport.upper()} odds",
        league=sport.upper(),
        category="odds_movement",
        url=BASE_URL.format(sport=SPORT_PATHS[sport]),
        default_frequency="hourly",
        storage_subdir=f"{sport}/action_network",
    )

    output_dir = ""
    with source_run(definition) as run:
        output_dir = str(run.storage_dir)

        url = BASE_URL.format(sport=SPORT_PATHS[sport])
        LOGGER.info("Fetching Action Network odds for %s using Selenium", sport.upper())
        
        with get_selenium_driver(headless=True, timeout=timeout) as driver:
            driver.get(url)
            
            # Wait for AWS WAF challenge to complete (check if challenge-container is gone)
            # Wait up to 60 seconds for page to load
            max_wait = 60
            waited = 0
            while waited < max_wait:
                html = driver.page_source
                if "challenge-container" not in html or "__NEXT_DATA__" in html:
                    # Either challenge passed or page loaded
                    break
                time.sleep(2)
                waited += 2
                LOGGER.debug("Waiting for Action Network page to load... (%d/%d)", waited, max_wait)
            
            # Wait for __NEXT_DATA__ script to be present
            wait_for_element(driver, By.ID, "__NEXT_DATA__", timeout=30)
            
            # Extract fully rendered page source
            html = extract_page_source(driver, wait_seconds=3.0)
            
            html_path = run.make_path("page.html")
            write_text(html, html_path)
            run.record_file(html_path, metadata={"url": url})

            payload = _fetch_json_blob(html)
            if not payload:
                run.set_message("No Action Network JSON payload found")
                run.set_raw_path(run.storage_dir)
                return output_dir

            df = _extract_moneyline_rows(payload, sport)
            if df.empty:
                run.set_message("No Action Network odds rows parsed")
                run.set_raw_path(run.storage_dir)
                return output_dir

            path = run.make_path("moneyline.csv")
            path.parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(str(path), index=False)
            run.record_file(path, metadata={"rows": len(df)}, records=len(df))

            run.set_records(len(df))
            run.set_message(f"Captured {len(df)} Action Network rows")
            run.set_raw_path(run.storage_dir)

    return output_dir


def ingest_nfl(*, timeout: int = 30) -> str:
    return _ingest("nfl", timeout=timeout)


def ingest_nba(*, timeout: int = 30) -> str:
    return _ingest("nba", timeout=timeout)


__all__ = ["ingest_nfl", "ingest_nba"]

