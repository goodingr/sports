"""Scrape historical NBA odds from OddsShark."""

from __future__ import annotations

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

BASE_URL = "https://www.oddsshark.com/nba/odds"
HISTORICAL_URL = "https://www.oddsshark.com/nba/scores/{date}"  # Format: YYYY-MM-DD


def _parse_odds_table(html: str, date: Optional[str] = None) -> pd.DataFrame:
    """Parse odds from OddsShark HTML table."""
    soup = BeautifulSoup(html, "lxml")
    rows: List[dict] = []
    
    # Find the main odds table
    table = soup.find("table", class_="odds-table") or soup.find("table", {"id": "odds-table"})
    if not table:
        # Try finding any table with odds data
        tables = soup.find_all("table")
        for t in tables:
            if "odds" in t.get("class", []) or "moneyline" in t.text.lower():
                table = t
                break
    
    if not table:
        LOGGER.warning("No odds table found in OddsShark HTML")
        return pd.DataFrame()
    
    tbody = table.find("tbody")
    if not tbody:
        tbody = table
    
    for tr in tbody.find_all("tr", recursive=False):
        cells = tr.find_all(["td", "th"])
        if len(cells) < 3:
            continue
        
        try:
            # Extract team names and odds
            team_cells = [c for c in cells if c.find("a") or c.find("span", class_="team")]
            if len(team_cells) < 2:
                continue
            
            team1 = team_cells[0].get_text(strip=True)
            team2 = team_cells[1].get_text(strip=True)
            
            # Find moneyline odds (usually in later columns)
            import re
            moneyline_cells = [c for c in cells if any(char in c.get_text() for char in ["+", "-"])]
            
            ml_values = []
            for ml_cell in moneyline_cells[:2]:
                ml_text = ml_cell.get_text(strip=True)
                # Extract number from text (handle cases like "+150" or "-180")
                match = re.search(r'([+-]?\d+)', ml_text)
                if match:
                    try:
                        ml_values.append(int(match.group(1)))
                    except ValueError:
                        continue
            
            if len(ml_values) >= 2:
                rows.append({
                    "date": date or datetime.now().strftime("%Y-%m-%d"),
                    "team": team1,
                    "opponent": team2,
                    "moneyline": ml_values[0],
                    "retrieved_at": datetime.utcnow().isoformat(),
                })
                rows.append({
                    "date": date or datetime.now().strftime("%Y-%m-%d"),
                    "team": team2,
                    "opponent": team1,
                    "moneyline": ml_values[1],
                    "retrieved_at": datetime.utcnow().isoformat(),
                })
                    
        except Exception as exc:  # noqa: BLE001
            LOGGER.debug("Error parsing row: %s", exc)
            continue
    
    return pd.DataFrame(rows)


def ingest(*, date: Optional[str] = None, timeout: int = 30) -> str:
    """Ingest OddsShark NBA odds for a specific date or current odds."""
    definition = SourceDefinition(
        key="oddsshark_nba",
        name="OddsShark NBA odds",
        league="NBA",
        category="odds",
        url=BASE_URL,
        default_frequency="hourly",
        storage_subdir="nba/oddsshark",
    )
    
    output_dir = ""
    with source_run(definition) as run:
        output_dir = str(run.storage_dir)
        
        if date:
            url = HISTORICAL_URL.format(date=date)
        else:
            url = BASE_URL
        
        LOGGER.info("Fetching OddsShark NBA odds from %s", url)
        
        with get_selenium_driver(headless=True, timeout=timeout) as driver:
            driver.get(url)
            
            # Wait for page to load
            time.sleep(3)
            
            # Try to find odds table
            wait_for_element(driver, By.TAG_NAME, "table", timeout=20)
            
            html = extract_page_source(driver, wait_seconds=2.0)
            
            html_path = run.make_path(f"page_{date or 'current'}.html")
            write_text(html, html_path)
            run.record_file(html_path, metadata={"url": url, "date": date})
            
            df = _parse_odds_table(html, date=date)
            
            if df.empty:
                run.set_message("No OddsShark odds rows parsed")
                run.set_raw_path(run.storage_dir)
                return output_dir
            
            csv_path = run.make_path("odds.csv")
            df.to_csv(csv_path, index=False)
            run.record_file(csv_path, metadata={"rows": len(df), "date": date}, records=len(df))
            
            run.set_records(len(df))
            run.set_message(f"Captured {len(df)} OddsShark odds rows")
            run.set_raw_path(run.storage_dir)
    
    return output_dir


__all__ = ["ingest"]

