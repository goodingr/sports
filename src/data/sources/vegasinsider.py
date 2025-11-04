"""Scrape historical NBA odds from VegasInsider."""

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

BASE_URL = "https://www.vegasinsider.com/nba/odds/las-vegas/"
HISTORICAL_URL = "https://www.vegasinsider.com/nba/scoreboard/?date={date}"  # Format: YYYY-MM-DD


def _parse_odds_table(html: str, date: Optional[str] = None) -> pd.DataFrame:
    """Parse odds from VegasInsider HTML."""
    soup = BeautifulSoup(html, "lxml")
    rows: List[dict] = []
    
    # Find odds tables - VegasInsider typically uses tables with class containing "odds" or "lines"
    tables = soup.find_all("table", class_=lambda x: x and ("odds" in str(x).lower() or "lines" in str(x).lower()))
    
    if not tables:
        # Try finding any table with moneyline data
        all_tables = soup.find_all("table")
        for table in all_tables:
            if "moneyline" in table.get_text().lower() or "+" in table.get_text():
                tables = [table]
                break
    
    if not tables:
        LOGGER.warning("No odds table found in VegasInsider HTML")
        return pd.DataFrame()
    
    for table in tables:
        tbody = table.find("tbody") or table
        for tr in tbody.find_all("tr", recursive=False):
            cells = tr.find_all(["td", "th"])
            if len(cells) < 3:
                continue
            
            try:
                # Extract team names (usually in first columns)
                team_names = []
                for cell in cells[:3]:
                    text = cell.get_text(strip=True)
                    # Look for team names (usually uppercase or have links)
                    if text and len(text) > 2 and (text.isupper() or cell.find("a")):
                        team_names.append(text)
                
                if len(team_names) < 2:
                    continue
                
                team1, team2 = team_names[0], team_names[1]
                
                # Find moneyline odds (look for + or - followed by numbers)
                import re
                ml_values = []
                for cell in cells:
                    text = cell.get_text(strip=True)
                    if text and ("+" in text or "-" in text):
                        # Extract the number
                        match = re.search(r'([+-]?\d+)', text)
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
    """Ingest VegasInsider NBA odds for a specific date or current odds."""
    definition = SourceDefinition(
        key="vegasinsider_nba",
        name="VegasInsider NBA odds",
        league="NBA",
        category="odds",
        url=BASE_URL,
        default_frequency="hourly",
        storage_subdir="nba/vegasinsider",
    )
    
    output_dir = ""
    with source_run(definition) as run:
        output_dir = str(run.storage_dir)
        
        if date:
            url = HISTORICAL_URL.format(date=date)
        else:
            url = BASE_URL
        
        LOGGER.info("Fetching VegasInsider NBA odds from %s", url)
        
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
                run.set_message("No VegasInsider odds rows parsed")
                run.set_raw_path(run.storage_dir)
                return output_dir
            
            csv_path = run.make_path("odds.csv")
            df.to_csv(csv_path, index=False)
            run.record_file(csv_path, metadata={"rows": len(df), "date": date}, records=len(df))
            
            run.set_records(len(df))
            run.set_message(f"Captured {len(df)} VegasInsider odds rows")
            run.set_raw_path(run.storage_dir)
    
    return output_dir


__all__ = ["ingest"]

