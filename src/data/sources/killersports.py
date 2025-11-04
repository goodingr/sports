"""Scrape historical NBA odds from Killersports.com."""

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

BASE_URL = "https://killersports.com/nba/query"
# Historical odds available via date parameter or game lookup


def _parse_odds_table(html: str, date: Optional[str] = None) -> pd.DataFrame:
    """Parse odds from Killersports HTML table."""
    soup = BeautifulSoup(html, "lxml")
    rows: List[dict] = []
    
    # Killersports typically uses tables with game data
    tables = soup.find_all("table")
    
    for table in tables:
        tbody = table.find("tbody") or table
        for tr in tbody.find_all("tr", recursive=False):
            cells = tr.find_all(["td", "th"])
            if len(cells) < 3:
                continue
            
            try:
                # Extract team names and scores
                team_names = []
                scores = []
                moneylines = []
                
                for cell in cells:
                    text = cell.get_text(strip=True)
                    # Look for team names (usually links or in specific cells)
                    if cell.find("a"):
                        team_names.append(text)
                    # Look for scores (numbers)
                    if text.isdigit() and len(text) <= 3:
                        scores.append(int(text))
                    # Look for moneyline odds (+ or - followed by numbers)
                    if text and ("+" in text or "-" in text):
                        import re
                        match = re.search(r'([+-]?\d+)', text)
                        if match:
                            try:
                                moneylines.append(int(match.group(1)))
                            except ValueError:
                                continue
                
                # If we found two teams and two moneylines, create records
                if len(team_names) >= 2 and len(moneylines) >= 2:
                    team1, team2 = team_names[0], team_names[1]
                    ml1, ml2 = moneylines[0], moneylines[1]
                    
                    rows.append({
                        "date": date or datetime.now().strftime("%Y-%m-%d"),
                        "team": team1,
                        "opponent": team2,
                        "moneyline": ml1,
                        "retrieved_at": datetime.utcnow().isoformat(),
                    })
                    rows.append({
                        "date": date or datetime.now().strftime("%Y-%m-%d"),
                        "team": team2,
                        "opponent": team1,
                        "moneyline": ml2,
                        "retrieved_at": datetime.utcnow().isoformat(),
                    })
                    
            except Exception as exc:  # noqa: BLE001
                LOGGER.debug("Error parsing row: %s", exc)
                continue
    
    return pd.DataFrame(rows)


def _query_games_by_date(date: str, timeout: int = 30) -> pd.DataFrame:
    """Query Killersports for games on a specific date."""
    # Killersports query format: https://killersports.com/nba/query?sd=2024-01-15
    url = f"{BASE_URL}?sd={date}"
    
    with get_selenium_driver(headless=True, timeout=timeout) as driver:
        driver.get(url)
        
        # Wait for page to load
        time.sleep(3)
        
        # Try to find results table
        wait_for_element(driver, By.TAG_NAME, "table", timeout=20)
        
        html = extract_page_source(driver, wait_seconds=2.0)
        
        return _parse_odds_table(html, date=date)


def ingest(*, date: Optional[str] = None, timeout: int = 30) -> str:
    """Ingest Killersports NBA odds for a specific date or current odds."""
    definition = SourceDefinition(
        key="killersports_nba",
        name="Killersports NBA odds",
        league="NBA",
        category="odds",
        url=BASE_URL,
        default_frequency="hourly",
        storage_subdir="nba/killersports",
    )
    
    output_dir = ""
    with source_run(definition) as run:
        output_dir = str(run.storage_dir)
        
        if date:
            LOGGER.info("Fetching Killersports NBA odds for %s", date)
            df = _query_games_by_date(date, timeout=timeout)
            
            if df.empty:
                run.set_message("No Killersports odds rows parsed")
                run.set_raw_path(run.storage_dir)
                return output_dir
            
            csv_path = run.make_path(f"odds_{date}.csv")
            df.to_csv(csv_path, index=False)
            run.record_file(csv_path, metadata={"rows": len(df), "date": date}, records=len(df))
            
            run.set_records(len(df))
            run.set_message(f"Captured {len(df)} Killersports odds rows")
        else:
            # For current odds, try the main query page
            url = BASE_URL
            LOGGER.info("Fetching Killersports NBA odds from %s", url)
            
            with get_selenium_driver(headless=True, timeout=timeout) as driver:
                driver.get(url)
                time.sleep(3)
                wait_for_element(driver, By.TAG_NAME, "table", timeout=20)
                html = extract_page_source(driver, wait_seconds=2.0)
                
                html_path = run.make_path("page_current.html")
                write_text(html, html_path)
                run.record_file(html_path, metadata={"url": url})
                
                df = _parse_odds_table(html)
                
                if df.empty:
                    run.set_message("No Killersports odds rows parsed")
                    run.set_raw_path(run.storage_dir)
                    return output_dir
                
                csv_path = run.make_path("odds.csv")
                df.to_csv(csv_path, index=False)
                run.record_file(csv_path, metadata={"rows": len(df)}, records=len(df))
                
                run.set_records(len(df))
                run.set_message(f"Captured {len(df)} Killersports odds rows")
        
        run.set_raw_path(run.storage_dir)
    
    return output_dir


__all__ = ["ingest"]

