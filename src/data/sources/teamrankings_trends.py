"""Scrape TeamRankings NBA over/under trends and betting data."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import List, Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup

from .utils import DEFAULT_HEADERS, SourceDefinition, source_run, write_dataframe, write_text

LOGGER = logging.getLogger(__name__)

BASE_URL = "https://www.teamrankings.com/nba/trends/ou_trends"
# Historical data available via date ranges or season filters


def _parse_trends_table(html: str, date: Optional[str] = None) -> pd.DataFrame:
    """Parse over/under trends table from TeamRankings."""
    soup = BeautifulSoup(html, "lxml")
    rows: List[dict] = []
    
    # Find the main trends table
    table = soup.find("table", class_=lambda x: x and ("trends" in str(x).lower() or "ou" in str(x).lower()))
    
    if not table:
        # Try finding any table
        tables = soup.find_all("table")
        for t in tables:
            if "over" in t.get_text().lower() or "under" in t.get_text().lower():
                table = t
                break
    
    if not table:
        LOGGER.warning("No trends table found in TeamRankings HTML")
        return pd.DataFrame()
    
    tbody = table.find("tbody") or table
    
    for tr in tbody.find_all("tr", recursive=False):
        cells = tr.find_all(["td", "th"])
        if len(cells) < 3:
            continue
        
        try:
            # Extract team name (usually first column)
            team_name = None
            for cell in cells[:3]:
                text = cell.get_text(strip=True)
                if text and len(text) > 2:
                    # Look for team name (usually has a link or is in first column)
                    if cell.find("a") or (len(text) > 2 and not text.isdigit() and not any(char in text for char in ["+", "-", "%"])):
                        team_name = text
                        break
            
            if not team_name:
                continue
            
            # Extract over/under percentages and totals
            over_pct = None
            under_pct = None
            avg_total = None
            
            for cell in cells:
                text = cell.get_text(strip=True)
                # Look for percentages
                if "%" in text:
                    try:
                        pct = float(text.replace("%", ""))
                        if over_pct is None:
                            over_pct = pct
                        elif under_pct is None:
                            under_pct = pct
                    except ValueError:
                        pass
                # Look for totals (numbers around 200-250 for NBA)
                try:
                    total = float(text)
                    if 180 <= total <= 280:
                        avg_total = total
                except ValueError:
                    pass
            
            # Extract moneyline if available (look for + or -)
            moneyline = None
            for cell in cells:
                text = cell.get_text(strip=True)
                if text and ("+" in text or "-" in text):
                    import re
                    match = re.search(r'([+-]?\d+)', text)
                    if match:
                        try:
                            moneyline = int(match.group(1))
                            break
                        except ValueError:
                            continue
            
            rows.append({
                "date": date or datetime.now().strftime("%Y-%m-%d"),
                "team": team_name,
                "over_pct": over_pct,
                "under_pct": under_pct,
                "avg_total": avg_total,
                "moneyline": moneyline,
                "retrieved_at": datetime.utcnow().isoformat(),
            })
                    
        except Exception as exc:  # noqa: BLE001
            LOGGER.debug("Error parsing row: %s", exc)
            continue
    
    return pd.DataFrame(rows)


def ingest(*, date: Optional[str] = None, timeout: int = 30) -> str:
    """Ingest TeamRankings NBA trends for a specific date or current trends."""
    definition = SourceDefinition(
        key="teamrankings_trends_nba",
        name="TeamRankings NBA trends",
        league="NBA",
        category="trends",
        url=BASE_URL,
        default_frequency="daily",
        storage_subdir="nba/teamrankings_trends",
    )
    
    output_dir = ""
    with source_run(definition) as run:
        output_dir = str(run.storage_dir)
        
        url = BASE_URL
        if date:
            # TeamRankings may support date filtering via query params
            url = f"{BASE_URL}?date={date}"
        
        LOGGER.info("Fetching TeamRankings NBA trends from %s", url)
        
        response = requests.get(url, timeout=timeout, headers=DEFAULT_HEADERS)
        response.raise_for_status()
        
        html_path = run.make_path(f"trends_{date or 'current'}.html")
        write_text(response.text, html_path)
        run.record_file(html_path, metadata={"url": url, "date": date})
        
        df = _parse_trends_table(response.text, date=date)
        
        if df.empty:
            # Try using pandas read_html as fallback
            try:
                tables = pd.read_html(response.text)
                if tables:
                    df = tables[0]
                    df["date"] = date or datetime.now().strftime("%Y-%m-%d")
                    df["retrieved_at"] = datetime.utcnow().isoformat()
                    LOGGER.info("Used pandas read_html fallback, found %d rows", len(df))
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("Pandas read_html also failed: %s", exc)
        
        if df.empty:
            run.set_message("No TeamRankings trends rows parsed")
            run.set_raw_path(run.storage_dir)
            return output_dir
        
        csv_path = run.make_path("trends.csv")
        df.to_csv(csv_path, index=False)
        run.record_file(csv_path, metadata={"rows": len(df), "date": date}, records=len(df))
        
        run.set_records(len(df))
        run.set_message(f"Captured {len(df)} TeamRankings trends rows")
        run.set_raw_path(run.storage_dir)
    
    return output_dir


__all__ = ["ingest"]

