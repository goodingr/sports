"""
Web scraper for ESPN NBA injuries page.
Fallback source when API is unavailable.
"""
import logging
import re
from datetime import datetime, timezone
from typing import Dict, List, Optional

import requests
from bs4 import BeautifulSoup

LOGGER = logging.getLogger(__name__)

ESPN_INJURIES_URL = "https://www.espn.com/nba/injuries"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/91.0.4472.124 Safari/537.36"
)


def scrape_injuries() -> List[Dict]:
    """
    Scrape injury data from ESPN NBA injuries page.
    Returns a list of dictionaries containing injury details.
    """
    try:
        response = requests.get(
            ESPN_INJURIES_URL,
            headers={"User-Agent": USER_AGENT},
            timeout=30,
        )
        response.raise_for_status()
    except requests.RequestException as exc:
        LOGGER.error("Failed to fetch ESPN injuries page: %s", exc)
        return []

    soup = BeautifulSoup(response.content, "html.parser")
    injuries = []
    
    # ESPN injuries page structure:
    # Divs with class 'Table__Title' contain the team name
    # Followed by a Table with rows for each player
    
    # Finding all team sections
    # The structure is often:
    # <div class="ResponsiveTable">
    #   <div class="Table__Title">Team Name</div>
    #   <table class="Table">...</table>
    # </div>
    
    tables = soup.find_all("div", class_="ResponsiveTable")
    
    for table_div in tables:
        title_div = table_div.find("div", class_="Table__Title")
        if not title_div:
            continue
            
        team_name = title_div.get_text(strip=True)
        
        # Find the table body
        tbody = table_div.find("tbody")
        if not tbody:
            continue
            
        rows = tbody.find_all("tr")
        for row in rows:
            cols = row.find_all("td")
            if not cols:
                continue
                
            # Expected columns: Name, Position, Date, Status, Comment
            # But sometimes it varies. Usually:
            # 0: Name (with link)
            # 1: Position
            # 2: Date
            # 3: Status
            # 4: Comment
            
            if len(cols) < 4:
                continue
                
            name_col = cols[0]
            pos_col = cols[1]
            date_col = cols[2]
            status_col = cols[3]
            comment_col = cols[4] if len(cols) > 4 else None
            
            player_name = name_col.get_text(strip=True)
            player_id = None
            
            # Extract player ID from link if available
            # Link format: https://www.espn.com/nba/player/_/id/12345/firstname-lastname
            link = name_col.find("a")
            if link and link.get("href"):
                match = re.search(r"/id/(\d+)/", link["href"])
                if match:
                    player_id = match.group(1)
            
            position = pos_col.get_text(strip=True)
            date_str = date_col.get_text(strip=True)
            status = status_col.get_text(strip=True)
            comment = comment_col.get_text(strip=True) if comment_col else ""
            
            injuries.append({
                "team": team_name,
                "player_name": player_name,
                "player_id": player_id,
                "position": position,
                "date": date_str,
                "status": status,
                "comment": comment,
                "scraped_at": datetime.now(timezone.utc).isoformat(),
            })
            
    LOGGER.info("Scraped %d injuries from ESPN", len(injuries))
    return injuries


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    data = scrape_injuries()
    for item in data[:5]:
        print(item)
