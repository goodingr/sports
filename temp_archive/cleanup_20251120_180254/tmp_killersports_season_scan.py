import json
import sys
import time
from pathlib import Path

from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By

sys.path.append(str(Path(__file__).resolve().parent / "src"))
from data.sources.selenium_utils import extract_page_source, get_selenium_driver, wait_for_element  # noqa: E402

QUERY_TEMPLATE = "https://killersports.com/query?filter=NHL&sdql=season%3D{season}&qt=games&show=5000&future=0&init=1"


def count_rows(html: str) -> int:
    soup = BeautifulSoup(html, "lxml")
    table = soup.find("table", id="DT_Table")
    if not table:
        return 0
    body = table.find("tbody")
    rows = body.find_all("tr") if body else table.find_all("tr")
    return sum(1 for tr in rows if tr.find_all("td"))


def main(start: int = 1990, end: int = 2025) -> None:
    results = {}

    season = start
    while season <= end:
        with get_selenium_driver(headless=True, timeout=30) as driver:
            while season <= end:
                url = QUERY_TEMPLATE.format(season=season)
                try:
                    driver.get(url)
                    wait_for_element(driver, By.ID, "DT_Table", timeout=25)
                    html = extract_page_source(driver, wait_seconds=1.5)
                    rows = count_rows(html)
                    results[season] = rows
                    print(f"season {season}: {rows} rows")
                    season += 1
                    time.sleep(0.5)
                except Exception as exc:  # noqa: BLE001
                    print(f"season {season}: driver error ({exc}), refreshing session")
                    break  # leave inner loop to start a fresh driver

    Path("tmp_killersports_season_counts.json").write_text(
        json.dumps(results, indent=2)
    )


if __name__ == "__main__":
    start = int(sys.argv[1]) if len(sys.argv) > 1 else 1990
    end = int(sys.argv[2]) if len(sys.argv) > 2 else 2025
    main(start, end)
