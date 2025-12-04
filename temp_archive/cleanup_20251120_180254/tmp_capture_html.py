import sys
from pathlib import Path
from selenium.webdriver.common.by import By

sys.path.append(str(Path('src').resolve()))
from data.sources.selenium_utils import get_selenium_driver, wait_for_element, extract_page_source  # noqa: E402

url = "https://killersports.com/query?filter=NHL&sdql=season%3D2024&qt=games&show=5000&future=0&init=1"
with get_selenium_driver(headless=True, timeout=30) as driver:
    driver.get(url)
    wait_for_element(driver, By.ID, 'DT_Table', timeout=25)
    html = extract_page_source(driver, wait_seconds=2.0)
Path('tmp_kill_selenium.html').write_text(html, encoding='utf-8')
print('saved html')
