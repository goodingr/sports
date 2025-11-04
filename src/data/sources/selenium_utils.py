"""Selenium utilities for scraping JavaScript-rendered content."""

from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Optional

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

LOGGER = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 30


def _create_chrome_options(headless: bool = True) -> Options:
    """Create Chrome options with anti-detection settings."""
    options = Options()
    if headless:
        options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36"
    )
    return options


@contextmanager
def get_selenium_driver(
    headless: bool = True,
    timeout: int = DEFAULT_TIMEOUT,
) -> WebDriver:
    """Context manager for Selenium WebDriver."""
    options = _create_chrome_options(headless=headless)
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    
    try:
        driver.implicitly_wait(timeout)
        yield driver
    finally:
        driver.quit()


def wait_for_element(
    driver: WebDriver,
    by: By,
    value: str,
    timeout: int = DEFAULT_TIMEOUT,
) -> Optional[object]:
    """Wait for an element to be present and return it."""
    try:
        element = WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((by, value))
        )
        return element
    except Exception as exc:  # noqa: BLE001
        LOGGER.debug("Element not found: %s=%s (%s)", by, value, exc)
        return None


def wait_for_text_in_element(
    driver: WebDriver,
    by: By,
    value: str,
    text: str,
    timeout: int = DEFAULT_TIMEOUT,
) -> bool:
    """Wait for text to appear in an element."""
    try:
        WebDriverWait(driver, timeout).until(
            EC.text_to_be_present_in_element((by, value), text)
        )
        return True
    except Exception:  # noqa: BLE001
        return False


def wait_for_script_variable(
    driver: WebDriver,
    variable_name: str,
    timeout: int = DEFAULT_TIMEOUT,
) -> Optional[object]:
    """Wait for a JavaScript variable to be defined and return its value."""
    try:
        WebDriverWait(driver, timeout).until(
            lambda d: d.execute_script(f"return typeof {variable_name} !== 'undefined';")
        )
        return driver.execute_script(f"return {variable_name};")
    except Exception as exc:  # noqa: BLE001
        LOGGER.debug("Variable %s not found: %s", variable_name, exc)
        return None


def extract_page_source(driver: WebDriver, wait_seconds: float = 2.0) -> str:
    """Extract fully rendered HTML after waiting for JavaScript execution."""
    import time
    time.sleep(wait_seconds)  # Allow time for dynamic content to load
    return driver.page_source


__all__ = [
    "get_selenium_driver",
    "wait_for_element",
    "wait_for_text_in_element",
    "wait_for_script_variable",
    "extract_page_source",
]

