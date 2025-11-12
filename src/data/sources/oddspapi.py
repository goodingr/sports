"""Client helpers for OddsPAPI v4 REST API."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
import json
import logging
import time
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional

import requests

from src.data.config import RAW_DATA_DIR


LOGGER = logging.getLogger(__name__)

ODDSPAPI_BASE_URL = "https://api.oddspapi.io/v4"
ODDSPAPI_DEFAULT_TIMEOUT = 30
ODDSPAPI_MAX_FIXTURE_WINDOW_DAYS = 9  # API requires from/to range <=10 days

BOOKMAKER_TITLES = {
    "pinnacle": "Pinnacle",
    "bet365": "Bet365",
    "williamhill": "William Hill",
}

H2H_MARKET_ID = "101"
H2H_OUTCOME_MAP = {
    "101": "home",
    "102": "draw",
    "103": "away",
}


def _decimal_to_american(decimal_price: Optional[float]) -> Optional[int]:
    if decimal_price is None or decimal_price <= 1.0:
        return None
    if decimal_price >= 2.0:
        return int(round((decimal_price - 1.0) * 100))
    return int(round(-100.0 / (decimal_price - 1.0)))


def _chunks_for_range(
    start_date: date,
    end_date: date,
    window_days: int = ODDSPAPI_MAX_FIXTURE_WINDOW_DAYS,
) -> Iterator[tuple[date, date]]:
    cursor = start_date
    delta = timedelta(days=window_days)
    one_day = timedelta(days=1)
    while cursor <= end_date:
        chunk_end = min(cursor + delta, end_date)
        if chunk_end == cursor:
            chunk_end = cursor + timedelta(days=1)
        yield cursor, chunk_end
        cursor = chunk_end + one_day


@dataclass
class OddsPapiClient:
    api_key: str
    timeout: int = ODDSPAPI_DEFAULT_TIMEOUT
    session: requests.Session = requests.Session()
    cooldown_seconds: float = 0.0

    def _request(
        self,
        path: str,
        params: Optional[Dict[str, object]] = None,
        *,
        empty_response: Optional[object] = None,
        max_retries: int = 5,
    ):
        url = f"{ODDSPAPI_BASE_URL}{path}"
        query = dict(params or {})
        query["apiKey"] = self.api_key
        attempt = 0
        last_exc: Optional[requests.HTTPError] = None
        while True:
            attempt += 1
            response = self.session.get(url, params=query, timeout=self.timeout)
            if response.status_code == 429 and attempt <= max_retries:
                retry_after = float(response.headers.get("Retry-After") or 5.0)
                LOGGER.warning(
                    "Rate limited by OddsPAPI (429). Sleeping %.1fs (attempt %s/%s)",
                    retry_after,
                    attempt,
                    max_retries,
                )
                time.sleep(retry_after)
                continue
            if response.status_code == 404 and empty_response is not None:
                LOGGER.debug("OddsPAPI returned 404 for %s with params %s", path, params)
                return empty_response
            try:
                response.raise_for_status()
                break
            except requests.HTTPError as exc:
                last_exc = exc
                if attempt >= max_retries:
                    raise
                LOGGER.warning(
                    "OddsPAPI request failed for %s (attempt %s/%s): %s",
                    path,
                    attempt,
                    max_retries,
                    exc,
                )
                time.sleep(min(self.cooldown_seconds or 1.0, 5.0))
        if last_exc:
            LOGGER.debug("Recovered from previous errors on %s", path)
        if self.cooldown_seconds:
            time.sleep(self.cooldown_seconds)
        return response.json()

    def iter_fixtures(
        self,
        tournament_id: int,
        start_date: date,
        end_date: date,
    ) -> Iterator[Dict]:
        for chunk_start, chunk_end in _chunks_for_range(start_date, end_date):
            payload = self._request(
                "/fixtures",
                {
                    "tournamentId": tournament_id,
                    "from": chunk_start.isoformat(),
                    "to": chunk_end.isoformat(),
                },
                empty_response=[],
            )
            for fixture in payload:
                yield fixture

    def get_odds(
        self,
        fixture_id: str,
        bookmakers: Iterable[str],
        markets: Iterable[str],
    ) -> Dict:
        params = {
            "fixtureId": fixture_id,
            "bookmakers": ",".join(bookmakers),
            "markets": ",".join(str(m) for m in markets),
        }
        return self._request("/odds", params)

    def get_historical_odds(
        self,
        fixture_id: str,
        bookmakers: Iterable[str],
    ) -> Dict:
        params = {
            "fixtureId": fixture_id,
            "bookmakers": ",".join(bookmakers),
        }
        return self._request("/historical-odds", params)


def store_raw_payload(data: Dict[str, object], league: str) -> str:
    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    raw_dir = RAW_DATA_DIR / "odds" / "oddspapi"
    raw_dir.mkdir(parents=True, exist_ok=True)
    raw_path = raw_dir / f"{league.lower()}_{timestamp}.json"
    raw_path.write_text(json.dumps(data, indent=2), encoding="utf-8")
    return str(raw_path)
