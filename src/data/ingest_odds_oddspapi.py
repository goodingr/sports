"""Ingest soccer odds from OddsPAPI."""
from __future__ import annotations

import argparse
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Optional

from src.data.config import load_env
from src.data.sources.oddspapi import (
    BOOKMAKER_TITLES,
    H2H_MARKET_ID,
    H2H_OUTCOME_MAP,
    OddsPapiClient,
    _decimal_to_american,
    store_raw_payload,
)
from src.data.team_mappings import normalize_team_code
from src.db.loaders import load_odds_snapshot


LOGGER = logging.getLogger(__name__)

LEAGUE_TOURNAMENT_IDS = {
    "EPL": 17,
    "LALIGA": 8,
    "BUNDESLIGA": 35,  # Germany
    "SERIEA": 23,
    "LIGUE1": 34,
}

LEAGUE_SPORT_KEYS = {
    "EPL": "soccer_epl",
    "LALIGA": "soccer_spain_la_liga",
    "BUNDESLIGA": "soccer_germany_bundesliga",
    "SERIEA": "soccer_italy_serie_a",
    "LIGUE1": "soccer_france_ligue_one",
}

DEFAULT_BOOKMAKERS = ["pinnacle", "bet365", "williamhill"]


def _latest_player_entry(players: Dict) -> Optional[Dict]:
    entries: List[Dict] = []
    for bucket in players.values():
        if isinstance(bucket, list):
            entries.extend([item for item in bucket if isinstance(item, dict)])
        elif isinstance(bucket, dict):
            if "price" in bucket or "changedAt" in bucket:
                entries.append(bucket)
            else:
                for value in bucket.values():
                    if isinstance(value, dict):
                        entries.append(value)
                    elif isinstance(value, list):
                        entries.extend([item for item in value if isinstance(item, dict)])
    if not entries:
        return None
    entries.sort(key=lambda row: row.get("changedAt") or row.get("createdAt") or "", reverse=True)
    return entries[0]


def _build_event(
    fixture: Dict,
    bookmaker_odds: Dict,
    league: str,
) -> Optional[Dict]:
    bookmakers_payload = []
    home_name = fixture.get("participant1Name")
    away_name = fixture.get("participant2Name")

    for bookmaker_slug, data in bookmaker_odds.items():
        markets = data.get("markets", {})
        h2h = markets.get(H2H_MARKET_ID) or markets.get(int(H2H_MARKET_ID))
        if not h2h:
            continue

        outcomes_payload = []
        for outcome_id, outcome_data in h2h.get("outcomes", {}).items():
            latest = _latest_player_entry(outcome_data.get("players", {}))
            if not latest:
                continue
            decimal_price = latest.get("price")
            american_price = _decimal_to_american(decimal_price)
            if american_price is None:
                continue
            label = H2H_OUTCOME_MAP.get(str(outcome_id)) or H2H_OUTCOME_MAP.get(str(int(outcome_id)))
            if label == "home":
                name = home_name
            elif label == "away":
                name = away_name
            elif label == "draw":
                name = "Draw"
            else:
                name = outcome_id
            outcomes_payload.append(
                {
                    "name": name,
                    "price": american_price,
                    "price_decimal": decimal_price,
                    "last_update": latest.get("changedAt") or latest.get("createdAt"),
                }
            )

        if not outcomes_payload:
            LOGGER.debug(
                "Fixture %s bookmaker %s missing H2H outcomes",
                fixture.get("fixtureId"),
                bookmaker_slug,
            )
            continue

        bookmakers_payload.append(
            {
                "key": bookmaker_slug,
                "title": BOOKMAKER_TITLES.get(bookmaker_slug, bookmaker_slug),
                "last_update": data.get("updatedAt") or fixture.get("updatedAt"),
                "markets": [
                    {
                        "key": "h2h",
                        "outcomes": outcomes_payload,
                    }
                ],
            }
        )

    if not bookmakers_payload:
        LOGGER.debug("No bookmakers retained for fixture %s", fixture.get("fixtureId"))
        return None

    return {
        "id": fixture.get("fixtureId"),
        "sport_title": "Soccer",
        "commence_time": fixture.get("startTime"),
        "home_team": home_name,
        "away_team": away_name,
        "bookmakers": bookmakers_payload,
    }


def ingest_window(
    client: OddsPapiClient,
    league: str,
    tournament_id: int,
    start_dt: datetime,
    end_dt: datetime,
    *,
    bookmakers: Iterable[str],
    mode: str = "live",
) -> None:
    LOGGER.info(
        "Fetching %s OddsPAPI fixtures for %s (%s to %s)",
        mode,
        league,
        start_dt.date(),
        end_dt.date(),
    )
    fixtures = list(
        client.iter_fixtures(tournament_id, start_dt.date(), end_dt.date())
    )
    if not fixtures:
        LOGGER.info("No fixtures returned for %s", league)
        return

    events: List[Dict] = []
    for fixture in fixtures:
        fixture_id = fixture.get("fixtureId")
        if not fixture_id:
            continue
        try:
            if mode == "historical":
                odds_payload = client.get_historical_odds(fixture_id, bookmakers)
                bookmaker_odds = odds_payload.get("bookmakers", {})
            else:
                odds_payload = client.get_odds(fixture_id, bookmakers, [H2H_MARKET_ID])
                bookmaker_odds = odds_payload.get("bookmakerOdds", {})
        except requests.HTTPError as exc:
            LOGGER.warning("OddsPAPI request failed for fixture %s: %s", fixture_id, exc)
            continue

        if not bookmaker_odds:
            LOGGER.debug("No bookmaker odds returned for fixture %s", fixture_id)
            continue

        LOGGER.debug(
            "Fixture %s bookmakers: %s",
            fixture_id,
            list(bookmaker_odds.keys()),
        )

        event = _build_event(fixture, bookmaker_odds, league)
        if event:
            events.append(event)
            LOGGER.debug(
                "Captured OddsPAPI data for fixture %s (%s bookmakers)",
                fixture_id,
                len(event["bookmakers"]),
            )
        else:
            LOGGER.debug("No usable prices for fixture %s", fixture_id)

    if not events:
        LOGGER.info("No bookmaker data found for %s fixtures", league)
        return

    payload = {
        "fetched_at": datetime.utcnow().isoformat(),
        "source": "oddspapi",
        "results": events,
    }
    raw_path = store_raw_payload(payload, f"{league}_oddspapi")
    sport_key = LEAGUE_SPORT_KEYS.get(league, "soccer_epl")
    load_odds_snapshot(payload, raw_path=raw_path, sport_key=sport_key)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest soccer odds via OddsPAPI")
    parser.add_argument(
        "--leagues",
        nargs="+",
        choices=sorted(LEAGUE_TOURNAMENT_IDS.keys()),
        default=["EPL", "LALIGA", "BUNDESLIGA", "SERIEA", "LIGUE1"],
    )
    parser.add_argument(
        "--bookmakers",
        nargs="+",
        default=DEFAULT_BOOKMAKERS,
        help="List of bookmaker slugs (max 3 as per OddsPAPI).",
    )
    parser.add_argument(
        "--from-date",
        type=str,
        default=None,
        help="ISO date (YYYY-MM-DD) inclusive start.",
    )
    parser.add_argument(
        "--to-date",
        type=str,
        default=None,
        help="ISO date (YYYY-MM-DD) inclusive end.",
    )
    parser.add_argument(
        "--days-back",
        type=int,
        default=1,
        help="Days back from today if from/to not provided.",
    )
    parser.add_argument(
        "--days-forward",
        type=int,
        default=7,
        help="Days forward from today if from/to not provided.",
    )
    parser.add_argument(
        "--mode",
        choices=["live", "historical"],
        default="live",
    )
    parser.add_argument(
        "--cooldown",
        type=float,
        default=0.25,
        help="Seconds to wait between OddsPAPI requests.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level))

    config = load_env()
    api_key = config.get("ODDSPAPI_API_KEY")
    if not api_key:
        raise SystemExit("ODDSPAPI_API_KEY is not configured in environment/.env")

    client = OddsPapiClient(api_key=api_key, cooldown_seconds=args.cooldown)

    if args.from_date and args.to_date:
        from_dt = datetime.fromisoformat(args.from_date)
        to_dt = datetime.fromisoformat(args.to_date)
    else:
        now = datetime.utcnow()
        from_dt = now - timedelta(days=args.days_back)
        to_dt = now + timedelta(days=args.days_forward)

    for league in args.leagues:
        tournament_id = LEAGUE_TOURNAMENT_IDS.get(league)
        if not tournament_id:
            LOGGER.warning("No tournament mapping for %s", league)
            continue
        ingest_window(
            client,
            league,
            tournament_id,
            from_dt,
            to_dt,
            bookmakers=args.bookmakers,
            mode=args.mode,
        )


if __name__ == "__main__":
    main()
