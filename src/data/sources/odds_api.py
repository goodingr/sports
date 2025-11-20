"""Automation-friendly wrapper for The Odds API snapshots."""

from __future__ import annotations

from pathlib import Path
from typing import Optional

from src.data.ingest_odds import LEAGUE_TO_SPORT_KEY, run as run_odds_api


def ingest(
    *,
    league: Optional[str] = None,
    sport: Optional[str] = None,
    market: Optional[str] = None,
    region: Optional[str] = None,
    force_refresh: bool = False,
    dotenv: Optional[str] = None,
) -> str:
    """
    Trigger a The Odds API snapshot and load it into the database.

    Parameters map directly to ``src.data.ingest_odds``:
    - ``league``: friendly league code (e.g., NCAAB, NHL). Preferred over ``sport``.
    - ``sport``: explicit Odds API sport key (e.g., basketball_ncaab).
    - ``market``: odds market (defaults to h2h per OddsAPI settings).
    - ``region``: odds region (e.g., us).
    - ``force_refresh``: bypass cached snapshots.
    - ``dotenv``: optional path to a .env file containing ODDS_API_KEY.
    """

    sport_key = None
    if league:
        league_upper = league.upper()
        if league_upper not in LEAGUE_TO_SPORT_KEY:
            raise ValueError(
                f"Unsupported league '{league}'. Options: {', '.join(sorted(LEAGUE_TO_SPORT_KEY))}"
            )
        sport_key = LEAGUE_TO_SPORT_KEY[league_upper]
    elif sport:
        sport_key = sport
    else:
        raise ValueError("Must supply either 'league' or 'sport' to ingest Odds API data.")

    dotenv_path = Path(dotenv) if dotenv else None
    output_path = run_odds_api(
        dotenv_path,
        sport_key=sport_key,
        market=market,
        region=region,
        force_refresh=force_refresh,
    )
    return str(output_path)


__all__ = ["ingest"]
