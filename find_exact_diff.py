
import requests
import pandas as pd
import logging
from src.dashboard.data import (
    load_forward_test_data, 
    calculate_totals_metrics, 
    DEFAULT_STAKE,
    DEFAULT_EDGE_THRESHOLD,
    _normalize_team_names
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def normalize_matchup_key(home, away, date_str):
    # Create a sorted tuple of teams to handle A vs B == B vs A
    teams = sorted([home.lower().strip(), away.lower().strip()])
    # Use date (YYYY-MM-DD) to handle slight time diffs
    date_key = date_str[:10]
    return (teams[0], teams[1], date_key)

def find_exact_diff():
    # 1. Fetch API data
    try:
        response = requests.get("http://localhost:8000/api/bets/history?limit=10000")
        if response.status_code != 200:
            logger.error(f"API Error: {response.status_code}")
            return
        api_data = response.json()
        api_bets = api_data.get("data", [])
    except Exception as e:
        logger.error(f"Connection Error: {e}")
        return

    # 2. Load Local Data
    df = load_forward_test_data(league=None, model_type="ensemble")
    from src.dashboard.data import _expand_totals
    totals = _expand_totals(df, stake=DEFAULT_STAKE)
    totals = _normalize_team_names(totals)
    if "edge" in totals.columns:
        totals = totals.sort_values("edge", ascending=False)
    if not totals.empty:
        totals = totals.drop_duplicates(
            subset=['home_team', 'away_team', 'commence_time', 'league', 'side'],
            keep='first'
        )
    mask = totals["edge"].notna() & (totals["edge"] >= DEFAULT_EDGE_THRESHOLD)
    recommended = totals.loc[mask].copy()
    local_bets = recommended[recommended["won"].notna()]

    # 3. Build Maps
    api_map = {}
    for bet in api_bets:
        key = normalize_matchup_key(bet['home_team'], bet['away_team'], bet['commence_time'])
        api_map[key] = bet

    local_map = {}
    for _, row in local_bets.iterrows():
        # Convert timestamp to string
        ts = row['commence_time'].isoformat()
        key = normalize_matchup_key(row['home_team'], row['away_team'], ts)
        local_map[key] = row

    # 4. Compare
    missing_in_api = []
    for key, row in local_map.items():
        if key not in api_map:
            missing_in_api.append(row)

    missing_in_local = []
    for key, bet in api_map.items():
        if key not in local_map:
            missing_in_local.append(bet)

    logger.info(f"Unique in Local (Missing in API): {len(missing_in_api)}")
    for row in missing_in_api:
        logger.info(f"  - {row['away_team']} @ {row['home_team']} ({row['commence_time']}) | Edge: {row['edge']}")

    logger.info(f"Unique in API (Missing in Local): {len(missing_in_local)}")
    for bet in missing_in_local:
        logger.info(f"  - {bet['away_team']} @ {bet['home_team']} ({bet['commence_time']})")

if __name__ == "__main__":
    find_exact_diff()
