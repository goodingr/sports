
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

def compare_api_vs_local():
    # 1. Fetch API data
    try:
        response = requests.get("http://localhost:8000/api/bets/history?limit=10000")
        if response.status_code != 200:
            logger.error(f"API Error: {response.status_code}")
            return
            
        api_data = response.json()
        api_bets = api_data.get("data", [])
        logger.info(f"API Bets Count: {len(api_bets)}")
        
        api_ids = set()
        for bet in api_bets:
            gid = bet.get("game_id")
            if gid:
                api_ids.add(gid)
                
    except Exception as e:
        logger.error(f"Connection Error: {e}")
        return

    # 2. Load Local Data (Dashboard Logic)
    df = load_forward_test_data(league=None, model_type="ensemble")
    
    # Apply Dashboard Logic
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
    recommended_completed = recommended[recommended["won"].notna()]
    
    logger.info(f"Local Bets Count: {len(recommended_completed)}")
    
    local_ids = set(recommended_completed["game_id"])
    
    # 3. Compare
    missing_in_api = local_ids - api_ids
    
    logger.info(f"Missing in API: {len(missing_in_api)}")
    if missing_in_api:
        for gid in missing_in_api:
            row = recommended_completed[recommended_completed["game_id"] == gid].iloc[0]
            logger.info(f"MISSING: {gid} | {row['away_team']} @ {row['home_team']} | {row['commence_time']} | Edge: {row['edge']} | League: {row['league']}")

if __name__ == "__main__":
    compare_api_vs_local()
