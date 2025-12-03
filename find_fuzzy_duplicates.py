
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

def find_fuzzy_duplicates():
    # 1. Load data
    df = load_forward_test_data(league=None, model_type="ensemble")
    
    # 2. Calculate metrics (using the NEW logic with normalization)
    metrics = calculate_totals_metrics(df, edge_threshold=DEFAULT_EDGE_THRESHOLD, stake=DEFAULT_STAKE)
    
    logger.info(f"Metrics 'Recommended Bets (Settled)': {metrics.recommended_completed}")
    
    # Get the underlying dataframe (we need to reconstruct it since metrics only returns summary)
    # We'll use the same logic as calculate_totals_metrics
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
    
    logger.info(f"Reconstructed Count: {len(recommended_completed)}")
    
    # 3. Check for fuzzy duplicates
    # Same teams, same side, different time?
    # Or same time, same side, slightly different teams (that normalization missed)?
    
    # Check for same teams + side, ignoring time
    dupes_time = recommended_completed[recommended_completed.duplicated(subset=['home_team', 'away_team', 'league', 'side'], keep=False)]
    if not dupes_time.empty:
        logger.info(f"Potential Time Duplicates: {len(dupes_time)}")
        # Check time difference
        groups = dupes_time.groupby(['home_team', 'away_team', 'league', 'side'])
        for name, group in groups:
            times = group['commence_time'].sort_values()
            diff = times.diff().dt.total_seconds().dropna()
            if (diff < 3600).any(): # Less than 1 hour difference
                logger.info(f"Fuzzy Time Duplicate: {name}")
                for _, row in group.iterrows():
                    logger.info(f"  - {row['commence_time']} | {row['game_id']}")

    # Check for same time + side, fuzzy teams?
    # We can't easily do fuzzy match here without a library, but we can check for "contains"
    
    # Check for duplicates if we ignore 'league'?
    dupes_league = recommended_completed[recommended_completed.duplicated(subset=['home_team', 'away_team', 'commence_time', 'side'], keep=False)]
    # Filter out ones that match on league (true duplicates)
    dupes_league = dupes_league[~dupes_league.duplicated(subset=['home_team', 'away_team', 'commence_time', 'league', 'side'], keep=False)]
    
    if not dupes_league.empty:
        logger.info(f"Different League Duplicates: {len(dupes_league)}")
        for _, row in dupes_league.iterrows():
            logger.info(f"  - {row['league']} | {row['home_team']} vs {row['away_team']}")

if __name__ == "__main__":
    find_fuzzy_duplicates()
