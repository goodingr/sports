import pandas as pd
import logging
from src.dashboard.data import (
    load_forward_test_data, 
    calculate_totals_metrics, 
    DEFAULT_STAKE,
    DEFAULT_EDGE_THRESHOLD
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def verify_fix():
    # 1. Load data
    df = load_forward_test_data(league=None, model_type="ensemble")
    
    # 2. Calculate metrics
    metrics = calculate_totals_metrics(df, edge_threshold=DEFAULT_EDGE_THRESHOLD, stake=DEFAULT_STAKE)
    
    logger.info(f"Metrics 'Recommended Bets (Settled)': {metrics.recommended_completed}")
    logger.info(f"Metrics 'Total Recommended': {metrics.recommended_bets}")

if __name__ == "__main__":
    verify_fix()

