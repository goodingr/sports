import logging
from src.data.sources.nba_rolling_metrics import ingest

logging.basicConfig(level=logging.INFO)
# Run for last few seasons to ensure we have data
ingest(seasons=[2023, 2024, 2025])
