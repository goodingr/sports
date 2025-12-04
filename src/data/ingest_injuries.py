"""
Ingest NBA injuries from ESPN.
"""
import logging
from src.data.sources import nba_injuries_espn

logging.basicConfig(level=logging.INFO)

if __name__ == "__main__":
    try:
        nba_injuries_espn.ingest()
    except Exception as e:
        logging.error(f"Failed to ingest NBA injuries: {e}")
