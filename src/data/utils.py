"""Utility functions for data ingestion."""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Union

from .config import RAW_DATA_DIR

LOGGER = logging.getLogger(__name__)

def save_raw_json(
    data: Union[Dict, List],
    league: str,
    source: str,
    timestamp: datetime = None
) -> Path:
    """
    Save raw JSON data to data/raw/results/<league>/<source>_<timestamp>.json.
    
    Args:
        data: The JSON serializable data to save.
        league: The league code (e.g., 'NFL', 'NBA').
        source: The source identifier (e.g., 'cfbd', 'espn', 'the-odds-api').
        timestamp: Optional timestamp. Defaults to current UTC time.
        
    Returns:
        Path to the saved file.
    """
    if timestamp is None:
        timestamp = datetime.now(timezone.utc)
        
    ts_str = timestamp.strftime("%Y-%m-%dT%H-%M-%SZ")
    
    # Create directory structure: data/raw/results/<league>
    out_dir = RAW_DATA_DIR / "results" / league
    out_dir.mkdir(parents=True, exist_ok=True)
    
    filename = f"{source}_{ts_str}.json"
    out_path = out_dir / filename
    
    try:
        out_path.write_text(json.dumps(data, indent=2), encoding="utf-8")
        LOGGER.info("Saved raw %s response to %s", source, out_path)
    except Exception as exc:
        LOGGER.warning("Failed to save raw JSON for %s: %s", source, exc)
        
    return out_path
