"""Load and process Lahman baseball database files."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import pandas as pd

from .utils import SourceDefinition, source_run

LOGGER = logging.getLogger(__name__)

# Default location for Lahman database files
DEFAULT_LAHMAN_DIR = Path("data/raw/sources/mlb/lahman")


def ingest(
    *,
    lahman_dir: Optional[str] = None,
    timeout: int = 60,  # noqa: ARG001
) -> str:
    """Load Lahman database CSV files and register them in the warehouse.
    
    Args:
        lahman_dir: Directory containing Lahman CSV files (default: data/raw/sources/mlb/lahman)
        timeout: Not used, kept for API consistency
    """
    definition = SourceDefinition(
        key="lahman",
        name="Lahman historical database",
        league="MLB",
        category="historical_stats",
        url="http://www.seanlahman.com/baseball-archive/statistics/",
        default_frequency="manual",
        storage_subdir="mlb/lahman",
    )
    
    lahman_path = Path(lahman_dir) if lahman_dir else DEFAULT_LAHMAN_DIR
    
    if not lahman_path.exists():
        raise FileNotFoundError(f"Lahman directory not found: {lahman_path}")
    
    output_dir = ""
    with source_run(definition) as run:
        output_dir = str(run.storage_dir)
        
        # List of expected Lahman files
        lahman_files = [
            "Teams.csv",
            "Batting.csv",
            "Pitching.csv",
            "Fielding.csv",
            "Master.csv",
            "Salaries.csv",
            "SeriesPost.csv",
            "AllstarFull.csv",
            "AwardsPlayers.csv",
            "AwardsManagers.csv",
            "HallOfFame.csv",
            "Managers.csv",
            "ManagersHalf.csv",
            "TeamsHalf.csv",
            "TeamsFranchises.csv",
            "BattingPost.csv",
            "PitchingPost.csv",
            "FieldingOF.csv",
            "AwardsSharePlayers.csv",
            "AwardsShareManagers.csv",
        ]
        
        total_records = 0
        files_processed = 0
        
        for filename in lahman_files:
            file_path = lahman_path / filename
            if not file_path.exists():
                LOGGER.debug("Lahman file not found: %s", filename)
                continue
            
            try:
                # Read CSV file
                df = pd.read_csv(file_path)
                records = len(df)
                
                # Copy to storage directory
                dest = run.make_path(filename)
                df.to_csv(dest, index=False)
                
                run.record_file(
                    dest,
                    metadata={"rows": records, "columns": list(df.columns)},
                    records=records,
                )
                
                total_records += records
                files_processed += 1
                LOGGER.info("Processed %s: %d records", filename, records)
                
            except Exception as exc:
                LOGGER.warning("Failed to process %s: %s", filename, exc)
                continue
        
        run.set_raw_path(run.storage_dir)
        run.set_message(f"Processed {files_processed} files with {total_records} total records")
        run.set_records(total_records)
    
    return output_dir


__all__ = ["ingest"]


