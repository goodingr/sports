"""Load NBA historical stats and betting data from Kaggle dataset."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import pandas as pd

from src.db.loaders import load_schedules

from .utils import SourceDefinition, source_run, write_dataframe, write_text

LOGGER = logging.getLogger(__name__)

KAGGLE_DATASET = "ehallmar/nba-historical-stats-and-betting-data"
# Dataset URL: https://www.kaggle.com/datasets/ehallmar/nba-historical-stats-and-betting-data


def _download_kaggle_dataset(dataset: str, output_dir: Path, *, unzip: bool = True) -> Path:
    """Download a Kaggle dataset using the Kaggle API."""
    try:
        import kaggle
        from kaggle.api.kaggle_api_extended import KaggleApi
        
        api = KaggleApi()
        api.authenticate()
        
        LOGGER.info("Downloading Kaggle dataset: %s", dataset)
        api.dataset_download_files(dataset, path=str(output_dir), unzip=unzip)
        
        return output_dir
    except ImportError:
        raise RuntimeError(
            "Kaggle API not installed. Install with: poetry add kaggle"
        )
    except Exception as exc:  # noqa: BLE001
        LOGGER.error("Failed to download Kaggle dataset: %s", exc)
        raise


def _load_betting_data(csv_path: Path) -> pd.DataFrame:
    """Load and normalize betting data from Kaggle dataset."""
    df = pd.read_csv(csv_path)
    
    LOGGER.info("Loaded CSV with columns: %s", list(df.columns))
    
    # Normalize column names to match our schema
    # Expected columns may vary - adjust based on actual dataset structure
    column_mapping = {
        "date": "gameday",
        "game_date": "gameday",
        "Date": "gameday",
        "GAME_DATE": "gameday",
        "home_team": "home_team",
        "HOME_TEAM": "home_team",
        "Home": "home_team",
        "away_team": "away_team",
        "AWAY_TEAM": "away_team",
        "visitor_team": "away_team",
        "Visitor": "away_team",
        "home_moneyline": "home_moneyline",
        "HOME_MONEYLINE": "home_moneyline",
        "away_moneyline": "away_moneyline",
        "AWAY_MONEYLINE": "away_moneyline",
        "visitor_moneyline": "away_moneyline",
        "home_score": "home_score",
        "HOME_SCORE": "home_score",
        "away_score": "away_score",
        "AWAY_SCORE": "away_score",
        "visitor_score": "away_score",
        "spread": "spread_line",
        "SPREAD": "spread_line",
        "total": "total_line",
        "TOTAL": "total_line",
        "over_under": "total_line",
        "game_id": "game_id",
        "GAME_ID": "game_id",
        "season": "season",
        "SEASON": "season",
    }
    
    # Rename columns if they exist
    for old_col, new_col in column_mapping.items():
        if old_col in df.columns and new_col not in df.columns:
            df.rename(columns={old_col: new_col}, inplace=True)
    
    # Generate game_id if missing
    if "game_id" not in df.columns or df["game_id"].isna().all():
        if "gameday" in df.columns and "home_team" in df.columns and "away_team" in df.columns:
            df["game_id"] = df.apply(
                lambda row: f"NBA_{row['gameday']}_{row['home_team']}_{row['away_team']}".replace(" ", "_").replace("/", "-"),
                axis=1
            )
    
    # Extract season from date if available
    if "season" not in df.columns or df["season"].isna().all():
        if "gameday" in df.columns:
            df["gameday"] = pd.to_datetime(df["gameday"], errors="coerce")
            df["season"] = df["gameday"].dt.year
            # NBA season spans two years, so adjust if month is before October
            df.loc[df["gameday"].dt.month < 10, "season"] = df.loc[df["gameday"].dt.month < 10, "season"] - 1
    
    return df


def ingest(*, dataset: Optional[str] = None, csv_path: Optional[str] = None, timeout: int = 300) -> str:  # noqa: ARG001
    """Ingest NBA historical betting data from Kaggle dataset.
    
    Args:
        dataset: Kaggle dataset name (e.g., "ehallmar/nba-historical-stats-and-betting-data")
        csv_path: Optional path to already-downloaded CSV file
        timeout: Download timeout (not used for local files)
    """
    definition = SourceDefinition(
        key="kaggle_nba_betting",
        name="Kaggle NBA historical betting data",
        league="NBA",
        category="odds",
        url=f"https://www.kaggle.com/datasets/{dataset or KAGGLE_DATASET}",
        default_frequency="manual",
        storage_subdir="nba/kaggle",
    )
    
    output_dir = ""
    with source_run(definition) as run:
        output_dir = str(run.storage_dir)
        
        if csv_path:
            # Load from local CSV file
            csv_file = Path(csv_path)
            if not csv_file.exists():
                raise FileNotFoundError(f"CSV file not found: {csv_path}")
            
            LOGGER.info("Loading Kaggle NBA betting data from %s", csv_path)
            df = _load_betting_data(csv_file)
            
        else:
            # Download from Kaggle
            dataset_name = dataset or KAGGLE_DATASET
            download_dir = run.storage_dir / "download"
            download_dir.mkdir(parents=True, exist_ok=True)
            
            try:
                _download_kaggle_dataset(dataset_name, download_dir, unzip=True)
                
                # Find CSV files in downloaded directory
                csv_files = list(download_dir.rglob("*.csv"))
                if not csv_files:
                    raise FileNotFoundError("No CSV files found in downloaded dataset")
                
                # Look for betting-related CSV files first
                betting_files = [f for f in csv_files if "bet" in f.name.lower() or "odds" in f.name.lower() or "spread" in f.name.lower()]
                if betting_files:
                    csv_file = betting_files[0]
                else:
                    # Use the first CSV found
                    csv_file = csv_files[0]
                
                LOGGER.info("Found CSV file: %s", csv_file)
                df = _load_betting_data(csv_file)
                
            except RuntimeError as exc:
                LOGGER.error("Kaggle download failed: %s", exc)
                run.set_message(f"Kaggle download failed: {exc}")
                run.set_raw_path(run.storage_dir)
                return output_dir
        
        if df.empty:
            run.set_message("No betting data found in CSV")
            run.set_raw_path(run.storage_dir)
            return output_dir
        
        # Save processed data
        parquet_path = run.make_path("betting_data.parquet")
        write_dataframe(df, parquet_path)
        run.record_file(
            parquet_path,
            metadata={"rows": len(df), "columns": list(df.columns)},
            records=len(df),
        )
        
        # Also save as CSV for inspection
        csv_output = run.make_path("betting_data.csv")
        df.to_csv(csv_output, index=False)
        run.record_file(csv_output, metadata={"rows": len(df)})
        
        # Load into database
        try:
            LOGGER.info("Loading Kaggle data into database...")
            load_schedules(df, source_version="kaggle", league="NBA")
            LOGGER.info("Successfully loaded %d rows into database", len(df))
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Failed to load data into database: %s", exc)
            run.set_message(f"Loaded {len(df)} rows but database load failed: {exc}")
        else:
            run.set_message(f"Loaded {len(df)} rows from Kaggle dataset into database")
        
        run.set_records(len(df))
        run.set_raw_path(run.storage_dir)
    
    return output_dir


__all__ = ["ingest"]

