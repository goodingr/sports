"""
Migrate predictions from Parquet files to the SQLite database.
"""

import logging
import sqlite3
from pathlib import Path
import pandas as pd
from src.db.core import connect
from src.predict.config import PREDICTIONS_DIR

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

def migrate_predictions():
    """Migrate all predictions from parquet files to the database."""
    if not PREDICTIONS_DIR.exists():
        LOGGER.warning(f"Predictions directory {PREDICTIONS_DIR} does not exist.")
        return

    # Find all model types
    model_types = [d.name for d in PREDICTIONS_DIR.iterdir() if d.is_dir()]
    
    total_records = 0
    
    with connect() as conn:
        for model_type in model_types:
            master_path = PREDICTIONS_DIR / model_type / "predictions_master.parquet"
            if not master_path.exists():
                continue
                
            LOGGER.info(f"Migrating {model_type} from {master_path}...")
            try:
                df = pd.read_parquet(master_path)
                if df.empty:
                    continue
                
                # Rename columns to match DB schema if needed
                # DB: game_id, model_type, predicted_at, home_prob, away_prob, home_moneyline, away_moneyline, home_edge, away_edge, home_implied_prob, away_implied_prob
                
                # Parquet columns (typical):
                # game_id, commence_time, home_team, away_team, home_moneyline, away_moneyline, 
                # home_predicted_prob, away_predicted_prob, home_edge, away_edge, 
                # home_implied_prob, away_implied_prob, league, predicted_at, result_updated_at
                
                # Map columns
                records = []
                for _, row in df.iterrows():
                    record = {
                        "game_id": row.get("game_id"),
                        "model_type": model_type,
                        "predicted_at": row.get("predicted_at", pd.Timestamp.now(tz="UTC")).isoformat() if isinstance(row.get("predicted_at"), pd.Timestamp) else str(row.get("predicted_at")),
                        "home_prob": row.get("home_predicted_prob"),
                        "away_prob": row.get("away_predicted_prob"),
                        "home_moneyline": row.get("home_moneyline"),
                        "away_moneyline": row.get("away_moneyline"),
                        "home_edge": row.get("home_edge"),
                        "away_edge": row.get("away_edge"),
                        "home_implied_prob": row.get("home_implied_prob"),
                        "away_implied_prob": row.get("away_implied_prob")
                    }
                    records.append(record)
                
                # Bulk insert
                cursor = conn.cursor()
                cursor.executemany("""
                    INSERT OR IGNORE INTO predictions (
                        game_id, model_type, predicted_at, 
                        home_prob, away_prob, home_moneyline, away_moneyline, 
                        home_edge, away_edge, home_implied_prob, away_implied_prob
                    ) VALUES (
                        :game_id, :model_type, :predicted_at, 
                        :home_prob, :away_prob, :home_moneyline, :away_moneyline, 
                        :home_edge, :away_edge, :home_implied_prob, :away_implied_prob
                    )
                """, records)
                
                count = cursor.rowcount
                LOGGER.info(f"Migrated {count} records for {model_type}")
                total_records += count
                
            except Exception as e:
                LOGGER.error(f"Failed to migrate {model_type}: {e}")
                
    LOGGER.info(f"Migration complete. Total records migrated: {total_records}")

if __name__ == "__main__":
    migrate_predictions()
