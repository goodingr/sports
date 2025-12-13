
import sqlite3
import pandas as pd
import logging
import sys
import os
sys.path.append(os.getcwd())
from src.db.core import DB_PATH

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

def fix_schema():
    LOGGER.info("Starting schema fix for predictions table...")
    
    conn = sqlite3.connect(DB_PATH)
    
    # 1. Read existing data
    df = pd.read_sql_query("SELECT * FROM predictions", conn)
    LOGGER.info(f"Read {len(df)} rows from predictions table")
    
    # 2. Deduplicate
    # Sort by predicted_at (newest first)
    if 'predicted_at' in df.columns:
        df['predicted_at'] = pd.to_datetime(df['predicted_at'], errors='coerce')
        df = df.sort_values('predicted_at', ascending=True) # Ascending so 'last' captures newest
    
    # But wait, if we want to prioritize rows with predicted_total_points, we should check that.
    # Actually, the newest rows HAVE the points, so just sorting by time is enough.
    
    # Drop duplicates keeping last (newest)
    initial_count = len(df)
    df = df.drop_duplicates(subset=['game_id', 'model_type'], keep='last')
    final_count = len(df)
    
    LOGGER.info(f"Deduplicated: {initial_count} -> {final_count} rows (-{initial_count - final_count})")
    
    # 3. Recreate table
    cursor = conn.cursor()
    
    # Drop existing table
    cursor.execute("DROP TABLE IF EXISTS predictions_backup")
    cursor.execute("ALTER TABLE predictions RENAME TO predictions_backup")
    
    # Create new table with CORRECT constraint
    # We match the schema but change UNIQUE constraint
    create_sql = """
    CREATE TABLE predictions (
        prediction_id INTEGER PRIMARY KEY AUTOINCREMENT,
        game_id TEXT NOT NULL,
        model_type TEXT NOT NULL,
        predicted_at TEXT NOT NULL,
        home_prob REAL,
        away_prob REAL,
        home_moneyline REAL,
        away_moneyline REAL,
        home_edge REAL,
        away_edge REAL,
        home_implied_prob REAL,
        away_implied_prob REAL,
        total_line REAL,
        over_prob REAL,
        under_prob REAL,
        over_moneyline REAL,
        under_moneyline REAL,
        over_edge REAL,
        under_edge REAL,
        over_implied_prob REAL,
        under_implied_prob REAL,
        predicted_total_points REAL,
        UNIQUE(game_id, model_type)
    );
    """
    cursor.execute(create_sql)
    
    # 4. Insert data back
    # Convert datetime back to string for sqlite
    df['predicted_at'] = df['predicted_at'].apply(lambda x: x.isoformat() if pd.notnull(x) else None)
    
    # Drop prediction_id to let it autoincrement fresh
    if 'prediction_id' in df.columns:
        df = df.drop(columns=['prediction_id'])
        
    # Write to DB
    df.to_sql('predictions', conn, if_exists='append', index=False)
    
    conn.commit()
    conn.close()
    LOGGER.info("Schema fix completed successfully!")

if __name__ == "__main__":
    fix_schema()
