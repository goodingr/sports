"""
Migrate predicted_total_points from parquet files to database and clean up duplicates.
"""
import pandas as pd
import sqlite3
from pathlib import Path
from datetime import datetime

DB_PATH = Path("data/betting.db")

def migrate_predicted_totals():
    """Migrate predicted_total_points from parquet files to database."""
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    models = ["ensemble", "gradient_boosting", "random_forest"]
    total_updated = 0
    
    for model_type in models:
        parquet_path = Path(f"data/forward_test/{model_type}/predictions_master.parquet")
        
        if not parquet_path.exists():
            print(f"Skipping {model_type} - parquet file not found")
            continue
            
        print(f"\nProcessing {model_type}...")
        df = pd.read_parquet(parquet_path)
        
        # Filter to rows with predicted_total_points
        has_totals = df[~df["predicted_total_points"].isna()].copy()
        print(f"  Found {len(has_totals)} predictions with predicted_total_points")
        
        # Update each row in the database
        updated = 0
        for _, row in has_totals.iterrows():
            game_id = row["game_id"]
            predicted_total = row["predicted_total_points"]
            
            # Update the most recent prediction for this game+model
            cursor.execute("""
                UPDATE predictions
                SET predicted_total_points = ?
                WHERE game_id = ?
                  AND model_type = ?
                  AND predicted_at = (
                      SELECT MAX(predicted_at)
                      FROM predictions
                      WHERE game_id = ?
                        AND model_type = ?
                  )
            """, (predicted_total, game_id, model_type, game_id, model_type))
            
            if cursor.rowcount > 0:
                updated += 1
        
        conn.commit()
        print(f"  Updated {updated} predictions with predicted_total_points")
        total_updated += updated
    
    print(f"\nTotal predictions updated: {total_updated}")
    conn.close()

def clean_duplicates():
    """Remove duplicate predictions, keeping only the most recent per game+model."""
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    print("\nCleaning up duplicate predictions...")
    
    # Find all duplicates (game_id + model_type with multiple predictions)
    cursor.execute("""
        SELECT game_id, model_type, COUNT(*) as count
        FROM predictions
        GROUP BY game_id, model_type
        HAVING COUNT(*) > 1
    """)
    
    duplicates = cursor.fetchall()
    print(f"Found {len(duplicates)} game+model combinations with duplicates")
    
    if not duplicates:
        print("No duplicates to clean")
        conn.close()
        return
    
    total_deleted = 0
    
    for game_id, model_type, count in duplicates:
        # Keep only the most recent prediction
        cursor.execute("""
            DELETE FROM predictions
            WHERE rowid NOT IN (
                SELECT rowid
                FROM predictions
                WHERE game_id = ?
                  AND model_type = ?
                ORDER BY predicted_at DESC
                LIMIT 1
            )
            AND game_id = ?
            AND model_type = ?
        """, (game_id, model_type, game_id, model_type))
        
        deleted = cursor.rowcount
        total_deleted += deleted
    
    conn.commit()
    print(f"Deleted {total_deleted} duplicate predictions")
    
    # Verify cleanup
    cursor.execute("""
        SELECT COUNT(*)
        FROM (
            SELECT game_id, model_type, COUNT(*) as count
            FROM predictions
            GROUP BY game_id, model_type
            HAVING COUNT(*) > 1
        )
    """)
    
    remaining = cursor.fetchone()[0]
    print(f"Remaining duplicates: {remaining}")
    
    conn.close()

if __name__ == "__main__":
    print("="*60)
    print("MIGRATION: Predicted Totals and Duplicate Cleanup")
    print("="*60)
    
    # Step 1: Migrate predicted_total_points
    migrate_predicted_totals()
    
    # Step 2: Clean up duplicates
    clean_duplicates()
    
    print("\n" + "="*60)
    print("Migration complete!")
    print("="*60)
