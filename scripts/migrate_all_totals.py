"""
Better migration: Copy ALL totals-related fields from parquet to database.
"""
import pandas as pd
import sqlite3
from pathlib import Path

DB_PATH = Path("data/betting.db")

def migrate_all_totals_fields():
    """Migrate all totals-related fields from parquet to database."""
    
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
        
        print(f"  Total rows in parquet: {len(df)}")
        print(f"  Columns: {df.columns.tolist()}")
        
        # Check what totals fields we have
        totals_cols = [col for col in df.columns if 'total' in col.lower() or 'over' in col.lower() or 'under' in col.lower()]
        print(f"  Totals-related columns: {totals_cols}")
        
        # Update each row
        updated = 0
        for _, row in df.iterrows():
            game_id = row["game_id"]
            
            # Prepare update values
            updates = {}
            if "predicted_total_points" in df.columns and pd.notna(row.get("predicted_total_points")):
                updates["predicted_total_points"] = float(row["predicted_total_points"])
            
            if "total_line" in df.columns and pd.notna(row.get("total_line")):
                updates["total_line"] = float(row["total_line"])
                
            if not updates:
                continue
            
            # Build UPDATE query
            set_clause = ", ".join([f"{k} = ?" for k in updates.keys()])
            values = list(updates.values())
            
            cursor.execute(f"""
                UPDATE predictions
                SET {set_clause}
                WHERE game_id = ?
                  AND model_type = ?
            """, values + [game_id, model_type])
            
            if cursor.rowcount > 0:
                updated += 1
        
        conn.commit()
        print(f"  Updated {updated} predictions")
        total_updated += updated
    
    print(f"\nTotal predictions updated: {total_updated}")
    
    # Show final stats
    cursor.execute("""
        SELECT model_type,
            COUNT(*) as total,
            COUNT(total_line) as with_line,
            COUNT(over_prob) as with_over_prob,
            COUNT(predicted_total_points) as with_predicted
        FROM predictions
        WHERE model_type IN ('ensemble', 'gradient_boosting', 'random_forest')
        GROUP BY model_type
    """)
    
    print("\nFinal Stats:")
    for row in cursor.fetchall():
        print(f"  {row[0]}:")
        print(f"    Total: {row[1]}, total_line: {row[2]}, over_prob: {row[3]}, predicted_total_points: {row[4]}")
    
    conn.close()

if __name__ == "__main__":
    migrate_all_totals_fields()
