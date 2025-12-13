"""
Improved duplicate cleanup - keep the prediction with the most complete totals data.
"""
import sqlite3
from pathlib import Path

DB_PATH = Path("data/betting.db")

def smart_cleanup_duplicates():
    """Remove duplicate predictions, keeping the one with most complete totals data."""
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    print("\nSmart cleanup of duplicate predictions...")
    
    # Find all duplicates
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
        # Get all predictions for this game+model
        cursor.execute("""
            SELECT rowid, 
                   CASE WHEN total_line IS NOT NULL THEN 1 ELSE 0 END +
                   CASE WHEN over_prob IS NOT NULL THEN 1 ELSE 0 END +
                   CASE WHEN predicted_total_points IS NOT NULL THEN 1 ELSE 0 END as completeness,
                   predicted_at
            FROM predictions
            WHERE game_id = ?
              AND model_type = ?
            ORDER BY completeness DESC, predicted_at DESC
        """, (game_id, model_type))
        
        predictions = cursor.fetchall()
        
        # Keep the first one (most complete, then most recent)
        keep_rowid = predictions[0][0]
        
        # Delete all others
        cursor.execute("""
            DELETE FROM predictions
            WHERE game_id = ?
              AND model_type = ?
              AND rowid != ?
        """, (game_id, model_type, keep_rowid))
        
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
    
    # Show stats
    cursor.execute("""
        SELECT 
            COUNT(*) as total,
            COUNT(total_line) as with_line,
            COUNT(over_prob) as with_over_prob,
            COUNT(predicted_total_points) as with_predicted
        FROM predictions
        WHERE model_type = 'gradient_boosting'
    """)
    
    stats = cursor.fetchone()
    print(f"\nGradient Boosting Stats:")
    print(f"  Total predictions: {stats[0]}")
    print(f"  With total_line: {stats[1]}")
    print(f"  With over_prob: {stats[2]}")
    print(f"  With predicted_total_points: {stats[3]}")
    
    conn.close()

if __name__ == "__main__":
    print("="*60)
    print("SMART DUPLICATE CLEANUP")
    print("="*60)
    
    smart_cleanup_duplicates()
    
    print("\n" + "="*60)
    print("Cleanup complete!")
    print("="*60)
