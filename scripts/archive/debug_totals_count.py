import sys
from pathlib import Path
import pandas as pd

# Add src to path
sys.path.append(str(Path.cwd()))

from src.dashboard.data import load_forward_test_data, filter_by_version, calculate_totals_metrics

def debug_totals_count():
    print("Loading Random Forest data...")
    df = load_forward_test_data(model_type="random_forest")
    
    print(f"\nRaw data: {len(df)} rows")
    print(f"Unique games: {df['game_id'].nunique() if 'game_id' in df.columns else 'N/A'}")
    
    # Test each version
    for version in ["v0.1", "v0.2", "v0.3"]:
        filtered = filter_by_version(df, version)
        print(f"\n{version}:")
        print(f"  Filtered rows: {len(filtered)}")
        print(f"  Unique games: {filtered['game_id'].nunique() if 'game_id' in filtered.columns and not filtered.empty else 0}")
        
        if not filtered.empty:
            metrics = calculate_totals_metrics(filtered)
            print(f"  Total predictions (from metrics): {metrics.total_predictions}")
            print(f"  Completed games: {metrics.completed_games}")
            print(f"  Pending games: {metrics.pending_games}")

if __name__ == "__main__":
    debug_totals_count()
