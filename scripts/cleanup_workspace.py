import os
import shutil
from datetime import datetime
from pathlib import Path

def cleanup_workspace():
    # Define the root directory
    root_dir = Path("C:/Users/Bobby/Desktop/sports")
    
    # Create archive directory with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    archive_dir = root_dir / "archive" / f"cleanup_{timestamp}"
    archive_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"Created archive directory: {archive_dir}")
    
    # Patterns/Files to move
    patterns = [
        "tmp_*",
        "check_*.py",
        "compare_*.py",
        "verify_*.py",
        "model_comparison.txt",
        "backtest_results.txt",
        "analyze_features.py",
        "inspect_parquet.py",
        "investigate_odds.py",
        "diagnose_odds_issue.py",
        "run_nba_metrics.py",
        "show_backtest.py",
        "simple_comparison.py",
        "test_import.py",
        "debug_team_matching.py",
        "temp.txt",
        "temp_dataset.ps1",
        "tmp_capture_html.py",
        "tmp_forward_check.py"
    ]
    
    moved_count = 0
    
    # Find and move files
    for pattern in patterns:
        for file_path in root_dir.glob(pattern):
            if file_path.is_file():
                dest_path = archive_dir / file_path.name
                try:
                    shutil.move(str(file_path), str(dest_path))
                    print(f"Moved: {file_path.name}")
                    moved_count += 1
                except Exception as e:
                    print(f"Error moving {file_path.name}: {e}")
                    
    print(f"\nCleanup complete. Moved {moved_count} files to {archive_dir}")

if __name__ == "__main__":
    cleanup_workspace()
