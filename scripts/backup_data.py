"""
Backup critical data files to prevent data loss.
Keeps the last 7 daily backups.
"""
import shutil
import os
from datetime import datetime
from pathlib import Path
import glob

# Configuration
BACKUP_ROOT = Path("backups")
FILES_TO_BACKUP = [
    Path("data/betting.db"),
    Path("data/forward_test/ensemble/predictions_master.parquet"),
]
RETENTION_DAYS = 7

def create_backup():
    """Create a timestamped backup of critical files."""
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    backup_dir = BACKUP_ROOT / timestamp
    
    print(f"Creating backup in {backup_dir}...")
    backup_dir.mkdir(parents=True, exist_ok=True)
    
    for file_path in FILES_TO_BACKUP:
        if file_path.exists():
            dest = backup_dir / file_path.name
            print(f"  Copying {file_path} -> {dest}")
            shutil.copy2(file_path, dest)
        else:
            print(f"  Warning: Source file {file_path} not found!")

def cleanup_old_backups():
    """Delete backups older than RETENTION_DAYS."""
    print(f"Cleaning up backups older than {RETENTION_DAYS} days...")
    
    # Get all backup directories
    backups = sorted([d for d in BACKUP_ROOT.iterdir() if d.is_dir()], key=os.path.getmtime)
    
    if len(backups) > RETENTION_DAYS:
        to_delete = backups[:-RETENTION_DAYS]
        for d in to_delete:
            print(f"  Deleting old backup: {d}")
            shutil.rmtree(d)
    else:
        print("  No old backups to delete.")

if __name__ == "__main__":
    try:
        create_backup()
        cleanup_old_backups()
        print("Backup completed successfully.")
    except Exception as e:
        print(f"Backup failed: {e}")
        # Don't exit with error, we don't want to stop the pipeline if backup fails, 
        # but we should alert. Actually, maybe we SHOULD stop? 
        # For now, let's print error but allow pipeline to continue, 
        # or maybe exit 1 to be safe? 
        # "We should not lose more than a day worth of data" -> Safety first.
        # If backup fails, we should probably stop to warn the user.
        exit(1)
