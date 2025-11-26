import shutil
import os
from datetime import datetime
import glob

def backup_database():
    # Configuration
    DB_FILE = "data/betting.db"
    BACKUP_DIR = "backups"
    RETENTION_DAYS = 7
    
    # Create backup directory if it doesn't exist
    if not os.path.exists(BACKUP_DIR):
        os.makedirs(BACKUP_DIR)
        print(f"Created backup directory: {BACKUP_DIR}")
    
    # Check if DB exists
    if not os.path.exists(DB_FILE):
        print(f"Database file not found: {DB_FILE}")
        return
    
    # Generate timestamped filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_file = os.path.join(BACKUP_DIR, f"betting_{timestamp}.db")
    
    # Copy file
    try:
        shutil.copy2(DB_FILE, backup_file)
        print(f"Successfully backed up database to: {backup_file}")
        
        # Verify size
        original_size = os.path.getsize(DB_FILE)
        backup_size = os.path.getsize(backup_file)
        print(f"Original size: {original_size/1024/1024:.2f} MB")
        print(f"Backup size: {backup_size/1024/1024:.2f} MB")
        
    except Exception as e:
        print(f"Error backing up database: {e}")
        return

    # Cleanup old backups
    try:
        backups = sorted(glob.glob(os.path.join(BACKUP_DIR, "betting_*.db")))
        
        # Keep only the last N backups (or based on date)
        # Here we'll just keep the last RETENTION_DAYS * 24 (assuming hourly backups) 
        # or just simple count for now. Let's keep last 10 backups.
        MAX_BACKUPS = 10
        
        if len(backups) > MAX_BACKUPS:
            to_delete = backups[:-MAX_BACKUPS]
            for f in to_delete:
                os.remove(f)
                print(f"Deleted old backup: {f}")
                
    except Exception as e:
        print(f"Error cleaning up old backups: {e}")

if __name__ == "__main__":
    backup_database()
