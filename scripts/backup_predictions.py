"""
Backup predictions before any cleanup operations.
Creates timestamped backups of prediction files.
"""
import shutil
from pathlib import Path
from datetime import datetime

def backup_predictions():
    """Create backups of all prediction master files."""
    backup_dir = Path("data/backups/predictions")
    backup_dir.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    model_types = ['ensemble', 'random_forest', 'gradient_boosting']
    
    backed_up = []
    for model_type in model_types:
        source = Path(f"data/forward_test/{model_type}/predictions_master.parquet")
        
        if not source.exists():
            print(f"Skipping {model_type} - file not found")
            continue
        
        # Create backup with timestamp
        backup_name = f"predictions_master_{model_type}_{timestamp}.parquet"
        dest = backup_dir / backup_name
        
        shutil.copy2(source, dest)
        backed_up.append(str(dest))
        print(f"✓ Backed up {model_type} to {dest}")
    
    return backed_up

if __name__ == "__main__":
    print("Creating backups of prediction files...")
    print("=" * 60)
    backups = backup_predictions()
    print(f"\n✓ Created {len(backups)} backups")
    print("\nBackup locations:")
    for backup in backups:
        print(f"  - {backup}")
