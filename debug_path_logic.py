
from src.dashboard.data import load_forward_test_data, MASTER_PREDICTIONS_PATH, FORWARD_TEST_DIR
from pathlib import Path

def debug_path():
    print(f"MASTER_PREDICTIONS_PATH: {MASTER_PREDICTIONS_PATH}")
    print(f"FORWARD_TEST_DIR: {FORWARD_TEST_DIR}")
    
    # Simulate what load_forward_test_data does
    path = MASTER_PREDICTIONS_PATH
    model_type = "ensemble"
    
    print(f"Initial Path: {path}")
    print(f"Path == MASTER: {path == MASTER_PREDICTIONS_PATH}")
    
    if path == MASTER_PREDICTIONS_PATH:
        path = FORWARD_TEST_DIR / model_type / "predictions_master.parquet"
        print(f"Updated Path: {path}")
    else:
        print("Path NOT updated")

if __name__ == "__main__":
    debug_path()
