import sys
import os

# Add project root to path
sys.path.insert(0, os.getcwd())

try:
    print("Attempting to import src.features.dataset.shared")
    import src.features.dataset.shared as shared
    print("Success importing shared")
except Exception as e:
    print(f"Failed to import shared: {e}")

try:
    print("Attempting to import src.features.dataset.nfl")
    import src.features.dataset.nfl as nfl
    print("Success importing nfl")
except Exception as e:
    print(f"Failed to import nfl: {e}")
