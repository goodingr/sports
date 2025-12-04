import logging
import traceback
from src.features.dataset.shared import build_base_dataset

logging.basicConfig(level=logging.INFO)

def reproduce():
    try:
        print("Attempting to build NCAAB dataset...")
        df = build_base_dataset([2024, 2025], "NCAAB")
        print("Build successful!")
        print(df.head())
    except Exception:
        traceback.print_exc()

if __name__ == "__main__":
    reproduce()
