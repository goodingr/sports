import logging
import sys
from src.data.sources.nba_rolling_metrics import ingest

logging.basicConfig(level=logging.INFO)

def main():
    # Calculate for recent seasons including current
    # 2021-2025 covers the range in the user's logs
    seasons = [2021, 2022, 2023, 2024, 2025]
    print(f"Calculating NBA rolling metrics for seasons: {seasons}")
    try:
        output_dir = ingest(seasons=seasons)
        print(f"Successfully generated metrics in: {output_dir}")
    except Exception as e:
        print(f"Error generating metrics: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
