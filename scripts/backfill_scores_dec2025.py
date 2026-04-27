
import logging
import sys
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(PROJECT_ROOT))

# Import backfill modules
from src.data import backfill_nfl, backfill_nba, backfill_cfb, backfill_soccer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
LOGGER = logging.getLogger(__name__)

def main():
    LOGGER.info("Starting backfill for December 2025 scores...")
    
    # NFL - Season 2025 (covering Dec 2025)
    try:
        LOGGER.info("=== Backfilling NFL (Season 2025) ===")
        backfill_nfl.run(seasons=[2025])
    except Exception as e:
        LOGGER.error(f"NFL backfill failed: {e}")

    # NBA - Season 2025 (2025-26 season)
    try:
        LOGGER.info("=== Backfilling NBA (Season 2025) ===")
        backfill_nba.run(seasons=[2025], season_type="Regular Season")
    except Exception as e:
        LOGGER.error(f"NBA backfill failed: {e}")

    # CFB - Season 2025 (Regular + Postseason)
    try:
        LOGGER.info("=== Backfilling CFB (Season 2025 - Regular) ===")
        backfill_cfb.ingest(seasons=[2025], season_type="regular")
        LOGGER.info("=== Backfilling CFB (Season 2025 - Postseason) ===")
        backfill_cfb.ingest(seasons=[2025], season_type="postseason")
    except Exception as e:
        LOGGER.error(f"CFB backfill failed: {e}")

    # Soccer - Dec 2025 Date Range
    try:
        LOGGER.info("=== Backfilling Soccer (Dec 2025) ===")
        backfill_soccer.run(
            leagues=["EPL", "LALIGA", "BUNDESLIGA", "SERIEA", "LIGUE1"],
            start_date="2025-12-01",
            end_date="2025-12-31"
        )
    except Exception as e:
        LOGGER.error(f"Soccer backfill failed: {e}")

    # NHL / NCAAB - Manual Note
    LOGGER.warning("=== NHL and NCAAB ===")
    LOGGER.warning("No dedicated backfill script found for NHL or NCAAB. Please verify these manually or use the dashboard to check if scores are present.")
    
    LOGGER.info("Backfill complete.")

if __name__ == "__main__":
    main()
