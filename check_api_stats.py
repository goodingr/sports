
import requests
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def check_api_stats():
    try:
        response = requests.get("http://localhost:8000/api/bets/stats")
        if response.status_code == 200:
            logger.info(f"API Stats: {response.json()}")
        else:
            logger.error(f"Error: {response.status_code}")
    except Exception as e:
        logger.error(f"Connection Error: {e}")

if __name__ == "__main__":
    check_api_stats()
