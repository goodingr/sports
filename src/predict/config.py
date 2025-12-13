"""Configuration constants for the prediction system."""

from pathlib import Path

# Directory paths
PREDICTIONS_DIR = Path("data/forward_test")
MODEL_REGISTRY_PATH = Path("models")

# League configuration
SUPPORTED_LEAGUES = [
    "NFL", "NBA", "CFB", "NCAAB", "NHL",
    "EPL", "LALIGA", "BUNDESLIGA", "SERIEA", "LIGUE1"
]

SOCCER_LEAGUES = {"EPL", "LALIGA", "BUNDESLIGA", "SERIEA", "LIGUE1"}

# Map leagues to sport keys (for internal consistency if needed)
LEAGUE_SPORT_KEYS = {
    "NFL": "americanfootball_nfl",
    "NBA": "basketball_nba",
    "CFB": "americanfootball_ncaaf",
    "NCAAB": "basketball_ncaab",
    "NHL": "icehockey_nhl",
    "EPL": "soccer_epl",
    "LALIGA": "soccer_spain_la_liga",
    "BUNDESLIGA": "soccer_germany_bundesliga",
    "SERIEA": "soccer_italy_serie_a",
    "LIGUE1": "soccer_france_ligue_one",
}
