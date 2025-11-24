"""Sportsbook URL mappings for bet card links."""

# Homepage URLs for major sportsbooks
SPORTSBOOK_URLS = {
    "draftkings": "https://sportsbook.draftkings.com",
    "fanduel": "https://sportsbook.fanduel.com",
    "betmgm": "https://sports.betmgm.com",
    "caesars": "https://www.caesars.com/sportsbook",
    "pointsbet": "https://pointsbet.com",
    "bet365": "https://www.bet365.com",
    "wynnbet": "https://www.wynnbet.com",
    "unibet": "https://www.unibet.com",
    "barstool": "https://www.barstoolsportsbook.com",
    "foxbet": "https://www.foxbet.com",
    "betrivers": "https://www.betrivers.com",
    "sugarhouse": "https://www.sugarhousecasino.com/sportsbook",
    "williamhill": "https://www.williamhill.com/us",
    "superbook": "https://www.superbook.com",
    "hardrock": "https://www.hardrock.bet",
    "espnbet": "https://espnbet.com",
    "fanatics": "https://fanaticssportsbook.com",
    "mybookie": "https://www.mybookie.ag",
    "mybookie.ag": "https://www.mybookie.ag",
    "bovada": "https://www.bovada.lv",
    "lowvig": "https://www.lowvig.ag",
    "lowvig.ag": "https://www.lowvig.ag",
    "betonline": "https://www.betonline.ag",
    "betonline.ag": "https://www.betonline.ag",
    "heritage": "https://www.heritagesports.eu",
    "pinnacle": "https://www.pinnacle.com",
    "circa": "https://www.circastadium.com/sportsbook",
    "betus": "https://www.betus.com.pa",
}


def get_sportsbook_url(book_name: str) -> str:
    """
    Get the homepage URL for a sportsbook.
    
    Args:
        book_name: Name of the sportsbook (case-insensitive)
        
    Returns:
        URL string, or empty string if sportsbook not found
    """
    if not book_name:
        return ""
    
    # Normalize the book name
    normalized = book_name.lower().strip()
    
    # Remove common suffixes/prefixes
    normalized = normalized.replace("sportsbook", "").strip()
    normalized = normalized.replace(" ", "")
    
    return SPORTSBOOK_URLS.get(normalized, "")
