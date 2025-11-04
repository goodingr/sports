"""Team name normalization across leagues."""

from __future__ import annotations

from typing import Dict


NFL_ALIASES: Dict[str, str] = {
    "arizona cardinals": "ARI",
    "atlanta falcons": "ATL",
    "baltimore ravens": "BAL",
    "buffalo bills": "BUF",
    "carolina panthers": "CAR",
    "chicago bears": "CHI",
    "cincinnati bengals": "CIN",
    "cleveland browns": "CLE",
    "dallas cowboys": "DAL",
    "denver broncos": "DEN",
    "detroit lions": "DET",
    "green bay packers": "GB",
    "houston texans": "HOU",
    "indianapolis colts": "IND",
    "jacksonville jaguars": "JAX",
    "kansas city chiefs": "KC",
    "las vegas raiders": "LV",
    "oakland raiders": "LV",
    "los angeles chargers": "LAC",
    "san diego chargers": "LAC",
    "los angeles rams": "LAR",
    "st. louis rams": "LAR",
    "miami dolphins": "MIA",
    "minnesota vikings": "MIN",
    "new england patriots": "NE",
    "new orleans saints": "NO",
    "new york giants": "NYG",
    "ny giants": "NYG",
    "new york jets": "NYJ",
    "ny jets": "NYJ",
    "philadelphia eagles": "PHI",
    "pittsburgh steelers": "PIT",
    "san francisco 49ers": "SF",
    "seattle seahawks": "SEA",
    "tampa bay buccaneers": "TB",
    "tennessee titans": "TEN",
    "houston oilers": "TEN",
    "washington commanders": "WAS",
    "washington football team": "WAS",
    "washington redskins": "WAS",
    "la rams": "LAR",
    "la chargers": "LAC",
}


NBA_ALIASES: Dict[str, str] = {
    "atlanta hawks": "ATL",
    "hawks": "ATL",
    "boston celtics": "BOS",
    "celtics": "BOS",
    "brooklyn nets": "BKN",
    "new jersey nets": "BKN",
    "nets": "BKN",
    "charlotte hornets": "CHA",
    "hornets": "CHA",
    "chicago bulls": "CHI",
    "bulls": "CHI",
    "cleveland cavaliers": "CLE",
    "cavaliers": "CLE",
    "cavs": "CLE",
    "dallas mavericks": "DAL",
    "mavericks": "DAL",
    "mavs": "DAL",
    "denver nuggets": "DEN",
    "nuggets": "DEN",
    "detroit pistons": "DET",
    "pistons": "DET",
    "golden state warriors": "GSW",
    "gs warriors": "GSW",
    "warriors": "GSW",
    "houston rockets": "HOU",
    "rockets": "HOU",
    "indiana pacers": "IND",
    "pacers": "IND",
    "la clippers": "LAC",
    "los angeles clippers": "LAC",
    "clippers": "LAC",
    "los angeles lakers": "LAL",
    "la lakers": "LAL",
    "lakers": "LAL",
    "memphis grizzlies": "MEM",
    "grizzlies": "MEM",
    "miami heat": "MIA",
    "heat": "MIA",
    "milwaukee bucks": "MIL",
    "bucks": "MIL",
    "minnesota timberwolves": "MIN",
    "timberwolves": "MIN",
    "wolves": "MIN",
    "new orleans pelicans": "NOP",
    "pelicans": "NOP",
    "new orleans hornets": "NOP",
    "new york knicks": "NYK",
    "knicks": "NYK",
    "oklahoma city thunder": "OKC",
    "okc thunder": "OKC",
    "thunder": "OKC",
    "seattle supersonics": "OKC",
    "orlando magic": "ORL",
    "magic": "ORL",
    "philadelphia 76ers": "PHI",
    "76ers": "PHI",
    "sixers": "PHI",
    "phoenix suns": "PHX",
    "suns": "PHX",
    "portland trail blazers": "POR",
    "portland blazers": "POR",
    "trail blazers": "POR",
    "sacramento kings": "SAC",
    "kings": "SAC",
    "san antonio spurs": "SAS",
    "spurs": "SAS",
    "toronto raptors": "TOR",
    "raptors": "TOR",
    "utah jazz": "UTA",
    "jazz": "UTA",
    "washington wizards": "WAS",
    "wizards": "WAS",
}


ALIAS_MAP = {
    "NFL": NFL_ALIASES,
    "NBA": NBA_ALIASES,
}


def normalize_team_code(league: str, name: str | None) -> str:
    if not name:
        return ""
    cleaned = name.strip()
    if not cleaned:
        return ""
    if len(cleaned) <= 3 and cleaned.isalpha():
        return cleaned.upper()

    aliases = ALIAS_MAP.get(league.upper()) or {}
    key = cleaned.lower()
    if key in aliases:
        return aliases[key]

    # remove punctuation and try again
    simplified = key.replace(".", "").replace("-", " ")
    simplified = " ".join(simplified.split())
    if simplified in aliases:
        return aliases[simplified]

    # try splitting words and taking first three letters
    words = simplified.split()
    if len(words) == 2 and words[1] in {"fc", "sc"}:
        words = words[:1]
    if len(words) >= 2:
        candidate = (words[0][:1] + words[-1][:2]).upper()
        if len(candidate) == 3:
            return candidate

    return cleaned.upper()

