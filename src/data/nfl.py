"""NFL-specific constants and helpers."""

from __future__ import annotations

from typing import Dict


NFL_TEAM_ALIASES: Dict[str, str] = {
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
    "new york jets": "NYJ",
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
}


NFL_TEAM_DIVISIONS: Dict[str, tuple[str, str]] = {
    "ARI": ("NFC", "West"),
    "ATL": ("NFC", "South"),
    "BAL": ("AFC", "North"),
    "BUF": ("AFC", "East"),
    "CAR": ("NFC", "South"),
    "CHI": ("NFC", "North"),
    "CIN": ("AFC", "North"),
    "CLE": ("AFC", "North"),
    "DAL": ("NFC", "East"),
    "DEN": ("AFC", "West"),
    "DET": ("NFC", "North"),
    "GB": ("NFC", "North"),
    "HOU": ("AFC", "South"),
    "IND": ("AFC", "South"),
    "JAX": ("AFC", "South"),
    "KC": ("AFC", "West"),
    "LAC": ("AFC", "West"),
    "LAR": ("NFC", "West"),
    "LV": ("AFC", "West"),
    "MIA": ("AFC", "East"),
    "MIN": ("NFC", "North"),
    "NE": ("AFC", "East"),
    "NO": ("NFC", "South"),
    "NYG": ("NFC", "East"),
    "NYJ": ("AFC", "East"),
    "PHI": ("NFC", "East"),
    "PIT": ("AFC", "North"),
    "SEA": ("NFC", "West"),
    "SF": ("NFC", "West"),
    "TB": ("NFC", "South"),
    "TEN": ("AFC", "South"),
    "WAS": ("NFC", "East"),
}


def normalize_team_name(name: str) -> str:
    """Return the standard team code given a team name."""

    key = name.strip().lower()
    if key in NFL_TEAM_ALIASES:
        return NFL_TEAM_ALIASES[key]
    return name.strip().upper()


def get_team_division(team: str) -> tuple[str, str] | None:
    """Return (conference, division) for a team code or friendly name."""

    code = normalize_team_name(team)
    return NFL_TEAM_DIVISIONS.get(code)


def get_team_conference(team: str) -> str | None:
    """Return the conference (AFC/NFC) for a given team."""

    division = get_team_division(team)
    if division is None:
        return None
    return division[0]


def is_division_game(team_a: str, team_b: str) -> bool:
    """Determine whether two teams are in the same division."""

    div_a = get_team_division(team_a)
    div_b = get_team_division(team_b)
    return bool(div_a and div_b and div_a == div_b)


def is_conference_game(team_a: str, team_b: str) -> bool:
    """Determine whether two teams belong to the same conference."""

    conf_a = get_team_conference(team_a)
    conf_b = get_team_conference(team_b)
    return bool(conf_a and conf_b and conf_a == conf_b)

