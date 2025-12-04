import json
import sys
import time
from pathlib import Path

import requests

ALL_LEAGUES = ["EPL", "La_liga", "Bundesliga", "Serie_A", "Ligue_1"]
seasons = list(range(2014, 2026))
base_dir = Path("data/raw/sources/understat")
base_dir.mkdir(parents=True, exist_ok=True)

target_vars = ["datesData", "teamsData", "playersData"]
headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

leagues = sys.argv[1:]
if not leagues:
    leagues = ALL_LEAGUES


def extract_var(html: str, var: str) -> str | None:
    marker = f"var {var}"
    idx = html.find(marker)
    if idx == -1:
        return None
    start = html.find("JSON.parse", idx)
    if start == -1:
        return None
    start_quote = html.find("'", start)
    if start_quote == -1:
        return None
    end_quote = start_quote + 1
    depth = 1
    while end_quote < len(html):
        if html[end_quote] == "'" and html[end_quote - 1] != "\\":
            break
        end_quote += 1
    else:
        return None
    return html[start_quote + 1 : end_quote]

summary = []
for league in leagues:
    for season in seasons:
        url = f"https://understat.com/league/{league}/{season}"
        try:
            resp = requests.get(url, headers=headers, timeout=30)
        except Exception as exc:
            summary.append((league, season, f"error: {exc}"))
            continue
        if resp.status_code != 200:
            summary.append((league, season, f"status {resp.status_code}"))
            continue
        html = resp.text
        league_dir = (base_dir / league)
        league_dir.mkdir(parents=True, exist_ok=True)
        saved = []
        for var in target_vars:
            payload = extract_var(html, var)
            if payload is None:
                continue
            decoded = bytes(payload, 'utf-8').decode('unicode_escape')
            data = json.loads(decoded)
            out_path = league_dir / f"{season}_{var.replace('Data','').lower()}.json"
            out_path.write_text(json.dumps(data, indent=2), encoding='utf-8')
            saved.append(var)
        summary.append((league, season, f"saved {saved}"))
        time.sleep(0.15)

for entry in summary:
    print(" | ".join(map(str, entry)))
