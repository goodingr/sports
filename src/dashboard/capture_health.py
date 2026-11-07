"""Show the daily NBA trial receipt and the capture it supports."""

import sqlite3
from contextlib import closing
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import dash_bootstrap_components as dbc
import yaml
from dash import html

from scripts.paper_capture_health import assess_capture_health
from src.predict.paper_ledger import load_trial

_EASTERN = ZoneInfo("America/New_York")
_STATUS = {
    "healthy": ("Daily receipt and evaluation verified", "success"),
    "waiting": ("Waiting for the first daily capture window to finish", "secondary"),
    "receipt_missing": ("Daily odds receipt is missing", "warning"),
    "receipt_invalid": ("Daily odds receipt could not be verified", "warning"),
    "receipt_error": ("Daily odds request returned an error", "danger"),
    "capture_missing": ("Receipt found without a matching capture run", "warning"),
    "capture_failed": ("Daily capture failed", "danger"),
    "capture_incomplete": ("Daily capture coverage is incomplete", "warning"),
    "empty_slate_unverified": ("Receipt was empty and slate coverage is unverified", "warning"),
    "trial_unregistered": ("Configured trial has no ledger registration", "warning"),
    "trial_mismatch": ("Configured trial differs from the ledger registration", "warning"),
}


def _time_text(stamp: str) -> str:
    return datetime.fromisoformat(stamp).astimezone(_EASTERN).strftime("%b %d, %I:%M:%S %p %Z")


def daily_capture_card(db_path: Path, rules_path: Path, now: datetime) -> dbc.Alert:
    try:
        trial = load_trial(rules_path)
        db_path = db_path.resolve()
        raw_dir = db_path.parent / "raw" / "odds" / "basketball_nba"
        with closing(sqlite3.connect(f"{db_path.as_uri()}?mode=ro", uri=True, timeout=2)) as conn:
            conn.row_factory = sqlite3.Row
            conn.execute("BEGIN")
            health = assess_capture_health(conn, trial, raw_dir, now, data_dir=db_path.parent)
    except (KeyError, OSError, ValueError, TypeError, sqlite3.Error, yaml.YAMLError):
        return dbc.Alert("Daily capture evidence is unavailable.", color="warning")

    label, color = _STATUS.get(health["status"], ("Daily capture evidence is unavailable.", "warning"))
    details = [html.Strong(label)]
    if "expected_at" in health:
        details.append(html.Div(
            f"Expected {_time_text(health['expected_at'])} · "
            f"Grace ends {_time_text(health['window_ends_at'])}"
        ))
    if "receipt_at" in health:
        details.append(html.Div(
            f"Odds receipt {_time_text(health['receipt_at'])} · {health['receipt_file']}"
        ))
    if "capture_at" in health:
        details.append(html.Div(f"Capture run {_time_text(health['capture_at'])}"))
    if "evaluated_fixtures" in health:
        details.append(html.Div(
            f"Evaluated {health['evaluated_fixtures']} / {health['eligible_fixtures']} eligible fixtures · "
            f"{health['below_edge_fixtures']} below the edge threshold"
        ))
    if "known_eligible_fixtures" in health and health["known_eligible_fixtures"] is not None:
        details.append(html.Div(
            f"Known schedule · {health['known_eligible_fixtures']} eligible · "
            f"{len(health['known_missing_fixture_ids'])} missing from receipt · "
            f"{len(health['known_unresolved_fixture_ids'])} unresolved"
        ))
    if "latest_attempt_at" in health:
        details.append(html.Div(
            f"Later attempt {_time_text(health['latest_attempt_at'])} · "
            f"{health['latest_attempt_status'].replace('_', ' ')}"
        ))
    return dbc.Alert(details, color=color)
