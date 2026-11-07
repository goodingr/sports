"""Show the last archived NBA discovery balance without making requests."""

import re
import sqlite3
from contextlib import closing
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import dash_bootstrap_components as dbc
from dash import html

from src.data.audit_nba_schedule import read_verified_nba_events_receipt
from src.data.ingest_nba_events import _INTERVAL, _SOURCE, _utc
from src.predict.paper_close_poll import _CLOSE_REQUEST_CREDITS, _CREDIT_RESERVE
from src.predict.paper_entry_poll import _REQUEST_CREDITS

_EASTERN = ZoneInfo("America/New_York")
_STALE_MINUTES = int(_INTERVAL.total_seconds() // 60) + 20


def discovery_quota_card(db_path: Path, now: datetime) -> dbc.Alert:
    try:
        db_path = db_path.resolve()
        with closing(sqlite3.connect(f"{db_path.as_uri()}?mode=ro", uri=True, timeout=2)) as conn:
            rows = conn.execute(
                """
                SELECT f.captured_at, f.storage_path, f.hash
                FROM source_files f JOIN data_sources s USING (source_id)
                WHERE s.source_key = ? AND julianday(f.captured_at) <= julianday(?)
                ORDER BY julianday(f.captured_at) DESC, f.captured_at DESC LIMIT 2
                """,
                (_SOURCE, now.isoformat()),
            ).fetchall()
        if not rows:
            return dbc.Alert("No NBA discovery receipt is available yet.", color="secondary")
        timestamp, stored_path, checksum = rows[0]
        observed = _utc(timestamp)
        if observed is None or observed > now or (len(rows) == 2 and _utc(rows[1][0]) == observed):
            raise ValueError("Invalid discovery receipt time")
        payload = read_verified_nba_events_receipt(
            stored_path, db_path.parent, source=_SOURCE, observed=observed, checksum=checksum
        )
        quota = payload.get("rate_limit_remaining")
        if not isinstance(quota, str) or re.fullmatch(r"[0-9]+", quota) is None:
            raise ValueError("Missing or invalid reported balance")
        remaining = int(quota)
    except (AttributeError, KeyError, OSError, TypeError, ValueError, sqlite3.Error):
        return dbc.Alert("Latest NBA discovery balance is unavailable or unverified.", color="warning")

    close_minimum = _CREDIT_RESERVE + _CLOSE_REQUEST_CREDITS
    entry_minimum = _CREDIT_RESERVE + _REQUEST_CREDITS
    color = "secondary"
    if remaining < close_minimum:
        status, color = "Reported balance was below both thresholds.", "danger"
    elif remaining < entry_minimum:
        status, color = "Reported balance was below the early entry threshold.", "warning"
    else:
        status = f"Reported balance was {remaining - _CREDIT_RESERVE:,} credits above the reserve."
    stale = now - observed > timedelta(minutes=_STALE_MINUTES)
    details = [
        html.Strong(f"{remaining:,} credits reported"),
        html.Div(f"Checksum-verified discovery · {observed.astimezone(_EASTERN):%b %d, %I:%M:%S %p %Z}"),
        html.Div(f"Reserve {_CREDIT_RESERVE} · Closing requires {close_minimum} · Early entry requires {entry_minimum}"),
        html.Div(status),
        html.Div("This receipt covers one key. Collectors check current quota before spending."),
    ]
    if stale:
        details.append(html.Div(
            f"This observation is more than {_STALE_MINUTES} minutes old.", className="fw-semibold"
        ))
    return dbc.Alert(details, color="warning" if stale else color)
