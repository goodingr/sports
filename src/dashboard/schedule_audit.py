"""Display saved NBA schedule evidence without contacting the provider."""

import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import dash_bootstrap_components as dbc
from dash import html

_EASTERN = ZoneInfo("America/New_York")
_STALE_AFTER = timedelta(hours=6)


def _timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.utcoffset() is None:
        raise ValueError("Audit timestamps must include a time zone")
    return parsed.astimezone(timezone.utc)


def _time_text(value: datetime) -> str:
    return value.astimezone(_EASTERN).strftime("%b %d, %I:%M %p %Z")


def _saved_card(day: date, report: dict[str, Any], now: datetime) -> dbc.Card:
    official = _timestamp(report["official_source"]["observed_at_utc"])
    audited = _timestamp(report["warehouse_audited_at_utc"])
    fixtures = report["fixtures"]
    if (
        report["report_version"] != 1
        or report["eastern_date"] != day.isoformat()
        or not isinstance(report["trial_id"], str)
        or not official <= audited <= now
        or not isinstance(fixtures, list)
        or len(fixtures) != report["scheduled_regular_season_games"]
    ):
        raise ValueError("Saved audit does not match the selected slate")

    if any(
        type(row[key]) is not bool
        for row in fixtures
        for key in ("provider_linked", "warehouse_matched", "early_entry", "deadline_passed")
    ):
        raise ValueError("Saved audit contains invalid fixture coverage")
    linked = sum(row["provider_linked"] for row in fixtures)
    matched = sum(row["warehouse_matched"] for row in fixtures)
    early_gaps = sum(row["early_entry"] and not row["provider_linked"] for row in fixtures)
    observed = report["provider_observed"]
    archives = report["provider_evidence"]
    checked = archives["archives_checked"]
    unavailable = archives["archives_unavailable_or_invalid"]
    if linked != report["provider_linked"] or matched != report["warehouse_matched"]:
        raise ValueError("Saved audit coverage totals disagree with fixture evidence")
    if (
        any(type(value) is not int for value in (observed, checked, unavailable))
        or not linked <= observed <= len(fixtures)
        or not 0 <= unavailable <= checked
    ):
        raise ValueError("Saved audit has invalid archive counts")

    details: list[Any] = [
        html.P(
            f"Warehouse matched {matched} of {len(fixtures)} · "
            f"Provider observed {observed} of {len(fixtures)} · "
            f"Verified provider links {linked} of {len(fixtures)} · "
            f"Early starts without a verified link {early_gaps}",
            className="fw-semibold mb-2",
        ),
        html.P(
            f"Official schedule observed {_time_text(official)} · "
            f"Warehouse checked {_time_text(audited)} · Trial {report['trial_id']}",
            className="small text-muted mb-2",
        ),
        html.P(f"Provider archives checked {checked}", className="small text-muted mb-2"),
    ]
    if unavailable:
        details.append(
            dbc.Alert(f"Provider archives unavailable or invalid · {unavailable}", color="warning")
        )
    if now - official > _STALE_AFTER or now - audited > _STALE_AFTER:
        details.append(dbc.Alert("Saved audit is more than six hours old.", color="warning"))
    if fixtures:
        details.append(
            dbc.Table(
                [
                    html.Thead(
                        html.Tr(
                            [html.Th("Matchup"), html.Th("Tipoff"), html.Th("Provider evidence")]
                        )
                    ),
                    html.Tbody(
                        [
                            html.Tr(
                                [
                                    html.Td(f"{row['away']} at {row['home']}"),
                                    html.Td(_time_text(_timestamp(row["official_start_utc"]))),
                                    html.Td(
                                        row["status"].replace("_", " ").capitalize()
                                        + (" · Early start" if row["early_entry"] else "")
                                        + (
                                            " · Entry deadline passed"
                                            if row["deadline_passed"]
                                            else ""
                                        )
                                    ),
                                ]
                            )
                            for row in fixtures
                        ]
                    ),
                ],
                size="sm",
                responsive=True,
                className="mb-2",
            )
        )
    else:
        details.append(html.P("No scheduled regular-season games in this saved observation."))
    details.append(
        html.A("Official NBA slate", href=f"https://www.nba.com/games?date={day.isoformat()}")
    )
    return dbc.Card(dbc.CardBody([html.H5(day.strftime("%a, %b %d")), *details]), className="mb-3")


def schedule_coverage_cards(directory: Path, now: datetime) -> list[Any]:
    today = now.astimezone(_EASTERN).date()
    cards = []
    for day in (today, today + timedelta(days=1)):
        try:
            latest = max((directory / day.isoformat()).glob("*.json"), default=None)
            if latest is None:
                cards.append(dbc.Alert(f"No saved audit for {day:%b %d}.", color="secondary"))
                continue
            cards.append(_saved_card(day, json.loads(latest.read_text(encoding="utf-8")), now))
        except (AttributeError, KeyError, OSError, TypeError, ValueError):
            cards.append(dbc.Alert(f"Saved audit for {day:%b %d} is unavailable.", color="warning"))
    return cards
