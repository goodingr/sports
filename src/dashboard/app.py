"""Dash application for forward testing monitoring."""

from __future__ import annotations

from datetime import date, timedelta
from io import StringIO
from typing import Optional

import dash_bootstrap_components as dbc
import pandas as pd
from dash import Dash, Input, Output, dcc, html

from . import components
from .data import (
    DEFAULT_EDGE_THRESHOLD,
    DEFAULT_STAKE,
    DISPLAY_TIMEZONE,
    SummaryMetrics,
    calculate_summary_metrics,
    get_completed_bets,
    get_performance_by_threshold,
    get_performance_over_time,
    get_recent_predictions,
    get_recommended_bets,
    get_upcoming_calendar,
    load_forward_test_data,
)

EXTERNAL_STYLESHEETS = [dbc.themes.FLATLY]


def _format_timestamp(ts: Optional[pd.Timestamp]) -> str:
    if ts is None or pd.isna(ts):
        return "Last updated: —"
    if ts.tzinfo is not None:
        localized = ts.tz_convert(DISPLAY_TIMEZONE)
    else:
        localized = ts.tz_localize(DISPLAY_TIMEZONE)
    tz_name = localized.tzname() or "ET"
    return f"Last updated: {localized.strftime('%Y-%m-%d %I:%M %p')} {tz_name}"


def _df_to_json(df: pd.DataFrame) -> str:
    if df.empty:
        return ""
    return df.to_json(date_format="iso", orient="records")


def _df_from_json(data: Optional[str]) -> pd.DataFrame:
    if not data:
        return pd.DataFrame()
    buffer = StringIO(data)
    df = pd.read_json(buffer, orient="records")
    for column in ("commence_time", "predicted_at", "result_updated_at"):
        if column in df.columns:
            df[column] = pd.to_datetime(df[column], errors="coerce")
    return df


def _filter_by_date(df: pd.DataFrame, start_date: Optional[str], end_date: Optional[str]) -> pd.DataFrame:
    if df.empty or "commence_time" not in df.columns:
        return df

    start_ts = pd.to_datetime(start_date) if start_date else None
    end_ts = pd.to_datetime(end_date) if end_date else None
    if end_ts is not None:
        end_ts = end_ts + pd.Timedelta(days=1) - pd.Timedelta(milliseconds=1)

    commence = df["commence_time"]
    if pd.api.types.is_datetime64_any_dtype(commence):
        tz = getattr(commence.dt, "tz", None)
    else:
        tz = None

    if tz is not None:
        if start_ts is not None:
            start_ts = start_ts.tz_localize(tz) if start_ts.tzinfo is None else start_ts.tz_convert(tz)
        if end_ts is not None:
            end_ts = end_ts.tz_localize(tz) if end_ts.tzinfo is None else end_ts.tz_convert(tz)
    else:
        if start_ts is not None and start_ts.tzinfo is not None:
            start_ts = start_ts.tz_convert("UTC").tz_localize(None)
        if end_ts is not None and end_ts.tzinfo is not None:
            end_ts = end_ts.tz_convert("UTC").tz_localize(None)

    mask = pd.Series(True, index=df.index)
    if start_ts is not None:
        mask &= df["commence_time"] >= start_ts
    if end_ts is not None:
        mask &= df["commence_time"] <= end_ts
    return df.loc[mask].copy()


initial_df = load_forward_test_data()
if not initial_df.empty and "commence_time" in initial_df.columns:
    min_date = initial_df["commence_time"].min().date()
    max_date = initial_df["commence_time"].max().date()
else:
    today = date.today()
    min_date = today - timedelta(days=7)
    max_date = today


app = Dash(__name__, external_stylesheets=EXTERNAL_STYLESHEETS, suppress_callback_exceptions=True, title="Forward Testing Dashboard")
server = app.server


app.layout = dbc.Container(
    [
        dcc.Store(id="forward-data-store", data=_df_to_json(initial_df)),
        html.Br(),
        dbc.Row(
            [
                dbc.Col(html.H2("Forward Testing Dashboard"), md=8),
                dbc.Col(
                    dbc.Button(
                        "Manual Refresh",
                        id="refresh-button",
                        color="primary",
                        className="float-md-end",
                    ),
                    md=2,
                ),
                dbc.Col(html.Div(id="last-updated-text", className="text-md-end text-muted"), md=2),
            ],
            className="align-items-center g-2",
        ),
        html.Hr(),
        html.Div("All times shown in Eastern Time (ET).", className="text-muted mb-3"),
        dbc.Row(
            [
                dbc.Col(
                    html.Div(
                        [
                            html.Label("League"),
                            dcc.Dropdown(
                                id="league-select",
                                options=[
                                    {"label": "All Leagues", "value": "all"},
                                    {"label": "NBA", "value": "NBA"},
                                    {"label": "NFL", "value": "NFL"},
                                    {"label": "CFB", "value": "CFB"},
                                ],
                                value="all",
                                clearable=False,
                            ),
                        ]
                    ),
                    md=2,
                ),
                dbc.Col(
                    dcc.DatePickerRange(
                        id="date-range-picker",
                        min_date_allowed=min_date,
                        max_date_allowed=max_date,
                        start_date=min_date,
                        end_date=max_date,
                        display_format="YYYY-MM-DD",
                    ),
                    md=3,
                ),
                dbc.Col(
                    html.Div(
                        [
                            html.Label("Edge Threshold"),
                            dcc.Slider(
                                id="edge-threshold-slider",
                                min=0.0,
                                max=0.2,
                                step=0.005,
                                value=DEFAULT_EDGE_THRESHOLD,
                                marks={
                                    0.0: "0%",
                                    0.05: "5%",
                                    0.1: "10%",
                                    0.15: "15%",
                                    0.2: "20%",
                                },
                                tooltip={"placement": "bottom", "always_visible": False},
                            ),
                        ]
                    ),
                    md=3,
                ),
                dbc.Col(
                    html.Div(
                        [
                            html.Label("Performance Period"),
                            dcc.Dropdown(
                                id="period-select",
                                options=[
                                    {"label": "Daily", "value": "D"},
                                    {"label": "Weekly", "value": "W"},
                                    {"label": "Monthly", "value": "M"},
                                ],
                                value="W",
                                clearable=False,
                            ),
                        ]
                    ),
                    md=3,
                ),
            ],
            className="g-3 mb-4",
        ),
        dcc.Tabs(
            id="dashboard-tabs",
            value="overview",
            children=[
                dcc.Tab(
                    label="Overview",
                    value="overview",
                    children=[
                        dcc.Loading(html.Div(id="summary-cards"), type="circle"),
                        html.Br(),
                        dcc.Loading(html.Div(id="bankroll-cards"), type="circle"),
                        html.Br(),
                        dcc.Loading(html.Div(id="cumulative-profit-chart"), type="circle"),
                    ],
                ),
                dcc.Tab(
                    label="Performance",
                    value="performance",
                    children=[
                        dcc.Loading(html.Div(id="roi-chart"), type="circle"),
                        html.Br(),
                        dcc.Loading(html.Div(id="win-rate-chart"), type="circle"),
                        html.Br(),
                        dcc.Loading(html.Div(id="period-chart"), type="circle"),
                    ],
                ),
                dcc.Tab(
                    label="Recent Predictions",
                    value="recent",
                    children=[dcc.Loading(html.Div(id="recent-predictions-table"), type="circle")],
                ),
                dcc.Tab(
                    label="Recommended Bets",
                    value="recommended",
                    children=[dcc.Loading(html.Div(id="recommended-bets-table"), type="circle")],
                ),
                dcc.Tab(
                    label="Edge Analysis",
                    value="edges",
                    children=[
                        dcc.Loading(html.Div(id="threshold-chart"), type="circle"),
                        html.Br(),
                        dcc.Loading(html.Div(id="threshold-table"), type="circle"),
                    ],
                ),
                dcc.Tab(
                    label="Bets",
                    value="bets",
                    children=[dcc.Loading(html.Div(id="completed-bets-table"), type="circle")],
                ),
                dcc.Tab(
                    label="Calendar",
                    value="calendar",
                    children=[dcc.Loading(html.Div(id="calendar-table"), type="circle")],
                ),
            ],
        ),
        html.Br(),
    ],
    fluid=True,
)


@app.callback(
    Output("forward-data-store", "data"),
    Output("last-updated-text", "children"),
    Input("refresh-button", "n_clicks"),
    Input("league-select", "value"),
    prevent_initial_call=False,
)
def refresh_data(n_clicks: Optional[int], league: str) -> tuple[str, str]:
    force_refresh = bool(n_clicks)
    league_filter = None if league == "all" else league
    df = load_forward_test_data(force_refresh=force_refresh, league=league_filter)
    metrics = calculate_summary_metrics(df)
    return _df_to_json(df), _format_timestamp(metrics.last_updated)


@app.callback(
    Output("summary-cards", "children"),
    Output("bankroll-cards", "children"),
    Output("cumulative-profit-chart", "children"),
    Output("roi-chart", "children"),
    Output("win-rate-chart", "children"),
    Output("period-chart", "children"),
    Output("threshold-chart", "children"),
    Output("threshold-table", "children"),
    Output("recent-predictions-table", "children"),
    Output("recommended-bets-table", "children"),
    Output("completed-bets-table", "children"),
    Output("calendar-table", "children"),
    Input("forward-data-store", "data"),
    Input("date-range-picker", "start_date"),
    Input("date-range-picker", "end_date"),
    Input("edge-threshold-slider", "value"),
    Input("period-select", "value"),
    Input("league-select", "value"),
)
def update_dashboard(
    data_json: Optional[str],
    start_date: Optional[str],
    end_date: Optional[str],
    edge_threshold: float,
    period: str,
    league: str,
):
    df = _df_from_json(data_json)
    
    # Add or fix league column if missing/None (for backward compatibility)
    if not df.empty:
        if "league" not in df.columns:
            if "game_id" in df.columns:
                # Infer league from game_id prefix
                df["league"] = df["game_id"].apply(
                    lambda x: "NFL" if isinstance(x, str) and x.startswith("NFL_") else "NBA"
                )
            else:
                # Default to NBA for old predictions without game_id info
                df["league"] = "NBA"
        else:
            # Fix None/NaN values in existing league column
            if "game_id" in df.columns:
                mask = df["league"].isna() | (df["league"].astype(str).str.lower() == "none")
                df.loc[mask, "league"] = df.loc[mask, "game_id"].apply(
                    lambda x: "NFL" if isinstance(x, str) and x.startswith("NFL_") else "NBA"
                )
            else:
                # Fill any None/NaN with NBA as default
                df["league"] = df["league"].fillna("NBA")
                df.loc[df["league"].astype(str).str.lower() == "none", "league"] = "NBA"
    
    # Filter by league if not "all"
    if league != "all" and not df.empty and "league" in df.columns:
        # Handle None/NaN values in league column
        df = df[df["league"].notna() & (df["league"].astype(str).str.upper() == league.upper())].copy()
    
    df = _filter_by_date(df, start_date, end_date)

    metrics: SummaryMetrics = calculate_summary_metrics(df, edge_threshold=edge_threshold)
    performance_df = get_performance_over_time(df, edge_threshold=edge_threshold)
    threshold_df = get_performance_by_threshold(df, stake=DEFAULT_STAKE)
    recent_df = get_recent_predictions(df)
    recommended_df = get_recommended_bets(df, edge_threshold=edge_threshold)
    completed_bets_df = get_completed_bets(df, edge_threshold=edge_threshold, stake=DEFAULT_STAKE)
    calendar_df = get_upcoming_calendar(df, edge_threshold=edge_threshold)

    period_chart_component = components.empty_state("No performance data yet.")
    if not performance_df.empty:
        freq = period or "W"
        period_chart_component = components.performance_by_period_chart(performance_df, period=freq)

    return (
        components.summary_cards(metrics),
        components.bankroll_cards(metrics),
        components.cumulative_profit_chart(performance_df),
        components.roi_over_time_chart(performance_df),
        components.win_rate_over_time_chart(performance_df),
        period_chart_component,
        components.performance_by_threshold_chart(threshold_df),
        components.performance_by_threshold_table(threshold_df),
        components.recent_predictions_table(recent_df),
        components.recommended_bets_table(recommended_df),
        components.completed_bets_table(completed_bets_df),
        components.calendar_table(calendar_df),
    )


def run(debug: bool = False, port: int = 8050, host: str = "0.0.0.0") -> None:
    app.run_server(debug=debug, port=port, host=host)


if __name__ == "__main__":
    run(debug=True)

