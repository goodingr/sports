"""Dash application for forward testing monitoring."""

from __future__ import annotations

import json
from datetime import date, timedelta
from io import StringIO
from typing import Optional, Tuple

import dash_bootstrap_components as dbc
import pandas as pd
from dash import Dash, Input, Output, State, dcc, html, ctx, no_update
from dash.exceptions import PreventUpdate

from . import components
from .data import (
    DEFAULT_EDGE_THRESHOLD,
    DEFAULT_STAKE,
    DISPLAY_TIMEZONE,
    SummaryMetrics,
    build_prediction_comparison,
    calculate_summary_metrics,
    calculate_totals_metrics,
    get_completed_bets,
    get_overunder_completed,
    get_overunder_recommendations,
    get_moneylines_for_recommended,
    get_totals_odds_for_recommended,
    get_performance_by_threshold,
    get_performance_over_time,
    get_performance_by_league,
    get_totals_performance_by_league,
    get_recommended_bets,
    get_totals_performance_by_threshold,
    get_totals_performance_over_time,
    get_default_version_value,
    get_version_options,
    filter_by_version,
    load_forward_test_data,
    summarize_prediction_comparison,
    compare_model_predictions,
)
from .components import (
    bankroll_cards,
    completed_bets_table,
    cumulative_profit_chart,
    multi_model_cumulative_profit_chart,
    cumulative_profit_by_league_chart,
    edge_distribution_chart,
    empty_state,
    metric_card,
    moneyline_detail_table,
    overunder_completed_table,
    overunder_recommended_table,
    performance_by_period_chart,
    performance_by_threshold_chart,
    performance_by_threshold_table,
    prediction_comparison_table,
    prediction_summary,
    recommended_bets_table,
    roi_over_time_chart,
    roi_by_league_chart,
    summary_cards,
    totals_detail_table,
    win_rate_over_time_chart,
    model_comparison_table,
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


def _apply_best_moneylines(recommended: pd.DataFrame, odds_df: pd.DataFrame) -> pd.DataFrame:
    if recommended.empty or odds_df.empty:
        return recommended

    odds = odds_df.copy()
    odds = odds.dropna(subset=["moneyline"])
    if odds.empty:
        return recommended

    odds["outcome"] = odds["outcome"].astype(str).str.lower()
    odds_game_key = "forward_game_id" if "forward_game_id" in odds.columns else "game_id"

    def _is_better(candidate: Optional[float], current: Optional[float]) -> bool:
        if candidate is None or (isinstance(candidate, float) and pd.isna(candidate)):
            return False
        if current is None or (isinstance(current, float) and pd.isna(current)):
            return True
        return candidate > current

    best_lookup: dict[tuple[Optional[str], Optional[str]], tuple[Optional[float], Optional[str]]] = {}
    for _, row in odds.iterrows():
        key = (row.get(odds_game_key), row.get("outcome"))
        value = row.get("moneyline")
        book = row.get("book")
        current = best_lookup.get(key)
        if current is None or _is_better(value, current[0]):
            best_lookup[key] = (value, book)

    updated = recommended.copy()
    best_values: list[Optional[float]] = []
    best_books: list[Optional[str]] = []
    for _, row in updated.iterrows():
        key = (
            row.get("game_id"),
            str(row.get("side")).lower() if row.get("side") is not None else None,
        )
        best = best_lookup.get(key)
        if best:
            best_values.append(best[0])
            best_books.append(best[1])
        else:
            best_values.append(row.get("moneyline"))
            best_books.append(row.get("moneyline_book"))

    updated["moneyline"] = best_values
    updated["moneyline_book"] = best_books
    return updated


initial_df = load_forward_test_data()
if not initial_df.empty and "commence_time" in initial_df.columns:
    min_date = initial_df["commence_time"].min().date()
    max_date = initial_df["commence_time"].max().date()
else:
    today = date.today()
    min_date = today - timedelta(days=7)
    max_date = today

LEAGUE_FILTER_OPTIONS = [
    {"label": "All Leagues", "value": "all"},
    {"label": "-- Football --", "value": "divider_football", "disabled": True},
    {"label": "NFL", "value": "NFL"},
    {"label": "CFB", "value": "CFB"},
    {"label": "-- Basketball --", "value": "divider_basketball", "disabled": True},
    {"label": "NBA", "value": "NBA"},
    {"label": "NCAAB", "value": "NCAAB"},
    {"label": "-- Hockey --", "value": "divider_hockey", "disabled": True},
    {"label": "NHL", "value": "NHL"},
    {"label": "-- Soccer --", "value": "divider_soccer", "disabled": True},
    {"label": "EPL", "value": "EPL"},
    {"label": "La Liga", "value": "LALIGA"},
    {"label": "Bundesliga", "value": "BUNDESLIGA"},
    {"label": "Serie A", "value": "SERIEA"},
    {"label": "Ligue 1", "value": "LIGUE1"},
]

def _build_version_dropdown_options() -> tuple[list[dict], str]:
    version_names = get_version_options()
    options = [{"label": "All Versions", "value": "all"}]
    for name in version_names:
        options.append({"label": name, "value": name})
    default_value = get_default_version_value()
    if default_value != "all" and default_value not in [opt["value"] for opt in options]:
        options.append({"label": default_value, "value": default_value})
    return options, default_value


VERSION_FILTER_OPTIONS, VERSION_DEFAULT_VALUE = _build_version_dropdown_options()


app = Dash(__name__, external_stylesheets=EXTERNAL_STYLESHEETS, suppress_callback_exceptions=True, title="Forward Testing Dashboard")
server = app.server


def _navbar(pathname: Optional[str]) -> dbc.Nav:
    pathname = pathname or "/"
    return dbc.Nav(
        [
            dbc.NavLink("Moneyline", href="/", active=pathname == "/" or pathname == ""),
            dbc.NavLink("Winner Predictions", href="/predictions", active=pathname == "/predictions"),
            dbc.NavLink("Over/Under", href="/overunder", active=pathname == "/overunder"),
        ],
        pills=True,
        className="mb-4 gap-2",
    )


def _dashboard_layout(pathname: Optional[str]) -> dbc.Container:
    return dbc.Container(
        [
            _navbar(pathname),
            dbc.Row(
                [
                    dbc.Col(html.H2("Moneyline Dashboard"), md=8),
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
                                html.Label("League", htmlFor="league-select"),
                                dcc.Dropdown(
                                    id="league-select",
                                    options=LEAGUE_FILTER_OPTIONS,
                                    value="all",
                                    clearable=False,
                                ),
                            ]
                        ),
                        md=2,
                    ),
                    dbc.Col(
                        html.Div(
                            [
                                html.Label("Model", htmlFor="model-type-dropdown"),
                                dcc.Dropdown(
                                    id="model-type-dropdown",
                                    options=[
                                        {"label": "Ensemble", "value": "ensemble"},
                                        {"label": "Random Forest", "value": "random_forest"},
                                        {"label": "Gradient Boosting", "value": "gradient_boosting"},
                                    ],
                                    value="ensemble",
                                    clearable=False,
                                ),
                            ]
                        ),
                        md=2,
                    ),
                    dbc.Col(
                        html.Div(
                            [
                                html.Label("Version", htmlFor="version-select"),
                                dcc.Dropdown(
                                    id="version-select",
                                    options=VERSION_FILTER_OPTIONS,
                                    value=VERSION_DEFAULT_VALUE,
                                    clearable=False,
                                ),
                            ]
                        ),
                        md=2,
                    ),
                    dbc.Col(
                        html.Div(
                            [
                                html.Label("Date Range", htmlFor="date-range-picker"),
                                dcc.DatePickerRange(
                                    id="date-range-picker",
                                    min_date_allowed=min_date,
                                    max_date_allowed=max_date,
                                    start_date=min_date,
                                    end_date=max_date,
                                    display_format="YYYY-MM-DD",
                                ),
                            ]
                        ),
                        md=3,
                    ),
                    dbc.Col(
                        html.Div(
                            [
                                html.Label("Edge Threshold", htmlFor="edge-threshold-slider"),
                                dcc.Slider(
                                    id="edge-threshold-slider",
                                    min=0.0,
                                    max=0.2,
                                    step=0.005,
                                    value=0.06,
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
                                html.Label("Performance Period", htmlFor="period-select"),
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
                        md=2,
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
                            html.Br(),
                            dcc.Loading(html.Div(id="multi-model-profit-chart"), type="circle"),
                            html.Br(),
                            dcc.Loading(html.Div(id="league-profit-chart"), type="circle"),
                            html.Br(),
                            dcc.Loading(html.Div(id="roi-by-league-chart"), type="circle"),
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
                        label="Recommended Bets",
                        value="recommended",
                        children=[
                            dcc.Loading(html.Div(id="recommended-bets-table"), type="circle"),
                            dbc.Modal(
                                [
                                    dbc.ModalHeader(dbc.ModalTitle(id="moneyline-modal-title")),
                                    dbc.ModalBody(html.Div(id="moneyline-modal-content")),
                                    dbc.ModalFooter(
                                        dbc.Button("Close", id="moneyline-modal-close", color="secondary", className="ms-auto")
                                    ),
                                ],
                                id="moneyline-modal",
                                is_open=False,
                                size="lg",
                            ),
                        ],
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
                ],
            ),
            html.Br(),
        ],
        fluid=True,
    )


def _predictions_layout(pathname: Optional[str]) -> dbc.Container:
    return dbc.Container(
        [
            _navbar(pathname),
            html.H2("Winner Predictions vs. Sportsbooks"),
            html.P(
                "Compare our model's projected winners against sportsbook consensus, track where we agree, "
                "and highlight situations where we outperform the market.",
                className="text-muted",
            ),
            dbc.Row(
                [
                    dbc.Col(
                        html.Div(
                            [
                                html.Label("League", htmlFor="predictions-league-select"),
                                dcc.Dropdown(
                                    id="predictions-league-select",
                                    options=LEAGUE_FILTER_OPTIONS,
                                    value="all",
                                    clearable=False,
                                ),
                            ]
                        ),
                        md=3,
                    ),
                    dbc.Col(
                        html.Div(
                            [
                                html.Label("Version", htmlFor="predictions-version-select"),
                                dcc.Dropdown(
                                    id="predictions-version-select",
                                    options=VERSION_FILTER_OPTIONS,
                                    value=VERSION_DEFAULT_VALUE,
                                    clearable=False,
                                ),
                            ]
                        ),
                        md=3,
                    ),
                    dbc.Col(
                        html.Div(
                            [
                                html.Label("Date Range", htmlFor="predictions-date-range"),
                                dcc.DatePickerRange(
                                    id="predictions-date-range",
                                    min_date_allowed=min_date,
                                    max_date_allowed=max_date,
                                    start_date=min_date,
                                    end_date=max_date,
                                    display_format="YYYY-MM-DD",
                                ),
                            ]
                        ),
                        md=4,
                    ),
                ],
                className="g-3 mb-4",
            ),
            dcc.Loading(html.Div(id="prediction-summary-cards"), type="circle"),
            html.Br(),
            dcc.Loading(html.Div(id="prediction-table"), type="circle"),
        ],
        fluid=True,
    )



def _comparison_layout(pathname: Optional[str]) -> dbc.Container:
    return dbc.Container(
        [
            _navbar(pathname),
            html.H2("Model Comparison"),
            html.P(
                "Compare performance and predictions across different model types.",
                className="text-muted",
            ),
            dbc.Row(
                [
                    dbc.Col(
                        html.Div(
                            [
                                html.Label("League", htmlFor="comparison-league-select"),
                                dcc.Dropdown(
                                    id="comparison-league-select",
                                    options=LEAGUE_FILTER_OPTIONS,
                                    value="all",
                                    clearable=False,
                                ),
                            ]
                        ),
                        md=3,
                    ),
                    dbc.Col(
                        html.Div(
                            [
                                html.Label("Date Range", htmlFor="comparison-date-range"),
                                dcc.DatePickerRange(
                                    id="comparison-date-range",
                                    min_date_allowed=min_date,
                                    max_date_allowed=max_date,
                                    start_date=min_date,
                                    end_date=max_date,
                                    display_format="YYYY-MM-DD",
                                ),
                            ]
                        ),
                        md=4,
                    ),
                ],
                className="g-3 mb-4",
            ),
            dcc.Loading(html.Div(id="comparison-profit-chart"), type="circle"),
            html.Br(),
            dcc.Loading(html.Div(id="comparison-table"), type="circle"),
        ],
        fluid=True,
    )


def _overunder_layout(pathname: Optional[str]) -> dbc.Container:
    return dbc.Container(
        [
            _navbar(pathname),
            dbc.Row(
                [
                    dbc.Col(html.H2("Over/Under Dashboard"), md=8),
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
                                html.Label("League", htmlFor="ou-league-select"),
                                dcc.Dropdown(
                                    id="ou-league-select",
                                    options=LEAGUE_FILTER_OPTIONS,
                                    value="all",
                                    clearable=False,
                                ),
                            ]
                        ),
                        md=2,
                    ),
                    dbc.Col(
                        html.Div(
                            [
                                html.Label("Model", htmlFor="ou-model-type-dropdown"),
                                dcc.Dropdown(
                                    id="ou-model-type-dropdown",
                                    options=[
                                        {"label": "Ensemble", "value": "ensemble"},
                                        {"label": "Random Forest", "value": "random_forest"},
                                        {"label": "Gradient Boosting", "value": "gradient_boosting"},
                                    ],
                                    value="ensemble",
                                    clearable=False,
                                ),
                            ]
                        ),
                        md=2,
                    ),
                    dbc.Col(
                        html.Div(
                            [
                                html.Label("Version", htmlFor="ou-version-select"),
                                dcc.Dropdown(
                                    id="ou-version-select",
                                    options=VERSION_FILTER_OPTIONS,
                                    value=VERSION_DEFAULT_VALUE,
                                    clearable=False,
                                ),
                            ]
                        ),
                        md=2,
                    ),
                    dbc.Col(
                        html.Div(
                            [
                                html.Label("Date Range", htmlFor="ou-date-range"),
                                dcc.DatePickerRange(
                                    id="ou-date-range",
                                    min_date_allowed=min_date,
                                    max_date_allowed=max_date,
                                    start_date=min_date,
                                    end_date=max_date,
                                    display_format="YYYY-MM-DD",
                                ),
                            ]
                        ),
                        md=4,
                    ),
                    dbc.Col(
                        html.Div(
                            [
                                html.Label("Edge Threshold", htmlFor="ou-edge-slider"),
                                dcc.Slider(
                                    id="ou-edge-slider",
                                    min=0.0,
                                    max=0.2,
                                    step=0.005,
                                    value=0.06,
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
                        md=4,
                    ),
                ],
                className="g-3 mb-4",
            ),
            dcc.Tabs(
                id="overunder-tabs",
                value="ou-overview",
                children=[
                    dcc.Tab(
                        label="Overview",
                        value="ou-overview",
                        children=[
                            dcc.Loading(html.Div(id="overunder-summary"), type="circle"),
                            html.Br(),
                            dcc.Loading(html.Div(id="overunder-bankroll"), type="circle"),
                            html.Br(),
                            dcc.Loading(html.Div(id="overunder-cumulative-profit"), type="circle"),
                            html.Br(),
                            dcc.Loading(html.Div(id="overunder-multi-model-profit"), type="circle"),
                            html.Br(),
                            dcc.Loading(html.Div(id="overunder-league-profit"), type="circle"),
                            html.Br(),
                            dcc.Loading(html.Div(id="overunder-roi-by-league"), type="circle"),
                        ],
                    ),
                    dcc.Tab(
                        label="Performance",
                        value="ou-performance",
                        children=[
                            dcc.Loading(html.Div(id="overunder-performance"), type="circle"),
                        ],
                    ),
                    dcc.Tab(
                        label="Recommended Totals",
                        value="ou-recommended",
                        children=[
                            dcc.Loading(html.Div(id="overunder-recommended-table"), type="circle"),
                            dbc.Modal(
                                [
                                    dbc.ModalHeader(dbc.ModalTitle(id="ou-odds-modal-title")),
                                    dbc.ModalBody(html.Div(id="ou-odds-modal-content")),
                                    dbc.ModalFooter(
                                        dbc.Button("Close", id="ou-odds-modal-close", color="secondary", className="ms-auto")
                                    ),
                                ],
                                id="ou-odds-modal",
                                is_open=False,
                                size="lg",
                            ),
                        ],
                    ),
                    dcc.Tab(
                        label="Completed Totals",
                        value="ou-completed",
                        children=[
                            dcc.Loading(html.Div(id="overunder-completed-table"), type="circle"),
                            dbc.Modal(
                                [
                                    dbc.ModalHeader(dbc.ModalTitle(id="ou-completed-odds-modal-title")),
                                    dbc.ModalBody(html.Div(id="ou-completed-odds-modal-content")),
                                    dbc.ModalFooter(
                                        dbc.Button("Close", id="ou-completed-odds-modal-close", color="secondary", className="ms-auto")
                                    ),
                                ],
                                id="ou-completed-odds-modal",
                                is_open=False,
                                size="lg",
                            ),
                        ],
                    ),
                    dcc.Tab(
                        label="Edge Analysis",
                        value="ou-edge",
                        children=[
                            dcc.Loading(html.Div(id="overunder-edge"), type="circle"),
                        ],
                    ),
                ],
            ),
        ],
        fluid=True,
    )


app.layout = html.Div(
    [
        dcc.Location(id="url"),
        dcc.Store(id="forward-data-store", data=_df_to_json(initial_df)),
        dcc.Store(id="book-odds-store"),
        dcc.Store(id="ou-book-odds-store"),
        html.Div(id="page-content", children=_dashboard_layout("/")),
    ]
)


@app.callback(Output("page-content", "children"), Input("url", "pathname"))
def render_page(pathname: Optional[str]):
    if pathname == "/predictions":
        return _predictions_layout(pathname)
    if pathname == "/overunder":
        return _overunder_layout(pathname)
    return _dashboard_layout(pathname)


@app.callback(
    Output("forward-data-store", "data"),
    Output("last-updated-text", "children"),
    Input("refresh-button", "n_clicks"),
    Input("model-type-dropdown", "value"),
    prevent_initial_call=False,
)
def refresh_data(n_clicks: Optional[int], model_type: str) -> tuple[str, str]:
    """Refresh the cached predictions when the manual refresh button is clicked or model type changes."""
    force_refresh = bool(n_clicks)
    # If triggered by dropdown (n_clicks is None or unchanged), we might not need force_refresh, 
    # but load_forward_test_data handles caching.
    # However, if we switch model type, we definitely want to load that model's data.
    
    # Use a default if model_type is None (initial load)
    model_type = model_type or "ensemble"
    
    df = load_forward_test_data(force_refresh=force_refresh, league=None, model_type=model_type)
    metrics = calculate_summary_metrics(df)
    return _df_to_json(df), _format_timestamp(metrics.last_updated)


@app.callback(
    Output("date-range-picker", "min_date_allowed"),
    Output("date-range-picker", "max_date_allowed"),
    Output("date-range-picker", "start_date"),
    Output("date-range-picker", "end_date"),
    Output("predictions-date-range", "min_date_allowed"),
    Output("predictions-date-range", "max_date_allowed"),
    Output("predictions-date-range", "start_date"),
    Output("predictions-date-range", "end_date"),
    Output("ou-date-range", "min_date_allowed"),
    Output("ou-date-range", "max_date_allowed"),
    Output("ou-date-range", "start_date"),
    Output("ou-date-range", "end_date"),
    Input("forward-data-store", "data"),
)
def sync_date_range_controls(data_json: Optional[str]):
    """Keep both date pickers aligned with the latest data window."""
    df = _df_from_json(data_json)
    def _payload(min_date: str, max_date: str) -> Tuple[str, ...]:
        return (
            min_date,
            max_date,
            min_date,
            max_date,
            min_date,
            max_date,
            min_date,
            max_date,
            min_date,
            max_date,
            min_date,
            max_date,
        )

    if df.empty or "commence_time" not in df.columns:
        default_start = (date.today() - timedelta(days=7)).isoformat()
        default_end = date.today().isoformat()
        return _payload(default_start, default_end)

    commence = pd.to_datetime(df["commence_time"], errors="coerce").dropna()
    if commence.empty:
        default_start = (date.today() - timedelta(days=7)).isoformat()
        default_end = date.today().isoformat()
        return _payload(default_start, default_end)

    min_date_allowed = commence.min().date().isoformat()
    max_date_allowed = commence.max().date().isoformat()

    return _payload(min_date_allowed, max_date_allowed)


@app.callback(
    Output("summary-cards", "children"),
    Output("bankroll-cards", "children"),
    Output("cumulative-profit-chart", "children"),
    Output("multi-model-profit-chart", "children"),
    Output("league-profit-chart", "children"),
    Output("roi-by-league-chart", "children"),
    Output("roi-chart", "children"),
    Output("win-rate-chart", "children"),
    Output("period-chart", "children"),
    Output("threshold-chart", "children"),
    Output("threshold-table", "children"),
    Output("recommended-bets-table", "children"),
    Output("completed-bets-table", "children"),
    Output("book-odds-store", "data"),
    Input("forward-data-store", "data"),
    Input("date-range-picker", "start_date"),
    Input("date-range-picker", "end_date"),
    Input("edge-threshold-slider", "value"),
    Input("period-select", "value"),
    Input("league-select", "value"),
    Input("version-select", "value"),
)
def update_dashboard(
    data_json: Optional[str],
    start_date: Optional[str],
    end_date: Optional[str],
    edge_threshold: float,
    period: str,
    league: str,
    version: str,
):
    df = _df_from_json(data_json)
    df = filter_by_version(df, version)
    
    # Add or fix league column if missing/None (for backward compatibility)
    if not df.empty:
        if "league" not in df.columns:
            if "game_id" in df.columns:
                # Infer league from game_id prefix
                def _infer_league(x):
                    if not isinstance(x, str):
                        return "NBA"
                    if x.startswith("NFL_"):
                        return "NFL"
                    if x.startswith("CFB_"):
                        return "CFB"
                    if x.startswith("EPL_"):
                        return "EPL"
                    if x.startswith("LALIGA_"):
                        return "LALIGA"
                    if x.startswith("BUNDESLIGA_"):
                        return "BUNDESLIGA"
                    if x.startswith("SERIEA_"):
                        return "SERIEA"
                    if x.startswith("LIGUE1_"):
                        return "LIGUE1"
                    return "NBA"
                df["league"] = df["game_id"].apply(_infer_league)
            else:
                # Default to NBA for old predictions without game_id info
                df["league"] = "NBA"
        else:
            # Fix None/NaN values in existing league column
            if "game_id" in df.columns:
                mask = df["league"].isna() | (df["league"].astype(str).str.lower() == "none")
                def _infer_league(x):
                    if not isinstance(x, str):
                        return "NBA"
                    if x.startswith("NFL_"):
                        return "NFL"
                    if x.startswith("CFB_"):
                        return "CFB"
                    if x.startswith("EPL_"):
                        return "EPL"
                    if x.startswith("LALIGA_"):
                        return "LALIGA"
                    if x.startswith("BUNDESLIGA_"):
                        return "BUNDESLIGA"
                    if x.startswith("SERIEA_"):
                        return "SERIEA"
                    if x.startswith("LIGUE1_"):
                        return "LIGUE1"
                    return "NBA"
                df.loc[mask, "league"] = df.loc[mask, "game_id"].apply(_infer_league)
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
    recommended_df = get_recommended_bets(df, edge_threshold=edge_threshold)
    completed_bets_df = get_completed_bets(df, edge_threshold=edge_threshold, stake=DEFAULT_STAKE)
    
    # Load performance data for all models for the multi-model cumulative profit chart
    model_performance = {}
    for model_type in ["ensemble", "random_forest", "gradient_boosting"]:
        try:
            model_df = load_forward_test_data(force_refresh=False, league=None, model_type=model_type)
            model_df = filter_by_version(model_df, version)
            
            # Apply same filters as main data
            if league != "all" and not model_df.empty and "league" in model_df.columns:
                model_df = model_df[model_df["league"].notna() & (model_df["league"].astype(str).str.upper() == league.upper())].copy()
            
            model_df = _filter_by_date(model_df, start_date, end_date)
            
            # Get performance over time for this model
            model_perf = get_performance_over_time(model_df, edge_threshold=edge_threshold)
            model_performance[model_type] = model_perf
        except Exception:
            # If model data doesn't exist or fails to load, skip it
            model_performance[model_type] = pd.DataFrame()

    # Calculate performance by league for the current model
    league_performance = get_performance_by_league(df, edge_threshold=edge_threshold, stake=DEFAULT_STAKE)

    book_odds_json = ""
    recommended_display_df = recommended_df
    if not recommended_df.empty:
        book_odds_df = get_moneylines_for_recommended(recommended_df)
        if not book_odds_df.empty:
            recommended_df = _apply_best_moneylines(recommended_df, book_odds_df)
            book_odds_json = book_odds_df.to_json(date_format="iso", orient="records")
        recommended_display_df = recommended_df.copy()
        if "moneyline" in recommended_display_df.columns:
            recommended_display_df["moneyline_value"] = recommended_display_df["moneyline"]
            recommended_display_df["moneyline"] = recommended_display_df["moneyline"].apply(
                lambda x: f"{x:+.0f}" if pd.notna(x) else ""
            )
    else:
        book_odds_json = ""

    period_chart_component = components.empty_state("No performance data yet.")
    if not performance_df.empty:
        freq = period or "W"
        period_chart_component = components.performance_by_period_chart(performance_df, period=freq)

    return (
        components.summary_cards(metrics),
        components.bankroll_cards(metrics),
        components.cumulative_profit_chart(performance_df),
        components.multi_model_cumulative_profit_chart(model_performance),
        components.cumulative_profit_by_league_chart(league_performance),
        components.roi_by_league_chart(league_performance),
        components.roi_over_time_chart(performance_df),
        components.win_rate_over_time_chart(performance_df),
        period_chart_component,
        components.performance_by_threshold_chart(threshold_df),
        components.performance_by_threshold_table(threshold_df),
        components.recommended_bets_table(recommended_display_df),
        components.completed_bets_table(completed_bets_df),
        book_odds_json,
    )


@app.callback(
    Output("prediction-summary-cards", "children"),
    Output("prediction-table", "children"),
    Input("forward-data-store", "data"),
    Input("predictions-league-select", "value"),
    Input("predictions-date-range", "start_date"),
    Input("predictions-date-range", "end_date"),
    Input("predictions-version-select", "value"),
)
def update_predictions_page(
    data_json: Optional[str],
    league: str,
    start_date: Optional[str],
    end_date: Optional[str],
    version: str,
):
    df = _df_from_json(data_json)
    df = filter_by_version(df, version)
    if not df.empty and league != "all" and "league" in df.columns:
        df = df[df["league"].notna() & (df["league"].astype(str).str.upper() == league.upper())].copy()

    df = _filter_by_date(df, start_date, end_date)

    comparison_df = build_prediction_comparison(df)
    if not comparison_df.empty and "actual_winner_side" in comparison_df.columns:
        comparison_df = comparison_df[comparison_df["actual_winner_side"].notna()].copy()
    if not comparison_df.empty and "commence_time" in comparison_df.columns:
        comparison_df = comparison_df.sort_values("commence_time", ascending=False)

    stats = summarize_prediction_comparison(comparison_df)

    return (
        components.prediction_summary(stats),
        components.prediction_comparison_table(comparison_df),
    )


@app.callback(
    Output("overunder-summary", "children"),
    Output("overunder-bankroll", "children"),
    Output("overunder-cumulative-profit", "children"),
    Output("overunder-multi-model-profit", "children"),
    Output("overunder-league-profit", "children"),
    Output("overunder-roi-by-league", "children"),
    Output("overunder-performance", "children"),
    Output("overunder-recommended-table", "children"),
    Output("overunder-completed-table", "children"),
    Output("overunder-edge", "children"),
    Output("ou-book-odds-store", "data"),
    Input("forward-data-store", "data"),
    Input("ou-league-select", "value"),
    Input("ou-model-type-dropdown", "value"),
    Input("ou-date-range", "start_date"),
    Input("ou-date-range", "end_date"),
    Input("ou-version-select", "value"),
    Input("ou-edge-slider", "value"),
)
def update_overunder_page(
    data_json: Optional[str], 
    league: str, 
    model_type: str,
    start_date: Optional[str], 
    end_date: Optional[str], 
    version: str, 
    edge_threshold: float
):
    # Load data for the selected model type
    df = load_forward_test_data(force_refresh=False, league=None, model_type=model_type or "ensemble")
    df = filter_by_version(df, version)
    if not df.empty and league != "all" and "league" in df.columns:
        df = df[df["league"].notna() & (df["league"].astype(str).str.upper() == league.upper())].copy()
    df = _filter_by_date(df, start_date, end_date)

    if df.empty:
        empty = components.empty_state("No totals data available yet.")
        return empty, empty, empty, empty, empty, empty, empty, empty, ""
    metrics = calculate_totals_metrics(df, edge_threshold=edge_threshold, stake=DEFAULT_STAKE)
    performance_df = get_totals_performance_over_time(df, edge_threshold=edge_threshold, stake=DEFAULT_STAKE)
    threshold_df = get_totals_performance_by_threshold(df, stake=DEFAULT_STAKE)
    recommended = get_overunder_recommendations(df, edge_threshold=edge_threshold)
    completed = get_overunder_completed(df, edge_threshold=edge_threshold, stake=DEFAULT_STAKE)
    
    # Load performance data for all models for the multi-model cumulative profit chart
    model_performance = {}
    for model_type_iter in ["ensemble", "random_forest", "gradient_boosting"]:
        try:
            model_df = load_forward_test_data(force_refresh=False, league=None, model_type=model_type_iter)
            model_df = filter_by_version(model_df, version)
            
            # Apply same filters as main data
            if league != "all" and not model_df.empty and "league" in model_df.columns:
                model_df = model_df[model_df["league"].notna() & (model_df["league"].astype(str).str.upper() == league.upper())].copy()
            
            model_df = _filter_by_date(model_df, start_date, end_date)
            
            # Get performance over time for this model
            model_perf = get_totals_performance_over_time(model_df, edge_threshold=edge_threshold, stake=DEFAULT_STAKE)
            model_performance[model_type_iter] = model_perf
        except Exception:
            # If model data doesn't exist or fails to load, skip it
            model_performance[model_type_iter] = pd.DataFrame()

    # Calculate performance by league for the current model (using totals-specific function)
    league_performance = get_totals_performance_by_league(df, edge_threshold=edge_threshold, stake=DEFAULT_STAKE)

    totals_odds_json = ""
    totals_odds_df = get_totals_odds_for_recommended(recommended)
    if not totals_odds_df.empty:
        totals_odds_json = totals_odds_df.to_json(date_format="iso", orient="records")
        
        # Merge fresh odds into recommended dataframe for the table display
        # Select the BEST odds (highest moneyline) for each game/side, regardless of line
        # We want to show what's actually available in sportsbooks
        best_odds = totals_odds_df.loc[totals_odds_df.groupby(['forward_game_id', 'outcome'])['moneyline'].idxmax()]
        
        recommended = recommended.merge(
            best_odds[['forward_game_id', 'outcome', 'book', 'moneyline', 'line', 'home_team_full', 'away_team_full']],
            left_on=['game_id', 'side'],
            right_on=['forward_game_id', 'outcome'],
            how='left',
            suffixes=('', '_sportsbook')
        )
        
        # Update columns with sportsbook data where available
        # NOTE: pandas merge only adds _sportsbook suffix if there's a column name collision
        # If 'recommended' doesn't have 'book' or 'line', merge creates 'book'/'line' not '_sportsbook'
        
        if 'book' not in recommended.columns:
            recommended['book'] = ""
            
        # Handle book column (might be 'book_sportsbook' or just from merge)
        if 'book_sportsbook' in recommended.columns:
            recommended['book'] = recommended['book_sportsbook'].fillna("")
            recommended = recommended.drop(columns=['book_sportsbook'], errors='ignore')
        
        # Handle moneyline column
        if 'moneyline_sportsbook' in recommended.columns:
            # Use sportsbook moneyline when available
            recommended['moneyline'] = recommended['moneyline_sportsbook'].fillna(recommended['moneyline'])
            recommended = recommended.drop(columns=['moneyline_sportsbook'], errors='ignore')
        
        # Handle line column - THIS IS THE KEY FIX
        # Check if we got 'line' from the merge (no collision) or 'line_sportsbook' (collision)
        line_col = 'line_sportsbook' if 'line_sportsbook' in recommended.columns else 'line'
        
        if line_col in recommended.columns:
            # IMPORTANT: Use the SPORTSBOOK's line, not the predicted line
            # Only update where we have sportsbook data
            has_sportsbook_data = recommended[line_col].notna()
            recommended.loc[has_sportsbook_data, 'total_line'] = recommended.loc[has_sportsbook_data, line_col]
            
            # CRITICAL: Regenerate the 'description' (Pick) column with the updated line
            # The description was created earlier with the predicted line, we need to update it
            if 'description' in recommended.columns and 'side' in recommended.columns:
                recommended.loc[has_sportsbook_data, 'description'] = recommended.loc[has_sportsbook_data].apply(
                    lambda row: f"{row['side'].title()} {row['total_line']:.1f}" if pd.notna(row['total_line']) else row['side'].title(),
                    axis=1
                )
            
            # Update team names with full names from odds data if available
            if 'home_team_full' in recommended.columns:
                recommended.loc[has_sportsbook_data, 'home_team'] = recommended.loc[has_sportsbook_data, 'home_team_full'].fillna(recommended.loc[has_sportsbook_data, 'home_team'])
            
            if 'away_team_full' in recommended.columns:
                recommended.loc[has_sportsbook_data, 'away_team'] = recommended.loc[has_sportsbook_data, 'away_team_full'].fillna(recommended.loc[has_sportsbook_data, 'away_team'])
            
            # For rows WITHOUT sportsbook data, show N/A instead of predicted values
            no_sportsbook_data = ~has_sportsbook_data
            if no_sportsbook_data.any():
                # Clear total_line - will show as empty/N/A in table
                recommended.loc[no_sportsbook_data, 'total_line'] = pd.NA
                # Clear moneyline - will show as empty/N/A in table
                recommended.loc[no_sportsbook_data, 'moneyline'] = pd.NA
                # Set book to empty string (will show as blank in table)
                recommended.loc[no_sportsbook_data, 'book'] = ""
                # Clear description (Pick column) - will show as blank in table
                if 'description' in recommended.columns:
                    recommended.loc[no_sportsbook_data, 'description'] = ""
                # Clear edge - these are not actionable bets
                if 'edge' in recommended.columns:
                    recommended.loc[no_sportsbook_data, 'edge'] = pd.NA
            
            # Drop the line column after using it
            recommended = recommended.drop(columns=[line_col, 'home_team_full', 'away_team_full'], errors='ignore')
        
        # Clean up merge columns
        recommended = recommended.drop(columns=['forward_game_id', 'outcome'], errors='ignore')

    # Logic to update completed bets with best sportsbook odds
    completed_odds_df = get_totals_odds_for_recommended(completed)
    if not completed_odds_df.empty:
        # Ensure line is numeric
        if "line" in completed_odds_df.columns:
            completed_odds_df["line"] = pd.to_numeric(completed_odds_df["line"], errors="coerce")
            
        # Filter for rows with valid lines - for totals, the line is essential
        valid_odds = completed_odds_df.dropna(subset=["line"])
        
        if not valid_odds.empty:
            # Select the BEST odds (highest moneyline) for each game/side
            best_completed_odds = valid_odds.loc[valid_odds.groupby(['forward_game_id', 'outcome'])['moneyline'].idxmax()]
            
            completed = completed.merge(
                best_completed_odds[['forward_game_id', 'outcome', 'book', 'moneyline', 'line', 'home_team_full', 'away_team_full']],
                left_on=['game_id', 'side'],
                right_on=['forward_game_id', 'outcome'],
                how='left',
                suffixes=('', '_sportsbook')
            )
            
            # Handle book column
            if 'book' not in completed.columns:
                completed['book'] = ""
            if 'book_sportsbook' in completed.columns:
                completed['book'] = completed['book_sportsbook'].fillna(completed['book'])
                completed = completed.drop(columns=['book_sportsbook'], errors='ignore')

            # Handle moneyline column
            if 'moneyline_sportsbook' in completed.columns:
                completed['moneyline'] = completed['moneyline_sportsbook'].fillna(completed['moneyline'])
                completed = completed.drop(columns=['moneyline_sportsbook'], errors='ignore')

            # Handle line column
            line_col = 'line_sportsbook' if 'line_sportsbook' in completed.columns else 'line'
            if line_col in completed.columns:
                # Ensure the source column is numeric too
                completed[line_col] = pd.to_numeric(completed[line_col], errors="coerce")
                
                has_sportsbook_data = completed[line_col].notna()
                completed.loc[has_sportsbook_data, 'total_line'] = completed.loc[has_sportsbook_data, line_col]
                
                # Regenerate description
                if 'description' in completed.columns and 'side' in completed.columns:
                    completed.loc[has_sportsbook_data, 'description'] = completed.loc[has_sportsbook_data].apply(
                        lambda row: f"{row['side'].title()} {row['total_line']:.1f}" if pd.notna(row['total_line']) else row['side'].title(),
                        axis=1
                    )
                
                # Update team names
                if 'home_team_full' in completed.columns:
                    completed.loc[has_sportsbook_data, 'home_team'] = completed.loc[has_sportsbook_data, 'home_team_full'].fillna(completed.loc[has_sportsbook_data, 'home_team'])
                if 'away_team_full' in completed.columns:
                    completed.loc[has_sportsbook_data, 'away_team'] = completed.loc[has_sportsbook_data, 'away_team_full'].fillna(completed.loc[has_sportsbook_data, 'away_team'])

                # Drop the line column after using it
                completed = completed.drop(columns=[line_col, 'home_team_full', 'away_team_full'], errors='ignore')
            
            # RE-CALCULATE WON and PROFIT based on new line and moneyline
            # We need to do this because the line might have changed from the predicted line
            # and the result (Win/Loss) depends on the line.
            
            # Helper for profit calculation
            def calculate_profit(row):
                if pd.isna(row.get('won')) or row.get('won') is None:
                    return 0.0
                
                stake = DEFAULT_STAKE
                ml = row.get('moneyline')
                
                if pd.isna(ml):
                    return 0.0
                    
                if row['won']:
                    if ml > 0:
                        return stake * (ml / 100.0)
                    else:
                        return stake * (100.0 / abs(ml))
                else:
                    return -stake

            # Only recalculate for rows where we have scores
            has_scores = completed['total_points'].notna()
            
            if has_scores.any():
                # Recalculate 'won'
                # If Over: total > line -> Win
                # If Under: total < line -> Win
                # If total == line -> Push (won = None or False? usually Push is not a win, profit 0)
                
                # We need to handle 'side' being 'over' or 'under' (case insensitive)
                is_over = completed['side'].str.lower() == 'over'
                is_under = completed['side'].str.lower() == 'under'
                
                # Initialize won as None
                completed.loc[has_scores, 'won'] = None
                
                # Over wins
                over_wins = is_over & (completed['total_points'] > completed['total_line'])
                completed.loc[has_scores & over_wins, 'won'] = True
                
                # Over losses
                over_losses = is_over & (completed['total_points'] < completed['total_line'])
                completed.loc[has_scores & over_losses, 'won'] = False
                
                # Under wins
                under_wins = is_under & (completed['total_points'] < completed['total_line'])
                completed.loc[has_scores & under_wins, 'won'] = True
                
                # Under losses
                under_losses = is_under & (completed['total_points'] > completed['total_line'])
                completed.loc[has_scores & under_losses, 'won'] = False
                
                # Pushes (total == line) remain None (or we can set explicitly if needed)
                # If won is None, profit should be 0 (Push)
                
                # Recalculate profit
                completed['profit'] = completed.apply(calculate_profit, axis=1)
            
            # Clean up merge columns
            completed = completed.drop(columns=['forward_game_id', 'outcome'], errors='ignore')

    recommended_table = components.overunder_recommended_table(recommended)
    completed_table = components.overunder_completed_table(completed)
    summary_section = components.summary_cards(metrics)
    bankroll_section = components.bankroll_cards(metrics)

    performance_section = components.empty_state("No completed totals with enough edge yet.")
    if not performance_df.empty:
        performance_section = html.Div(
            [
                dbc.Row(
                    [
                        dbc.Col(components.cumulative_profit_chart(performance_df), md=6),
                        dbc.Col(components.roi_over_time_chart(performance_df), md=6),
                    ],
                    className="g-3",
                ),
                html.Br(),
                dbc.Row(
                    [
                        dbc.Col(components.win_rate_over_time_chart(performance_df), md=6),
                        dbc.Col(components.performance_by_period_chart(performance_df, period="W"), md=6),
                    ],
                    className="g-3",
                ),
            ]
        )

    edge_section = components.empty_state("Not enough completed totals to analyze edge buckets yet.")
    if not threshold_df.empty:
        edge_section = dbc.Row(
            [
                dbc.Col(components.performance_by_threshold_chart(threshold_df), md=6),
                dbc.Col(components.performance_by_threshold_table(threshold_df), md=6),
            ],
            className="g-3",
        )

    return (
        summary_section,
        bankroll_section,
        components.cumulative_profit_chart(performance_df),
        components.multi_model_cumulative_profit_chart(model_performance),
        components.cumulative_profit_by_league_chart(league_performance),
        components.roi_by_league_chart(league_performance),
        performance_section,
        recommended_table,
        completed_table,
        edge_section,
        totals_odds_json,
    )


@app.callback(
    Output("moneyline-modal", "is_open"),
    Output("moneyline-modal-title", "children"),
    Output("moneyline-modal-content", "children"),
    Input("recommended-bets-table-datatable", "active_cell"),
    Input("moneyline-modal-close", "n_clicks"),
    State("recommended-bets-table-datatable", "data"),
    State("book-odds-store", "data"),
    prevent_initial_call=True,
)
def toggle_moneyline_modal(active_cell, close_clicks, table_data, book_odds_json):
    trigger = ctx.triggered_id if hasattr(ctx, "triggered_id") else None
    if trigger == "moneyline-modal-close":
        return False, no_update, no_update
    if trigger != "recommended-bets-table-datatable":
        raise PreventUpdate
    if not active_cell or active_cell.get("column_id") != "moneyline":
        raise PreventUpdate
    if not table_data:
        raise PreventUpdate

    row_index = active_cell.get("row")
    if row_index is None or row_index >= len(table_data):
        raise PreventUpdate

    row = table_data[row_index]
    game_id = row.get("game_id")
    if not game_id:
        raise PreventUpdate

    odds_df = pd.DataFrame()
    if book_odds_json:
        try:
            odds_df = pd.read_json(StringIO(book_odds_json), orient="records")
        except ValueError:
            odds_df = pd.DataFrame()

    if not odds_df.empty:
        key_column = "forward_game_id" if "forward_game_id" in odds_df.columns else "game_id"
        matchup_df = odds_df[odds_df[key_column] == game_id]
    else:
        matchup_df = pd.DataFrame()
    title_team = row.get("team") or "Selected team"
    opponent = row.get("opponent") or ""
    title = f"Moneylines for {title_team} vs. {opponent}".strip()

    home_team = row.get("home_team_name") or row.get("team")
    away_team = row.get("away_team_name") or row.get("opponent")
    if not home_team or not away_team:
        side = (row.get("side") or "").lower() if isinstance(row.get("side"), str) else ""
        if side == "home":
            home_team = row.get("team")
            away_team = row.get("opponent")
        elif side == "away":
            home_team = row.get("opponent")
            away_team = row.get("team")

    if matchup_df.empty:
        # Fallback to empty state if no sportsbook odds
        content = components.empty_state("No sportsbook moneylines available for this matchup.")
    else:
        content = components.moneyline_detail_table(matchup_df, home_team=home_team, away_team=away_team)

    return True, title, content


@app.callback(
    Output("ou-odds-modal", "is_open"),
    Output("ou-odds-modal-title", "children"),
    Output("ou-odds-modal-content", "children"),
    Input("overunder-recommended-table-datatable", "active_cell"),
    Input("ou-odds-modal-close", "n_clicks"),
    State("overunder-recommended-table-datatable", "data"),
    State("ou-book-odds-store", "data"),
    prevent_initial_call=True,
)
def toggle_overunder_modal(active_cell, close_clicks, table_data, book_odds_json):
    trigger = ctx.triggered_id if hasattr(ctx, "triggered_id") else None
    if trigger == "ou-odds-modal-close":
        return False, no_update, no_update
    if trigger != "overunder-recommended-table-datatable":
        raise PreventUpdate
    if not active_cell or active_cell.get("column_id") != "moneyline":
        raise PreventUpdate
    if not table_data:
        raise PreventUpdate

    row_index = active_cell.get("row")
    if row_index is None or row_index >= len(table_data):
        raise PreventUpdate

    row = table_data[row_index]
    game_id = row.get("game_id")
    if not game_id:
        raise PreventUpdate

    odds_df = pd.DataFrame()
    if book_odds_json:
        try:
            odds_df = pd.read_json(StringIO(book_odds_json), orient="records")
        except ValueError:
            odds_df = pd.DataFrame()

    matchup_df = pd.DataFrame()
    if not odds_df.empty:
        matchup_df = odds_df[odds_df["forward_game_id"] == game_id]

    home_team = row.get("home_team") or "Home"
    away_team = row.get("away_team") or "Away"
    title = f"Totals for {home_team} vs. {away_team}"

    def _parse_price(value):
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return None
        if isinstance(value, (int, float)):
            return float(value)
        try:
            return float(str(value).replace("+", "").strip())
        except (ValueError, TypeError):
            return None

    if matchup_df.empty:
        fallback_ml = _parse_price(row.get("moneyline_value")) or _parse_price(row.get("moneyline"))
        fallback_line = row.get("total_line_value")
        fallback_outcome = (row.get("side") or "over").title()
        if fallback_ml is not None or fallback_line is not None:
            fallback_rows = pd.DataFrame(
                [
                    {
                        "book": row.get("moneyline_book") or "Forward Test",
                        "outcome": fallback_outcome,
                        "moneyline": fallback_ml,
                        "line": fallback_line,
                    }
                ]
            )
            content = components.totals_detail_table(fallback_rows, home_team=home_team, away_team=away_team)
        else:
            content = components.empty_state("No sportsbook totals available for this matchup.")
    else:
        content = components.totals_detail_table(matchup_df, home_team=home_team, away_team=away_team)

    return True, title, content


@app.callback(
    Output("ou-completed-odds-modal", "is_open"),
    Output("ou-completed-odds-modal-title", "children"),
    Output("ou-completed-odds-modal-content", "children"),
    Input("overunder-completed-table-datatable", "active_cell"),
    Input("ou-completed-odds-modal-close", "n_clicks"),
    State("overunder-completed-table-datatable", "data"),
    State("ou-book-odds-store", "data"),
    prevent_initial_call=True,
)
def toggle_overunder_completed_modal(active_cell, close_clicks, table_data, book_odds_json):
    trigger = ctx.triggered_id if hasattr(ctx, "triggered_id") else None
    if trigger == "ou-completed-odds-modal-close":
        return False, no_update, no_update
    if trigger != "overunder-completed-table-datatable":
        raise PreventUpdate
    if not active_cell or active_cell.get("column_id") != "moneyline":
        raise PreventUpdate
    if not table_data:
        raise PreventUpdate

    row_index = active_cell.get("row")
    if row_index is None or row_index >= len(table_data):
        raise PreventUpdate

    row = table_data[row_index]
    game_id = row.get("game_id")
    if not game_id:
        raise PreventUpdate

    odds_df = pd.DataFrame()
    if book_odds_json:
        try:
            odds_df = pd.read_json(StringIO(book_odds_json), orient="records")
        except ValueError:
            odds_df = pd.DataFrame()

    matchup_df = pd.DataFrame()
    if not odds_df.empty:
        matchup_df = odds_df[odds_df["forward_game_id"] == game_id]

    # If no odds found in the store (common for completed bets), try fetching from DB
    if matchup_df.empty:
        # Construct a DataFrame from the single row to pass to the helper
        # We need to ensure the keys match what get_totals_odds_for_recommended expects
        row_data = row.copy()
        # Ensure commence_time is a timestamp if possible, though the helper handles strings
        row_df = pd.DataFrame([row_data])
        
        try:
            fetched_odds = get_totals_odds_for_recommended(row_df)
            if not fetched_odds.empty:
                matchup_df = fetched_odds
        except Exception:
            # If DB fetch fails, we'll fall back to the "Recorded" logic
            pass

    home_team = row.get("home_team") or "Home"
    away_team = row.get("away_team") or "Away"
    title = f"Totals for {home_team} vs. {away_team}"

    def _parse_price(value):
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return None
        if isinstance(value, (int, float)):
            return float(value)
        try:
            return float(str(value).replace("+", "").strip())
        except (ValueError, TypeError):
            return None

    if matchup_df.empty:
        fallback_ml = _parse_price(row.get("moneyline"))
        fallback_line = row.get("total_line")
        try:
             if isinstance(fallback_line, str):
                 fallback_line = float(fallback_line)
        except:
            pass
            
        description = row.get("description") or ""
        fallback_outcome = "Over" if "Over" in description else "Under" if "Under" in description else ""
        
        if fallback_ml is not None or fallback_line is not None:
            fallback_rows = pd.DataFrame(
                [
                    {
                        "book": "Recorded",
                        "outcome": fallback_outcome,
                        "moneyline": fallback_ml,
                        "line": fallback_line,
                    }
                ]
            )
            content = components.totals_detail_table(fallback_rows, home_team=home_team, away_team=away_team)
        else:
            content = components.empty_state("No sportsbook totals available for this matchup.")
    else:
        content = components.totals_detail_table(matchup_df, home_team=home_team, away_team=away_team)

    return True, title, content


def run(debug: bool = False, port: int = 8050, host: str = "0.0.0.0") -> None:
    app.run(debug=debug, port=port, host=host)


if __name__ == "__main__":
    run(debug=True)

