"""Dash application for forward testing monitoring."""

from __future__ import annotations

import json
import math
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
    get_accuracy_over_time_by_league,
    get_accuracy_difference_over_time_by_league,
    get_cumulative_accuracy_by_model,
    get_all_games,
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
    win_rate_over_time_chart,
    model_comparison_table,
    model_comparison_table,
    accuracy_by_league_chart,
    accuracy_difference_by_league_chart,
    cumulative_accuracy_by_model_chart,
    raw_data_table,
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
    return pd.read_json(StringIO(data), orient="records")


def _get_residual_std(league: str) -> float:
    """Get approximate residual standard deviation for totals model by league."""
    league = str(league).upper()
    if league == "NBA":
        return 12.0
    elif league == "NCAAB":
        return 14.0
    elif league == "NFL":
        return 9.0
    elif league == "CFB":
        return 14.0
    elif league == "NHL":
        return 1.8
    elif league in ["EPL", "LALIGA", "BUNDESLIGA", "SERIEA", "LIGUE1"]:
        return 1.2
    return 12.0  # Default fallback


def _filter_by_date(df: pd.DataFrame, start_date: Optional[str], end_date: Optional[str]) -> pd.DataFrame:
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
            dbc.NavLink("Raw Data", href="/data", active=pathname == "/data"),
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
            dcc.Loading(html.Div(id="accuracy-by-league-chart"), type="circle"),
            html.Br(),
            dcc.Loading(html.Div(id="cumulative-accuracy-chart"), type="circle"),
            html.Br(),
            dcc.Loading(html.Div(id="accuracy-diff-by-league-chart"), type="circle"),
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
    )


def _data_layout(pathname: Optional[str]) -> dbc.Container:
    return dbc.Container(
        [
            _navbar(pathname),
            dbc.Row(
                [
                    dbc.Col(html.H2("Raw Data Browser"), md=10),
                    dbc.Col(
                         dbc.Button("Refresh Data", id="refresh-data-btn", color="secondary"),
                         md=2,
                         className="text-end"
                    ),
                ],
                className="align-items-center mb-3"
            ),
            dcc.Tabs(
                id="data-tabs",
                value="games",
                children=[
                    dcc.Tab(label="All Games", value="games"),
                ],
            ),
            html.Br(),
            dcc.Loading(html.Div(id="raw-data-content"), type="circle"),
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
    if pathname == "/data":
        return _data_layout(pathname)
    return _dashboard_layout(pathname)


@app.callback(
    Output("forward-data-store", "data"),
    Output("last-updated-text", "children"),
    Input("refresh-button", "n_clicks"),
    Input("model-type-dropdown", "value"),
    Input("date-range-picker", "start_date"),
    Input("date-range-picker", "end_date"),
    prevent_initial_call=False,
)
def refresh_data(n_clicks: Optional[int], model_type: str, start_date: str, end_date: str) -> tuple[str, str]:
    """Refresh the cached predictions when the manual refresh button is clicked or model type changes."""
    force_refresh = bool(n_clicks)
    # If triggered by dropdown (n_clicks is None or unchanged), we might not need force_refresh, 
    # but load_forward_test_data handles caching.
    # However, if we switch model type, we definitely want to load that model's data.
    
    # Use a default if model_type is None (initial load)
    model_type = model_type or "ensemble"
    
    df = load_forward_test_data(
        force_refresh=force_refresh, 
        league=None, 
        model_type=model_type,
        start_date=start_date,
        end_date=end_date
    )
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
    import time
    t0 = time.time()
    
    # Load ALL models at once to avoid repeated DB calls and expansions
    # We ignore the data_json passed from the store because we want to batch load everything newly here
    # This is a deviation from the previous architecture but necessary for performance
    
    all_models = ["ensemble", "random_forest", "gradient_boosting"]
    # We don't have model_type in arguments here! 
    # Wait, the callback signature DOES NOT have model_type. 
    # It has start_date, end_date, edge_threshold, period, league, version.
    # Ah, the model_type is NOT passed to update_dashboard?
    # Let me check the signature.
    # The callback inputs are:
    # Input("forward-data-store", "data"),
    # Input("date-range-picker", "start_date"),
    # Input("date-range-picker", "end_date"),
    # Input("edge-threshold-slider", "value"),
    # Input("period-select", "value"),
    # Input("league-select", "value"),
    # Input("version-select", "value"),
    
    # It does NOT take model_type. The main dashboard is implicitly for "ensemble" or whatever was loaded?
    # Actually, the main dashboard usually shows "ensemble" vs books.
    # But wait, looking at my previous edit attempt (Step 9226), I used `model_type` variable.
    # Where did `model_type` come from? It wasn't in the arguments!
    # That explains why it might have failed or been weird.
    
    # The main dashboard ("Overview") is primarily for the Ensemble model (or whatever data_json contained).
    # But now we want to load everything.
    
    # Let's assume the main view is for "ensemble".
    model_type = "ensemble"
    
    all_models = ["ensemble", "random_forest", "gradient_boosting"]
    
    df_all = load_forward_test_data(
        force_refresh=False, 
        league=None, 
        model_type=all_models,
        start_date=start_date,
        end_date=end_date
    )
    df_all = filter_by_version(df_all, version)
    
    # Filter for the SELECTED model (Ensemble) for the main dashboard view
    if df_all.empty:
        df = pd.DataFrame()
    else:
        df = df_all[df_all["model_type"] == model_type].copy()
    
    t1 = time.time()
    print(f"DEBUG: SQL Batch Load & Version Filter: {t1-t0:.4f}s")

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
    
    # Apply league filters to MAIN df
    if not df.empty and league != "all" and "league" in df.columns:
        # Handle None/NaN values in league column
        df = df[df["league"].notna() & (df["league"].astype(str).str.upper() == league.upper())].copy()
    
    df = _filter_by_date(df, start_date, end_date)
    
    t2 = time.time()
    print(f"DEBUG: Logic & Filters: {t2-t1:.4f}s")

    # OPTIMIZATION: Expand predictions ONCE for ALL models
    # This avoids repeating the expensive expansion logic for performance/threshold/rec/completed calculations
    # AND for the multi-model comparison loop
    from .data import _expand_predictions
    t_exp = time.time()
    
    bets_all = None
    if not df_all.empty:
        bets_all = _expand_predictions(df_all, stake=DEFAULT_STAKE)
        
    # Slice bets for the selected model
    bets = None
    if bets_all is not None and not bets_all.empty:
         bets = bets_all[bets_all["model_type"] == (model_type or "ensemble")].copy()
         
         # Apply league filter to bets as well (since we filtered df above)
         if league != "all" and "league" in bets.columns:
             bets = bets[bets["league"].notna() & (bets["league"].str.upper() == league.upper())].copy()
             
    print(f"DEBUG: _expand_predictions (once for ALL models): {time.time()-t_exp:.4f}s")
    
    metrics: SummaryMetrics = calculate_summary_metrics(df, edge_threshold=edge_threshold, bets=bets)
    t3 = time.time()
    print(f"DEBUG: calculate_summary_metrics: {t3-t2:.4f}s")

    performance_df = get_performance_over_time(df, edge_threshold=edge_threshold, bets=bets)
    t4 = time.time()
    print(f"DEBUG: get_performance_over_time: {t4-t3:.4f}s")
    
    threshold_df = get_performance_by_threshold(df, stake=DEFAULT_STAKE, bets=bets)
    recommended_df = get_recommended_bets(df, edge_threshold=edge_threshold, bets=bets)
    completed_bets_df = get_completed_bets(df, edge_threshold=edge_threshold, stake=DEFAULT_STAKE, bets=bets)
    
    t5 = time.time()
    print(f"DEBUG: threshold/rec/completed: {t5-t4:.4f}s")
    
    # Load performance data for all models for the multi-model cumulative profit chart
    # NOW OPTIMIZED: Reuse the already loaded and expanded `bets_all` dataframe
    model_performance = {}
    for model_type_iter in all_models:
        try:
            # Slice from bets_all instead of reloading/expanding
            if bets_all is None or bets_all.empty:
                model_perf = pd.DataFrame()
            else:
                model_bets = bets_all[bets_all["model_type"] == model_type_iter].copy()
                
                # Apply league filter
                if league != "all" and not model_bets.empty and "league" in model_bets.columns:
                    model_bets = model_bets[model_bets["league"].notna() & (model_bets["league"].astype(str).str.upper() == league.upper())].copy()
                
                # We already filtered dates in SQL load
                
                # Get performance using the sliced bets
                # Note: We need a corresponding 'df' for get_performance_over_time signature, 
                # but since we pass 'bets', the 'df' is ignored for calculation.
                # efficiently we can pass an empty df or the sliced df (sliced df is safer)
                model_df_slice = df_all[df_all["model_type"] == model_type_iter]
                model_perf = get_performance_over_time(model_df_slice, edge_threshold=edge_threshold, stake=DEFAULT_STAKE, bets=model_bets)
                
            model_performance[model_type_iter] = model_perf
            
        except Exception as e:
            print(f"DEBUG: Error processing model {model_type_iter}: {e}")
            model_performance[model_type_iter] = pd.DataFrame()

    t6 = time.time()
    print(f"DEBUG: Multi-model loop (optimized): {t6-t5:.4f}s")

    # Calculate performance by league for the current model
    league_performance = get_performance_by_league(df, edge_threshold=edge_threshold, stake=DEFAULT_STAKE, bets=bets)

    book_odds_json = ""
    recommended_display_df = recommended_df
    if not recommended_df.empty:
        t_odds = time.time()
        book_odds_df = get_moneylines_for_recommended(recommended_df)
        print(f"DEBUG: get_moneylines_for_recommended: {time.time()-t_odds:.4f}s")
        
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
    
    t7 = time.time()
    print(f"DEBUG: Total Update Time: {t7-t0:.4f}s")

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
    Output("accuracy-by-league-chart", "children"),
    Output("cumulative-accuracy-chart", "children"),
    Output("accuracy-diff-by-league-chart", "children"),
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
    accuracy_df = get_accuracy_over_time_by_league(comparison_df)
    
    # Fetch multi-model data for cumulative accuracy chart
    # We need to load all models to compare them
    multi_model_df = compare_model_predictions(league=league, start_date=start_date, end_date=end_date)
    cumulative_accuracy_df = get_cumulative_accuracy_by_model(multi_model_df)
    
    accuracy_diff_df = get_accuracy_difference_over_time_by_league(comparison_df)

    return (
        components.prediction_summary(stats),
        components.accuracy_by_league_chart(accuracy_df),
        components.cumulative_accuracy_by_model_chart(cumulative_accuracy_df),
        components.accuracy_difference_by_league_chart(accuracy_diff_df),
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
    # Load ALL models at once to avoid repeated DB calls and expansions
    all_models = ["ensemble", "random_forest", "gradient_boosting"]
    if model_type and model_type not in all_models:
        all_models.append(model_type)
        
    df_all = load_forward_test_data(
        force_refresh=False, 
        league=None, 
        model_type=all_models,
        start_date=start_date,
        end_date=end_date
    )
    df_all = filter_by_version(df_all, version)
    
    # Filter for the SELECTED model for the main dashboard view
    if df_all.empty:
        df = pd.DataFrame()
    else:
        df = df_all[df_all["model_type"] == (model_type or "ensemble")].copy()
    
    if not df.empty and league != "all" and "league" in df.columns:
        df = df[df["league"].notna() & (df["league"].astype(str).str.upper() == league.upper())].copy()

    if df.empty:
        empty = components.empty_state("No totals data available yet.")
        return empty, empty, empty, empty, empty, empty, empty, empty, empty, empty, ""
        
    # OPTIMIZATION: Expand totals ONCE for ALL models
    from .data import _expand_totals
    totals_all = None
    if not df_all.empty:
        totals_all = _expand_totals(df_all, stake=DEFAULT_STAKE)
        
    # Slice totals for the selected model
    totals = None
    if totals_all is not None and not totals_all.empty:
        totals = totals_all[totals_all["model_type"] == (model_type or "ensemble")].copy()
        
        # Apply league filter
        if league != "all" and "league" in totals.columns:
            totals = totals[totals["league"].notna() & (totals["league"].str.upper() == league.upper())].copy()

    metrics = calculate_totals_metrics(df, edge_threshold=edge_threshold, stake=DEFAULT_STAKE, totals=totals)
    performance_df = get_totals_performance_over_time(df, edge_threshold=edge_threshold, stake=DEFAULT_STAKE, totals=totals)
    threshold_df = get_totals_performance_by_threshold(df, stake=DEFAULT_STAKE, totals=totals)
    recommended = get_overunder_recommendations(df, edge_threshold=edge_threshold, totals=totals)
    completed = get_overunder_completed(df, edge_threshold=edge_threshold, stake=DEFAULT_STAKE, totals=totals)
    
    # Load performance data for all models for the multi-model cumulative profit chart
    # NOW OPTIMIZED: Reuse the already loaded and expanded `totals_all` dataframe
    model_performance = {}
    for model_type_iter in all_models:
        try:
            # Slice from totals_all
            if totals_all is None or totals_all.empty:
                model_perf = pd.DataFrame()
            else:
                model_totals = totals_all[totals_all["model_type"] == model_type_iter].copy()
                
                # Apply league filter
                if league != "all" and not model_totals.empty and "league" in model_totals.columns:
                    model_totals = model_totals[model_totals["league"].notna() & (model_totals["league"].astype(str).str.upper() == league.upper())].copy()
                
                # Use sliced totals
                model_df_slice = df_all[df_all["model_type"] == model_type_iter]
                model_perf = get_totals_performance_over_time(model_df_slice, edge_threshold=edge_threshold, stake=DEFAULT_STAKE, totals=model_totals)
                
            model_performance[model_type_iter] = model_perf
        except Exception:
            model_performance[model_type_iter] = pd.DataFrame()

    # Calculate performance by league for the current model (using totals-specific function)
    league_performance = get_totals_performance_by_league(df, edge_threshold=edge_threshold, stake=DEFAULT_STAKE, totals=totals)

    totals_odds_json = ""
    totals_odds_df = get_totals_odds_for_recommended(recommended)
    if not totals_odds_df.empty:
        totals_odds_json = totals_odds_df.to_json(date_format="iso", orient="records")
        
        # Merge fresh odds into recommended dataframe for the table display
        # Select the BEST odds based on EDGE (not just highest moneyline) for each game/side
        # We want to show the most profitable bet available in sportsbooks
        
        # Create lookups for edge calculation
        game_id_to_pred = recommended.set_index('game_id')['predicted_total_points'].to_dict()
        game_id_to_league = recommended.set_index('game_id')['league'].to_dict()
        
        def _calc_edge_row(row):
            gid = row['forward_game_id']
            pred = game_id_to_pred.get(gid)
            if pd.isna(pred) or pd.isna(row['line']) or pd.isna(row['moneyline']):
                return -100.0
            
            line = float(row['line'])
            ml = float(row['moneyline'])
            outcome = str(row['outcome']).lower()
            
            # Implied prob
            if ml > 0:
                imp = 100 / (ml + 100)
            else:
                imp = -ml / (-ml + 100)
            
            # Pred prob
            diff = pred - line
            league = game_id_to_league.get(gid, 'NBA')
            std = _get_residual_std(league)
                
            try:
                over_prob = 0.5 * (1.0 + math.erf(diff / (std * math.sqrt(2.0))))
                over_prob = max(0.0, min(1.0, over_prob))
                under_prob = 1.0 - over_prob
                prob = over_prob if outcome == 'over' else under_prob
                return prob - imp
            except Exception:
                return -100.0

        totals_odds_df['calc_edge'] = totals_odds_df.apply(_calc_edge_row, axis=1)
        
        # Sort by edge descending
        totals_odds_df = totals_odds_df.sort_values('calc_edge', ascending=False)
        
        # Group by game/outcome and take first (best edge)
        best_odds = totals_odds_df.groupby(['forward_game_id', 'outcome']).first().reset_index()
        
        # Create a lookup for side flipping: (game_id, side) -> row
        odds_lookup = {}
        for _, row in best_odds.iterrows():
            key = (row['forward_game_id'], row['outcome'].lower())
            odds_lookup[key] = row.to_dict()

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
            
            # Re-evaluate predictions for rows with updated lines
            # If the line moved, our original 'side' and 'edge' might be wrong
            if 'predicted_total_points' in recommended.columns:
                def reevaluate_prediction(row):
                    if pd.isna(row.get('total_line')) or pd.isna(row.get('predicted_total_points')):
                        return row
                    
                    line = float(row['total_line'])
                    pred_total = float(row['predicted_total_points'])
                    
                    # Calculate new side
                    diff = pred_total - line
                    new_side = "over" if diff > 0 else "under"
                    
                    # Calculate new probability
                    # We need residual_std for this. Use approximation based on league.
                    league = row.get('league', 'NBA')
                    residual_std = _get_residual_std(league)
                    
                    try:
                        over_prob = 0.5 * (1.0 + math.erf(diff / (residual_std * math.sqrt(2.0))))
                        over_prob = max(0.0, min(1.0, over_prob))
                        under_prob = 1.0 - over_prob
                    except Exception:
                        over_prob = 0.5
                        under_prob = 0.5
                        
                    new_prob = over_prob if new_side == "over" else under_prob
                    
                    # Calculate new edge
                    original_side = row.get('side')
                    
                    if new_side == original_side:
                        row['predicted_prob'] = new_prob
                        
                        # Recalculate edge
                        ml = row.get('moneyline')
                        if pd.notna(ml):
                            if ml > 0:
                                implied = 100 / (ml + 100)
                            else:
                                implied = -ml / (-ml + 100)
                            
                            row['implied_prob'] = implied
                            row['edge'] = new_prob - implied
                            
                        # Update description
                        row['description'] = f"{new_side.title()} {line:.1f}"
                    else:
                        # Side flipped! Check if we have odds for the new side
                        game_id = row.get('game_id')
                        new_odds = odds_lookup.get((game_id, new_side))
                        
                        if new_odds:
                            # We have odds for the new side!
                            row['side'] = new_side
                            row['predicted_prob'] = new_prob
                            
                            # Update odds info
                            ml = new_odds.get('moneyline')
                            row['moneyline'] = ml
                            row['book'] = new_odds.get('book')
                            
                            # Recalculate edge
                            if pd.notna(ml):
                                if ml > 0:
                                    implied = 100 / (ml + 100)
                                else:
                                    implied = -ml / (-ml + 100)
                                
                                row['implied_prob'] = implied
                                row['edge'] = new_prob - implied
                            else:
                                row['edge'] = -1.0
                                
                            row['description'] = f"{new_side.title()} {line:.1f}"
                        else:
                            # No odds for the new side, mark as invalid
                            row['edge'] = -1.0 
                            row['description'] = f"{original_side.title()} {line:.1f} (Line Moved)"
                            row['predicted_prob'] = new_prob 
                        
                    return row

                # Apply re-evaluation
                recommended.loc[has_sportsbook_data] = recommended.loc[has_sportsbook_data].apply(reevaluate_prediction, axis=1)

            # Filter out bets that no longer have a positive edge after re-evaluation
            recommended = recommended[recommended["edge"] >= edge_threshold]
            
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
            
            # Deduplicate columns to prevent indexing errors
            completed = completed.loc[:, ~completed.columns.duplicated()]

            # Determine if games have started (User Request: Freeze odds at kickoff)
            now = pd.Timestamp.now(tz=DISPLAY_TIMEZONE)
            
            if 'commence_time' in completed.columns:
                # Convert to datetime if not already
                if not pd.api.types.is_datetime64_any_dtype(completed['commence_time']):
                    completed['commence_time'] = pd.to_datetime(completed['commence_time'], errors='coerce', utc=True)
                
                # Convert to display timezone
                commence_time_tz = completed['commence_time'].dt.tz_convert(DISPLAY_TIMEZONE)
                
                # Check if game is in the future (has not started)
                is_future = commence_time_tz > now
            else:
                # Fallback: assume NOT future (freeze) to be safe
                is_future = pd.Series(False, index=completed.index)

            # Handle line column
            line_col = 'line_sportsbook' if 'line_sportsbook' in completed.columns else 'line'
            if line_col in completed.columns:
                completed[line_col] = pd.to_numeric(completed[line_col], errors="coerce")
                has_sportsbook_data = completed[line_col].notna()
                
                # Ensure mask is 1D
                if isinstance(has_sportsbook_data, pd.DataFrame):
                    has_sportsbook_data = has_sportsbook_data.iloc[:, 0]
                
                # Update mask: Only update if we have data AND the game hasn't started
                mask_update = has_sportsbook_data & is_future
                
                # Update line
                completed.loc[mask_update, 'total_line'] = completed.loc[mask_update, line_col]
                
                # Update moneyline if available
                if 'moneyline_sportsbook' in completed.columns:
                    completed.loc[mask_update, 'moneyline'] = completed.loc[mask_update, 'moneyline_sportsbook']
                
                # Update book if available
                if 'book_sportsbook' in completed.columns:
                    completed.loc[mask_update, 'book'] = completed.loc[mask_update, 'book_sportsbook']
                
                # Regenerate description
                if 'description' in completed.columns and 'side' in completed.columns:
                    completed.loc[mask_update, 'description'] = completed.loc[mask_update].apply(
                        lambda row: f"{row['side'].title()} {row['total_line']:.1f}" if pd.notna(row['total_line']) else row['side'].title(),
                        axis=1
                    )
                
                # Update team names (optional, but good to keep consistent)
                if 'home_team_full' in completed.columns:
                    completed.loc[mask_update, 'home_team'] = completed.loc[mask_update, 'home_team_full'].fillna(completed.loc[mask_update, 'home_team'])
                if 'away_team_full' in completed.columns:
                    completed.loc[mask_update, 'away_team'] = completed.loc[mask_update, 'away_team_full'].fillna(completed.loc[mask_update, 'away_team'])

            # Cleanup: Drop sportsbook columns now that we're done with them
            cols_to_drop = ['book_sportsbook', 'moneyline_sportsbook', 'line_sportsbook', 'line', 'forward_game_id', 'outcome', 'home_team_full', 'away_team_full']
            completed = completed.drop(columns=cols_to_drop, errors='ignore')


            
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

            # Only recalculate for rows where we have scores AND the game is actually complete (has a result)
            # This prevents ongoing games from prematurely showing win/loss
            has_scores_and_final = completed['total_points'].notna() & completed['result'].notna()
            
            if has_scores_and_final.any():
                # Recalculate 'won'
                # If Over: total > line -> Win
                # If Under: total < line -> Win
                # If total == line -> Push (won = None or False? usually Push is not a win, profit 0)
                
                # We need to handle 'side' being 'over' or 'under' (case insensitive)
                is_over = completed['side'].str.lower() == 'over'
                is_under = completed['side'].str.lower() == 'under'
                
                # Initialize won as None
                completed.loc[has_scores_and_final, 'won'] = None
                
                # Over wins
                over_wins = is_over & (completed['total_points'] > completed['total_line'])
                completed.loc[has_scores_and_final & over_wins, 'won'] = True
                
                # Over losses
                over_losses = is_over & (completed['total_points'] < completed['total_line'])
                completed.loc[has_scores_and_final & over_losses, 'won'] = False
                
                # Under wins
                under_wins = is_under & (completed['total_points'] < completed['total_line'])
                completed.loc[has_scores_and_final & under_wins, 'won'] = True
                
                # Under losses
                under_losses = is_under & (completed['total_points'] > completed['total_line'])
                completed.loc[has_scores_and_final & under_losses, 'won'] = False
                
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


    return True, title, content


@app.callback(
    Output("raw-data-content", "children"),
    Input("url", "pathname"),
    Input("refresh-data-btn", "n_clicks"),
)
def update_raw_data(pathname: Optional[str], n_clicks: Optional[int]):
    if pathname != "/data":
        return no_update
        
    # Fetch raw data
    try:
        df = get_all_games(limit=5000)
    except Exception as e:
        return components.empty_state(f"Error fetching data: {e}")
        
    return components.raw_data_table(df)


def run(debug: bool = False, port: int = 8050, host: str = "0.0.0.0") -> None:
    app.run(debug=debug, port=port, host=host)


if __name__ == "__main__":
    run(debug=True)

