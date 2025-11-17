"""Dash application for forward testing monitoring."""

from __future__ import annotations

import json
from datetime import date, timedelta
from io import StringIO
from typing import Optional

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
    get_completed_bets,
    get_moneylines_for_recommended,
    get_performance_by_threshold,
    get_performance_over_time,
    get_recommended_bets,
    get_default_version_value,
    get_version_options,
    filter_by_version,
    load_forward_test_data,
    summarize_prediction_comparison,
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
            dbc.NavLink("Dashboard", href="/", active=pathname == "/" or pathname == ""),
            dbc.NavLink("Winner Predictions", href="/predictions", active=pathname == "/predictions"),
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


app.layout = html.Div(
    [
        dcc.Location(id="url"),
        dcc.Store(id="forward-data-store", data=_df_to_json(initial_df)),
        dcc.Store(id="book-odds-store"),
        html.Div(id="page-content", children=_dashboard_layout("/")),
    ]
)


@app.callback(Output("page-content", "children"), Input("url", "pathname"))
def render_page(pathname: Optional[str]):
    if pathname == "/predictions":
        return _predictions_layout(pathname)
    return _dashboard_layout(pathname)


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
    Output("date-range-picker", "min_date_allowed"),
    Output("date-range-picker", "max_date_allowed"),
    Output("date-range-picker", "start_date"),
    Output("date-range-picker", "end_date"),
    Output("predictions-date-range", "min_date_allowed"),
    Output("predictions-date-range", "max_date_allowed"),
    Output("predictions-date-range", "start_date"),
    Output("predictions-date-range", "end_date"),
    Input("forward-data-store", "data"),
)
def sync_date_range_controls(data_json: Optional[str]):
    """Keep both date pickers aligned with the latest data window."""
    df = _df_from_json(data_json)
    if df.empty or "commence_time" not in df.columns:
        default_start = (date.today() - timedelta(days=7)).isoformat()
        default_end = date.today().isoformat()
        return (
            default_start,
            default_end,
            default_start,
            default_end,
            default_start,
            default_end,
            default_start,
            default_end,
        )

    commence = pd.to_datetime(df["commence_time"], errors="coerce")
    commence = commence.dropna()
    if commence.empty:
        default_start = (date.today() - timedelta(days=7)).isoformat()
        default_end = date.today().isoformat()
        return (
            default_start,
            default_end,
            default_start,
            default_end,
            default_start,
            default_end,
            default_start,
            default_end,
        )

    min_date_allowed = commence.min().date().isoformat()
    max_date_allowed = commence.max().date().isoformat()

    return (
        min_date_allowed,
        max_date_allowed,
        min_date_allowed,
        max_date_allowed,
        min_date_allowed,
        max_date_allowed,
        min_date_allowed,
        max_date_allowed,
    )


@app.callback(
    Output("summary-cards", "children"),
    Output("bankroll-cards", "children"),
    Output("cumulative-profit-chart", "children"),
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
        # Fallback to the stored moneyline on the recommendation itself (if available)
        def _parse_moneyline(value):
            if value is None or (isinstance(value, float) and pd.isna(value)):
                return None
            if isinstance(value, (int, float)):
                return float(value)
            try:
                return float(str(value).replace("+", "").strip())
            except (ValueError, TypeError):
                return None

        fallback_value = row.get("moneyline_value")
        fallback_ml = _parse_moneyline(fallback_value)
        if fallback_ml is None:
            fallback_ml = _parse_moneyline(row.get("moneyline"))

        fallback_side = (row.get("side") or "").lower() if isinstance(row.get("side"), str) else ""
        if not fallback_side:
            team_name = (row.get("team") or "").upper()
            if team_name and home_team and team_name == str(home_team).upper():
                fallback_side = "home"
            elif team_name and away_team and team_name == str(away_team).upper():
                fallback_side = "away"

        if fallback_ml is not None:
            fallback_book = row.get("moneyline_book") or "Forward Test"
            fallback_rows = pd.DataFrame(
                [
                    {
                        "book": fallback_book,
                        "outcome": fallback_side or "home",
                        "moneyline": fallback_ml,
                    }
                ]
            )
            content = components.moneyline_detail_table(fallback_rows, home_team=home_team, away_team=away_team)
        else:
            content = components.empty_state("No sportsbook moneylines available for this matchup.")
    else:
        content = components.moneyline_detail_table(matchup_df, home_team=home_team, away_team=away_team)

    return True, title, content


def run(debug: bool = False, port: int = 8050, host: str = "0.0.0.0") -> None:
    app.run_server(debug=debug, port=port, host=host)


if __name__ == "__main__":
    run(debug=True)

