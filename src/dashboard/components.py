"""Reusable Dash components for the forward testing dashboard."""

from __future__ import annotations

from typing import Optional

import dash_bootstrap_components as dbc
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash import dash_table, dcc, html

from .data import DISPLAY_TIMEZONE, SummaryMetrics


def _format_number(value: Optional[float], digits: int = 0) -> str:
    if value is None or (isinstance(value, float) and (pd.isna(value) or pd.isnull(value))):
        return "—"
    return f"{value:,.{digits}f}"


def _format_percent(value: Optional[float], digits: int = 1) -> str:
    if value is None or (isinstance(value, float) and (pd.isna(value) or pd.isnull(value))):
        return "—"
    return f"{value * 100:.{digits}f}%"


def _format_currency(value: Optional[float], digits: int = 0) -> str:
    if value is None or (isinstance(value, float) and (pd.isna(value) or pd.isnull(value))):
        return "—"
    prefix = "-$" if value < 0 else "$"
    return f"{prefix}{abs(value):,.{digits}f}"


def _format_datetime(value: Optional[pd.Timestamp]) -> str:
    if value is None or pd.isna(value):
        return ""
    if isinstance(value, pd.Timestamp):
        if value.tzinfo is None:
            localized = value.tz_localize(DISPLAY_TIMEZONE)
        else:
            localized = value.tz_convert(DISPLAY_TIMEZONE)
        tz_name = localized.tzname() or "ET"
        return f"{localized.strftime('%Y-%m-%d %I:%M %p')} {tz_name}"
    return str(value)


def metric_card(title: str, value: str, *, subtitle: Optional[str] = None, color: str = "primary") -> dbc.Card:
    return dbc.Card(
        dbc.CardBody(
            [
                html.H6(title, className="card-title mb-2 text-uppercase text-muted"),
                html.H3(value, className="mb-0"),
                html.Small(subtitle or "", className="text-muted"),
            ]
        ),
        className=f"h-100 border-{color} border-2",
    )


def summary_cards(metrics: SummaryMetrics) -> dbc.Row:
    cards = [
        dbc.Col(metric_card("Total Predictions", _format_number(metrics.total_predictions)), md=2),
        dbc.Col(metric_card("Completed", _format_number(metrics.completed_games)), md=2),
        dbc.Col(metric_card("Pending", _format_number(metrics.pending_games)), md=2),
        dbc.Col(
            metric_card(
                "Recommended Bets",
                _format_number(metrics.recommended_bets),
                subtitle=f"{_format_number(metrics.recommended_completed)} settled",
                color="info",
            ),
            md=2,
        ),
        dbc.Col(
            metric_card(
                "Win Rate",
                _format_percent(metrics.win_rate),
                subtitle="Completed recommendations",
                color="success",
            ),
            md=2,
        ),
        dbc.Col(
            metric_card(
                "ROI",
                _format_percent(metrics.roi),
                subtitle=_format_currency(metrics.net_profit, 0) + " net",
                color="success" if (metrics.net_profit or 0) >= 0 else "danger",
            ),
            md=2,
        ),
    ]

    return dbc.Row(cards, className="g-3")


def bankroll_cards(metrics: SummaryMetrics) -> dbc.Row:
    """Bankroll statistics cards."""
    cards = [
        dbc.Col(
            metric_card(
                "Starting Bankroll",
                _format_currency(metrics.starting_bankroll, 0),
                color="primary",
            ),
            md=3,
        ),
        dbc.Col(
            metric_card(
                "Current Bankroll",
                _format_currency(metrics.current_bankroll, 0),
                subtitle=_format_currency(metrics.net_profit, 0) + " profit/loss",
                color="success" if metrics.current_bankroll >= metrics.starting_bankroll else "danger",
            ),
            md=3,
        ),
        dbc.Col(
            metric_card(
                "Total Staked",
                _format_currency(metrics.total_staked, 0),
                subtitle=f"{_format_number(metrics.recommended_completed)} bets",
                color="info",
            ),
            md=3,
        ),
        dbc.Col(
            metric_card(
                "Bankroll Growth",
                _format_percent(metrics.bankroll_growth),
                subtitle=_format_currency(metrics.current_bankroll - metrics.starting_bankroll, 0),
                color="success" if (metrics.bankroll_growth or 0) >= 0 else "danger",
            ),
            md=3,
        ),
    ]

    return dbc.Row(cards, className="g-3")


def cumulative_profit_chart(performance_df: pd.DataFrame) -> dcc.Graph:
    fig = go.Figure()

    if not performance_df.empty:
        fig.add_trace(
            go.Scatter(
                x=performance_df["date"],
                y=performance_df["cumulative_profit"],
                mode="lines+markers",
                name="Cumulative Profit",
                line=dict(color="#1f77b4", width=3),
            )
        )

    fig.update_layout(
        title="Cumulative Profit",
        xaxis_title="Date",
        yaxis_title="Profit ($)",
        template="plotly_white",
        height=360,
    )

    return dcc.Graph(figure=fig, config={"displayModeBar": False})


def roi_over_time_chart(performance_df: pd.DataFrame) -> dcc.Graph:
    fig = go.Figure()

    if not performance_df.empty:
        fig.add_trace(
            go.Scatter(
                x=performance_df["date"],
                y=performance_df["roi"],
                mode="lines+markers",
                name="ROI",
                line=dict(color="#ff7f0e", width=3),
            )
        )

    fig.update_layout(
        title="ROI Over Time",
        xaxis_title="Date",
        yaxis_tickformat=".1%",
        template="plotly_white",
        height=320,
    )

    return dcc.Graph(figure=fig, config={"displayModeBar": False})


def win_rate_over_time_chart(performance_df: pd.DataFrame) -> dcc.Graph:
    fig = go.Figure()

    if not performance_df.empty:
        fig.add_trace(
            go.Scatter(
                x=performance_df["date"],
                y=performance_df["win_rate"],
                mode="lines+markers",
                name="Win Rate",
                line=dict(color="#2ca02c", width=3),
            )
        )

    fig.update_layout(
        title="Win Rate Over Time",
        xaxis_title="Date",
        yaxis_tickformat=".1%",
        template="plotly_white",
        height=320,
    )

    return dcc.Graph(figure=fig, config={"displayModeBar": False})


def performance_by_period_chart(performance_df: pd.DataFrame, period: str = "W") -> dcc.Graph:
    fig = go.Figure()
    if not performance_df.empty:
        df = performance_df.copy()
        df["period"] = df["date"].dt.to_period(period).dt.to_timestamp()
        grouped = df.groupby("period").agg(profit=("profit", "sum"))
        grouped = grouped.reset_index()
        fig = px.bar(grouped, x="period", y="profit", labels={"profit": "Profit ($)", "period": "Period"})
        fig.update_traces(marker_color="#636EFA")
    fig.update_layout(
        title="Profit by Period",
        template="plotly_white",
        height=320,
    )
    return dcc.Graph(figure=fig, config={"displayModeBar": False})


def performance_by_threshold_chart(threshold_df: pd.DataFrame) -> dcc.Graph:
    fig = go.Figure()
    if not threshold_df.empty:
        fig = px.bar(
            threshold_df,
            x="bucket_label",
            y="roi",
            labels={"bucket_label": "Edge Bucket", "roi": "ROI"},
        )
        fig.update_traces(marker_color="#9467bd")
        fig.update_yaxes(tickformat=".1%")

    fig.update_layout(
        title="ROI by Edge Bucket",
        template="plotly_white",
        height=320,
    )
    return dcc.Graph(figure=fig, config={"displayModeBar": False})


def performance_by_threshold_table(threshold_df: pd.DataFrame) -> dash_table.DataTable:
    if threshold_df.empty:
        threshold_df = pd.DataFrame(columns=["bucket_label", "bets", "wins", "win_rate", "profit", "roi"])

    table_df = threshold_df.copy()
    table_df["win_rate"] = table_df["win_rate"].apply(
        lambda x: f"{x * 100:.1f}%" if pd.notna(x) else "—"
    )
    table_df["roi"] = table_df["roi"].apply(lambda x: f"{x * 100:.1f}%" if pd.notna(x) else "—")
    table_df.rename(
        columns={"bucket_label": "Edge Bucket", "bets": "Bets", "wins": "Wins", "profit": "Net Profit ($)"},
        inplace=True,
    )

    columns = [
        {"name": col, "id": col}
        for col in ["Edge Bucket", "Bets", "Wins", "win_rate", "roi", "Net Profit ($)"]
    ]

    return dash_table.DataTable(
        columns=columns,
        data=table_df.to_dict("records"),
        style_table={"overflowX": "auto"},
        style_cell={"padding": "0.5rem", "textAlign": "center"},
        style_header={"fontWeight": "bold"},
    )


def recent_predictions_table(predictions: pd.DataFrame, *, page_size: int = 20) -> dash_table.DataTable:
    df = predictions.copy()
    if df.empty:
        df = pd.DataFrame(columns=[
            "predicted_at",
            "commence_time",
            "team",
            "opponent",
            "moneyline",
            "predicted_prob",
            "implied_prob",
            "edge",
            "result",
            "won",
        ])

    df["predicted_at"] = df["predicted_at"].apply(_format_datetime)
    df["commence_time"] = df["commence_time"].apply(_format_datetime)
    df["predicted_prob"] = df["predicted_prob"].apply(lambda x: f"{x:.3f}" if pd.notna(x) else "")
    df["implied_prob"] = df["implied_prob"].apply(lambda x: f"{x:.3f}" if pd.notna(x) else "")
    df["edge"] = df["edge"].apply(lambda x: f"{x:.3f}" if pd.notna(x) else "")
    
    # Create winner column - show which team won instead of home/away
    def get_winner(row):
        result = row.get("result")
        if pd.isna(result) or result is None:
            return ""
        if result == "tie":
            return "Tie"
        if result == "home":
            # Need to check if this bet is for home or away team
            # If result is "home", the home team won
            # But we need to know which team is home - this is tricky without the original game data
            # For now, show the result but indicate it's home team
            return f"{row.get('team', 'Home')} (Home)" if row.get("side") == "home" else "Opponent (Home)"
        elif result == "away":
            return f"{row.get('team', 'Away')} (Away)" if row.get("side") == "away" else "Opponent (Away)"
        return "—"
    
    # Better approach: if we have won column, use that
    if "won" in df.columns:
        def get_winner_from_won(row):
            if pd.isna(row.get("won")):
                return ""
            if row.get("won") is True:
                return row.get("team", "—")
            elif row.get("won") is False:
                return row.get("opponent", "—")
            return ""
        df["winner"] = df.apply(get_winner_from_won, axis=1)
    else:
        df["winner"] = df.apply(get_winner, axis=1)

    columns = [
        {"name": "Predicted", "id": "predicted_at"},
        {"name": "Commence", "id": "commence_time"},
        {"name": "Team", "id": "team"},
        {"name": "Opponent", "id": "opponent"},
        {"name": "Moneyline", "id": "moneyline"},
        {"name": "Pred Prob", "id": "predicted_prob"},
        {"name": "Impl Prob", "id": "implied_prob"},
        {"name": "Edge", "id": "edge"},
        {"name": "Winner", "id": "winner"},
    ]

    return dash_table.DataTable(
        columns=columns,
        data=df.to_dict("records"),
        page_size=page_size,
        sort_action="native",
        filter_action="native",
        style_table={"overflowX": "auto"},
        style_cell={"padding": "0.5rem", "textAlign": "center"},
        style_header={"fontWeight": "bold"},
    )


def recommended_bets_table(recommended: pd.DataFrame) -> dash_table.DataTable:
    df = recommended.copy()
    if df.empty:
        df = pd.DataFrame(columns=["commence_time", "team", "opponent", "moneyline", "predicted_prob", "edge"])

    df["commence_time"] = df["commence_time"].apply(_format_datetime)
    df["predicted_prob"] = df["predicted_prob"].apply(lambda x: f"{x:.3f}" if pd.notna(x) else "")
    df["edge"] = df["edge"].apply(lambda x: f"{x:.3f}" if pd.notna(x) else "")

    columns = [
        {"name": "Commence", "id": "commence_time"},
        {"name": "Team", "id": "team"},
        {"name": "Opponent", "id": "opponent"},
        {"name": "Moneyline", "id": "moneyline"},
        {"name": "Pred Prob", "id": "predicted_prob"},
        {"name": "Edge", "id": "edge"},
    ]

    return dash_table.DataTable(
        columns=columns,
        data=df.to_dict("records"),
        sort_action="native",
        style_table={"overflowX": "auto"},
        style_cell={"padding": "0.5rem", "textAlign": "center"},
        style_header={"fontWeight": "bold"},
    )


def calendar_table(calendar_df: pd.DataFrame) -> dash_table.DataTable:
    df = calendar_df.copy()
    if df.empty:
        df = pd.DataFrame(columns=["date", "team", "opponent", "edge", "commence_time", "moneyline"])

    df["date"] = df["date"].astype(str)
    df["commence_time"] = df["commence_time"].apply(_format_datetime)
    df["edge"] = df["edge"].apply(lambda x: f"{x:.3f}" if pd.notna(x) else "")

    columns = [
        {"name": "Date", "id": "date"},
        {"name": "Team", "id": "team"},
        {"name": "Opponent", "id": "opponent"},
        {"name": "Edge", "id": "edge"},
        {"name": "Commence", "id": "commence_time"},
        {"name": "Moneyline", "id": "moneyline"},
    ]

    return dash_table.DataTable(
        columns=columns,
        data=df.to_dict("records"),
        sort_action="native",
        style_table={"overflowX": "auto"},
        style_cell={"padding": "0.5rem", "textAlign": "center"},
        style_header={"fontWeight": "bold"},
    )


def completed_bets_table(bets_df: pd.DataFrame, *, page_size: int = 25) -> dash_table.DataTable:
    """Table showing individual completed bet results."""
    df = bets_df.copy()
    if df.empty:
        df = pd.DataFrame(columns=[
            "commence_time",
            "team",
            "opponent",
            "moneyline",
            "edge",
            "won",
            "profit",
            "stake",
            "home_score",
            "away_score",
            "result",
        ])

    # Format columns
    if "commence_time" in df.columns:
        df["start_time"] = df["commence_time"].apply(_format_datetime)
    else:
        df["start_time"] = ""
    
    df["edge"] = df["edge"].apply(lambda x: f"{x:.3f}" if pd.notna(x) else "")
    df["won"] = df["won"].apply(lambda x: "Win" if x is True else "Loss" if x is False else "")
    df["profit"] = df["profit"].apply(lambda x: _format_currency(x, 2) if pd.notna(x) else "—")
    df["stake"] = df["stake"].apply(lambda x: _format_currency(x, 0) if pd.notna(x) else "—")
    
    # Create score column
    if "home_score" in df.columns and "away_score" in df.columns:
        df["score"] = df.apply(
            lambda row: f"{row['home_score']}-{row['away_score']}" 
            if pd.notna(row.get("home_score")) and pd.notna(row.get("away_score")) 
            else "—",
            axis=1
        )
    else:
        df["score"] = "—"
    
    # Create winner column - show which team won
    def get_winner(row):
        if pd.isna(row.get("won")):
            return ""
        if row.get("won") is True:
            return row.get("team", "—")
        else:
            return row.get("opponent", "—")
    
    df["winner"] = df.apply(get_winner, axis=1)

    columns = [
        {"name": "Start Time", "id": "start_time"},
        {"name": "Team Bet", "id": "team"},
        {"name": "Opponent", "id": "opponent"},
        {"name": "Moneyline", "id": "moneyline"},
        {"name": "Edge", "id": "edge"},
        {"name": "Stake", "id": "stake"},
        {"name": "Winner", "id": "winner"},
        {"name": "Result", "id": "won"},
        {"name": "Profit/Loss", "id": "profit"},
        {"name": "Score", "id": "score"},
    ]

    return dash_table.DataTable(
        columns=columns,
        data=df.to_dict("records"),
        page_size=page_size,
        sort_action="native",
        filter_action="native",
        style_table={"overflowX": "auto"},
        style_cell={"padding": "0.5rem", "textAlign": "center"},
        style_header={"fontWeight": "bold"},
        style_data_conditional=[
            {
                "if": {"filter_query": "{won} = Win"},
                "backgroundColor": "#d4edda",
                "color": "black",
            },
            {
                "if": {"filter_query": "{won} = Loss"},
                "backgroundColor": "#f8d7da",
                "color": "black",
            },
            {
                "if": {"state": "selected"},
                "backgroundColor": "#007bff",
                "color": "white",
            },
        ],
        style_cell_conditional=[
            {
                "if": {"state": "selected"},
                "backgroundColor": "#007bff",
                "color": "white",
            },
        ],
    )


def empty_state(message: str) -> html.Div:
    return html.Div(
        dbc.Alert(message, color="warning", className="text-center"),
        className="my-4",
    )


__all__ = [
    "metric_card",
    "summary_cards",
    "bankroll_cards",
    "cumulative_profit_chart",
    "roi_over_time_chart",
    "win_rate_over_time_chart",
    "performance_by_period_chart",
    "performance_by_threshold_chart",
    "performance_by_threshold_table",
    "recent_predictions_table",
    "recommended_bets_table",
    "completed_bets_table",
    "calendar_table",
    "empty_state",
]

