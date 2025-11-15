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


def _format_moneyline(value: Optional[float]) -> str:
    if value is None or (isinstance(value, float) and (pd.isna(value) or pd.isnull(value))):
        return ""
    value_int = int(value)
    return f"+{value_int}" if value_int >= 0 else f"{value_int}"


def _format_datetime(value: Optional[pd.Timestamp]) -> str:
    if value is None or pd.isna(value):
        return ""
    if isinstance(value, pd.Timestamp):
        if value.tzinfo is None:
            localized = value.tz_localize(DISPLAY_TIMEZONE)
        else:
            localized = value.tz_convert(DISPLAY_TIMEZONE)
        
        # Format as "Nov 5 6pm", "Dec 12 8:30pm", "Jan 1 11:45am"
        month = localized.strftime('%b')  # Nov, Dec, Jan
        day = str(localized.day)  # 5, 12, 1 (no leading zero)
        hour = localized.hour
        minute = localized.minute
        
        # Convert to 12-hour format
        if hour == 0:
            hour_12 = 12
            period = 'am'
        elif hour < 12:
            hour_12 = hour
            period = 'am'
        elif hour == 12:
            hour_12 = 12
            period = 'pm'
        else:
            hour_12 = hour - 12
            period = 'pm'
        
        # Format time: include minutes only if not :00
        if minute == 0:
            time_str = f"{hour_12}{period}"
        else:
            time_str = f"{hour_12}:{minute:02d}{period}"
        
        return f"{month} {day} {time_str}"
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
            "commence_time",
            "team",
            "opponent",
            "moneyline",
            "predicted_prob",
            "implied_prob",
            "edge",
        ])

    # Sort by datetime BEFORE formatting to ensure proper date ordering
    if "commence_time" in df.columns and not df.empty:
        # Check if commence_time is still a datetime (not already formatted)
        if pd.api.types.is_datetime64_any_dtype(df["commence_time"]):
            df = df.sort_values("commence_time", ascending=False, na_position='last')
        elif df["commence_time"].dtype == 'object':
            # Try to convert back to datetime for sorting if it's a string
            try:
                df["_sort_time"] = pd.to_datetime(df["commence_time"], errors='coerce')
                df = df.sort_values("_sort_time", ascending=False, na_position='last')
                df = df.drop(columns=["_sort_time"])
            except Exception:
                pass  # If conversion fails, keep original order

    # Now format the datetime column for display
    df["commence_time"] = df["commence_time"].apply(_format_datetime)
    df["predicted_prob"] = df["predicted_prob"].apply(lambda x: f"{x:.3f}" if pd.notna(x) else "")
    df["implied_prob"] = df["implied_prob"].apply(lambda x: f"{x:.3f}" if pd.notna(x) else "")
    df["edge"] = df["edge"].apply(lambda x: f"{x:.3f}" if pd.notna(x) else "")

    columns = [
        {"name": "Start Date", "id": "commence_time", "sortable": False},  # Disable sorting on formatted date
        {"name": "Team", "id": "team"},
        {"name": "Opponent", "id": "opponent"},
        {"name": "Moneyline", "id": "moneyline"},
        {"name": "Pred Prob", "id": "predicted_prob"},
        {"name": "Impl Prob", "id": "implied_prob"},
        {"name": "Edge", "id": "edge"},
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
        df = pd.DataFrame(
            columns=[
                "commence_time",
                "team",
                "opponent",
                "moneyline",
                "predicted_prob",
                "implied_prob",
                "edge",
                "moneyline_display",
            ]
        )

    df["commence_time"] = df["commence_time"].apply(_format_datetime)
    df["predicted_prob"] = df["predicted_prob"].apply(lambda x: f"{x:.3f}" if pd.notna(x) else "")
    df["implied_prob"] = df["implied_prob"].apply(lambda x: f"{x:.3f}" if pd.notna(x) else "")
    df["edge"] = df["edge"].apply(lambda x: f"{x:.3f}" if pd.notna(x) else "")
    def _render_moneyline(text: str, index: int) -> str:
        if not text:
            return ""
        return (
            "<div style='text-align:center; width:100%;'>"
            f"<span class='moneyline-link' data-row-index='{index}' style='color:#0d6efd; text-decoration:underline;'>"
            f"{text}"
            "</span>"
            "</div>"
        )

    df["row_index"] = range(len(df))
    df["moneyline_display"] = df.apply(lambda row: _render_moneyline(row["moneyline"], int(row["row_index"])), axis=1)

    columns = [
        {"name": "Commence", "id": "commence_time"},
        {"name": "Team", "id": "team"},
        {"name": "Opponent", "id": "opponent"},
        {"name": "Moneyline", "id": "moneyline_display", "presentation": "markdown"},
        {"name": "Pred Prob", "id": "predicted_prob"},
        {"name": "Impl Prob", "id": "implied_prob"},
        {"name": "Edge", "id": "edge"},
        {"name": "Row Index", "id": "row_index", "hideable": True, "hidden": True},
    ]

    return dash_table.DataTable(
        id="recommended-bets-table-datatable",
        columns=columns,
        data=df.to_dict("records"),
        sort_action="native",
        markdown_options={"html": True},
        style_table={"overflowX": "auto"},
        style_cell={"padding": "0.5rem", "textAlign": "center"},
        style_header={"fontWeight": "bold"},
        style_data_conditional=[
            {
                "if": {"column_id": "moneyline_display"},
                "color": "#0d6efd",
                "textDecoration": "underline",
                "cursor": "pointer",
                "textAlign": "center",
            }
        ],
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


def prediction_summary(stats: PredictionComparisonStats) -> dbc.Row:
    cards = [
        dbc.Col(metric_card("Games", _format_number(stats.total_games), color="primary"), md=2),
        dbc.Col(
            metric_card(
                "Agreement Rate",
                _format_percent(stats.agreement_rate),
                subtitle="Our vs. sportsbook picks",
                color="info",
            ),
            md=2,
        ),
        dbc.Col(
            metric_card(
                "We Beat Books",
                _format_number(stats.we_right_books_wrong),
                subtitle="Our pick correct, books wrong",
                color="success",
            ),
            md=2,
        ),
        dbc.Col(
            metric_card(
                "Books Beat Us",
                _format_number(stats.books_right_we_wrong),
                subtitle="Books correct, our pick wrong",
                color="danger",
            ),
            md=2,
        ),
        dbc.Col(
            metric_card(
                "Both Correct",
                _format_number(stats.both_correct),
                color="success",
            ),
            md=2,
        ),
        dbc.Col(
            metric_card(
                "Both Wrong / Pending",
                f"{_format_number(stats.both_wrong)} / {_format_number(stats.pending)}",
                color="secondary",
            ),
            md=2,
        ),
        dbc.Col(
            metric_card(
                "Our Accuracy",
                _format_percent(stats.our_accuracy),
                subtitle="Completed games",
                color="primary",
            ),
            md=2,
        ),
        dbc.Col(
            metric_card(
                "Books Accuracy",
                _format_percent(stats.book_accuracy),
                subtitle="Completed games",
                color="dark",
            ),
            md=2,
        ),
    ]
    return dbc.Row(cards, className="g-3")


def prediction_comparison_table(predictions: pd.DataFrame):
    if predictions.empty:
        return empty_state("No predictions available for the selected filters.")

    display = predictions.copy()
    display["commence_display"] = display["commence_time"].apply(_format_datetime)
    display["matchup"] = display.apply(
        lambda row: f"{row.get('away_team') or '?'} @ {row.get('home_team') or '?'}", axis=1
    )
    display["our_pick_display"] = display.apply(
        lambda row: f"{row.get('our_pick_team') or '—'} ({_format_percent(row.get('our_pick_prob'))})",
        axis=1,
    )
    display["book_pick_display"] = display.apply(
        lambda row: f"{row.get('book_pick_team') or '—'} ({_format_percent(row.get('book_pick_prob'))})",
        axis=1,
    )
    display["agreement_display"] = display["agreement"].apply(lambda val: "Yes" if val else "No")
    display["actual_display"] = display.apply(
        lambda row: row.get("actual_winner_team") or "Pending",
        axis=1,
    )
    display["prob_gap_display"] = display["probability_gap"].apply(
        lambda gap: f"{gap * 100:+.1f} pp" if gap is not None and not pd.isna(gap) else "—"
    )

    columns = [
        {"name": "Commence (ET)", "id": "commence_display"},
        {"name": "League", "id": "league"},
        {"name": "Matchup", "id": "matchup"},
        {"name": "Our Pick", "id": "our_pick_display"},
        {"name": "Sportsbook Pick", "id": "book_pick_display"},
        {"name": "Agree?", "id": "agreement_display"},
        {"name": "Outcome", "id": "comparison_outcome"},
        {"name": "Actual Result", "id": "actual_display"},
        {"name": "Prob Gap", "id": "prob_gap_display"},
    ]

    data = display[
        [
            "commence_display",
            "league",
            "matchup",
            "our_pick_display",
            "book_pick_display",
            "agreement_display",
            "comparison_outcome",
            "actual_display",
            "prob_gap_display",
        ]
    ].to_dict("records")

    return dash_table.DataTable(
        columns=columns,
        data=data,
        page_size=25,
        sort_action="native",
        filter_action="none",
        style_table={"overflowX": "auto"},
        style_cell={"textAlign": "left", "padding": "0.5rem"},
        style_header={"fontWeight": "bold", "backgroundColor": "#f8f9fa"},
    )


def moneyline_detail_table(book_rows: pd.DataFrame, *, home_team: Optional[str], away_team: Optional[str]):
    if book_rows.empty:
        return empty_state("No sportsbook moneylines available for this matchup.")

    df = book_rows.copy()
    if "book" in df.columns:
        df = df[~df["book"].astype(str).str.contains("kaggle", case=False, na=False)].copy()
    if df.empty:
        return empty_state("No sportsbook moneylines available for this matchup.")

    df["outcome"] = df["outcome"].astype(str).str.lower()
    pivot = df.pivot_table(index="book", columns="outcome", values="moneyline", aggfunc="first").reset_index()

    def _fmt(val: Optional[float]) -> str:
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return ""
        return f"{val:+.0f}"

    if "home" in pivot.columns:
        pivot["home_display"] = pivot["home"].apply(_fmt)
    else:
        pivot["home_display"] = ""
    if "away" in pivot.columns:
        pivot["away_display"] = pivot["away"].apply(_fmt)
    else:
        pivot["away_display"] = ""
    if "draw" in pivot.columns:
        pivot["draw_display"] = pivot["draw"].apply(_fmt)

    columns = [
        {"name": "Sportsbook", "id": "book"},
        {"name": f"Home ({home_team or 'Home'})", "id": "home_display"},
        {"name": f"Away ({away_team or 'Away'})", "id": "away_display"},
    ]

    display_columns = ["book", "home_display", "away_display"]
    if "draw_display" in pivot.columns:
        columns.append({"name": "Draw", "id": "draw_display"})
        display_columns.append("draw_display")

    return dash_table.DataTable(
        columns=columns,
        data=pivot[display_columns].to_dict("records"),
        sort_action="native",
        style_table={"overflowX": "auto"},
        style_cell={"padding": "0.5rem", "textAlign": "center"},
        style_header={"fontWeight": "bold"},
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
    "prediction_summary",
    "prediction_comparison_table",
    "moneyline_detail_table",
]

