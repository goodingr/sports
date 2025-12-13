"""Reusable Dash components for the forward testing dashboard."""

from __future__ import annotations

from typing import Optional

import dash_bootstrap_components as dbc
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash import dash_table, dcc, html

from .data import DISPLAY_TIMEZONE, SummaryMetrics
from datetime import datetime


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



def _extend_series_to_now(df: pd.DataFrame, date_col: str) -> pd.DataFrame:
    """Extend a time-series DataFrame to the current time by forward-filling the last value."""
    if df.empty:
        return df
    
    df = df.copy()
    
    # Ensure date_col is datetime
    if not pd.api.types.is_datetime64_any_dtype(df[date_col]):
        try:
            df[date_col] = pd.to_datetime(df[date_col])
        except Exception:
            return df

    last_date = df[date_col].max()
    
    # Get current time in display timezone
    now = datetime.now(DISPLAY_TIMEZONE)
    
    # Handle timezone compatibility
    is_aware = df[date_col].dt.tz is not None
    if not is_aware:
        now = now.replace(tzinfo=None)
        
    if last_date < now:
        # Create a new row with 'now' as date and values from the last row
        last_row = df.iloc[[-1]].copy()
        last_row[date_col] = now
        return pd.concat([df, last_row], ignore_index=True)
        
    return df


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
        plot_df = _extend_series_to_now(performance_df, "date")
        fig.add_trace(
            go.Scatter(
                x=plot_df["date"],
                y=plot_df["cumulative_profit"],
                mode="lines+markers",
                name="Cumulative Profit",
                line=dict(color="#1f77b4", width=3),
                hovertemplate="Date: %{x}<br>Profit: $%{y:.0f}<extra></extra>",
            )
        )

    fig.update_layout(
        title="Cumulative Profit",
        xaxis_title="Date",
        yaxis_title="Profit ($)",
        yaxis=dict(tickformat="$.0f"),
        template="plotly_white",
        height=360,
    )

    return dcc.Graph(figure=fig, config={"displayModeBar": False})


def multi_model_cumulative_profit_chart(
    model_performance: dict[str, pd.DataFrame]
) -> dcc.Graph:
    """
    Create cumulative profit chart with separate lines for each model.
    
    Args:
        model_performance: Dict mapping model names to their performance DataFrames
                          Each DataFrame should have 'date' and 'cumulative_profit' columns
    """
    fig = go.Figure()
    
    # Define colors for each model
    model_colors = {
        "ensemble": "#1f77b4",  # Blue
        "random_forest": "#2ca02c",  # Green
        "gradient_boosting": "#ff7f0e",  # Orange
    }
    
    model_labels = {
        "ensemble": "Ensemble",
        "random_forest": "Random Forest",
        "gradient_boosting": "Gradient Boosting",
    }
    
    # Add a line for each model
    for model_type, perf_df in model_performance.items():
        if perf_df is not None and not perf_df.empty:
            plot_df = _extend_series_to_now(perf_df, "date")
            fig.add_trace(
                go.Scatter(
                    x=plot_df["date"],
                    y=plot_df["cumulative_profit"],
                    mode="lines+markers",
                    name=model_labels.get(model_type, model_type),
                    line=dict(
                        color=model_colors.get(model_type, "#636EFA"),
                        width=2.5
                    ),
                    marker=dict(size=6),
                    hovertemplate="Date: %{x}<br>Profit: $%{y:.0f}<extra></extra>",
                )
            )
    
    fig.update_layout(
        title="Cumulative Profit by Model",
        xaxis_title="Date",
        yaxis_title="Profit ($)",
        yaxis=dict(tickformat="$.0f"),
        template="plotly_white",
        height=400,
        hovermode="x unified",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
    )
    
    return dcc.Graph(figure=fig, config={"displayModeBar": False})


def cumulative_profit_by_league_chart(
    performance_by_league: dict[str, pd.DataFrame]
) -> dcc.Graph:
    """
    Create cumulative profit chart with separate lines for each league.
    
    Args:
        performance_by_league: Dict mapping league names to their performance DataFrames
                              Each DataFrame should have 'date' and 'cumulative_profit' columns
    """
    fig = go.Figure()
    
    # Add a line for each league
    for league, perf_df in performance_by_league.items():
        if perf_df is not None and not perf_df.empty:
            plot_df = _extend_series_to_now(perf_df, "date")
            fig.add_trace(
                go.Scatter(
                    x=plot_df["date"],
                    y=plot_df["cumulative_profit"],
                    mode="lines+markers",
                    name=league,
                    line=dict(width=2.5),
                    marker=dict(size=6),
                    hovertemplate="Date: %{x}<br>Profit: $%{y:.0f}<extra></extra>",
                )
            )
    
    fig.update_layout(
        title="Cumulative Profit by League",
        xaxis_title="Date",
        yaxis_title="Profit ($)",
        yaxis=dict(tickformat="$.0f"),
        template="plotly_white",
        height=400,
        hovermode="x unified",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
    )
    
    return dcc.Graph(figure=fig, config={"displayModeBar": False})


def accuracy_by_league_chart(accuracy_df: pd.DataFrame) -> dcc.Graph:
    """
    Create cumulative accuracy chart with separate lines for each league.
    
    Args:
        accuracy_df: DataFrame with 'commence_time', 'league', and 'accuracy' columns
    """
    fig = go.Figure()
    
    if not accuracy_df.empty:
        # Get unique leagues
        leagues = accuracy_df["league"].unique()
        
        for league in leagues:
            league_df = accuracy_df[accuracy_df["league"] == league]
            plot_df = _extend_series_to_now(league_df, "commence_time")
            fig.add_trace(
                go.Scatter(
                    x=plot_df["commence_time"],
                    y=plot_df["accuracy"],
                    mode="lines+markers",
                    name=league,
                    line=dict(width=2.5),
                    marker=dict(size=6),
                    hovertemplate="Date: %{x}<br>Accuracy: %{y:.1%}<extra></extra>",
                )
            )
    
    fig.update_layout(
        title="Cumulative Accuracy by League",
        xaxis_title="Date",
        yaxis_title="Accuracy",
        yaxis=dict(tickformat=".0%"),
        template="plotly_white",
        height=400,
        hovermode="x unified",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
    )
    
    return dcc.Graph(figure=fig, config={"displayModeBar": False})


def accuracy_difference_by_league_chart(diff_df: pd.DataFrame) -> dcc.Graph:
    """
    Create cumulative accuracy difference chart (Our - Book) with separate lines for each league.
    
    Args:
        diff_df: DataFrame with 'commence_time', 'league', and 'accuracy_diff' columns
    """
    fig = go.Figure()
    
    if not diff_df.empty:
        # Get unique leagues
        leagues = diff_df["league"].unique()
        
        for league in leagues:
            league_df = diff_df[diff_df["league"] == league]
            plot_df = _extend_series_to_now(league_df, "commence_time")
            fig.add_trace(
                go.Scatter(
                    x=plot_df["commence_time"],
                    y=plot_df["accuracy_diff"],
                    mode="lines+markers",
                    name=league,
                    line=dict(width=2.5),
                    marker=dict(size=6),
                    hovertemplate="Date: %{x}<br>Advantage: %{y:+.1%}<extra></extra>",
                )
            )
            
        # Add zero line to indicate parity
        fig.add_hline(
            y=0, 
            line_dash="dash", 
            line_color="gray",
            annotation_text="Parity (0%)",
            annotation_position="bottom right"
        )
    
    fig.update_layout(
        title="Accuracy Advantage vs Books (Cumulative)",
        xaxis_title="Date",
        yaxis_title="Accuracy Difference",
        yaxis=dict(tickformat="+.0%"),
        template="plotly_white",
        height=400,
        hovermode="x unified",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
    )
    
    return dcc.Graph(figure=fig, config={"displayModeBar": False})


def roi_by_league_chart(
    performance_by_league: dict[str, pd.DataFrame]
) -> dcc.Graph:
    """
    Create ROI chart with separate lines for each league.
    
    Args:
        performance_by_league: Dict mapping league names to their performance DataFrames
                              Each DataFrame should have 'date' and 'roi' columns
    """
    fig = go.Figure()
    
    # Add a line for each league
    for league, perf_df in performance_by_league.items():
        if perf_df is not None and not perf_df.empty and "roi" in perf_df.columns:
            plot_df = _extend_series_to_now(perf_df, "date")
            fig.add_trace(
                go.Scatter(
                    x=plot_df["date"],
                    y=plot_df["roi"],
                    mode="lines+markers",
                    name=league,
                    line=dict(width=2.5),
                    marker=dict(size=6),
                )
            )
    
    fig.update_layout(
        title="ROI by League",
        xaxis_title="Date",
        yaxis_title="ROI (%)",
        template="plotly_white",
        height=400,
        hovermode="x unified",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
    )
    
    return dcc.Graph(figure=fig, config={"displayModeBar": False})


def edge_distribution_chart(predictions_df: pd.DataFrame) -> dcc.Graph:
    """
    Create a histogram showing the distribution of prediction edges.
    
    Args:
        predictions_df: DataFrame with prediction data including edge values
    """
    fig = go.Figure()
    
    if not predictions_df.empty:
        # Check if this is game-level data (has home_edge/away_edge) or bet-level data (has edge)
        edges = []
        
        if "edge" in predictions_df.columns:
            # Bet-level data - use edge column directly
            edges = predictions_df["edge"].dropna().tolist()
        elif "home_edge" in predictions_df.columns and "away_edge" in predictions_df.columns:
            # Game-level data - extract both home and away edges
            home_edges = predictions_df["home_edge"].dropna().tolist()
            away_edges = predictions_df["away_edge"].dropna().tolist()
            edges = home_edges + away_edges
            
            # Also check for draw_edge if it exists (soccer)
            if "draw_edge" in predictions_df.columns:
                draw_edges = predictions_df["draw_edge"].dropna().tolist()
                edges.extend(draw_edges)
        
        if edges:
            fig.add_trace(
                go.Histogram(
                    x=edges,
                    nbinsx=30,
                    marker=dict(
                        color="#636EFA",
                        line=dict(color="#ffffff", width=1)
                    ),
                    name="Edge Distribution",
                )
            )
            
            # Add a vertical line at 0 to show positive vs negative edge
            fig.add_vline(
                x=0, 
                line_dash="dash", 
                line_color="red",
                annotation_text="No Edge",
                annotation_position="top"
            )
    
    fig.update_layout(
        title="Prediction Edge Distribution",
        xaxis_title="Edge",
        yaxis_title="Count",
        template="plotly_white",
        height=350,
        showlegend=False,
        xaxis=dict(tickformat=".1%"),
    )
    
    return dcc.Graph(figure=fig, config={"displayModeBar": False})


def roi_over_time_chart(performance_df: pd.DataFrame) -> dcc.Graph:
    fig = go.Figure()

    if not performance_df.empty:
        plot_df = _extend_series_to_now(performance_df, "date")
        fig.add_trace(
            go.Scatter(
                x=plot_df["date"],
                y=plot_df["roi"],
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
        plot_df = _extend_series_to_now(performance_df, "date")
        fig.add_trace(
            go.Scatter(
                x=plot_df["date"],
                y=plot_df["win_rate"],
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


def cumulative_accuracy_by_model_chart(accuracy_df: pd.DataFrame) -> dcc.Graph:
    fig = go.Figure()

    if not accuracy_df.empty:
        models = accuracy_df["model"].unique()
        colors = {"Ensemble": "#636EFA", "Random Forest": "#EF553B", "Gradient Boosting": "#00CC96"}
        
        for model in models:
            model_data = accuracy_df[accuracy_df["model"] == model].copy()
            plot_df = _extend_series_to_now(model_data, "date")
            
            fig.add_trace(
                go.Scatter(
                    x=plot_df["date"],
                    y=plot_df["accuracy"],
                    mode="lines",
                    name=model,
                    line=dict(width=3, color=colors.get(model)),
                )
            )

    fig.update_layout(
        title="Cumulative Accuracy by Model",
        xaxis_title="Date",
        yaxis_title="Accuracy",
        yaxis_tickformat=".1%",
        template="plotly_white",
        height=350,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
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
            columns=["commence_time", "league", "team", "opponent", "moneyline", "predicted_prob", "implied_prob", "edge"]
        )

    df["commence_time"] = df["commence_time"].apply(_format_datetime)
    if "league" not in df.columns:
        df["league"] = ""
    else:
        df["league"] = df["league"].fillna("").astype(str)
    df["predicted_prob"] = df["predicted_prob"].apply(lambda x: f"{x:.3f}" if pd.notna(x) else "")
    df["implied_prob"] = df["implied_prob"].apply(lambda x: f"{x:.3f}" if pd.notna(x) else "")
    df["edge"] = df["edge"].apply(lambda x: f"{x:.3f}" if pd.notna(x) else "")

    columns = [
        {"name": "Commence", "id": "commence_time"},
        {"name": "League", "id": "league"},
        {"name": "Team", "id": "team"},
        {"name": "Opponent", "id": "opponent"},
        {"name": "Moneyline", "id": "moneyline"},
        {"name": "Pred Prob", "id": "predicted_prob"},
        {"name": "Impl Prob", "id": "implied_prob"},
        {"name": "Edge", "id": "edge"},
    ]

    return dash_table.DataTable(
        id="recommended-bets-table-datatable",
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

    if "league" not in df.columns:
        df["league"] = ""
    else:
        df["league"] = df["league"].fillna("").astype(str)
    
    df["edge"] = df["edge"].apply(lambda x: f"{x:.3f}" if pd.notna(x) else "")
    
    # Create winner column - show which team won
    # Must run this BEFORE overwriting 'won' with string status
    def get_winner(row):
        # Check for NaN (Ongoing)
        if pd.isna(row.get("won")):
            return ""
        if row.get("won") is True:
            return row.get("team", "—")
        elif row.get("won") is False:
            return row.get("opponent", "—")
        return "" # Should not happen for bools
    
    df["winner"] = df.apply(get_winner, axis=1)

    def format_result(row):
        if row["won"] is True:
            return "Win"
        elif row["won"] is False:
            return "Loss"
        elif pd.notna(row.get("result")):
            # If we have a result (game is final) but won is None, it's a Push (or at least finished)
            return "Push"
        else:
            return "Ongoing"

    df["won"] = df.apply(format_result, axis=1)

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

    columns = [
        {"name": "Start Time", "id": "start_time"},
        {"name": "League", "id": "league"},
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


def overunder_recommended_table(totals_df: pd.DataFrame) -> dash_table.DataTable:
    df = totals_df.copy()
    if df.empty:
        df = pd.DataFrame(
            columns=[
                "game_id",
                "commence_time",
                "league",
                "home_team",
                "away_team",
                "description",
                "total_line",
                "moneyline",
                "predicted_prob",
                "edge",
                "side",
            ]
        )

    if "commence_time" in df.columns:
        if not pd.api.types.is_datetime64_any_dtype(df["commence_time"]):
            df["commence_time"] = pd.to_datetime(df["commence_time"], errors="coerce")
        df = df.sort_values("commence_time", ascending=True, na_position="last")
        df["commence_time"] = df["commence_time"].apply(_format_datetime)
    for col in ("home_team", "away_team"):
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str)
    df["moneyline_value"] = df["moneyline"]
    df["total_line_value"] = df["total_line"]
    df["moneyline"] = df["moneyline"].apply(lambda x: f"{x:+.0f}" if pd.notna(x) else "")
    df["total_line"] = df["total_line"].apply(lambda x: f"{x:.1f}" if pd.notna(x) else "")
    df["predicted_prob"] = df["predicted_prob"].apply(lambda x: f"{x:.3f}" if pd.notna(x) else "")
    df["edge"] = df["edge"].apply(lambda x: f"{x:.3f}" if pd.notna(x) else "")
    if "predicted_total_points" in df.columns:
        df["predicted_total_points"] = df["predicted_total_points"].apply(
            lambda x: f"{x:.1f}" if pd.notna(x) else ""
        )
    else:
        df["predicted_total_points"] = ""

    columns = [
        {"name": "Commence", "id": "commence_time"},
        {"name": "League", "id": "league"},
        {"name": "Home", "id": "home_team"},
        {"name": "Away", "id": "away_team"},
        {"name": "Pick", "id": "description"},
        {"name": "Total Line", "id": "total_line"},
        {"name": "Pred Total", "id": "predicted_total_points"},
        {"name": "Odds", "id": "moneyline"},
        {"name": "Pred Prob", "id": "predicted_prob"},
        {"name": "Edge", "id": "edge"},
    ]

    return dash_table.DataTable(
        id="overunder-recommended-table-datatable",
        columns=columns,
        data=df.to_dict("records"),
        sort_action="native",
        filter_action="native",
        style_table={"overflowX": "auto"},
        style_cell={"padding": "0.5rem", "textAlign": "center"},
        style_header={"fontWeight": "bold"},
    )


def overunder_completed_table(totals_df: pd.DataFrame) -> dash_table.DataTable:
    df = totals_df.copy()
    if df.empty:
        df = pd.DataFrame(
            columns=[
                "commence_time",
                "league",
                "home_team",
                "away_team",
                "description",
                "total_line",
                "moneyline",
                "predicted_prob",
                "edge",
                "won",
                "profit",
                "total_points",
            ]
        )

    if "commence_time" in df.columns:
        if not pd.api.types.is_datetime64_any_dtype(df["commence_time"]):
            df["commence_time"] = pd.to_datetime(df["commence_time"], errors="coerce")
        df = df.sort_values("commence_time", ascending=False, na_position="last")
        df["commence_time"] = df["commence_time"].apply(_format_datetime)
    for col in ("home_team", "away_team"):
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str)
    df["total_line"] = df["total_line"].apply(lambda x: f"{x:.1f}" if pd.notna(x) else "")
    df["moneyline"] = df["moneyline"].apply(lambda x: f"{x:+.0f}" if pd.notna(x) else "")
    df["predicted_prob"] = df["predicted_prob"].apply(lambda x: f"{x:.3f}" if pd.notna(x) else "")
    df["edge"] = df["edge"].apply(lambda x: f"{x:.3f}" if pd.notna(x) else "")
    if "predicted_total_points" in df.columns:
        df["predicted_total_points"] = df["predicted_total_points"].apply(
            lambda x: f"{x:.1f}" if pd.notna(x) else ""
        )
    else:
        df["predicted_total_points"] = ""
    
    def format_result(row):
        if row["won"] is True:
            return "Win"
        elif row["won"] is False:
            return "Loss"
        elif pd.notna(row.get("result")):
            # If we have a result (game is final) but won is None, it's a Push
            return "Push"
        else:
            return "Ongoing"

    df["won"] = df.apply(format_result, axis=1)
    df["profit"] = df["profit"].apply(lambda x: _format_currency(x, 2) if pd.notna(x) else "")

    columns = [
        {"name": "Commence", "id": "commence_time"},
        {"name": "League", "id": "league"},
        {"name": "Home", "id": "home_team"},
        {"name": "Away", "id": "away_team"},
        {"name": "Pick", "id": "description"},
        {"name": "Total Line", "id": "total_line"},
        {"name": "Pred Total", "id": "predicted_total_points"},
        {"name": "Total Points", "id": "total_points"},
        {"name": "Odds", "id": "moneyline"},
        {"name": "Pred Prob", "id": "predicted_prob"},
        {"name": "Edge", "id": "edge"},
        {"name": "Result", "id": "won"},
        {"name": "Profit/Loss", "id": "profit"},
    ]

    return dash_table.DataTable(
        id="overunder-completed-table-datatable",
        columns=columns,
        data=df.to_dict("records"),
        sort_action="native",
        filter_action="native",
        style_table={"overflowX": "auto"},
        style_cell={"padding": "0.5rem", "textAlign": "center"},
        style_header={"fontWeight": "bold"},
        style_data_conditional=[
            {
                "if": {"filter_query": "{profit} = $0.00"},
                "backgroundColor": "#e0e0e0",
                "color": "#666666",
            },
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


def totals_detail_table(
    book_rows: pd.DataFrame,
    *,
    home_team: Optional[str],
    away_team: Optional[str],
) -> dash_table.DataTable | html.Div:
    if book_rows.empty:
        return empty_state("No sportsbook totals available for this matchup.")

    df = book_rows.copy()
    if "book" in df.columns:
        df = df[~df["book"].astype(str).str.contains("kaggle", case=False, na=False)].copy()
    if df.empty:
        return empty_state("No sportsbook totals available for this matchup.")

    df["outcome"] = df["outcome"].astype(str).str.title()
    df["line_display"] = df["line"].apply(lambda x: f"{x:.1f}" if pd.notna(x) else "")
    df["moneyline_display"] = df["moneyline"].apply(lambda x: f"{x:+.0f}" if pd.notna(x) else "")
    df = df.sort_values(["book", "outcome"])

    columns = [
        {"name": "Sportsbook", "id": "book"},
        {"name": "Outcome", "id": "outcome"},
        {"name": "Line", "id": "line_display"},
        {"name": "Price", "id": "moneyline_display"},
    ]

    return dash_table.DataTable(
        columns=columns,
        data=df[["book", "outcome", "line_display", "moneyline_display"]].to_dict("records"),
        sort_action="native",
        style_table={"overflowX": "auto"},
        style_cell={"padding": "0.5rem", "textAlign": "center"},
        style_header={"fontWeight": "bold"},
    )


def model_comparison_table(comparison_df: pd.DataFrame) -> dash_table.DataTable:
    """Table showing side-by-side comparison of model predictions."""
    if comparison_df.empty:
        return empty_state("No comparison data available.")

    df = comparison_df.copy()
    
    # Format common columns
    if "commence_time" in df.columns:
        df["commence_display"] = df["commence_time"].apply(_format_datetime)
    else:
        df["commence_display"] = ""
        
    df["matchup"] = df.apply(
        lambda row: f"{row.get('away_team') or '?'} @ {row.get('home_team') or '?'}", axis=1
    )
    
    # Helper to format model prediction
    def _format_pred(row, model_prefix):
        home_prob = row.get(f"{model_prefix}_home_prob")
        away_prob = row.get(f"{model_prefix}_away_prob")
        
        if pd.isna(home_prob) or pd.isna(away_prob):
            return "—"
            
        if home_prob > away_prob:
            winner = row.get("home_team")
            prob = home_prob
        else:
            winner = row.get("away_team")
            prob = away_prob
            
        return f"{winner} ({prob:.1%})"

    # Format predictions for each model
    models = ["ensemble", "random_forest", "gradient_boosting"]
    for model in models:
        df[f"{model}_display"] = df.apply(lambda row: _format_pred(row, model), axis=1)

    # Format result
    def _format_result(row):
        res = row.get("result")
        if pd.isna(res):
            return "Pending"
        if res == "home":
            return f"{row.get('home_team')} Win"
        elif res == "away":
            return f"{row.get('away_team')} Win"
        return str(res)

    df["result_display"] = df.apply(_format_result, axis=1)

    columns = [
        {"name": "Time", "id": "commence_display"},
        {"name": "Matchup", "id": "matchup"},
        {"name": "Ensemble", "id": "ensemble_display"},
        {"name": "Random Forest", "id": "random_forest_display"},
        {"name": "Gradient Boosting", "id": "gradient_boosting_display"},
        {"name": "Result", "id": "result_display"},
    ]

    return dash_table.DataTable(
        columns=columns,
        data=df.to_dict("records"),
        sort_action="native",
        filter_action="native",
        style_table={"overflowX": "auto"},
        style_cell={"padding": "0.5rem", "textAlign": "center"},
        style_header={"fontWeight": "bold"},
        style_data_conditional=[
            {
                "if": {"filter_query": "{result_display} != Pending"},
                "backgroundColor": "#f8f9fa",
            },
        ],
    )


__all__ = [
    "metric_card",
    "summary_cards",
    "bankroll_cards",
    "cumulative_profit_chart",
    "multi_model_cumulative_profit_chart",
    "cumulative_profit_by_league_chart",
    "edge_distribution_chart",
    "roi_over_time_chart",
    "win_rate_over_time_chart",
    "performance_by_period_chart",
    "performance_by_threshold_chart",
    "performance_by_threshold_table",
    "recent_predictions_table",
    "recommended_bets_table",
    "completed_bets_table",
    "overunder_recommended_table",
    "overunder_completed_table",
    "calendar_table",
    "empty_state",
    "prediction_summary",
    "prediction_comparison_table",
    "moneyline_detail_table",
    "totals_detail_table",
    "model_comparison_table",
    "raw_data_table",
]


def raw_data_table(df: pd.DataFrame, *, page_size: int = 50) -> dash_table.DataTable:
    """
    Generic table to display raw dataframe content.
    Auto-generates columns based on the dataframe.
    """
    if df.empty:
        return empty_state("No data returned from database.")

    # Create columns config
    columns = [{"name": i, "id": i} for i in df.columns]

    # Format datetime columns for readability if possible, else stringify
    data = df.copy()
    for col in data.columns:
        if pd.api.types.is_datetime64_any_dtype(data[col]):
            data[col] = data[col].apply(str)

    return dash_table.DataTable(
        data=data.to_dict("records"),
        columns=columns,
        page_size=page_size,
        sort_action="native",
        filter_action="native",
        style_table={"overflowX": "auto"},
        style_cell={"textAlign": "left", "padding": "0.5rem", "minWidth": "100px"},
        style_header={"fontWeight": "bold", "backgroundColor": "#f8f9fa"},
    )

