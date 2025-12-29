"""
Price-related chart components.

Provides chart functions for price analysis visualizations.
"""

from typing import Optional

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from dashboard.charts.base import is_empty, render_chart
from dashboard.config import DashboardConfig


def create_price_by_day_figure(df: pd.DataFrame) -> Optional[go.Figure]:
    """
    Create a bar chart showing average prices by day of week.

    Args:
        df: Filtered flight DataFrame.

    Returns:
        Plotly Figure object, or None if data is empty.
    """
    if is_empty(df):
        return None

    config = DashboardConfig.charts

    daily_avg = (
        df.groupby(["day_of_week", "day_num"])["price_amount"].mean().reset_index()
    )
    daily_avg = daily_avg.sort_values("day_num")

    return px.bar(
        daily_avg,
        x="day_of_week",
        y="price_amount",
        text_auto=".0f",
        color="price_amount",
        color_continuous_scale=config.price_color_scale,
        labels={"price_amount": "Average price", "day_of_week": "Day"},
        height=config.standard_height,
    )


def render_price_by_day_chart(df: pd.DataFrame) -> None:
    """
    Render price by day chart to Streamlit.

    Args:
        df: Filtered flight DataFrame with day_of_week and price_amount columns.
    """
    fig = create_price_by_day_figure(df)
    if fig:
        render_chart(fig)


def create_price_distribution_figure(df: pd.DataFrame) -> Optional[go.Figure]:
    """
    Create a box plot showing price distribution by airline.

    Args:
        df: Filtered flight DataFrame.

    Returns:
        Plotly Figure object, or None if data is empty.
    """
    if is_empty(df):
        return None

    config = DashboardConfig.charts

    return px.box(
        df,
        x="carrier_code",
        y="price_amount",
        color="carrier_code",
        points="all",
        height=config.standard_height,
        title="Price stability",
    )


def render_price_distribution_chart(df: pd.DataFrame) -> None:
    """
    Render price distribution chart to Streamlit.

    Args:
        df: Filtered flight DataFrame with carrier_code and price_amount columns.
    """
    fig = create_price_distribution_figure(df)
    if fig:
        render_chart(fig)


def create_price_calendar_figure(df: pd.DataFrame) -> Optional[go.Figure]:
    """
    Create a line chart showing minimum daily prices over time.

    Args:
        df: Filtered flight DataFrame.

    Returns:
        Plotly Figure object, or None if data is empty.
    """
    if is_empty(df):
        return None

    daily_trend = df.groupby("departure_date")["price_amount"].min().reset_index()

    return px.line(
        daily_trend,
        x="departure_date",
        y="price_amount",
        markers=True,
        title="Lowest available price per day",
    )


def render_price_calendar_chart(df: pd.DataFrame) -> None:
    """
    Render price calendar chart to Streamlit.

    Args:
        df: Filtered flight DataFrame with departure_date and price_amount columns.
    """
    fig = create_price_calendar_figure(df)
    if fig:
        render_chart(fig)
