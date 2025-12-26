"""
Quality and comfort chart components.

Provides chart functions for comfort and eco analysis.
"""

from typing import Optional

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from dashboard.charts.base import is_empty, render_chart
from dashboard.config import DashboardConfig


def create_value_matrix_figure(df: pd.DataFrame) -> Optional[go.Figure]:
    """
    Create a scatter plot showing price vs comfort score.

    Args:
        df: Filtered flight DataFrame.

    Returns:
        Plotly Figure object, or None if data is empty.
    """
    if is_empty(df):
        return None

    return px.scatter(
        df,
        x="price_amount",
        y="comfort_score",
        color="carrier_name",
        size="seat_pitch_num",
        hover_data=["aircraft_model", "dest_iata"],
        labels={"price_amount": "Price", "comfort_score": "Comfort Score (0-4)"},
    )


def render_value_matrix_chart(df: pd.DataFrame) -> None:
    """
    Render value matrix chart to Streamlit.

    Args:
        df: Filtered flight DataFrame with price, comfort_score, and carrier columns.
    """
    fig = create_value_matrix_figure(df)
    if fig:
        render_chart(fig)


def create_eco_chart_figure(df: pd.DataFrame) -> Optional[go.Figure]:
    """
    Create a horizontal bar chart showing routes with lowest CO2 emissions.

    Args:
        df: Filtered flight DataFrame.

    Returns:
        Plotly Figure object, or None if data is empty.
    """
    if is_empty(df):
        return None

    config = DashboardConfig.charts

    eco_df = (
        df.groupby(["dest_iata", "carrier_name"])["co2_kg"]
        .mean()
        .reset_index()
        .sort_values("co2_kg")
    )

    return px.bar(
        eco_df.head(10),
        x="co2_kg",
        y="carrier_name",
        orientation="h",
        color="co2_kg",
        color_continuous_scale=config.eco_color_scale,
        text_auto=".1f",
        title="Top 10 eco-friendly routes (lowest kg CO2)",
    )


def render_eco_chart(df: pd.DataFrame) -> None:
    """
    Render eco chart to Streamlit.

    Args:
        df: Filtered flight DataFrame with dest_iata, carrier_name, and co2_kg columns.
    """
    fig = create_eco_chart_figure(df)
    if fig:
        render_chart(fig)
