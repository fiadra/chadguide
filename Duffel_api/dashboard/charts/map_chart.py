"""
Route map visualization component.

Provides the interactive map showing flight routes from origin.
"""

from typing import Optional

import pandas as pd
import plotly.graph_objects as go

from dashboard.charts.base import is_empty, render_chart
from dashboard.config import DashboardConfig


def create_route_map_figure(df: pd.DataFrame) -> Optional[go.Figure]:
    """
    Create an interactive map showing flight routes.

    Args:
        df: Filtered flight DataFrame.

    Returns:
        Plotly Figure object, or None if data is empty.
    """
    if is_empty(df):
        return None

    config = DashboardConfig.charts

    # Aggregate to unique routes
    routes_geo = df.drop_duplicates(
        subset=["origin_iata", "dest_iata", "carrier_code"]
    )

    fig = go.Figure()

    # Draw route lines
    for _, row in routes_geo.iterrows():
        fig.add_trace(
            go.Scattergeo(
                lon=[row["origin_lon"], row["dest_lon"]],
                lat=[row["origin_lat"], row["dest_lat"]],
                mode="lines",
                line=dict(width=config.route_line_width, color=config.route_line_color),
                opacity=config.route_line_opacity,
                hoverinfo="none",
            )
        )

    # Draw destination markers (size based on offer count)
    dest_counts = df["dest_iata"].value_counts()

    for dest_code in routes_geo["dest_iata"].unique():
        dest_data = routes_geo[routes_geo["dest_iata"] == dest_code].iloc[0]
        count = dest_counts[dest_code]

        fig.add_trace(
            go.Scattergeo(
                lon=[dest_data["dest_lon"]],
                lat=[dest_data["dest_lat"]],
                mode="markers",
                marker=dict(
                    size=config.destination_marker_base_size
                    + (count * config.destination_marker_scale),
                    color=config.destination_marker_color,
                    symbol="circle",
                ),
                name=dest_code,
                text=f"{dest_code}: {count} offers",
                hoverinfo="text",
            )
        )

    # Draw origin marker
    origin_row = routes_geo.iloc[0]
    fig.add_trace(
        go.Scattergeo(
            lon=[origin_row["origin_lon"]],
            lat=[origin_row["origin_lat"]],
            mode="markers",
            marker=dict(
                size=config.origin_marker_size,
                color=config.origin_marker_color,
                symbol="star",
            ),
            name=origin_row["origin_iata"],
        )
    )

    fig.update_layout(
        geo=dict(
            scope=config.map_scope,
            projection_type=config.map_projection,
            showland=True,
            landcolor=config.land_color,
            countrycolor=config.country_border_color,
        ),
        margin={"r": 0, "t": 0, "l": 0, "b": 0},
        height=config.map_height,
    )

    return fig


def render_route_map(df: pd.DataFrame) -> None:
    """
    Render route map to Streamlit.

    Args:
        df: Filtered flight DataFrame with origin/destination coordinates and IATA codes.
    """
    fig = create_route_map_figure(df)
    if fig:
        render_chart(fig)
