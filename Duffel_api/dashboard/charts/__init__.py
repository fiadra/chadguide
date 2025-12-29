"""
Charts module for data visualization components.

Provides Plotly chart creation and rendering functions for the dashboard.
"""

from dashboard.charts.map_chart import create_route_map_figure, render_route_map
from dashboard.charts.price_charts import (
    create_price_by_day_figure,
    create_price_calendar_figure,
    create_price_distribution_figure,
    render_price_by_day_chart,
    render_price_calendar_chart,
    render_price_distribution_chart,
)
from dashboard.charts.quality_charts import (
    create_eco_chart_figure,
    create_value_matrix_figure,
    render_eco_chart,
    render_value_matrix_chart,
)

__all__ = [
    # Price charts
    "create_price_by_day_figure",
    "render_price_by_day_chart",
    "create_price_distribution_figure",
    "render_price_distribution_chart",
    "create_price_calendar_figure",
    "render_price_calendar_chart",
    # Quality charts
    "create_value_matrix_figure",
    "render_value_matrix_chart",
    "create_eco_chart_figure",
    "render_eco_chart",
    # Map
    "create_route_map_figure",
    "render_route_map",
]
