"""
Components module for reusable UI elements.

Provides insight cards, route table, and styling components for the dashboard.
"""

from dashboard.components.insight_cards import render_insight_cards
from dashboard.components.kpi_cards import calculate_kpis, render_kpi_cards
from dashboard.components.route_detail import render_route_detail
from dashboard.components.route_table import render_route_kpi_cards, render_route_table
from dashboard.components.styles import apply_custom_css, apply_page_config

__all__ = [
    "apply_page_config",
    "apply_custom_css",
    "calculate_kpis",
    "render_kpi_cards",
    "render_insight_cards",
    "render_route_table",
    "render_route_kpi_cards",
    "render_route_detail",
]
