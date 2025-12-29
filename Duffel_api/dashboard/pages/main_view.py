"""
Main dashboard view - Route-centric analysis.

Provides the primary dashboard interface with route summary table
and expandable route details.
"""

import pandas as pd
import streamlit as st

from dashboard.charts.map_chart import render_route_map
from dashboard.components.insight_cards import render_insight_cards
from dashboard.components.route_detail import render_route_detail
from dashboard.components.route_table import render_route_table
from dashboard.components.sidebar_sections import (
    render_advanced_filters,
    render_quick_filters,
)
from dashboard.services.city_service import get_city_name
from dashboard.services.filter_service import apply_filters
from dashboard.types import FilterState


def render_main_view(df: pd.DataFrame) -> None:
    """
    Render the main dashboard view.

    Args:
        df: Full flight DataFrame from data service.
    """
    # Render sidebar filters
    quick_filters = render_quick_filters(df)
    advanced_filters = render_advanced_filters(df, quick_filters["origin"])

    # Combine into full filter state
    filters = FilterState(
        origin=quick_filters["origin"],
        direct_only=quick_filters["direct_only"],
        max_price=quick_filters["max_price"],
        max_duration_minutes=quick_filters["max_duration_minutes"],
        destinations=advanced_filters["destinations"],
        airlines=advanced_filters["airlines"],
        date_range=advanced_filters["date_range"],
        require_wifi=advanced_filters["require_wifi"],
        require_baggage=advanced_filters["require_baggage"],
    )

    # Apply filters
    df_filtered = apply_filters(df, filters)

    # Main content - use city name in title
    origin_city = get_city_name(filters["origin"])
    st.title(f"Flights from {origin_city}")

    if df_filtered.empty:
        st.warning("No flights match your criteria. Try adjusting the filters.")
        return

    # Insight cards (actionable recommendations)
    render_insight_cards(df_filtered, filters["origin"])

    st.markdown("---")

    # Route map (visible by default for visual overview)
    with st.expander("Route Map", expanded=True):
        render_route_map(df_filtered)

    st.markdown("---")

    # Route table with selection
    selected_dest = render_route_table(
        df_filtered, filters["origin"], filters["max_price"]
    )

    # Route detail panel (if a route is selected)
    if selected_dest:
        render_route_detail(df_filtered, filters["origin"], selected_dest)
