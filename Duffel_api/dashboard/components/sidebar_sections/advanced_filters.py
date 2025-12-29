"""
Advanced filters component for sidebar.

Provides additional filters in a collapsible expander.
"""

from datetime import date
from typing import List, Tuple

import pandas as pd
import streamlit as st

from dashboard.types import AdvancedFilterState


def render_advanced_filters(
    df: pd.DataFrame, origin: str
) -> AdvancedFilterState:
    """
    Render advanced filters in a collapsible expander.

    Args:
        df: Full flight DataFrame for populating options.
        origin: Selected origin airport (to filter destinations).

    Returns:
        AdvancedFilterState with selected values.
    """
    # Get available options based on origin
    origin_df = df[df["origin_iata"] == origin]

    available_destinations: List[str] = sorted(
        origin_df["dest_iata"].unique().tolist()
    )
    available_airlines: List[str] = sorted(df["carrier_name"].unique().tolist())

    min_date: date = df["departure_date"].min().date()
    max_date: date = df["departure_date"].max().date()

    with st.sidebar.expander("Advanced Filters", expanded=False):
        # Destination filter
        selected_destinations: List[str] = st.multiselect(
            "Destinations:",
            available_destinations,
            default=available_destinations,
            help="Filter by specific destinations (default: all)",
        )
        # If empty, use all
        if not selected_destinations:
            selected_destinations = available_destinations

        # Airline filter
        selected_airlines: List[str] = st.multiselect(
            "Airlines:",
            available_airlines,
            default=available_airlines,
            help="Filter by specific airlines (default: all)",
        )
        # If empty, use all
        if not selected_airlines:
            selected_airlines = available_airlines

        # Date range
        selected_dates = st.date_input(
            "Date range:",
            value=[min_date, max_date],
            min_value=min_date,
            max_value=max_date,
            help="Filter by departure date range",
        )

        # Ensure valid date range tuple
        if isinstance(selected_dates, tuple) and len(selected_dates) == 2:
            date_range: Tuple[date, date] = selected_dates
        else:
            date_range = (min_date, max_date)

        st.markdown("---")
        st.markdown("**Amenities**")

        # Comfort filters
        require_wifi: bool = st.checkbox("WiFi required")
        require_baggage: bool = st.checkbox("Checked baggage included")

    return AdvancedFilterState(
        destinations=selected_destinations,
        airlines=selected_airlines,
        date_range=date_range,
        require_wifi=require_wifi,
        require_baggage=require_baggage,
    )
