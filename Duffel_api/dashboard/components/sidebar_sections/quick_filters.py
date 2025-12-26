"""
Quick filters component for sidebar.

Provides essential filters that are always visible,
including budget presets for quick selection.
"""

from typing import List, Optional

import pandas as pd
import streamlit as st

from dashboard.services.city_service import get_city_with_code
from dashboard.types import QuickFilterState

# Budget preset definitions
BUDGET_PRESETS = {
    "Any budget": None,
    "Budget (< €100)": 100,
    "Mid-range (< €200)": 200,
    "Premium (< €400)": 400,
}


def render_quick_filters(df: pd.DataFrame) -> QuickFilterState:
    """
    Render quick filters (always visible in sidebar).

    Includes budget presets for faster selection and
    displays city names for better UX.

    Args:
        df: Full flight DataFrame for populating options.

    Returns:
        QuickFilterState with selected values.
    """
    st.sidebar.header("Filters")

    # Origin selection with city names
    origins: List[str] = sorted(df["origin_iata"].unique().tolist())
    origin_options = {get_city_with_code(code): code for code in origins}

    selected_origin_display: str = st.sidebar.selectbox(
        "Departure from:",
        list(origin_options.keys()),
        help="Select your departure airport",
    )
    selected_origin = origin_options[selected_origin_display]

    st.sidebar.markdown("---")
    st.sidebar.subheader("Quick Filters")

    # Direct flights only
    direct_only: bool = st.sidebar.checkbox(
        "Direct flights only",
        value=True,
        help="Show only non-stop flights",
    )

    # Budget presets (radio buttons for quick selection)
    st.sidebar.markdown("**Budget:**")
    selected_budget = st.sidebar.radio(
        "Budget preset",
        list(BUDGET_PRESETS.keys()),
        index=0,
        label_visibility="collapsed",
        help="Quick budget selection",
    )
    max_price: Optional[int] = BUDGET_PRESETS[selected_budget]

    # Custom price override (collapsible)
    max_price_available = int(df["price_amount"].max()) if not df.empty else 1000
    with st.sidebar.expander("Custom price"):
        custom_price: int = st.slider(
            "Max price (EUR):",
            min_value=0,
            max_value=max_price_available,
            value=max_price if max_price else max_price_available,
            step=10,
            help="Set a custom maximum price",
        )
        if custom_price < max_price_available:
            max_price = custom_price

    # Max duration
    max_duration_available = (
        int(df["duration_minutes"].max()) if "duration_minutes" in df.columns and df["duration_minutes"].notna().any() else 600
    )
    # Round up to nearest hour for better UX
    max_duration_hours = (max_duration_available + 59) // 60

    duration_hours: int = st.sidebar.slider(
        "Max duration (hours):",
        min_value=1,
        max_value=max_duration_hours,
        value=max_duration_hours,
        help="Filter by maximum flight duration",
    )

    max_duration_minutes: Optional[int] = duration_hours * 60
    # If slider is at max, treat as "no filter"
    if duration_hours == max_duration_hours:
        max_duration_minutes = None

    return QuickFilterState(
        origin=selected_origin,
        direct_only=direct_only,
        max_price=max_price,
        max_duration_minutes=max_duration_minutes,
    )
