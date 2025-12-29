"""
Dashboard module for Flight Intelligence visualization.

This module provides the main dashboard application for analyzing
flight data collected by the scanner.

Usage:
    from dashboard import run_dashboard
    run_dashboard()
"""

import streamlit as st

from dashboard.pages.main_view import render_main_view
from dashboard.services.data_service import get_cached_flight_data


def run_dashboard() -> None:
    """
    Main dashboard application entry point.

    Loads data and renders the route-centric main view.
    """
    df = get_cached_flight_data()

    if df.empty:
        st.error("Database is empty. Run 'run_scanner.py' first!")
        st.stop()

    render_main_view(df)


__all__ = ["run_dashboard"]
