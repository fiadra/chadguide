"""
Flight Intelligence Dashboard - Entry Point.

A Streamlit-based interactive dashboard for visualizing and analyzing
flight data collected by the flight scanner. Provides price analysis,
comfort metrics, route mapping, and detailed data exploration.

Usage:
    streamlit run dashboard.py
"""

import logging

from dashboard import run_dashboard
from dashboard.components.styles import apply_custom_css, apply_page_config

# Configure logging for console output
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

# Apply Streamlit page configuration (must be first st call)
apply_page_config()
apply_custom_css()

# Run the main dashboard
run_dashboard()
