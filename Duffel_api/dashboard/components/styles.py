"""
Styling module for dashboard appearance.

Provides functions for applying page configuration and custom CSS.
"""

import streamlit as st

from dashboard.config import DashboardConfig


def apply_page_config() -> None:
    """
    Apply Streamlit page configuration.

    Must be called before any other Streamlit commands.
    """
    config = DashboardConfig.page
    st.set_page_config(
        page_title=config.title,
        page_icon=config.icon,
        layout=config.layout,
        initial_sidebar_state=config.sidebar_state,
    )


def apply_custom_css() -> None:
    """
    Apply custom CSS styling to the dashboard.

    Injects custom CSS for metric cards, labels, and values
    based on settings defined in DashboardConfig.style.
    """
    style = DashboardConfig.style

    css = f"""
    <style>
        /* Metric card container styling */
        .metric-card {{
            background-color: {style.metric_card_bg};
            border: 1px solid {style.metric_card_border};
            border-radius: 10px;
            padding: 20px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        }}

        /* Main metric value - city names, prices - aggressive targeting */
        [data-testid="stMetricValue"],
        [data-testid="stMetricValue"] *,
        [data-testid="stMetricValue"] div,
        [data-testid="stMetricValue"] span,
        [data-testid="stMetricValue"] p {{
            font-size: {style.metric_value_size} !important;
            font-weight: 700 !important;
            color: {style.metric_value_color} !important;
        }}

        /* Metric label - "Best Deal", "Day Tip" - aggressive targeting */
        [data-testid="stMetricLabel"],
        [data-testid="stMetricLabel"] *,
        [data-testid="stMetricLabel"] div,
        [data-testid="stMetricLabel"] span,
        [data-testid="stMetricLabel"] p,
        [data-testid="stMetricLabel"] label {{
            font-size: {style.metric_label_size} !important;
            color: {style.metric_label_color} !important;
            font-weight: 600 !important;
        }}

        /* Delta text - "67% below avg", "Save 10%" */
        [data-testid="stMetricDelta"] {{
            font-size: {style.metric_delta_size};
            font-weight: 600;
            min-height: 24px;
        }}

        /* All delta text - force visible color */
        [data-testid="stMetricDelta"],
        [data-testid="stMetricDelta"] *,
        [data-testid="stMetricDelta"] div,
        [data-testid="stMetricDelta"] span {{
            color: {style.metric_delta_color} !important;
            font-weight: 600 !important;
        }}

        /* Hide delta arrows for cleaner look */
        [data-testid="stMetricDelta"] svg {{
            display: none;
        }}

        /* Card-like appearance for each metric - consistent sizing */
        [data-testid="stMetric"] {{
            background-color: {style.metric_card_bg} !important;
            border: 1px solid {style.metric_card_border} !important;
            border-radius: 8px;
            padding: 16px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.08);
            min-height: 110px;
        }}

        /* Ensure all metric columns have equal height */
        [data-testid="stHorizontalBlock"] > div {{
            flex: 1;
        }}
        [data-testid="stHorizontalBlock"] [data-testid="stMetric"] {{
            height: 100%;
        }}
    </style>
    """
    st.markdown(css, unsafe_allow_html=True)
