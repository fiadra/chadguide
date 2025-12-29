"""
Base chart utilities and common functions.

Provides helper functions used across all chart components.
"""

from typing import Optional

import pandas as pd
import plotly.graph_objects as go
import streamlit as st


def render_chart(fig: go.Figure, use_container_width: bool = True) -> None:
    """
    Render a Plotly figure in Streamlit.

    Args:
        fig: Plotly figure to render.
        use_container_width: Whether to expand to container width.
    """
    st.plotly_chart(fig, use_container_width=use_container_width)


def is_empty(df: pd.DataFrame) -> bool:
    """
    Check if DataFrame is empty and should skip rendering.

    Args:
        df: DataFrame to check.

    Returns:
        True if DataFrame is empty, False otherwise.
    """
    return df.empty
