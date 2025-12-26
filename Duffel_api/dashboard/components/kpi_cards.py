"""
KPI cards component for dashboard header metrics.

Provides functions for calculating and displaying KPI metrics.
"""

import pandas as pd
import streamlit as st

from dashboard.types import KPIMetrics


def calculate_kpis(df: pd.DataFrame) -> KPIMetrics:
    """
    Calculate KPI metrics from filtered data.

    Args:
        df: Filtered flight DataFrame.

    Returns:
        KPIMetrics with calculated values.
    """
    if df.empty:
        return KPIMetrics(
            lowest_price=None,
            average_price=None,
            currency=None,
            destination_count=0,
            best_value_airline=None,
        )

    lowest_price = df["price_amount"].min()
    average_price = df["price_amount"].mean()
    currency = df.iloc[0]["currency"]
    destination_count = df["dest_iata"].nunique()

    # Find best value airline (most flights below average price)
    cheap_flights = df[df["price_amount"] < average_price]
    best_value_airline = None
    if not cheap_flights.empty:
        best_value_airline = cheap_flights["carrier_name"].mode()[0]

    return KPIMetrics(
        lowest_price=lowest_price,
        average_price=average_price,
        currency=currency,
        destination_count=destination_count,
        best_value_airline=best_value_airline,
    )


def render_kpi_cards(kpis: KPIMetrics) -> None:
    """
    Render the KPI cards row.

    Args:
        kpis: Calculated KPI metrics.
    """
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        if kpis["lowest_price"] is not None:
            st.metric(
                "Lowest Price", f"{kpis['lowest_price']:.2f} {kpis['currency']}"
            )
        else:
            st.metric("Lowest Price", "-")

    with col2:
        if kpis["average_price"] is not None:
            st.metric(
                "Average Price", f"{kpis['average_price']:.0f} {kpis['currency']}"
            )
        else:
            st.metric("Average Price", "-")

    with col3:
        st.metric("Available Destinations", f"{kpis['destination_count']}")

    with col4:
        if kpis["best_value_airline"]:
            st.metric("Best Value Airline", kpis["best_value_airline"])
        else:
            st.metric("Best Value Airline", "-")
