"""
Route table component for displaying route summaries.

Provides an interactive table showing aggregated route data
with the ability to select routes for detailed analysis.
"""

from typing import Optional

import pandas as pd
import streamlit as st

from dashboard.services.city_service import get_city_with_code
from dashboard.services.route_service import format_duration, get_route_summary


def render_route_table(
    df: pd.DataFrame, origin: str, max_price: Optional[int] = None
) -> Optional[str]:
    """
    Render the route summary table and return selected destination.

    Args:
        df: Full flight DataFrame.
        origin: Selected origin airport.
        max_price: Optional maximum price filter.

    Returns:
        Selected destination IATA code, or None if no selection.
    """
    # Get route summary
    route_summary = get_route_summary(df, origin)

    if route_summary.empty:
        st.info("No routes found for the selected filters.")
        return None

    # Apply price filter if set
    if max_price is not None:
        route_summary = route_summary[route_summary["min_price"] <= max_price]

    if route_summary.empty:
        st.info("No routes match your price criteria.")
        return None

    # Prepare display DataFrame
    display_df = route_summary.copy()

    # Format destination with city name
    display_df["destination"] = display_df["dest_iata"].apply(get_city_with_code)

    # Format duration column
    display_df["duration"] = display_df["min_duration"].apply(format_duration)

    # Create price range string
    display_df["price_range"] = display_df.apply(
        lambda r: f"{r['min_price']:.0f} - {r['max_price']:.0f}", axis=1
    )

    # Select and rename columns for display
    display_df = display_df[
        ["destination", "num_airlines", "min_price", "avg_price", "duration"]
    ].rename(
        columns={
            "destination": "Destination",
            "num_airlines": "Airlines",
            "min_price": "From (EUR)",
            "avg_price": "Avg (EUR)",
            "duration": "Duration",
        }
    )

    st.subheader("Available Routes")
    st.caption("Click a row to see airline breakdown for that route")

    # Use Streamlit dataframe with selection
    event = st.dataframe(
        display_df,
        column_config={
            "Destination": st.column_config.TextColumn("Destination", width="small"),
            "Airlines": st.column_config.NumberColumn(
                "Airlines",
                help="Number of airlines serving this route",
                width="small",
            ),
            "From (EUR)": st.column_config.NumberColumn(
                "From", format="%.0f EUR", width="small"
            ),
            "Avg (EUR)": st.column_config.NumberColumn(
                "Avg", format="%.0f EUR", width="small"
            ),
            "Duration": st.column_config.TextColumn("Duration", width="small"),
        },
        use_container_width=True,
        hide_index=True,
        on_select="rerun",
        selection_mode="single-row",
    )

    # Handle row selection
    if event.selection and event.selection.rows:
        selected_row = event.selection.rows[0]
        selected_dest = route_summary.iloc[selected_row]["dest_iata"]
        return selected_dest

    return None


def render_route_kpi_cards(route_summary: pd.DataFrame) -> None:
    """
    Render KPI cards for route-level metrics.

    Args:
        route_summary: Route summary DataFrame from get_route_summary().
    """
    if route_summary.empty:
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Routes", "0")
        with col2:
            st.metric("Avg Price", "-")
        with col3:
            st.metric("Best Deal", "-")
        with col4:
            st.metric("Airlines", "0")
        return

    # Calculate KPIs
    num_routes = len(route_summary)
    avg_price = route_summary["avg_price"].mean()

    # Best deal
    best_idx = route_summary["min_price"].idxmin()
    best_deal = route_summary.loc[best_idx]
    best_dest_iata = best_deal["dest_iata"]
    best_dest = get_city_with_code(best_dest_iata)
    best_price = best_deal["min_price"]

    # Total unique airlines (sum of unique per route is an overcount, but gives sense of coverage)
    total_airline_coverage = route_summary["num_airlines"].sum()

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Routes Available", f"{num_routes}")

    with col2:
        st.metric("Avg Route Price", f"{avg_price:.0f} EUR")

    with col3:
        st.metric("Best Deal", f"{best_dest}: {best_price:.0f} EUR")

    with col4:
        st.metric("Airline Options", f"{total_airline_coverage}")
