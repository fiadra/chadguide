"""
Route detail component for airline comparison.

Provides detailed breakdown of airlines serving a specific route.
"""

import pandas as pd
import plotly.express as px
import streamlit as st

from dashboard.services.route_service import (
    format_duration,
    get_route_airline_breakdown,
)


def render_route_detail(df: pd.DataFrame, origin: str, dest: str) -> None:
    """
    Render detailed route analysis with airline comparison.

    Args:
        df: Full flight DataFrame.
        origin: Origin airport IATA code.
        dest: Destination airport IATA code.
    """
    st.markdown("---")
    st.subheader(f"Route Details: {origin} → {dest}")

    # Get airline breakdown
    airline_df = get_route_airline_breakdown(df, origin, dest)

    if airline_df.empty:
        st.info("No airline data available for this route.")
        return

    # Create two columns: table and chart
    col1, col2 = st.columns([3, 2])

    with col1:
        st.markdown("**Airline Comparison**")

        # Prepare display DataFrame
        display_df = airline_df.copy()

        # Format duration
        display_df["duration"] = display_df["min_duration"].apply(format_duration)

        # Format price range
        display_df["price_range"] = display_df.apply(
            lambda r: f"{r['min_price']:.0f} - {r['max_price']:.0f} EUR", axis=1
        )

        # Format baggage
        display_df["baggage"] = display_df["max_baggage"].apply(
            lambda x: f"{int(x)} bag{'s' if x != 1 else ''}" if x > 0 else "No bags"
        )

        # Format WiFi
        display_df["wifi"] = display_df["has_wifi"].apply(
            lambda x: "Yes" if x else "No"
        )

        # Select columns for display
        display_df = display_df[
            ["carrier_name", "price_range", "baggage", "wifi", "duration", "num_offers"]
        ].rename(
            columns={
                "carrier_name": "Airline",
                "price_range": "Price Range",
                "baggage": "Checked Bags",
                "wifi": "WiFi",
                "duration": "Duration",
                "num_offers": "Offers",
            }
        )

        st.dataframe(
            display_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Airline": st.column_config.TextColumn("Airline", width="medium"),
                "Price Range": st.column_config.TextColumn("Price Range", width="medium"),
                "Checked Bags": st.column_config.TextColumn("Bags", width="small"),
                "WiFi": st.column_config.TextColumn("WiFi", width="small"),
                "Duration": st.column_config.TextColumn("Duration", width="small"),
                "Offers": st.column_config.NumberColumn("Offers", width="small"),
            },
        )

    with col2:
        st.markdown("**Price Comparison**")

        # Create horizontal bar chart comparing airlines
        fig = px.bar(
            airline_df,
            y="carrier_name",
            x="avg_price",
            orientation="h",
            color="avg_price",
            color_continuous_scale="RdYlGn_r",
            labels={"carrier_name": "Airline", "avg_price": "Avg Price (EUR)"},
        )
        fig.update_layout(
            showlegend=False,
            height=250,
            margin=dict(l=0, r=0, t=10, b=0),
            coloraxis_showscale=False,
        )
        st.plotly_chart(fig, use_container_width=True)

    # Price calendar for this route
    _render_route_price_calendar(df, origin, dest)


def _render_route_price_calendar(df: pd.DataFrame, origin: str, dest: str) -> None:
    """
    Render price calendar for a specific route.

    Args:
        df: Full flight DataFrame.
        origin: Origin airport IATA code.
        dest: Destination airport IATA code.
    """
    # Filter to route
    route_df = df[(df["origin_iata"] == origin) & (df["dest_iata"] == dest)].copy()

    if route_df.empty:
        return

    # Get min price per day
    daily_prices = (
        route_df.groupby(route_df["departure_date"].dt.date)["price_amount"]
        .min()
        .reset_index()
    )
    daily_prices.columns = ["date", "min_price"]

    st.markdown("**Best Price by Date**")

    fig = px.line(
        daily_prices,
        x="date",
        y="min_price",
        markers=True,
        labels={"date": "Date", "min_price": "Min Price (EUR)"},
    )
    fig.update_layout(
        height=200,
        margin=dict(l=0, r=0, t=10, b=0),
    )
    fig.update_traces(
        line_color="#3b82f6",
        marker_color="#3b82f6",
    )

    st.plotly_chart(fig, use_container_width=True)
