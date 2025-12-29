"""
Insight cards component for actionable flight recommendations.

Replaces traditional KPI metrics with user-centric insights
that help travelers make better booking decisions.
"""

from typing import Optional

import pandas as pd
import streamlit as st

from dashboard.services.city_service import get_city_name
from dashboard.services.insights_service import (
    get_best_deal,
    get_cheapest_day_insight,
    get_price_range_context,
)

__all__ = ["render_insight_cards"]


def render_insight_cards(df: pd.DataFrame, origin: str) -> Optional[str]:
    """
    Render actionable insight cards for the dashboard.

    Displays smart recommendations instead of raw metrics,
    helping users understand the best opportunities at a glance.

    Args:
        df: Filtered flight DataFrame.
        origin: Selected origin IATA code.

    Returns:
        Selected destination IATA code if user clicks a card, None otherwise.
    """
    selected_dest: Optional[str] = None

    # Get insights
    best_deal = get_best_deal(df, origin)
    day_insight = get_cheapest_day_insight(df, origin)

    # Calculate additional context
    origin_df = df[df["origin_iata"] == origin]
    num_destinations = origin_df["dest_iata"].nunique() if not origin_df.empty else 0
    num_airlines = origin_df["carrier_name"].nunique() if not origin_df.empty else 0

    # Create 4-column layout for insight cards
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        _render_best_deal_card(best_deal)

    with col2:
        _render_day_tip_card(day_insight)

    with col3:
        _render_destinations_card(num_destinations, origin)

    with col4:
        _render_airlines_card(num_airlines)

    return selected_dest


def _render_best_deal_card(best_deal: Optional[dict]) -> None:
    """
    Render the best deal insight card.

    Args:
        best_deal: Best deal insight data or None.
    """
    if best_deal:
        city = best_deal["city_name"]
        price = best_deal["price"]
        savings = best_deal["savings_vs_avg"]

        # Show savings context if significant
        if savings > 15:
            savings_text = f"{savings:.0f}% below avg"
        else:
            savings_text = "Good price"

        st.metric(
            label="Best Deal",
            value=f"{city} €{price:.0f}",
            delta=savings_text,
            delta_color="normal",
        )
    else:
        st.metric(label="Best Deal", value="No data", delta=None)


def _render_day_tip_card(day_insight: Optional[dict]) -> None:
    """
    Render the cheapest day tip card.

    Args:
        day_insight: Day insight data or None.
    """
    if day_insight and day_insight["savings_percent"] > 5:
        day = day_insight["cheapest_day"]
        savings = day_insight["savings_percent"]

        st.metric(
            label="Day Tip",
            value=f"Fly {day}",
            delta=f"Save {savings:.0f}%",
            delta_color="normal",
        )
    elif day_insight:
        st.metric(
            label="Day Tip",
            value="Any day",
            delta="Prices stable",
            delta_color="off",
        )
    else:
        st.metric(label="Day Tip", value="No data", delta=None)


def _render_destinations_card(num_destinations: int, origin: str) -> None:
    """
    Render the destinations count card.

    Args:
        num_destinations: Number of available destinations.
        origin: Origin IATA code for context.
    """
    origin_city = get_city_name(origin)

    # Provide context based on number of routes
    if num_destinations > 20:
        context = "Great coverage"
    elif num_destinations > 10:
        context = "Good coverage"
    elif num_destinations > 0:
        context = "Some options"
    else:
        context = "No routes"

    st.metric(
        label=f"From {origin_city}",
        value=f"{num_destinations} routes",
        delta=context,
        delta_color="off",
    )


def _render_airlines_card(num_airlines: int) -> None:
    """
    Render the airlines count card.

    Args:
        num_airlines: Number of airlines serving the origin.
    """
    if num_airlines > 5:
        competition = "High competition"
    elif num_airlines > 2:
        competition = "Good options"
    else:
        competition = "Limited"

    st.metric(
        label="Airlines",
        value=f"{num_airlines} carriers",
        delta=competition,
        delta_color="off",
    )


def render_price_alert(
    df: pd.DataFrame, origin: str, dest: str, current_price: float
) -> None:
    """
    Render a price alert banner if current price is exceptional.

    Args:
        df: Flight DataFrame for comparison.
        origin: Origin IATA code.
        dest: Destination IATA code.
        current_price: The price to evaluate.
    """
    context = get_price_range_context(current_price, df, origin, dest)

    if context == "Lowest price!":
        st.success(f"🎉 **Lowest price found!** €{current_price:.0f} is the best we've seen for this route.")
    elif "below avg" in context:
        st.info(f"💡 **Good deal!** This price is {context}.")
