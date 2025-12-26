"""
Insights service for smart dashboard recommendations.

Provides functions for calculating actionable insights
from flight data to help users make better decisions.
"""

import logging
from typing import Optional, TypedDict

import pandas as pd

from dashboard.services.city_service import get_city_name

logger = logging.getLogger(__name__)

__all__ = [
    "BestDealInsight",
    "DayInsight",
    "get_best_deal",
    "get_cheapest_day_insight",
    "get_price_range_context",
]


class BestDealInsight(TypedDict):
    """Best deal insight data."""

    dest_iata: str
    city_name: str
    price: float
    savings_vs_avg: float  # Percentage savings vs average


class DayInsight(TypedDict):
    """Day of week insight data."""

    cheapest_day: str
    cheapest_price: float
    expensive_day: str
    expensive_price: float
    savings_percent: float


def get_best_deal(df: pd.DataFrame, origin: str) -> Optional[BestDealInsight]:
    """
    Find the best deal from an origin.

    Args:
        df: Flight DataFrame.
        origin: Origin IATA code.

    Returns:
        BestDealInsight with the cheapest destination, or None if no data.
    """
    origin_df = df[df["origin_iata"] == origin]

    if origin_df.empty:
        return None

    # Find cheapest flight
    cheapest_idx = origin_df["price_amount"].idxmin()
    cheapest = origin_df.loc[cheapest_idx]

    dest_iata = cheapest["dest_iata"]
    price = cheapest["price_amount"]

    # Calculate average price for context
    avg_price = origin_df["price_amount"].mean()
    savings = ((avg_price - price) / avg_price) * 100 if avg_price > 0 else 0

    return BestDealInsight(
        dest_iata=dest_iata,
        city_name=get_city_name(dest_iata),
        price=price,
        savings_vs_avg=savings,
    )


def get_cheapest_day_insight(df: pd.DataFrame, origin: str) -> Optional[DayInsight]:
    """
    Find the cheapest and most expensive days to fly.

    Args:
        df: Flight DataFrame with day_of_week column.
        origin: Origin IATA code.

    Returns:
        DayInsight with day comparison, or None if no data.
    """
    origin_df = df[df["origin_iata"] == origin]

    if origin_df.empty or "day_of_week" not in origin_df.columns:
        return None

    # Calculate average price by day of week
    daily_avg = origin_df.groupby("day_of_week")["price_amount"].mean()

    if daily_avg.empty:
        return None

    cheapest_day = daily_avg.idxmin()
    cheapest_price = daily_avg.min()
    expensive_day = daily_avg.idxmax()
    expensive_price = daily_avg.max()

    savings = (
        ((expensive_price - cheapest_price) / expensive_price) * 100
        if expensive_price > 0
        else 0
    )

    return DayInsight(
        cheapest_day=cheapest_day,
        cheapest_price=cheapest_price,
        expensive_day=expensive_day,
        expensive_price=expensive_price,
        savings_percent=savings,
    )


def get_cheapest_day_for_route(
    df: pd.DataFrame, origin: str, dest: str
) -> Optional[str]:
    """
    Find the cheapest day of week for a specific route.

    Args:
        df: Flight DataFrame.
        origin: Origin IATA code.
        dest: Destination IATA code.

    Returns:
        Day name (e.g., "Tuesday") or None if no data.
    """
    route_df = df[(df["origin_iata"] == origin) & (df["dest_iata"] == dest)]

    if route_df.empty or "day_of_week" not in route_df.columns:
        return None

    daily_avg = route_df.groupby("day_of_week")["price_amount"].mean()

    if daily_avg.empty:
        return None

    return daily_avg.idxmin()


def get_price_range_context(
    price: float, df: pd.DataFrame, origin: str, dest: Optional[str] = None
) -> str:
    """
    Get context for whether a price is good or bad.

    Args:
        price: The price to evaluate.
        df: Flight DataFrame for comparison.
        origin: Origin IATA code.
        dest: Optional destination IATA code for route-specific context.

    Returns:
        Context string like "32% below average" or "15% above average".
    """
    if dest:
        compare_df = df[(df["origin_iata"] == origin) & (df["dest_iata"] == dest)]
    else:
        compare_df = df[df["origin_iata"] == origin]

    if compare_df.empty:
        return ""

    avg_price = compare_df["price_amount"].mean()
    min_price = compare_df["price_amount"].min()
    max_price = compare_df["price_amount"].max()

    if price <= min_price:
        return "Lowest price!"

    if avg_price > 0:
        diff_percent = ((price - avg_price) / avg_price) * 100

        if diff_percent < -20:
            return f"{abs(diff_percent):.0f}% below avg"
        elif diff_percent < 0:
            return "Below average"
        elif diff_percent < 20:
            return "Average price"
        else:
            return f"{diff_percent:.0f}% above avg"

    return ""


def get_route_insights(
    df: pd.DataFrame, origin: str, dest: str
) -> dict:
    """
    Get all insights for a specific route.

    Args:
        df: Flight DataFrame.
        origin: Origin IATA code.
        dest: Destination IATA code.

    Returns:
        Dictionary with various route insights.
    """
    route_df = df[(df["origin_iata"] == origin) & (df["dest_iata"] == dest)]

    if route_df.empty:
        return {}

    insights = {
        "cheapest_day": get_cheapest_day_for_route(df, origin, dest),
        "min_price": route_df["price_amount"].min(),
        "max_price": route_df["price_amount"].max(),
        "avg_price": route_df["price_amount"].mean(),
        "num_airlines": route_df["carrier_name"].nunique(),
        "num_offers": len(route_df),
    }

    # Price volatility
    price_std = route_df["price_amount"].std()
    if insights["avg_price"] > 0:
        volatility = (price_std / insights["avg_price"]) * 100
        if volatility < 10:
            insights["price_stability"] = "Very stable"
        elif volatility < 25:
            insights["price_stability"] = "Stable"
        elif volatility < 50:
            insights["price_stability"] = "Variable"
        else:
            insights["price_stability"] = "Highly variable"

    return insights
