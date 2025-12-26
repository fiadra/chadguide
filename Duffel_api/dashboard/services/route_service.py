"""
Route aggregation service for dashboard.

Provides functions for aggregating flight offers to route level
and comparing airlines on specific routes.
"""

import logging
import re
from typing import Optional

import pandas as pd

from dashboard.types import RouteKPIs

logger = logging.getLogger(__name__)

__all__ = [
    "parse_duration_to_minutes",
    "format_duration",
    "get_route_summary",
    "get_route_airline_breakdown",
    "get_route_kpis",
]


def parse_duration_to_minutes(iso_duration: Optional[str]) -> Optional[int]:
    """
    Convert ISO 8601 duration to minutes.

    Args:
        iso_duration: Duration string like 'PT2H30M' or 'PT1H' or 'PT45M'.

    Returns:
        Total minutes as integer, or None if parsing fails.

    Examples:
        >>> parse_duration_to_minutes('PT2H30M')
        150
        >>> parse_duration_to_minutes('PT1H')
        60
        >>> parse_duration_to_minutes('PT45M')
        45
    """
    if not iso_duration:
        return None

    try:
        hours = 0
        minutes = 0

        # Extract hours
        hours_match = re.search(r"(\d+)H", iso_duration)
        if hours_match:
            hours = int(hours_match.group(1))

        # Extract minutes
        minutes_match = re.search(r"(\d+)M", iso_duration)
        if minutes_match:
            minutes = int(minutes_match.group(1))

        return hours * 60 + minutes
    except (ValueError, TypeError):
        return None


def format_duration(minutes: Optional[int]) -> str:
    """
    Format minutes as human-readable duration.

    Args:
        minutes: Duration in minutes.

    Returns:
        Formatted string like '2h 30m'.
    """
    if minutes is None or pd.isna(minutes):
        return "-"

    hours = int(minutes) // 60
    mins = int(minutes) % 60

    if hours > 0 and mins > 0:
        return f"{hours}h {mins}m"
    elif hours > 0:
        return f"{hours}h"
    else:
        return f"{mins}m"


def get_route_summary(df: pd.DataFrame, origin: str) -> pd.DataFrame:
    """
    Aggregate offers to route level for a given origin.

    Args:
        df: Full flight DataFrame with all offers.
        origin: Origin airport IATA code.

    Returns:
        DataFrame with route-level aggregations:
        - dest_iata: Destination airport code
        - num_airlines: Number of unique airlines serving route
        - num_offers: Total number of offers
        - min_price: Lowest price
        - avg_price: Average price
        - max_price: Highest price
        - min_duration: Shortest flight duration (minutes)
        - avg_duration: Average flight duration (minutes)
    """
    # Filter to origin
    origin_df = df[df["origin_iata"] == origin].copy()
    logger.debug("Filtering routes from origin %s: %d offers", origin, len(origin_df))

    if origin_df.empty:
        logger.debug("No routes found for origin %s", origin)
        return pd.DataFrame()

    # Aggregate by destination
    route_agg = (
        origin_df.groupby("dest_iata")
        .agg(
            num_airlines=("carrier_name", "nunique"),
            num_offers=("price_amount", "count"),
            min_price=("price_amount", "min"),
            avg_price=("price_amount", "mean"),
            max_price=("price_amount", "max"),
            min_duration=("duration_minutes", "min"),
            avg_duration=("duration_minutes", "mean"),
        )
        .reset_index()
    )

    # Round price columns
    route_agg["avg_price"] = route_agg["avg_price"].round(0)
    route_agg["min_price"] = route_agg["min_price"].round(2)
    route_agg["max_price"] = route_agg["max_price"].round(2)

    # Sort by number of airlines (competition) descending
    route_agg = route_agg.sort_values("num_airlines", ascending=False)
    logger.debug("Generated route summary: %d routes from %s", len(route_agg), origin)

    return route_agg


def get_route_airline_breakdown(
    df: pd.DataFrame, origin: str, dest: str
) -> pd.DataFrame:
    """
    Get airline comparison for a specific route.

    Args:
        df: Full flight DataFrame.
        origin: Origin airport IATA code.
        dest: Destination airport IATA code.

    Returns:
        DataFrame with airline-level breakdown:
        - carrier_name: Airline name
        - carrier_code: Airline IATA code
        - min_price: Lowest price
        - max_price: Highest price
        - avg_price: Average price
        - num_offers: Number of offers
        - min_duration: Shortest duration (minutes)
        - has_wifi: Whether any flight has WiFi
        - max_baggage: Maximum checked bags included
    """
    # Filter to route
    route_df = df[(df["origin_iata"] == origin) & (df["dest_iata"] == dest)].copy()
    logger.debug("Airline breakdown for %s->%s: %d offers", origin, dest, len(route_df))

    if route_df.empty:
        logger.debug("No offers found for route %s->%s", origin, dest)
        return pd.DataFrame()

    # Aggregate by airline
    airline_agg = (
        route_df.groupby(["carrier_name", "carrier_code"])
        .agg(
            min_price=("price_amount", "min"),
            max_price=("price_amount", "max"),
            avg_price=("price_amount", "mean"),
            num_offers=("price_amount", "count"),
            min_duration=("duration_minutes", "min"),
            has_wifi=("has_wifi", "any"),
            max_baggage=("baggage_checked", "max"),
        )
        .reset_index()
    )

    # Round prices
    airline_agg["min_price"] = airline_agg["min_price"].round(2)
    airline_agg["max_price"] = airline_agg["max_price"].round(2)
    airline_agg["avg_price"] = airline_agg["avg_price"].round(0)

    # Sort by min price
    airline_agg = airline_agg.sort_values("min_price")

    return airline_agg


def get_route_kpis(route_summary: pd.DataFrame) -> RouteKPIs:
    """
    Calculate route-level KPIs.

    Args:
        route_summary: Route summary DataFrame from get_route_summary().

    Returns:
        RouteKPIs with calculated values:
        - num_routes: Number of available routes
        - avg_route_price: Average price across all routes
        - best_deal_dest: Destination with lowest min price
        - best_deal_price: Lowest price found
    """
    if route_summary.empty:
        return RouteKPIs(
            num_routes=0,
            avg_route_price=None,
            best_deal_dest=None,
            best_deal_price=None,
        )

    best_deal_idx = route_summary["min_price"].idxmin()
    best_deal_row = route_summary.loc[best_deal_idx]

    return RouteKPIs(
        num_routes=len(route_summary),
        avg_route_price=route_summary["avg_price"].mean(),
        best_deal_dest=best_deal_row["dest_iata"],
        best_deal_price=best_deal_row["min_price"],
    )
