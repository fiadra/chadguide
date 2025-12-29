"""
Filter service module for applying user-selected filters.

Provides functions for filtering flight data based on
user selections from the sidebar.
"""

import pandas as pd

from dashboard.types import FilterOptions, FilterState

__all__ = [
    "apply_filters",
    "get_available_options",
]


def apply_filters(df: pd.DataFrame, filters: FilterState) -> pd.DataFrame:
    """
    Apply user-selected filters to the flight data.

    Args:
        df: The complete flight DataFrame.
        filters: FilterState containing all user selections.

    Returns:
        Filtered DataFrame matching all criteria.
    """
    # Start with origin filter (always required)
    mask = df["origin_iata"] == filters["origin"]

    # Destination filter
    if filters["destinations"]:
        mask &= df["dest_iata"].isin(filters["destinations"])

    # Airline filter
    if filters["airlines"]:
        mask &= df["carrier_name"].isin(filters["airlines"])

    # Price filter (optional)
    if filters["max_price"] is not None:
        mask &= df["price_amount"] <= filters["max_price"]

    # Date range filter
    date_start, date_end = filters["date_range"]
    mask &= (df["departure_date"].dt.date >= date_start) & (
        df["departure_date"].dt.date <= date_end
    )

    # Direct flights filter
    if filters["direct_only"]:
        mask &= df["is_direct"]

    # Duration filter (optional, allow NaN values through)
    if filters["max_duration_minutes"] is not None:
        mask &= df["duration_minutes"].isna() | (
            df["duration_minutes"] <= filters["max_duration_minutes"]
        )

    # Comfort filters
    if filters["require_wifi"]:
        mask &= df["has_wifi"]
    if filters["require_baggage"]:
        mask &= df["baggage_checked"] > 0

    return df[mask]


def get_available_options(df: pd.DataFrame, origin: str) -> FilterOptions:
    """
    Get available filter options based on current data and selected origin.

    Args:
        df: The complete flight DataFrame.
        origin: Selected origin airport IATA code.

    Returns:
        FilterOptions with available destinations, airlines, dates, and price range.
    """
    origin_df = df[df["origin_iata"] == origin]

    return FilterOptions(
        origins=df["origin_iata"].unique().tolist(),
        destinations=origin_df["dest_iata"].unique().tolist(),
        airlines=df["carrier_name"].unique().tolist(),
        min_date=df["departure_date"].min().date(),
        max_date=df["departure_date"].max().date(),
        max_price=int(df["price_amount"].max()) if not df.empty else 1000,
    )
