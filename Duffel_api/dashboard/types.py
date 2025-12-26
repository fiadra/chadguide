"""
Type definitions for the dashboard module.

Provides TypedDict classes for structured data used across
the dashboard components.
"""

from datetime import date
from typing import List, Optional, TypedDict


class FilterState(TypedDict):
    """User-selected filter values from sidebar."""

    # Required
    origin: str

    # Quick filters
    direct_only: bool
    max_price: Optional[int]
    max_duration_minutes: Optional[int]

    # Advanced filters (in collapsed section)
    destinations: List[str]
    airlines: List[str]
    date_range: tuple[date, date]
    require_wifi: bool
    require_baggage: bool


class QuickFilterState(TypedDict):
    """Quick filter values (always visible)."""

    origin: str
    direct_only: bool
    max_price: Optional[int]
    max_duration_minutes: Optional[int]


class AdvancedFilterState(TypedDict):
    """Advanced filter values (in expander)."""

    destinations: List[str]
    airlines: List[str]
    date_range: tuple[date, date]
    require_wifi: bool
    require_baggage: bool


class RouteKPIs(TypedDict):
    """Route-level KPI values for display."""

    num_routes: int
    avg_route_price: Optional[float]
    best_deal_dest: Optional[str]
    best_deal_price: Optional[float]


class FilterOptions(TypedDict):
    """Available options for filter dropdowns."""

    origins: List[str]
    destinations: List[str]
    airlines: List[str]
    min_date: date
    max_date: date
    max_price: int
    max_duration: int


# Keep old KPIMetrics for backward compatibility
class KPIMetrics(TypedDict):
    """Calculated KPI values for display (legacy)."""

    lowest_price: Optional[float]
    average_price: Optional[float]
    currency: Optional[str]
    destination_count: int
    best_value_airline: Optional[str]
