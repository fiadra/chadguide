"""
Services module for data loading and processing.

Provides data access, city lookups, insights, and business logic for the dashboard.
"""

from dashboard.services.city_service import (
    format_origin_destination,
    get_city_name,
    get_city_with_code,
    get_country,
)
from dashboard.services.data_service import get_cached_flight_data, load_flight_data
from dashboard.services.filter_service import apply_filters, get_available_options
from dashboard.services.insights_service import (
    get_best_deal,
    get_cheapest_day_for_route,
    get_cheapest_day_insight,
    get_price_range_context,
    get_route_insights,
)
from dashboard.services.route_service import (
    format_duration,
    get_route_airline_breakdown,
    get_route_kpis,
    get_route_summary,
    parse_duration_to_minutes,
)

__all__ = [
    # Data service
    "get_cached_flight_data",
    "load_flight_data",
    # Filter service
    "apply_filters",
    "get_available_options",
    # Route service
    "get_route_summary",
    "get_route_airline_breakdown",
    "get_route_kpis",
    "parse_duration_to_minutes",
    "format_duration",
    # City service
    "get_city_name",
    "get_city_with_code",
    "get_country",
    "format_origin_destination",
    # Insights service
    "get_best_deal",
    "get_cheapest_day_insight",
    "get_cheapest_day_for_route",
    "get_price_range_context",
    "get_route_insights",
]
