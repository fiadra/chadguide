"""
Dashboard configuration module.

Centralizes all configuration values, magic numbers, and defaults
used throughout the dashboard application.
"""

from dataclasses import dataclass, field
from typing import List


@dataclass(frozen=True)
class PageConfig:
    """Streamlit page configuration."""

    title: str = "Flight Intelligence Dashboard"
    icon: str = "airplane"
    layout: str = "wide"
    sidebar_state: str = "expanded"


@dataclass(frozen=True)
class ChartConfig:
    """Default chart configuration values."""

    # Chart dimensions
    standard_height: int = 400
    map_height: int = 600
    table_height: int = 600

    # Color scales
    price_color_scale: str = "RdYlGn_r"
    eco_color_scale: str = "Teal"

    # Map styling
    route_line_color: str = "#3b82f6"
    route_line_width: int = 1
    route_line_opacity: float = 0.5
    destination_marker_color: str = "#ef4444"
    destination_marker_base_size: int = 8
    destination_marker_scale: float = 0.2
    origin_marker_color: str = "#10b981"
    origin_marker_size: int = 15

    # Map geo settings
    map_scope: str = "europe"
    map_projection: str = "azimuthal equal area"
    land_color: str = "#f3f4f6"
    country_border_color: str = "#e5e7eb"


@dataclass(frozen=True)
class DataConfig:
    """Data processing configuration."""

    database_path: str = "Duffel_api/flights.db"
    default_seat_pitch: int = 29
    seat_pitch_threshold: int = 30
    comfort_score_max: int = 4


@dataclass(frozen=True)
class DisplayConfig:
    """Display and formatting configuration."""

    co2_max_display: int = 300
    table_columns: tuple = (
        "departure_date",
        "carrier_name",
        "flight_number",
        "origin_iata",
        "dest_iata",
        "price_amount",
        "currency",
        "aircraft_model",
        "has_wifi",
        "baggage_checked",
        "co2_kg",
    )


@dataclass(frozen=True)
class StyleConfig:
    """CSS styling configuration."""

    metric_card_bg: str = "#f8f9fa"
    metric_card_border: str = "#e9ecef"
    metric_value_color: str = "#1e293b"  # Darker for better contrast
    metric_label_color: str = "#475569"  # Slightly darker
    metric_value_size: str = "28px"
    metric_label_size: str = "14px"
    # Delta/insight text styling
    metric_delta_color: str = "#059669"  # Green for positive insights
    metric_delta_neutral: str = "#6b7280"  # Gray for neutral
    metric_delta_size: str = "14px"


class DashboardConfig:
    """Main configuration container providing access to all config sections."""

    page = PageConfig()
    charts = ChartConfig()
    data = DataConfig()
    display = DisplayConfig()
    style = StyleConfig()
