"""Tests for filter service module."""

from datetime import date

import pandas as pd
import pytest

from dashboard.services.filter_service import apply_filters, get_available_options
from dashboard.types import FilterState


class TestApplyFilters:
    """Tests for the apply_filters function."""

    def test_filter_by_origin(self, sample_flight_data: pd.DataFrame) -> None:
        """Test filtering by origin airport."""
        filters = FilterState(
            origin="WAW",
            direct_only=False,
            max_price=1000,
            max_duration_minutes=None,
            destinations=["BCN", "PAR"],
            airlines=["Ryanair", "Vueling", "Air France"],
            date_range=(date(2024, 7, 1), date(2024, 7, 31)),
            require_wifi=False,
            require_baggage=False,
        )

        result = apply_filters(sample_flight_data, filters)

        assert len(result) == 3
        assert all(result["origin_iata"] == "WAW")

    def test_filter_by_destination(self, sample_flight_data: pd.DataFrame) -> None:
        """Test filtering by destination."""
        filters = FilterState(
            origin="WAW",
            direct_only=False,
            max_price=1000,
            max_duration_minutes=None,
            destinations=["BCN"],
            airlines=["Ryanair", "Vueling", "Air France"],
            date_range=(date(2024, 7, 1), date(2024, 7, 31)),
            require_wifi=False,
            require_baggage=False,
        )

        result = apply_filters(sample_flight_data, filters)

        assert len(result) == 2
        assert all(result["dest_iata"] == "BCN")

    def test_filter_by_price(self, sample_flight_data: pd.DataFrame) -> None:
        """Test filtering by maximum price."""
        filters = FilterState(
            origin="WAW",
            direct_only=False,
            max_price=75,
            max_duration_minutes=None,
            destinations=["BCN", "PAR"],
            airlines=["Ryanair", "Vueling", "Air France"],
            date_range=(date(2024, 7, 1), date(2024, 7, 31)),
            require_wifi=False,
            require_baggage=False,
        )

        result = apply_filters(sample_flight_data, filters)

        assert len(result) == 1
        assert result.iloc[0]["price_amount"] == 50.0

    def test_filter_require_wifi(self, sample_flight_data: pd.DataFrame) -> None:
        """Test WiFi requirement filter."""
        filters = FilterState(
            origin="WAW",
            direct_only=False,
            max_price=1000,
            max_duration_minutes=None,
            destinations=["BCN", "PAR"],
            airlines=["Ryanair", "Vueling", "Air France"],
            date_range=(date(2024, 7, 1), date(2024, 7, 31)),
            require_wifi=True,
            require_baggage=False,
        )

        result = apply_filters(sample_flight_data, filters)

        assert len(result) == 2
        assert all(result["has_wifi"])

    def test_filter_require_baggage(self, sample_flight_data: pd.DataFrame) -> None:
        """Test checked baggage requirement filter."""
        filters = FilterState(
            origin="WAW",
            direct_only=False,
            max_price=1000,
            max_duration_minutes=None,
            destinations=["BCN", "PAR"],
            airlines=["Ryanair", "Vueling", "Air France"],
            date_range=(date(2024, 7, 1), date(2024, 7, 31)),
            require_wifi=False,
            require_baggage=True,
        )

        result = apply_filters(sample_flight_data, filters)

        assert len(result) == 2
        assert all(result["baggage_checked"] > 0)

    def test_filter_by_date_range(self, sample_flight_data: pd.DataFrame) -> None:
        """Test filtering by date range."""
        filters = FilterState(
            origin="WAW",
            direct_only=False,
            max_price=1000,
            max_duration_minutes=None,
            destinations=["BCN", "PAR"],
            airlines=["Ryanair", "Vueling", "Air France"],
            date_range=(date(2024, 7, 15), date(2024, 7, 16)),
            require_wifi=False,
            require_baggage=False,
        )

        result = apply_filters(sample_flight_data, filters)

        assert len(result) == 2

    def test_filter_empty_result(self, sample_flight_data: pd.DataFrame) -> None:
        """Test that impossible filters return empty DataFrame."""
        filters = FilterState(
            origin="WAW",
            direct_only=False,
            max_price=10,  # Too low
            max_duration_minutes=None,
            destinations=["BCN"],
            airlines=["Ryanair"],
            date_range=(date(2024, 7, 1), date(2024, 7, 31)),
            require_wifi=False,
            require_baggage=False,
        )

        result = apply_filters(sample_flight_data, filters)

        assert len(result) == 0

    def test_filter_direct_only(self, sample_flight_data: pd.DataFrame) -> None:
        """Test direct flights only filter."""
        filters = FilterState(
            origin="WAW",
            direct_only=True,
            max_price=1000,
            max_duration_minutes=None,
            destinations=["BCN", "PAR"],
            airlines=["Ryanair", "Vueling", "Air France"],
            date_range=(date(2024, 7, 1), date(2024, 7, 31)),
            require_wifi=False,
            require_baggage=False,
        )

        result = apply_filters(sample_flight_data, filters)

        # PAR flight is not direct, so only BCN flights remain
        assert len(result) == 2
        assert all(result["is_direct"])

    def test_filter_by_max_duration(self, sample_flight_data: pd.DataFrame) -> None:
        """Test filtering by maximum duration."""
        filters = FilterState(
            origin="WAW",
            direct_only=False,
            max_price=1000,
            max_duration_minutes=155,
            destinations=["BCN", "PAR"],
            airlines=["Ryanair", "Vueling", "Air France"],
            date_range=(date(2024, 7, 1), date(2024, 7, 31)),
            require_wifi=False,
            require_baggage=False,
        )

        result = apply_filters(sample_flight_data, filters)

        # Only flights with duration <= 155 min (150, 155)
        assert len(result) == 2
        assert all(result["duration_minutes"] <= 155)


class TestGetAvailableOptions:
    """Tests for the get_available_options function."""

    def test_returns_correct_destinations(
        self, sample_flight_data: pd.DataFrame
    ) -> None:
        """Test that destinations are filtered by origin."""
        options = get_available_options(sample_flight_data, "WAW")

        assert set(options["destinations"]) == {"BCN", "PAR"}

    def test_returns_all_airlines(self, sample_flight_data: pd.DataFrame) -> None:
        """Test that all airlines are returned."""
        options = get_available_options(sample_flight_data, "WAW")

        assert set(options["airlines"]) == {"Ryanair", "Vueling", "Air France", "LOT"}

    def test_returns_correct_date_range(
        self, sample_flight_data: pd.DataFrame
    ) -> None:
        """Test that correct date range is returned."""
        options = get_available_options(sample_flight_data, "WAW")

        assert options["min_date"] == date(2024, 7, 15)
        assert options["max_date"] == date(2024, 7, 18)

    def test_returns_correct_max_price(self, sample_flight_data: pd.DataFrame) -> None:
        """Test that correct max price is returned."""
        options = get_available_options(sample_flight_data, "WAW")

        assert options["max_price"] == 200
