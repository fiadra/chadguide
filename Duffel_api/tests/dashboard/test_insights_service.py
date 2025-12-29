"""
Tests for insights_service module.

Tests smart insight calculation functions.
"""

import pandas as pd
import pytest

from dashboard.services.insights_service import (
    get_best_deal,
    get_cheapest_day_for_route,
    get_cheapest_day_insight,
    get_price_range_context,
    get_route_insights,
)


@pytest.fixture
def sample_flight_data() -> pd.DataFrame:
    """Create sample flight data for testing."""
    return pd.DataFrame({
        "origin_iata": ["WAW", "WAW", "WAW", "WAW", "WAW", "KRK"],
        "dest_iata": ["BCN", "BCN", "PAR", "PAR", "BCN", "BCN"],
        "price_amount": [45.0, 120.0, 150.0, 200.0, 80.0, 60.0],
        "day_of_week": ["Tuesday", "Friday", "Monday", "Friday", "Tuesday", "Monday"],
        "carrier_name": ["Ryanair", "Vueling", "Air France", "LOT", "Ryanair", "Ryanair"],
    })


class TestGetBestDeal:
    """Tests for get_best_deal function."""

    def test_finds_cheapest_destination(self, sample_flight_data: pd.DataFrame) -> None:
        """Test that the cheapest flight is identified."""
        result = get_best_deal(sample_flight_data, "WAW")

        assert result is not None
        assert result["dest_iata"] == "BCN"
        assert result["price"] == 45.0

    def test_includes_city_name(self, sample_flight_data: pd.DataFrame) -> None:
        """Test that city name is included in result."""
        result = get_best_deal(sample_flight_data, "WAW")

        assert result is not None
        assert result["city_name"] == "Barcelona"

    def test_calculates_savings(self, sample_flight_data: pd.DataFrame) -> None:
        """Test that savings percentage is calculated."""
        result = get_best_deal(sample_flight_data, "WAW")

        assert result is not None
        assert result["savings_vs_avg"] > 0

    def test_empty_origin_returns_none(self, sample_flight_data: pd.DataFrame) -> None:
        """Test that nonexistent origin returns None."""
        result = get_best_deal(sample_flight_data, "XXX")

        assert result is None

    def test_empty_dataframe_returns_none(self) -> None:
        """Test that empty DataFrame returns None."""
        empty_df = pd.DataFrame(columns=["origin_iata", "dest_iata", "price_amount"])
        result = get_best_deal(empty_df, "WAW")

        assert result is None


class TestGetCheapestDayInsight:
    """Tests for get_cheapest_day_insight function."""

    def test_finds_cheapest_day(self, sample_flight_data: pd.DataFrame) -> None:
        """Test that cheapest day is identified."""
        result = get_cheapest_day_insight(sample_flight_data, "WAW")

        assert result is not None
        assert result["cheapest_day"] == "Tuesday"

    def test_finds_expensive_day(self, sample_flight_data: pd.DataFrame) -> None:
        """Test that most expensive day is identified."""
        result = get_cheapest_day_insight(sample_flight_data, "WAW")

        assert result is not None
        assert result["expensive_day"] == "Friday"

    def test_calculates_savings_percent(self, sample_flight_data: pd.DataFrame) -> None:
        """Test that savings percentage is calculated."""
        result = get_cheapest_day_insight(sample_flight_data, "WAW")

        assert result is not None
        assert result["savings_percent"] > 0

    def test_empty_origin_returns_none(self, sample_flight_data: pd.DataFrame) -> None:
        """Test that nonexistent origin returns None."""
        result = get_cheapest_day_insight(sample_flight_data, "XXX")

        assert result is None

    def test_no_day_column_returns_none(self) -> None:
        """Test that missing day_of_week column returns None."""
        df = pd.DataFrame({
            "origin_iata": ["WAW"],
            "dest_iata": ["BCN"],
            "price_amount": [100.0],
        })
        result = get_cheapest_day_insight(df, "WAW")

        assert result is None


class TestGetCheapestDayForRoute:
    """Tests for get_cheapest_day_for_route function."""

    def test_finds_cheapest_day_for_route(self, sample_flight_data: pd.DataFrame) -> None:
        """Test that cheapest day for specific route is found."""
        result = get_cheapest_day_for_route(sample_flight_data, "WAW", "BCN")

        assert result == "Tuesday"

    def test_nonexistent_route_returns_none(self, sample_flight_data: pd.DataFrame) -> None:
        """Test that nonexistent route returns None."""
        result = get_cheapest_day_for_route(sample_flight_data, "WAW", "XXX")

        assert result is None


class TestGetPriceRangeContext:
    """Tests for get_price_range_context function."""

    def test_lowest_price_detected(self, sample_flight_data: pd.DataFrame) -> None:
        """Test that lowest price is detected."""
        result = get_price_range_context(45.0, sample_flight_data, "WAW")

        assert result == "Lowest price!"

    def test_below_average_detected(self, sample_flight_data: pd.DataFrame) -> None:
        """Test that below average prices are detected."""
        result = get_price_range_context(80.0, sample_flight_data, "WAW")

        assert "below" in result.lower() or "Below" in result

    def test_above_average_detected(self, sample_flight_data: pd.DataFrame) -> None:
        """Test that above average prices are detected."""
        result = get_price_range_context(180.0, sample_flight_data, "WAW")

        assert "above" in result.lower() or "Average" in result

    def test_empty_data_returns_empty_string(self) -> None:
        """Test that empty data returns empty string."""
        empty_df = pd.DataFrame(columns=["origin_iata", "dest_iata", "price_amount"])
        result = get_price_range_context(100.0, empty_df, "WAW")

        assert result == ""


class TestGetRouteInsights:
    """Tests for get_route_insights function."""

    def test_returns_insights_dict(self, sample_flight_data: pd.DataFrame) -> None:
        """Test that insights dictionary is returned."""
        result = get_route_insights(sample_flight_data, "WAW", "BCN")

        assert isinstance(result, dict)
        assert "min_price" in result
        assert "max_price" in result
        assert "avg_price" in result

    def test_calculates_price_stats(self, sample_flight_data: pd.DataFrame) -> None:
        """Test that price statistics are calculated."""
        result = get_route_insights(sample_flight_data, "WAW", "BCN")

        assert result["min_price"] == 45.0
        assert result["max_price"] == 120.0

    def test_counts_airlines(self, sample_flight_data: pd.DataFrame) -> None:
        """Test that airlines are counted."""
        result = get_route_insights(sample_flight_data, "WAW", "BCN")

        assert result["num_airlines"] == 2  # Ryanair and Vueling

    def test_nonexistent_route_returns_empty(self, sample_flight_data: pd.DataFrame) -> None:
        """Test that nonexistent route returns empty dict."""
        result = get_route_insights(sample_flight_data, "WAW", "XXX")

        assert result == {}
