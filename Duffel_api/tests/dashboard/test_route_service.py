"""Tests for route service module."""

import pandas as pd
import pytest

from dashboard.services.route_service import (
    format_duration,
    get_route_airline_breakdown,
    get_route_kpis,
    get_route_summary,
    parse_duration_to_minutes,
)


class TestParseDurationToMinutes:
    """Tests for the parse_duration_to_minutes function."""

    def test_hours_and_minutes(self) -> None:
        """Test parsing duration with hours and minutes."""
        assert parse_duration_to_minutes("PT2H30M") == 150

    def test_hours_only(self) -> None:
        """Test parsing duration with only hours."""
        assert parse_duration_to_minutes("PT1H") == 60
        assert parse_duration_to_minutes("PT3H") == 180

    def test_minutes_only(self) -> None:
        """Test parsing duration with only minutes."""
        assert parse_duration_to_minutes("PT45M") == 45
        assert parse_duration_to_minutes("PT90M") == 90

    def test_zero_duration(self) -> None:
        """Test parsing zero duration."""
        assert parse_duration_to_minutes("PT0H0M") == 0
        assert parse_duration_to_minutes("PT0M") == 0

    def test_none_input(self) -> None:
        """Test that None input returns None."""
        assert parse_duration_to_minutes(None) is None

    def test_empty_string(self) -> None:
        """Test that empty string returns None."""
        assert parse_duration_to_minutes("") is None

    def test_invalid_format(self) -> None:
        """Test that invalid format returns None or 0."""
        result = parse_duration_to_minutes("invalid")
        assert result == 0 or result is None

    def test_large_values(self) -> None:
        """Test parsing large duration values."""
        assert parse_duration_to_minutes("PT12H45M") == 765
        assert parse_duration_to_minutes("PT24H") == 1440


class TestFormatDuration:
    """Tests for the format_duration function."""

    def test_hours_and_minutes(self) -> None:
        """Test formatting duration with hours and minutes."""
        assert format_duration(150) == "2h 30m"
        assert format_duration(90) == "1h 30m"

    def test_hours_only(self) -> None:
        """Test formatting duration with exact hours."""
        assert format_duration(60) == "1h"
        assert format_duration(120) == "2h"

    def test_minutes_only(self) -> None:
        """Test formatting duration with only minutes."""
        assert format_duration(45) == "45m"
        assert format_duration(30) == "30m"

    def test_zero_minutes(self) -> None:
        """Test formatting zero minutes."""
        assert format_duration(0) == "0m"

    def test_none_input(self) -> None:
        """Test that None input returns dash."""
        assert format_duration(None) == "-"

    def test_nan_input(self) -> None:
        """Test that NaN input returns dash."""
        assert format_duration(float("nan")) == "-"


class TestGetRouteSummary:
    """Tests for the get_route_summary function."""

    @pytest.fixture
    def route_test_data(self) -> pd.DataFrame:
        """Create test data with duration_minutes column."""
        return pd.DataFrame(
            {
                "origin_iata": ["WAW", "WAW", "WAW", "WAW", "KRK"],
                "dest_iata": ["BCN", "BCN", "PAR", "PAR", "BCN"],
                "carrier_name": ["Ryanair", "Vueling", "Air France", "LOT", "Ryanair"],
                "price_amount": [50.0, 100.0, 150.0, 180.0, 75.0],
                "duration_minutes": [150, 155, 180, 175, 160],
            }
        )

    def test_aggregates_by_destination(self, route_test_data: pd.DataFrame) -> None:
        """Test that offers are aggregated by destination."""
        result = get_route_summary(route_test_data, "WAW")

        assert len(result) == 2  # BCN and PAR
        assert set(result["dest_iata"].tolist()) == {"BCN", "PAR"}

    def test_counts_unique_airlines(self, route_test_data: pd.DataFrame) -> None:
        """Test that unique airlines are counted correctly."""
        result = get_route_summary(route_test_data, "WAW")

        bcn_row = result[result["dest_iata"] == "BCN"].iloc[0]
        par_row = result[result["dest_iata"] == "PAR"].iloc[0]

        assert bcn_row["num_airlines"] == 2  # Ryanair, Vueling
        assert par_row["num_airlines"] == 2  # Air France, LOT

    def test_calculates_min_price(self, route_test_data: pd.DataFrame) -> None:
        """Test that minimum price is calculated correctly."""
        result = get_route_summary(route_test_data, "WAW")

        bcn_row = result[result["dest_iata"] == "BCN"].iloc[0]
        assert bcn_row["min_price"] == 50.0

    def test_calculates_avg_price(self, route_test_data: pd.DataFrame) -> None:
        """Test that average price is calculated correctly."""
        result = get_route_summary(route_test_data, "WAW")

        bcn_row = result[result["dest_iata"] == "BCN"].iloc[0]
        assert bcn_row["avg_price"] == 75.0  # (50 + 100) / 2, rounded

    def test_calculates_max_price(self, route_test_data: pd.DataFrame) -> None:
        """Test that maximum price is calculated correctly."""
        result = get_route_summary(route_test_data, "WAW")

        bcn_row = result[result["dest_iata"] == "BCN"].iloc[0]
        assert bcn_row["max_price"] == 100.0

    def test_calculates_min_duration(self, route_test_data: pd.DataFrame) -> None:
        """Test that minimum duration is calculated correctly."""
        result = get_route_summary(route_test_data, "WAW")

        bcn_row = result[result["dest_iata"] == "BCN"].iloc[0]
        assert bcn_row["min_duration"] == 150

    def test_counts_offers(self, route_test_data: pd.DataFrame) -> None:
        """Test that offer count is correct."""
        result = get_route_summary(route_test_data, "WAW")

        bcn_row = result[result["dest_iata"] == "BCN"].iloc[0]
        par_row = result[result["dest_iata"] == "PAR"].iloc[0]

        assert bcn_row["num_offers"] == 2
        assert par_row["num_offers"] == 2

    def test_empty_origin_returns_empty(self, route_test_data: pd.DataFrame) -> None:
        """Test that non-existent origin returns empty DataFrame."""
        result = get_route_summary(route_test_data, "XXX")
        assert result.empty

    def test_sorted_by_num_airlines(self, route_test_data: pd.DataFrame) -> None:
        """Test that results are sorted by number of airlines descending."""
        # Add more variation in airline count
        data = pd.DataFrame(
            {
                "origin_iata": ["WAW"] * 5,
                "dest_iata": ["BCN", "BCN", "BCN", "PAR", "ROM"],
                "carrier_name": ["A", "B", "C", "A", "A"],
                "price_amount": [50.0] * 5,
                "duration_minutes": [150] * 5,
            }
        )

        result = get_route_summary(data, "WAW")

        # BCN should be first with 3 airlines
        assert result.iloc[0]["dest_iata"] == "BCN"
        assert result.iloc[0]["num_airlines"] == 3


class TestGetRouteAirlineBreakdown:
    """Tests for the get_route_airline_breakdown function."""

    @pytest.fixture
    def airline_test_data(self) -> pd.DataFrame:
        """Create test data for airline breakdown."""
        return pd.DataFrame(
            {
                "origin_iata": ["WAW"] * 4,
                "dest_iata": ["BCN"] * 4,
                "carrier_name": ["Ryanair", "Ryanair", "Vueling", "Vueling"],
                "carrier_code": ["FR", "FR", "VY", "VY"],
                "price_amount": [50.0, 60.0, 80.0, 100.0],
                "duration_minutes": [150, 155, 160, 165],
                "has_wifi": [False, False, True, True],
                "baggage_checked": [0, 0, 1, 2],
            }
        )

    def test_groups_by_airline(self, airline_test_data: pd.DataFrame) -> None:
        """Test that data is grouped by airline."""
        result = get_route_airline_breakdown(airline_test_data, "WAW", "BCN")

        assert len(result) == 2
        assert set(result["carrier_name"].tolist()) == {"Ryanair", "Vueling"}

    def test_calculates_price_range(self, airline_test_data: pd.DataFrame) -> None:
        """Test that price range is calculated per airline."""
        result = get_route_airline_breakdown(airline_test_data, "WAW", "BCN")

        ryanair = result[result["carrier_name"] == "Ryanair"].iloc[0]
        assert ryanair["min_price"] == 50.0
        assert ryanair["max_price"] == 60.0

    def test_detects_wifi(self, airline_test_data: pd.DataFrame) -> None:
        """Test that WiFi availability is detected."""
        result = get_route_airline_breakdown(airline_test_data, "WAW", "BCN")

        ryanair = result[result["carrier_name"] == "Ryanair"].iloc[0]
        vueling = result[result["carrier_name"] == "Vueling"].iloc[0]

        assert ryanair["has_wifi"] == False
        assert vueling["has_wifi"] == True

    def test_max_baggage(self, airline_test_data: pd.DataFrame) -> None:
        """Test that maximum baggage is calculated."""
        result = get_route_airline_breakdown(airline_test_data, "WAW", "BCN")

        ryanair = result[result["carrier_name"] == "Ryanair"].iloc[0]
        vueling = result[result["carrier_name"] == "Vueling"].iloc[0]

        assert ryanair["max_baggage"] == 0
        assert vueling["max_baggage"] == 2

    def test_sorted_by_min_price(self, airline_test_data: pd.DataFrame) -> None:
        """Test that results are sorted by minimum price."""
        result = get_route_airline_breakdown(airline_test_data, "WAW", "BCN")

        assert result.iloc[0]["carrier_name"] == "Ryanair"  # Cheapest
        assert result.iloc[1]["carrier_name"] == "Vueling"

    def test_empty_route_returns_empty(self, airline_test_data: pd.DataFrame) -> None:
        """Test that non-existent route returns empty DataFrame."""
        result = get_route_airline_breakdown(airline_test_data, "WAW", "XXX")
        assert result.empty

    def test_wrong_origin_returns_empty(self, airline_test_data: pd.DataFrame) -> None:
        """Test that wrong origin returns empty DataFrame."""
        result = get_route_airline_breakdown(airline_test_data, "XXX", "BCN")
        assert result.empty


class TestGetRouteKpis:
    """Tests for the get_route_kpis function."""

    def test_empty_dataframe(self) -> None:
        """Test that empty DataFrame returns zero KPIs."""
        result = get_route_kpis(pd.DataFrame())

        assert result["num_routes"] == 0
        assert result["avg_route_price"] is None
        assert result["best_deal_dest"] is None
        assert result["best_deal_price"] is None

    def test_calculates_num_routes(self) -> None:
        """Test that number of routes is calculated correctly."""
        route_summary = pd.DataFrame(
            {
                "dest_iata": ["BCN", "PAR", "ROM"],
                "min_price": [50.0, 80.0, 100.0],
                "avg_price": [75.0, 90.0, 120.0],
            }
        )

        result = get_route_kpis(route_summary)

        assert result["num_routes"] == 3

    def test_calculates_avg_route_price(self) -> None:
        """Test that average route price is calculated correctly."""
        route_summary = pd.DataFrame(
            {
                "dest_iata": ["BCN", "PAR"],
                "min_price": [50.0, 80.0],
                "avg_price": [100.0, 200.0],
            }
        )

        result = get_route_kpis(route_summary)

        assert result["avg_route_price"] == 150.0  # (100 + 200) / 2

    def test_finds_best_deal(self) -> None:
        """Test that best deal is identified correctly."""
        route_summary = pd.DataFrame(
            {
                "dest_iata": ["BCN", "PAR", "ROM"],
                "min_price": [80.0, 50.0, 100.0],  # PAR is cheapest
                "avg_price": [90.0, 60.0, 120.0],
            }
        )

        result = get_route_kpis(route_summary)

        assert result["best_deal_dest"] == "PAR"
        assert result["best_deal_price"] == 50.0

    def test_single_route(self) -> None:
        """Test KPIs with single route."""
        route_summary = pd.DataFrame(
            {
                "dest_iata": ["BCN"],
                "min_price": [50.0],
                "avg_price": [75.0],
            }
        )

        result = get_route_kpis(route_summary)

        assert result["num_routes"] == 1
        assert result["best_deal_dest"] == "BCN"
        assert result["best_deal_price"] == 50.0
