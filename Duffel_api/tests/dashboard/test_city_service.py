"""
Tests for city_service module.

Tests IATA code to city name lookup functions.
"""

import pytest

from dashboard.services.city_service import (
    format_origin_destination,
    get_city_name,
    get_city_with_code,
    get_country,
)


class TestGetCityName:
    """Tests for get_city_name function."""

    def test_known_airport_returns_city(self) -> None:
        """Test that known IATA codes return correct city names."""
        assert get_city_name("BCN") == "Barcelona"
        assert get_city_name("WAW") == "Warsaw"
        assert get_city_name("LHR") == "London Heathrow"

    def test_unknown_airport_returns_code(self) -> None:
        """Test that unknown IATA codes return the code itself."""
        assert get_city_name("XXX") == "XXX"
        assert get_city_name("ZZZ") == "ZZZ"

    def test_case_insensitive(self) -> None:
        """Test that lookup is case insensitive."""
        assert get_city_name("bcn") == "Barcelona"
        assert get_city_name("Bcn") == "Barcelona"
        assert get_city_name("BCN") == "Barcelona"


class TestGetCityWithCode:
    """Tests for get_city_with_code function."""

    def test_known_airport_formats_correctly(self) -> None:
        """Test that known airports return 'City (CODE)' format."""
        assert get_city_with_code("BCN") == "Barcelona (BCN)"
        assert get_city_with_code("WAW") == "Warsaw (WAW)"

    def test_unknown_airport_returns_code_only(self) -> None:
        """Test that unknown airports return just the code."""
        assert get_city_with_code("XXX") == "XXX"


class TestGetCountry:
    """Tests for get_country function."""

    def test_known_airport_returns_country(self) -> None:
        """Test that known airports return correct country."""
        assert get_country("BCN") == "Spain"
        assert get_country("WAW") == "Poland"
        assert get_country("LHR") == "UK"

    def test_unknown_airport_returns_none(self) -> None:
        """Test that unknown airports return None."""
        assert get_country("XXX") is None


class TestFormatOriginDestination:
    """Tests for format_origin_destination function."""

    def test_formats_route_with_arrow(self) -> None:
        """Test that routes are formatted with arrow."""
        assert format_origin_destination("WAW", "BCN") == "Warsaw → Barcelona"

    def test_unknown_codes_use_codes(self) -> None:
        """Test that unknown codes fall back to IATA codes."""
        assert format_origin_destination("XXX", "YYY") == "XXX → YYY"

    def test_mixed_known_unknown(self) -> None:
        """Test mix of known and unknown codes."""
        assert format_origin_destination("WAW", "XXX") == "Warsaw → XXX"
        assert format_origin_destination("XXX", "BCN") == "XXX → Barcelona"
