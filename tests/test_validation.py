"""
Tests for validation module.

Tests input validation functions and custom exceptions.
"""

import pandas as pd
import pytest

from src.dijkstra.exceptions import (
    DijkstraError,
    EmptyFlightsError,
    InvalidAirportError,
    InvalidTimeRangeError,
    MissingColumnsError,
    ValidationError,
)
from src.dijkstra.validation import (
    REQUIRED_COLUMNS,
    validate_airport_exists,
    validate_dijkstra_inputs,
    validate_flights_df,
    validate_required_cities,
    validate_time_range,
)


# -------------------------
# Fixtures
# -------------------------


@pytest.fixture
def valid_flights_df() -> pd.DataFrame:
    """Create a valid flights DataFrame."""
    return pd.DataFrame({
        "departure_airport": ["WAW", "BCN", "FCO"],
        "arrival_airport": ["BCN", "FCO", "WAW"],
        "dep_time": [100, 200, 300],
        "arr_time": [150, 250, 350],
        "price": [50, 60, 70],
    })


@pytest.fixture
def empty_flights_df() -> pd.DataFrame:
    """Create an empty flights DataFrame with correct columns."""
    return pd.DataFrame(columns=list(REQUIRED_COLUMNS))


@pytest.fixture
def missing_columns_df() -> pd.DataFrame:
    """Create a DataFrame missing required columns."""
    return pd.DataFrame({
        "departure_airport": ["WAW"],
        "arrival_airport": ["BCN"],
        # Missing: dep_time, arr_time, price
    })


# -------------------------
# Exception hierarchy tests
# -------------------------


class TestExceptionHierarchy:
    """Tests for exception class hierarchy."""

    def test_validation_error_is_dijkstra_error(self) -> None:
        """Test that ValidationError inherits from DijkstraError."""
        assert issubclass(ValidationError, DijkstraError)

    def test_empty_flights_error_is_validation_error(self) -> None:
        """Test that EmptyFlightsError inherits from ValidationError."""
        assert issubclass(EmptyFlightsError, ValidationError)

    def test_invalid_airport_error_is_validation_error(self) -> None:
        """Test that InvalidAirportError inherits from ValidationError."""
        assert issubclass(InvalidAirportError, ValidationError)

    def test_invalid_time_range_error_is_validation_error(self) -> None:
        """Test that InvalidTimeRangeError inherits from ValidationError."""
        assert issubclass(InvalidTimeRangeError, ValidationError)

    def test_missing_columns_error_is_validation_error(self) -> None:
        """Test that MissingColumnsError inherits from ValidationError."""
        assert issubclass(MissingColumnsError, ValidationError)


# -------------------------
# Exception message tests
# -------------------------


class TestExceptionMessages:
    """Tests for exception error messages."""

    def test_empty_flights_error_message(self) -> None:
        """Test EmptyFlightsError has descriptive message."""
        error = EmptyFlightsError()
        assert "empty" in str(error).lower()

    def test_invalid_airport_error_message(self) -> None:
        """Test InvalidAirportError includes airport code."""
        error = InvalidAirportError("XYZ", "test context")
        assert "XYZ" in str(error)
        assert "test context" in str(error)

    def test_invalid_time_range_error_message(self) -> None:
        """Test InvalidTimeRangeError includes time values."""
        error = InvalidTimeRangeError(100, 50)
        assert "100" in str(error)
        assert "50" in str(error)

    def test_missing_columns_error_message(self) -> None:
        """Test MissingColumnsError lists missing columns."""
        error = MissingColumnsError({"col_a", "col_b"})
        message = str(error)
        assert "col_a" in message
        assert "col_b" in message


# -------------------------
# validate_flights_df tests
# -------------------------


class TestValidateFlightsDf:
    """Tests for validate_flights_df function."""

    def test_valid_df_passes(self, valid_flights_df: pd.DataFrame) -> None:
        """Test that valid DataFrame passes validation."""
        # Should not raise
        validate_flights_df(valid_flights_df)

    def test_empty_df_raises(self, empty_flights_df: pd.DataFrame) -> None:
        """Test that empty DataFrame raises EmptyFlightsError."""
        with pytest.raises(EmptyFlightsError):
            validate_flights_df(empty_flights_df)

    def test_missing_columns_raises(self, missing_columns_df: pd.DataFrame) -> None:
        """Test that DataFrame with missing columns raises MissingColumnsError."""
        with pytest.raises(MissingColumnsError) as exc_info:
            validate_flights_df(missing_columns_df)

        # Verify missing columns are reported
        assert "dep_time" in str(exc_info.value)
        assert "arr_time" in str(exc_info.value)
        assert "price" in str(exc_info.value)

    def test_extra_columns_allowed(self, valid_flights_df: pd.DataFrame) -> None:
        """Test that extra columns don't cause validation failure."""
        df_with_extra = valid_flights_df.copy()
        df_with_extra["extra_column"] = "extra"

        # Should not raise
        validate_flights_df(df_with_extra)


# -------------------------
# validate_airport_exists tests
# -------------------------


class TestValidateAirportExists:
    """Tests for validate_airport_exists function."""

    def test_existing_departure_airport_passes(
        self, valid_flights_df: pd.DataFrame
    ) -> None:
        """Test that existing departure airport passes validation."""
        validate_airport_exists("WAW", valid_flights_df)

    def test_existing_arrival_airport_passes(
        self, valid_flights_df: pd.DataFrame
    ) -> None:
        """Test that existing arrival airport passes validation."""
        validate_airport_exists("BCN", valid_flights_df)

    def test_nonexistent_airport_raises(
        self, valid_flights_df: pd.DataFrame
    ) -> None:
        """Test that nonexistent airport raises InvalidAirportError."""
        with pytest.raises(InvalidAirportError) as exc_info:
            validate_airport_exists("XYZ", valid_flights_df)

        assert exc_info.value.airport == "XYZ"

    def test_custom_context_in_error(
        self, valid_flights_df: pd.DataFrame
    ) -> None:
        """Test that custom context appears in error message."""
        with pytest.raises(InvalidAirportError) as exc_info:
            validate_airport_exists("XYZ", valid_flights_df, "custom context")

        assert "custom context" in str(exc_info.value)


# -------------------------
# validate_required_cities tests
# -------------------------


class TestValidateRequiredCities:
    """Tests for validate_required_cities function."""

    def test_all_cities_exist_passes(self, valid_flights_df: pd.DataFrame) -> None:
        """Test that existing cities pass validation."""
        validate_required_cities({"WAW", "BCN"}, valid_flights_df)

    def test_empty_set_passes(self, valid_flights_df: pd.DataFrame) -> None:
        """Test that empty required cities set passes."""
        validate_required_cities(set(), valid_flights_df)

    def test_nonexistent_city_raises(self, valid_flights_df: pd.DataFrame) -> None:
        """Test that nonexistent city raises InvalidAirportError."""
        with pytest.raises(InvalidAirportError) as exc_info:
            validate_required_cities({"WAW", "XYZ"}, valid_flights_df)

        assert exc_info.value.airport == "XYZ"

    def test_error_context_mentions_required_cities(
        self, valid_flights_df: pd.DataFrame
    ) -> None:
        """Test that error message mentions required cities."""
        with pytest.raises(InvalidAirportError) as exc_info:
            validate_required_cities({"XYZ"}, valid_flights_df)

        assert "required cities" in str(exc_info.value)


# -------------------------
# validate_time_range tests
# -------------------------


class TestValidateTimeRange:
    """Tests for validate_time_range function."""

    def test_valid_range_passes(self) -> None:
        """Test that valid time range passes validation."""
        validate_time_range(0, 100)

    def test_equal_times_passes(self) -> None:
        """Test that equal T_min and T_max passes."""
        validate_time_range(100, 100)

    def test_invalid_range_raises(self) -> None:
        """Test that T_min > T_max raises InvalidTimeRangeError."""
        with pytest.raises(InvalidTimeRangeError) as exc_info:
            validate_time_range(100, 50)

        assert exc_info.value.t_min == 100
        assert exc_info.value.t_max == 50

    def test_negative_times_allowed(self) -> None:
        """Test that negative times are allowed if range is valid."""
        validate_time_range(-100, -50)

    def test_float_times_allowed(self) -> None:
        """Test that float times are handled correctly."""
        validate_time_range(0.5, 1.5)


# -------------------------
# validate_dijkstra_inputs tests
# -------------------------


class TestValidateDijkstraInputs:
    """Tests for validate_dijkstra_inputs function."""

    def test_valid_inputs_pass(self, valid_flights_df: pd.DataFrame) -> None:
        """Test that valid inputs pass all validation."""
        validate_dijkstra_inputs(
            flights_df=valid_flights_df,
            start_city="WAW",
            required_cities={"BCN"},
            t_min=0,
            t_max=500,
        )

    def test_empty_df_raises_first(self, empty_flights_df: pd.DataFrame) -> None:
        """Test that empty DataFrame is checked first."""
        with pytest.raises(EmptyFlightsError):
            validate_dijkstra_inputs(
                flights_df=empty_flights_df,
                start_city="WAW",
                required_cities={"BCN"},
                t_min=100,  # Invalid range, but should fail on empty first
                t_max=50,
            )

    def test_missing_columns_raises(self, missing_columns_df: pd.DataFrame) -> None:
        """Test that missing columns are detected."""
        with pytest.raises(MissingColumnsError):
            validate_dijkstra_inputs(
                flights_df=missing_columns_df,
                start_city="WAW",
                required_cities=set(),
                t_min=0,
                t_max=100,
            )

    def test_invalid_time_range_raises(self, valid_flights_df: pd.DataFrame) -> None:
        """Test that invalid time range is detected."""
        with pytest.raises(InvalidTimeRangeError):
            validate_dijkstra_inputs(
                flights_df=valid_flights_df,
                start_city="WAW",
                required_cities=set(),
                t_min=100,
                t_max=50,
            )

    def test_invalid_start_city_raises(self, valid_flights_df: pd.DataFrame) -> None:
        """Test that invalid start city is detected."""
        with pytest.raises(InvalidAirportError) as exc_info:
            validate_dijkstra_inputs(
                flights_df=valid_flights_df,
                start_city="XYZ",
                required_cities=set(),
                t_min=0,
                t_max=100,
            )

        assert exc_info.value.airport == "XYZ"

    def test_invalid_required_city_raises(self, valid_flights_df: pd.DataFrame) -> None:
        """Test that invalid required city is detected."""
        with pytest.raises(InvalidAirportError) as exc_info:
            validate_dijkstra_inputs(
                flights_df=valid_flights_df,
                start_city="WAW",
                required_cities={"ABC"},
                t_min=0,
                t_max=100,
            )

        assert exc_info.value.airport == "ABC"
