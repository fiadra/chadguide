"""
Tests for FlightDataExpanderService.

Tests cover:
- Week offset calculation for various date ranges
- Flight data expansion (time shifting)
- Edge cases (empty data, single week, multiple weeks)
"""

from datetime import datetime

import pandas as pd
import pytest

from src.flight_router.services.flight_data_expander_service import (
    EPOCH_REFERENCE,
    FlightDataExpanderService,
)


# =============================================================================
# HELPERS
# =============================================================================


def to_epoch_minutes(dt: datetime) -> float:
    """Convert datetime to epoch minutes (matching service logic)."""
    return (dt - EPOCH_REFERENCE).total_seconds() / 60


def create_sample_flights_df() -> pd.DataFrame:
    """Create sample flight DataFrame for base week."""
    # Base week: 2026-07-13 (Mon) to 2026-07-19 (Sun)
    return pd.DataFrame(
        {
            "departure_airport": ["WAW", "WAW", "BCN"],
            "arrival_airport": ["BCN", "FCO", "WAW"],
            "dep_time": [
                to_epoch_minutes(datetime(2026, 7, 13, 10, 0)),  # Monday
                to_epoch_minutes(datetime(2026, 7, 14, 14, 30)),  # Tuesday
                to_epoch_minutes(datetime(2026, 7, 15, 8, 0)),  # Wednesday
            ],
            "arr_time": [
                to_epoch_minutes(datetime(2026, 7, 13, 13, 0)),
                to_epoch_minutes(datetime(2026, 7, 14, 17, 0)),
                to_epoch_minutes(datetime(2026, 7, 15, 11, 30)),
            ],
            "price": [150.0, 200.0, 175.0],
            "carrier_code": ["LO", "LO", "VY"],
        }
    )


# =============================================================================
# FIXTURES
# =============================================================================


@pytest.fixture
def expander() -> FlightDataExpanderService:
    """Create expander service instance."""
    return FlightDataExpanderService()


@pytest.fixture
def sample_flights() -> pd.DataFrame:
    """Create sample flights DataFrame."""
    return create_sample_flights_df()


# =============================================================================
# WEEK OFFSET CALCULATION TESTS
# =============================================================================


class TestWeekOffsetCalculation:
    """Tests for get_week_offsets_for_range()."""

    def test_range_within_base_week_returns_zero_only(
        self, expander: FlightDataExpanderService
    ):
        """Range fully within base week returns only [0]."""
        # Base week: 2026-07-13 to 2026-07-19
        t_min = to_epoch_minutes(datetime(2026, 7, 14, 0, 0))
        t_max = to_epoch_minutes(datetime(2026, 7, 16, 23, 59))

        offsets = expander.get_week_offsets_for_range(t_min, t_max)

        assert offsets == [0]

    def test_range_one_week_after_base(self, expander: FlightDataExpanderService):
        """Range one week after base week."""
        # Range: 2026-07-20 to 2026-07-26
        t_min = to_epoch_minutes(datetime(2026, 7, 20, 0, 0))
        t_max = to_epoch_minutes(datetime(2026, 7, 26, 23, 59))

        offsets = expander.get_week_offsets_for_range(t_min, t_max)

        assert offsets == [7]

    def test_range_spanning_two_weeks(self, expander: FlightDataExpanderService):
        """Range spanning base week and one week after."""
        # Range: 2026-07-15 to 2026-07-22 (Wed to Wed)
        t_min = to_epoch_minutes(datetime(2026, 7, 15, 0, 0))
        t_max = to_epoch_minutes(datetime(2026, 7, 22, 23, 59))

        offsets = expander.get_week_offsets_for_range(t_min, t_max)

        assert 0 in offsets  # Base week
        assert 7 in offsets  # Next week

    def test_range_spanning_multiple_weeks(self, expander: FlightDataExpanderService):
        """Range spanning 5 weeks."""
        # Range: 2026-07-13 to 2026-08-15 (about 5 weeks)
        t_min = to_epoch_minutes(datetime(2026, 7, 13, 0, 0))
        t_max = to_epoch_minutes(datetime(2026, 8, 15, 23, 59))

        offsets = expander.get_week_offsets_for_range(t_min, t_max)

        assert 0 in offsets
        assert 7 in offsets
        assert 14 in offsets
        assert 21 in offsets
        assert 28 in offsets
        assert len(offsets) == 5

    def test_range_before_base_week(self, expander: FlightDataExpanderService):
        """Range one week before base week."""
        # Range: 2026-07-06 to 2026-07-12 (week before)
        t_min = to_epoch_minutes(datetime(2026, 7, 6, 0, 0))
        t_max = to_epoch_minutes(datetime(2026, 7, 12, 23, 59))

        offsets = expander.get_week_offsets_for_range(t_min, t_max)

        assert offsets == [-7]

    def test_range_far_in_future(self, expander: FlightDataExpanderService):
        """Range several months in future."""
        # Range: 2026-10-01 to 2026-10-07 (about 12 weeks after base)
        t_min = to_epoch_minutes(datetime(2026, 10, 1, 0, 0))
        t_max = to_epoch_minutes(datetime(2026, 10, 7, 23, 59))

        offsets = expander.get_week_offsets_for_range(t_min, t_max)

        # Should have one offset around 77-84 days (11-12 weeks)
        assert len(offsets) >= 1
        assert all(offset > 70 for offset in offsets)


# =============================================================================
# DATA EXPANSION TESTS
# =============================================================================


class TestDataExpansion:
    """Tests for expand_for_date_range()."""

    def test_no_expansion_within_base_week(
        self,
        expander: FlightDataExpanderService,
        sample_flights: pd.DataFrame,
    ):
        """No expansion when range is within base week."""
        t_min = to_epoch_minutes(datetime(2026, 7, 13, 0, 0))
        t_max = to_epoch_minutes(datetime(2026, 7, 19, 23, 59))

        result = expander.expand_for_date_range(sample_flights, t_min, t_max)

        # Should return same DataFrame (or equal)
        assert len(result) == len(sample_flights)
        pd.testing.assert_frame_equal(result, sample_flights)

    def test_expansion_doubles_for_two_weeks(
        self,
        expander: FlightDataExpanderService,
        sample_flights: pd.DataFrame,
    ):
        """Data doubles when spanning two weeks."""
        # Range: 2026-07-13 to 2026-07-26 (2 weeks)
        t_min = to_epoch_minutes(datetime(2026, 7, 13, 0, 0))
        t_max = to_epoch_minutes(datetime(2026, 7, 26, 23, 59))

        result = expander.expand_for_date_range(sample_flights, t_min, t_max)

        assert len(result) == len(sample_flights) * 2

    def test_expansion_shifts_times_correctly(
        self,
        expander: FlightDataExpanderService,
        sample_flights: pd.DataFrame,
    ):
        """Expanded data has correctly shifted dep_time and arr_time."""
        # Range: 2026-07-20 to 2026-07-26 (one week after base)
        t_min = to_epoch_minutes(datetime(2026, 7, 20, 0, 0))
        t_max = to_epoch_minutes(datetime(2026, 7, 26, 23, 59))

        result = expander.expand_for_date_range(sample_flights, t_min, t_max)

        # Offset should be +7 days = +7*24*60 minutes
        offset_minutes = 7 * 24 * 60

        # Check first flight's times are shifted
        original_dep = sample_flights.iloc[0]["dep_time"]
        shifted_dep = result.iloc[0]["dep_time"]
        assert shifted_dep == original_dep + offset_minutes

        original_arr = sample_flights.iloc[0]["arr_time"]
        shifted_arr = result.iloc[0]["arr_time"]
        assert shifted_arr == original_arr + offset_minutes

    def test_expansion_preserves_other_columns(
        self,
        expander: FlightDataExpanderService,
        sample_flights: pd.DataFrame,
    ):
        """Non-time columns are preserved during expansion."""
        # Range: 2026-07-20 to 2026-07-26
        t_min = to_epoch_minutes(datetime(2026, 7, 20, 0, 0))
        t_max = to_epoch_minutes(datetime(2026, 7, 26, 23, 59))

        result = expander.expand_for_date_range(sample_flights, t_min, t_max)

        # Prices should be unchanged
        assert result.iloc[0]["price"] == sample_flights.iloc[0]["price"]

        # Airports should be unchanged
        assert (
            result.iloc[0]["departure_airport"]
            == sample_flights.iloc[0]["departure_airport"]
        )
        assert (
            result.iloc[0]["arrival_airport"]
            == sample_flights.iloc[0]["arrival_airport"]
        )

        # Carrier should be unchanged
        assert result.iloc[0]["carrier_code"] == sample_flights.iloc[0]["carrier_code"]

    def test_empty_dataframe_returns_empty(
        self, expander: FlightDataExpanderService
    ):
        """Empty input returns empty output."""
        empty_df = pd.DataFrame(
            columns=["departure_airport", "arrival_airport", "dep_time", "arr_time", "price"]
        )
        t_min = to_epoch_minutes(datetime(2026, 7, 13, 0, 0))
        t_max = to_epoch_minutes(datetime(2026, 8, 13, 23, 59))

        result = expander.expand_for_date_range(empty_df, t_min, t_max)

        assert len(result) == 0


# =============================================================================
# PROPERTY TESTS
# =============================================================================


class TestProperties:
    """Tests for service properties."""

    def test_base_week_start_minutes(self, expander: FlightDataExpanderService):
        """base_week_start_minutes returns correct value."""
        expected = to_epoch_minutes(datetime(2026, 7, 13, 0, 0, 0))
        assert expander.base_week_start_minutes == expected

    def test_base_week_end_minutes(self, expander: FlightDataExpanderService):
        """base_week_end_minutes returns correct value."""
        expected = to_epoch_minutes(datetime(2026, 7, 19, 23, 59, 59))
        assert expander.base_week_end_minutes == expected
