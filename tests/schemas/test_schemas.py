"""
Tests for flight_router schema definitions.

Validates that:
1. strict="filter" works (extra columns pass through)
2. Invalid data types raise SchemaError
3. Schema inheritance works correctly
4. Dataclass constraints work as expected
"""

import pandas as pd
import pandera as pa
import pytest

from src.flight_router.schemas.flight import (
    CoreFlightSchema,
    ExtendedFlightSchema,
    FlightDataFrame,
)
from src.flight_router.schemas.constraints import (
    TravelConstraints,
    TravelConstraintsSchema,
)
from src.flight_router.schemas.route import (
    RouteSegment,
    RouteResult,
    RouteSegmentSchema,
    RouteResultSchema,
)


# -------------------------
# Fixtures
# -------------------------


@pytest.fixture
def valid_core_flight_df() -> pd.DataFrame:
    """Create a valid DataFrame matching CoreFlightSchema."""
    return pd.DataFrame({
        "departure_airport": ["WAW", "BCN", "FCO"],
        "arrival_airport": ["BCN", "FCO", "WAW"],
        "dep_time": [100.0, 200.0, 300.0],
        "arr_time": [150.0, 250.0, 350.0],
        "price": [50.0, 60.0, 70.0],
    })


@pytest.fixture
def valid_extended_flight_df() -> pd.DataFrame:
    """Create a valid DataFrame matching ExtendedFlightSchema."""
    return pd.DataFrame({
        "departure_airport": ["WAW", "BCN"],
        "arrival_airport": ["BCN", "FCO"],
        "dep_time": [100.0, 200.0],
        "arr_time": [150.0, 250.0],
        "price": [50.0, 60.0],
        "carrier_code": ["LO", "FR"],
        "carrier_name": ["LOT Polish Airlines", "Ryanair"],
        "terminal_origin": ["A", "B"],
        "terminal_dest": ["C", "D"],
        "transfer_time_mins": [60.0, 45.0],
        "baggage_included": [1, 0],
    })


@pytest.fixture
def df_with_extra_columns() -> pd.DataFrame:
    """Create DataFrame with extra columns beyond CoreFlightSchema."""
    return pd.DataFrame({
        "departure_airport": ["WAW"],
        "arrival_airport": ["BCN"],
        "dep_time": [100.0],
        "arr_time": [150.0],
        "price": [50.0],
        "extra_column_1": ["extra_value"],
        "extra_column_2": [12345],
        "custom_metadata": [{"key": "value"}],
    })


# -------------------------
# CoreFlightSchema Tests
# -------------------------


class TestCoreFlightSchema:
    """Tests for CoreFlightSchema validation."""

    def test_valid_df_passes(self, valid_core_flight_df: pd.DataFrame) -> None:
        """Test that valid DataFrame passes validation."""
        validated = CoreFlightSchema.validate(valid_core_flight_df)
        assert len(validated) == 3
        assert list(validated.columns) == [
            "departure_airport", "arrival_airport", "dep_time", "arr_time", "price"
        ]

    def test_extra_columns_preserved_with_strict_filter(
        self, df_with_extra_columns: pd.DataFrame
    ) -> None:
        """
        Test that strict='filter' preserves extra columns.

        This is CRITICAL for forward compatibility - providers can add
        new fields without breaking the core contract.
        """
        validated = CoreFlightSchema.validate(df_with_extra_columns)

        # Core columns should be present
        assert "departure_airport" in validated.columns
        assert "price" in validated.columns

        # Extra columns should be preserved (not filtered out)
        assert "extra_column_1" in validated.columns
        assert "extra_column_2" in validated.columns
        assert "custom_metadata" in validated.columns

        # Values should be preserved
        assert validated["extra_column_1"].iloc[0] == "extra_value"
        assert validated["extra_column_2"].iloc[0] == 12345

    def test_missing_required_column_raises(self) -> None:
        """Test that missing required column raises SchemaError."""
        df = pd.DataFrame({
            "departure_airport": ["WAW"],
            "arrival_airport": ["BCN"],
            "dep_time": [100.0],
            # Missing: arr_time, price
        })

        with pytest.raises(pa.errors.SchemaError):
            CoreFlightSchema.validate(df)

    def test_invalid_type_raises(self) -> None:
        """Test that invalid data type raises SchemaError."""
        df = pd.DataFrame({
            "departure_airport": ["WAW"],
            "arrival_airport": ["BCN"],
            "dep_time": ["not_a_number"],  # Should be float
            "arr_time": [150.0],
            "price": [50.0],
        })

        with pytest.raises(pa.errors.SchemaError):
            CoreFlightSchema.validate(df)

    def test_negative_price_raises(self) -> None:
        """Test that negative price raises SchemaError."""
        df = pd.DataFrame({
            "departure_airport": ["WAW"],
            "arrival_airport": ["BCN"],
            "dep_time": [100.0],
            "arr_time": [150.0],
            "price": [-50.0],  # Negative price
        })

        with pytest.raises(pa.errors.SchemaError):
            CoreFlightSchema.validate(df)

    def test_negative_time_raises(self) -> None:
        """Test that negative time raises SchemaError."""
        df = pd.DataFrame({
            "departure_airport": ["WAW"],
            "arrival_airport": ["BCN"],
            "dep_time": [-100.0],  # Negative time
            "arr_time": [150.0],
            "price": [50.0],
        })

        with pytest.raises(pa.errors.SchemaError):
            CoreFlightSchema.validate(df)

    def test_coercion_works(self) -> None:
        """Test that coerce=True converts compatible types."""
        df = pd.DataFrame({
            "departure_airport": ["WAW"],
            "arrival_airport": ["BCN"],
            "dep_time": [100],  # int instead of float
            "arr_time": [150],
            "price": [50],
        })

        validated = CoreFlightSchema.validate(df)

        # Should be coerced to float
        assert validated["dep_time"].dtype == float
        assert validated["arr_time"].dtype == float
        assert validated["price"].dtype == float

    def test_empty_df_passes(self) -> None:
        """Test that empty DataFrame with correct columns passes."""
        df = pd.DataFrame({
            "departure_airport": pd.Series([], dtype=str),
            "arrival_airport": pd.Series([], dtype=str),
            "dep_time": pd.Series([], dtype=float),
            "arr_time": pd.Series([], dtype=float),
            "price": pd.Series([], dtype=float),
        })

        validated = CoreFlightSchema.validate(df)
        assert len(validated) == 0


# -------------------------
# ExtendedFlightSchema Tests
# -------------------------


class TestExtendedFlightSchema:
    """Tests for ExtendedFlightSchema validation."""

    def test_valid_extended_df_passes(
        self, valid_extended_flight_df: pd.DataFrame
    ) -> None:
        """Test that valid extended DataFrame passes validation."""
        validated = ExtendedFlightSchema.validate(valid_extended_flight_df)
        assert len(validated) == 2
        assert "carrier_code" in validated.columns
        assert "baggage_included" in validated.columns

    def test_inherits_core_validation(self) -> None:
        """Test that ExtendedFlightSchema enforces core field constraints."""
        df = pd.DataFrame({
            "departure_airport": ["WAW"],
            "arrival_airport": ["BCN"],
            "dep_time": [-100.0],  # Invalid: negative
            "arr_time": [150.0],
            "price": [50.0],
            "carrier_code": ["LO"],
        })

        with pytest.raises(pa.errors.SchemaError):
            ExtendedFlightSchema.validate(df)

    def test_nullable_extended_fields(self, valid_core_flight_df: pd.DataFrame) -> None:
        """Test that extended fields can be None/NaN."""
        # Add extended columns with null values
        df = valid_core_flight_df.copy()
        df["carrier_code"] = [None, "FR", None]
        df["baggage_included"] = [None, 1, None]

        validated = ExtendedFlightSchema.validate(df)

        # Should pass with null values
        assert len(validated) == 3
        assert pd.isna(validated["carrier_code"].iloc[0])
        assert validated["carrier_code"].iloc[1] == "FR"

    def test_core_only_df_passes_extended_validation(
        self, valid_core_flight_df: pd.DataFrame
    ) -> None:
        """
        Test that DataFrame with only core columns passes extended validation.

        This ensures backward compatibility - old data works with new schema.
        """
        validated = ExtendedFlightSchema.validate(valid_core_flight_df)
        assert len(validated) == 3

    def test_extra_columns_preserved_in_extended(self) -> None:
        """Test that extra columns beyond extended schema are preserved."""
        df = pd.DataFrame({
            "departure_airport": ["WAW"],
            "arrival_airport": ["BCN"],
            "dep_time": [100.0],
            "arr_time": [150.0],
            "price": [50.0],
            "carrier_code": ["LO"],
            "future_field": ["some_value"],  # Not in schema
        })

        validated = ExtendedFlightSchema.validate(df)
        assert "future_field" in validated.columns


# -------------------------
# TravelConstraints Tests
# -------------------------


class TestTravelConstraints:
    """Tests for TravelConstraints dataclass."""

    def test_valid_constraints(self) -> None:
        """Test creating valid constraints."""
        constraints = TravelConstraints.create(
            start_city="WAW",
            required_cities={"BCN", "FCO"},
            t_min=0.0,
            t_max=1000.0,
        )

        assert constraints.start_city == "WAW"
        assert constraints.required_cities == frozenset({"BCN", "FCO"})
        assert constraints.t_min == 0.0
        assert constraints.t_max == 1000.0

    def test_empty_start_city_raises(self) -> None:
        """Test that empty start city raises ValueError."""
        with pytest.raises(ValueError, match="start_city cannot be empty"):
            TravelConstraints.create(start_city="", t_min=0, t_max=100)

    def test_invalid_time_range_raises(self) -> None:
        """Test that t_min > t_max raises ValueError."""
        with pytest.raises(ValueError, match="t_min.*must be <= t_max"):
            TravelConstraints.create(
                start_city="WAW",
                t_min=100.0,
                t_max=50.0,  # Invalid: t_min > t_max
            )

    def test_negative_max_stops_raises(self) -> None:
        """Test that negative max_stops raises ValueError."""
        with pytest.raises(ValueError, match="max_stops must be >= 0"):
            TravelConstraints.create(
                start_city="WAW",
                t_min=0,
                t_max=100,
                max_stops=-1,
            )

    def test_negative_max_price_raises(self) -> None:
        """Test that negative max_price raises ValueError."""
        with pytest.raises(ValueError, match="max_price must be >= 0"):
            TravelConstraints.create(
                start_city="WAW",
                t_min=0,
                t_max=100,
                max_price=-100.0,
            )

    def test_constraints_are_immutable(self) -> None:
        """Test that TravelConstraints are frozen (immutable)."""
        constraints = TravelConstraints.create(start_city="WAW", t_min=0, t_max=100)

        with pytest.raises(AttributeError):
            constraints.start_city = "BCN"  # type: ignore

    def test_with_time_window_creates_new_instance(self) -> None:
        """Test that with_time_window creates a new instance."""
        original = TravelConstraints.create(start_city="WAW", t_min=0, t_max=100)
        updated = original.with_time_window(50, 200)

        # Original unchanged
        assert original.t_min == 0
        assert original.t_max == 100

        # New instance has updated values
        assert updated.t_min == 50
        assert updated.t_max == 200
        assert updated.start_city == "WAW"

    def test_required_cities_converted_to_frozenset(self) -> None:
        """Test that mutable set is converted to frozenset."""
        mutable_set = {"BCN", "FCO"}
        constraints = TravelConstraints.create(
            start_city="WAW",
            required_cities=mutable_set,
            t_min=0,
            t_max=100,
        )

        assert isinstance(constraints.required_cities, frozenset)


# -------------------------
# TravelConstraintsSchema Tests
# -------------------------


class TestTravelConstraintsSchema:
    """Tests for TravelConstraintsSchema validation."""

    def test_valid_constraints_df_passes(self) -> None:
        """Test that valid constraints DataFrame passes."""
        df = pd.DataFrame({
            "start_city": ["WAW", "BCN"],
            "t_min": [0.0, 100.0],
            "t_max": [1000.0, 2000.0],
        })

        validated = TravelConstraintsSchema.validate(df)
        assert len(validated) == 2

    def test_invalid_start_city_length_raises(self) -> None:
        """Test that invalid start_city length raises SchemaError."""
        df = pd.DataFrame({
            "start_city": ["W"],  # Too short (min 2)
            "t_min": [0.0],
            "t_max": [1000.0],
        })

        with pytest.raises(pa.errors.SchemaError):
            TravelConstraintsSchema.validate(df)

    def test_extra_columns_preserved(self) -> None:
        """Test that extra columns are preserved."""
        df = pd.DataFrame({
            "start_city": ["WAW"],
            "t_min": [0.0],
            "t_max": [1000.0],
            "custom_field": ["custom_value"],
        })

        validated = TravelConstraintsSchema.validate(df)
        assert "custom_field" in validated.columns


# -------------------------
# RouteSegment Tests
# -------------------------


class TestRouteSegment:
    """Tests for RouteSegment dataclass."""

    def test_segment_duration(self) -> None:
        """Test segment duration calculation."""
        segment = RouteSegment(
            segment_index=0,
            departure_airport="WAW",
            arrival_airport="BCN",
            dep_time=100.0,
            arr_time=250.0,
            price=50.0,
        )

        assert segment.duration == 150.0

    def test_segment_is_immutable(self) -> None:
        """Test that RouteSegment is frozen."""
        segment = RouteSegment(
            segment_index=0,
            departure_airport="WAW",
            arrival_airport="BCN",
            dep_time=100.0,
            arr_time=250.0,
            price=50.0,
        )

        with pytest.raises(AttributeError):
            segment.price = 100.0  # type: ignore


# -------------------------
# RouteResult Tests
# -------------------------


class TestRouteResult:
    """Tests for RouteResult dataclass."""

    @pytest.fixture
    def sample_segments(self) -> list[RouteSegment]:
        """Create sample route segments."""
        return [
            RouteSegment(0, "WAW", "BCN", 100.0, 200.0, 50.0),
            RouteSegment(1, "BCN", "FCO", 250.0, 350.0, 60.0),
            RouteSegment(2, "FCO", "WAW", 400.0, 500.0, 70.0),
        ]

    def test_from_segments_creates_route(
        self, sample_segments: list[RouteSegment]
    ) -> None:
        """Test creating RouteResult from segments."""
        route = RouteResult.from_segments(route_id=1, segments=sample_segments)

        assert route.route_id == 1
        assert route.num_segments == 3
        assert route.start_city == "WAW"
        assert route.end_city == "WAW"

    def test_total_cost_calculation(
        self, sample_segments: list[RouteSegment]
    ) -> None:
        """Test total cost is sum of segment prices."""
        route = RouteResult.from_segments(route_id=1, segments=sample_segments)

        assert route.total_cost == 180.0  # 50 + 60 + 70

    def test_total_time_calculation(
        self, sample_segments: list[RouteSegment]
    ) -> None:
        """Test total time is last arr_time - first dep_time."""
        route = RouteResult.from_segments(route_id=1, segments=sample_segments)

        assert route.total_time == 400.0  # 500 - 100

    def test_route_cities(self, sample_segments: list[RouteSegment]) -> None:
        """Test route_cities returns ordered list."""
        route = RouteResult.from_segments(route_id=1, segments=sample_segments)

        assert route.route_cities == ["WAW", "BCN", "FCO", "WAW"]

    def test_visited_cities(self, sample_segments: list[RouteSegment]) -> None:
        """Test visited_cities returns frozenset."""
        route = RouteResult.from_segments(route_id=1, segments=sample_segments)

        assert route.visited_cities == frozenset({"BCN", "FCO", "WAW"})

    def test_empty_segments_raises(self) -> None:
        """Test that empty segments list raises ValueError."""
        with pytest.raises(ValueError, match="at least one segment"):
            RouteResult.from_segments(route_id=1, segments=[])

    def test_route_is_immutable(self, sample_segments: list[RouteSegment]) -> None:
        """Test that RouteResult is frozen."""
        route = RouteResult.from_segments(route_id=1, segments=sample_segments)

        with pytest.raises(AttributeError):
            route.route_id = 2  # type: ignore


# -------------------------
# RouteSegmentSchema Tests
# -------------------------


class TestRouteSegmentSchema:
    """Tests for RouteSegmentSchema validation."""

    def test_valid_segment_df_passes(self) -> None:
        """Test that valid segment DataFrame passes."""
        df = pd.DataFrame({
            "segment_index": [0, 1, 2],
            "departure_airport": ["WAW", "BCN", "FCO"],
            "arrival_airport": ["BCN", "FCO", "WAW"],
            "dep_time": [100.0, 250.0, 400.0],
            "arr_time": [200.0, 350.0, 500.0],
            "price": [50.0, 60.0, 70.0],
        })

        validated = RouteSegmentSchema.validate(df)
        assert len(validated) == 3

    def test_negative_segment_index_raises(self) -> None:
        """Test that negative segment_index raises SchemaError."""
        df = pd.DataFrame({
            "segment_index": [-1],
            "departure_airport": ["WAW"],
            "arrival_airport": ["BCN"],
            "dep_time": [100.0],
            "arr_time": [200.0],
            "price": [50.0],
        })

        with pytest.raises(pa.errors.SchemaError):
            RouteSegmentSchema.validate(df)


# -------------------------
# RouteResultSchema Tests
# -------------------------


class TestRouteResultSchema:
    """Tests for RouteResultSchema validation."""

    def test_valid_result_df_passes(self) -> None:
        """Test that valid result DataFrame passes."""
        df = pd.DataFrame({
            "route_id": [0, 1],
            "total_cost": [180.0, 200.0],
            "total_time": [400.0, 450.0],
            "num_segments": [3, 4],
            "start_city": ["WAW", "BCN"],
            "end_city": ["WAW", "FCO"],
            "departure_time": [100.0, 150.0],
            "arrival_time": [500.0, 600.0],
        })

        validated = RouteResultSchema.validate(df)
        assert len(validated) == 2

    def test_zero_segments_raises(self) -> None:
        """Test that num_segments < 1 raises SchemaError."""
        df = pd.DataFrame({
            "route_id": [0],
            "total_cost": [0.0],
            "total_time": [0.0],
            "num_segments": [0],  # Invalid: must be >= 1
            "start_city": ["WAW"],
            "end_city": ["WAW"],
            "departure_time": [100.0],
            "arrival_time": [100.0],
        })

        with pytest.raises(pa.errors.SchemaError):
            RouteResultSchema.validate(df)

    def test_extra_columns_preserved(self) -> None:
        """Test that extra columns are preserved."""
        df = pd.DataFrame({
            "route_id": [0],
            "total_cost": [180.0],
            "total_time": [400.0],
            "num_segments": [3],
            "start_city": ["WAW"],
            "end_city": ["WAW"],
            "departure_time": [100.0],
            "arrival_time": [500.0],
            "algorithm_name": ["dijkstra"],  # Extra field
        })

        validated = RouteResultSchema.validate(df)
        assert "algorithm_name" in validated.columns
        assert validated["algorithm_name"].iloc[0] == "dijkstra"
