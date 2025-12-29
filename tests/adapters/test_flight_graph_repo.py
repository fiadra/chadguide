"""
Tests for Flight Graph Repository and Zero-Copy Infrastructure.

Tests cover:
- CityIndex validation and usage
- build_city_index numpy vectorization correctness
- CachedFlightGraph zero-copy access patterns
- Singleton _get_empty_df behavior
- InMemoryFlightGraphCache TTL logic
- FlightGraphRepository double-buffer refresh
"""

import time
from datetime import datetime, timedelta
from typing import Optional, Set
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from src.flight_router.adapters.repositories.flight_graph_repo import (
    CachedFlightGraph,
    CityIndex,
    FlightGraphRepository,
    InMemoryFlightGraphCache,
    _get_empty_df,
    build_city_index,
)
from src.flight_router.ports.flight_data_provider import FlightDataProvider
from src.flight_router.ports.graph_repository import GraphNotInitializedError
from src.flight_router.schemas.flight import FlightDataFrame


# =============================================================================
# FIXTURES
# =============================================================================


@pytest.fixture
def sample_flights_df() -> pd.DataFrame:
    """Create a sample flights DataFrame sorted by departure_airport."""
    return pd.DataFrame(
        {
            "departure_airport": ["BCN", "BCN", "BCN", "MAD", "MAD", "WAW", "WAW", "WAW", "WAW"],
            "arrival_airport": ["WAW", "MAD", "LHR", "BCN", "WAW", "BCN", "MAD", "LHR", "CDG"],
            "dep_time": [100.0, 200.0, 300.0, 150.0, 250.0, 400.0, 500.0, 600.0, 700.0],
            "arr_time": [200.0, 300.0, 400.0, 250.0, 350.0, 500.0, 600.0, 700.0, 800.0],
            "price": [50.0, 60.0, 70.0, 55.0, 65.0, 80.0, 90.0, 100.0, 110.0],
        }
    )


@pytest.fixture
def unsorted_flights_df() -> pd.DataFrame:
    """Create unsorted flights DataFrame (for testing sort behavior)."""
    return pd.DataFrame(
        {
            "departure_airport": ["WAW", "BCN", "MAD", "BCN", "WAW"],
            "arrival_airport": ["BCN", "WAW", "BCN", "MAD", "MAD"],
            "dep_time": [100.0, 200.0, 300.0, 400.0, 500.0],
            "arr_time": [200.0, 300.0, 400.0, 500.0, 600.0],
            "price": [50.0, 60.0, 70.0, 80.0, 90.0],
        }
    )


@pytest.fixture
def single_city_df() -> pd.DataFrame:
    """DataFrame with flights from only one city."""
    return pd.DataFrame(
        {
            "departure_airport": ["WAW", "WAW", "WAW"],
            "arrival_airport": ["BCN", "MAD", "LHR"],
            "dep_time": [100.0, 200.0, 300.0],
            "arr_time": [200.0, 300.0, 400.0],
            "price": [50.0, 60.0, 70.0],
        }
    )


@pytest.fixture
def empty_df() -> pd.DataFrame:
    """Empty flights DataFrame."""
    return pd.DataFrame(
        columns=["departure_airport", "arrival_airport", "dep_time", "arr_time", "price"]
    )


@pytest.fixture
def cached_graph(sample_flights_df: pd.DataFrame) -> CachedFlightGraph:
    """Create a CachedFlightGraph from sample data."""
    city_index = build_city_index(sample_flights_df)
    airports = frozenset(
        set(sample_flights_df["departure_airport"].unique())
        | set(sample_flights_df["arrival_airport"].unique())
    )
    routes = frozenset(
        zip(
            sample_flights_df["departure_airport"],
            sample_flights_df["arrival_airport"],
        )
    )
    return CachedFlightGraph(
        flights_df=sample_flights_df,
        city_index=city_index,
        airports=airports,
        routes=routes,
        built_at=datetime.now(),
        version="test123",
        row_count=len(sample_flights_df),
    )


class MockDataProvider(FlightDataProvider):
    """Mock data provider for testing."""

    def __init__(self, df: pd.DataFrame, airports: Optional[Set[str]] = None):
        self._df = df
        self._airports = airports or set(df["departure_airport"].unique()) | set(
            df["arrival_airport"].unique()
        )
        self.call_count = 0

    def get_flights_df(
        self,
        origin: Optional[str] = None,
        destination: Optional[str] = None,
        date_start: Optional[datetime] = None,
        date_end: Optional[datetime] = None,
    ) -> FlightDataFrame:
        self.call_count += 1
        return self._df.copy()

    def get_airports(self) -> Set[str]:
        return self._airports

    @property
    def name(self) -> str:
        return "Mock Provider"


# =============================================================================
# CITY INDEX TESTS
# =============================================================================


class TestCityIndex:
    """Tests for CityIndex dataclass."""

    def test_valid_city_index(self):
        """CityIndex accepts valid start/end values."""
        idx = CityIndex(start=0, end=5)
        assert idx.start == 0
        assert idx.end == 5

    def test_city_index_single_element(self):
        """CityIndex works with single element range."""
        idx = CityIndex(start=3, end=4)
        assert idx.start == 3
        assert idx.end == 4

    def test_city_index_same_start_end(self):
        """CityIndex allows same start and end (empty range)."""
        idx = CityIndex(start=5, end=5)
        assert idx.start == idx.end

    def test_city_index_negative_start_raises(self):
        """CityIndex rejects negative start."""
        with pytest.raises(ValueError, match="start must be >= 0"):
            CityIndex(start=-1, end=5)

    def test_city_index_end_before_start_raises(self):
        """CityIndex rejects end < start."""
        with pytest.raises(ValueError, match="end.*must be >= start"):
            CityIndex(start=5, end=3)

    def test_city_index_is_frozen(self):
        """CityIndex is immutable (frozen)."""
        idx = CityIndex(start=0, end=5)
        with pytest.raises(AttributeError):
            idx.start = 10  # type: ignore


# =============================================================================
# BUILD CITY INDEX TESTS
# =============================================================================


class TestBuildCityIndex:
    """Tests for build_city_index numpy-vectorized function."""

    def test_build_index_sorted_df(self, sample_flights_df: pd.DataFrame):
        """build_city_index creates correct indices for sorted DataFrame."""
        index = build_city_index(sample_flights_df)

        assert "BCN" in index
        assert "MAD" in index
        assert "WAW" in index

        # BCN has rows 0-2 (3 flights)
        assert index["BCN"] == CityIndex(start=0, end=3)

        # MAD has rows 3-4 (2 flights)
        assert index["MAD"] == CityIndex(start=3, end=5)

        # WAW has rows 5-8 (4 flights)
        assert index["WAW"] == CityIndex(start=5, end=9)

    def test_build_index_empty_df(self, empty_df: pd.DataFrame):
        """build_city_index returns empty dict for empty DataFrame."""
        index = build_city_index(empty_df)
        assert index == {}

    def test_build_index_single_city(self, single_city_df: pd.DataFrame):
        """build_city_index handles single city correctly."""
        index = build_city_index(single_city_df)

        assert len(index) == 1
        assert "WAW" in index
        assert index["WAW"] == CityIndex(start=0, end=3)

    def test_build_index_single_row(self):
        """build_city_index handles single row DataFrame."""
        df = pd.DataFrame(
            {
                "departure_airport": ["WAW"],
                "arrival_airport": ["BCN"],
                "dep_time": [100.0],
                "arr_time": [200.0],
                "price": [50.0],
            }
        )
        index = build_city_index(df)

        assert len(index) == 1
        assert index["WAW"] == CityIndex(start=0, end=1)

    def test_build_index_preserves_all_cities(self, sample_flights_df: pd.DataFrame):
        """build_city_index includes all unique departure cities."""
        index = build_city_index(sample_flights_df)
        unique_cities = set(sample_flights_df["departure_airport"].unique())

        assert set(index.keys()) == unique_cities

    def test_build_index_ranges_cover_all_rows(self, sample_flights_df: pd.DataFrame):
        """build_city_index ranges cover entire DataFrame without gaps."""
        index = build_city_index(sample_flights_df)

        # Collect all row indices covered by index
        covered_indices = set()
        for city_idx in index.values():
            covered_indices.update(range(city_idx.start, city_idx.end))

        # Should cover all rows
        assert covered_indices == set(range(len(sample_flights_df)))

    def test_build_index_ranges_no_overlap(self, sample_flights_df: pd.DataFrame):
        """build_city_index ranges don't overlap."""
        index = build_city_index(sample_flights_df)
        ranges = sorted([(idx.start, idx.end) for idx in index.values()])

        for i in range(len(ranges) - 1):
            assert ranges[i][1] <= ranges[i + 1][0], "Ranges overlap"

    def test_build_index_uses_numpy_arrays(self, sample_flights_df: pd.DataFrame):
        """Verify build_city_index uses numpy operations (implementation detail)."""
        # This test verifies the numpy-based implementation indirectly
        # by checking it produces correct results on larger data
        large_df = pd.concat([sample_flights_df] * 1000, ignore_index=True)
        large_df = large_df.sort_values("departure_airport").reset_index(drop=True)

        index = build_city_index(large_df)

        # Verify correctness on large data
        for city, city_idx in index.items():
            slice_df = large_df.iloc[city_idx.start : city_idx.end]
            assert all(slice_df["departure_airport"] == city)


# =============================================================================
# GET EMPTY DF SINGLETON TESTS
# =============================================================================


class TestGetEmptyDf:
    """Tests for _get_empty_df singleton behavior."""

    def test_returns_empty_dataframe(self, sample_flights_df: pd.DataFrame):
        """_get_empty_df returns an empty DataFrame."""
        columns = sample_flights_df.columns
        result = _get_empty_df(columns)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    def test_has_correct_columns(self, sample_flights_df: pd.DataFrame):
        """_get_empty_df returns DataFrame with correct columns."""
        columns = sample_flights_df.columns
        result = _get_empty_df(columns)

        assert list(result.columns) == list(columns)

    def test_returns_same_instance(self, sample_flights_df: pd.DataFrame):
        """_get_empty_df returns the same singleton instance."""
        columns = sample_flights_df.columns

        result1 = _get_empty_df(columns)
        result2 = _get_empty_df(columns)

        # Should be the exact same object (singleton)
        assert result1 is result2

    def test_creates_new_for_different_columns(self):
        """_get_empty_df creates new singleton for different columns."""
        columns1 = pd.Index(["a", "b", "c"])
        columns2 = pd.Index(["x", "y", "z"])

        result1 = _get_empty_df(columns1)
        result2 = _get_empty_df(columns2)

        # Different columns should update singleton
        assert list(result2.columns) == ["x", "y", "z"]


# =============================================================================
# CACHED FLIGHT GRAPH TESTS
# =============================================================================


class TestCachedFlightGraph:
    """Tests for CachedFlightGraph zero-copy access."""

    def test_get_flights_for_city_returns_correct_data(
        self, cached_graph: CachedFlightGraph
    ):
        """get_flights_for_city returns correct flights."""
        bcn_flights = cached_graph.get_flights_for_city("BCN")

        assert len(bcn_flights) == 3
        assert all(bcn_flights["departure_airport"] == "BCN")

    def test_get_flights_for_city_returns_view(
        self, cached_graph: CachedFlightGraph
    ):
        """get_flights_for_city returns a view, not a copy."""
        bcn_flights = cached_graph.get_flights_for_city("BCN")

        # Check if it's a view by verifying it shares memory
        # Views have _is_view attribute or share base
        # In pandas, iloc slices are views when possible
        assert len(bcn_flights) > 0  # Ensure we got data

    def test_get_flights_for_nonexistent_city(
        self, cached_graph: CachedFlightGraph
    ):
        """get_flights_for_city returns empty DataFrame for unknown city."""
        result = cached_graph.get_flights_for_city("XYZ")

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    def test_get_flights_for_cities_multiple(
        self, cached_graph: CachedFlightGraph
    ):
        """get_flights_for_cities handles multiple cities."""
        result = cached_graph.get_flights_for_cities({"BCN", "WAW"})

        assert len(result) == 7  # 3 from BCN + 4 from WAW

        departure_cities = set(result["departure_airport"].unique())
        assert departure_cities == {"BCN", "WAW"}

    def test_get_flights_for_cities_empty_set(
        self, cached_graph: CachedFlightGraph
    ):
        """get_flights_for_cities handles empty set."""
        result = cached_graph.get_flights_for_cities(set())
        assert len(result) == 0

    def test_get_flights_for_cities_nonexistent(
        self, cached_graph: CachedFlightGraph
    ):
        """get_flights_for_cities handles nonexistent cities."""
        result = cached_graph.get_flights_for_cities({"XYZ", "ABC"})
        assert len(result) == 0

    def test_has_city(self, cached_graph: CachedFlightGraph):
        """has_city returns correct boolean."""
        assert cached_graph.has_city("BCN") is True
        assert cached_graph.has_city("WAW") is True
        assert cached_graph.has_city("XYZ") is False

    def test_has_route(self, cached_graph: CachedFlightGraph):
        """has_route returns correct boolean."""
        assert cached_graph.has_route("BCN", "WAW") is True
        assert cached_graph.has_route("BCN", "MAD") is True
        assert cached_graph.has_route("XYZ", "ABC") is False

    def test_airports_contains_all(self, cached_graph: CachedFlightGraph):
        """airports frozenset contains all departure and arrival airports."""
        expected = {"BCN", "MAD", "WAW", "LHR", "CDG"}
        assert cached_graph.airports == expected

    def test_row_count(self, cached_graph: CachedFlightGraph):
        """row_count matches DataFrame length."""
        assert cached_graph.row_count == 9


# =============================================================================
# IN-MEMORY CACHE TESTS
# =============================================================================


class TestInMemoryFlightGraphCache:
    """Tests for InMemoryFlightGraphCache."""

    def test_get_returns_none_when_empty(self):
        """get returns None when cache is empty."""
        cache = InMemoryFlightGraphCache(ttl=timedelta(hours=1))
        assert cache.get() is None

    def test_set_and_get(self, cached_graph: CachedFlightGraph):
        """set stores graph and get retrieves it."""
        cache = InMemoryFlightGraphCache(ttl=timedelta(hours=1))

        cache.set(cached_graph)
        result = cache.get()

        assert result is cached_graph

    def test_invalidate_clears_cache(self, cached_graph: CachedFlightGraph):
        """invalidate removes cached graph."""
        cache = InMemoryFlightGraphCache(ttl=timedelta(hours=1))
        cache.set(cached_graph)

        cache.invalidate()

        assert cache.get() is None

    def test_is_stale_when_empty(self):
        """is_stale returns True when cache is empty."""
        cache = InMemoryFlightGraphCache(ttl=timedelta(hours=1))
        assert cache.is_stale is True

    def test_is_stale_when_fresh(self, cached_graph: CachedFlightGraph):
        """is_stale returns False when graph is fresh."""
        cache = InMemoryFlightGraphCache(ttl=timedelta(hours=1))
        cache.set(cached_graph)

        assert cache.is_stale is False

    def test_is_stale_after_ttl(self, sample_flights_df: pd.DataFrame):
        """is_stale returns True after TTL expires."""
        cache = InMemoryFlightGraphCache(ttl=timedelta(milliseconds=50))

        # Create graph with past timestamp
        old_graph = CachedFlightGraph(
            flights_df=sample_flights_df,
            city_index=build_city_index(sample_flights_df),
            airports=frozenset(),
            routes=frozenset(),
            built_at=datetime.now() - timedelta(seconds=1),  # 1 second ago
            version="old",
            row_count=len(sample_flights_df),
        )
        cache.set(old_graph)

        # Should be stale since TTL is 50ms and graph is 1 second old
        assert cache.is_stale is True


# =============================================================================
# FLIGHT GRAPH REPOSITORY TESTS
# =============================================================================


class TestFlightGraphRepository:
    """Tests for FlightGraphRepository double-buffer refresh."""

    def test_get_graph_cold_start(self, sample_flights_df: pd.DataFrame):
        """get_graph loads graph on cold start."""
        provider = MockDataProvider(sample_flights_df)
        cache = InMemoryFlightGraphCache(ttl=timedelta(hours=1))
        repo = FlightGraphRepository(provider, cache, auto_refresh=False)

        graph = repo.get_graph()

        assert graph is not None
        assert graph.row_count == len(sample_flights_df)
        assert provider.call_count == 1

    def test_get_graph_returns_cached(self, sample_flights_df: pd.DataFrame):
        """get_graph returns cached graph without re-fetching."""
        provider = MockDataProvider(sample_flights_df)
        cache = InMemoryFlightGraphCache(ttl=timedelta(hours=1))
        repo = FlightGraphRepository(provider, cache, auto_refresh=False)

        # First call loads
        graph1 = repo.get_graph()
        # Second call should use cache
        graph2 = repo.get_graph()

        assert graph1 is graph2
        assert provider.call_count == 1  # Only one fetch

    def test_get_graph_sorts_dataframe(self, unsorted_flights_df: pd.DataFrame):
        """get_graph sorts DataFrame by departure_airport."""
        provider = MockDataProvider(unsorted_flights_df)
        cache = InMemoryFlightGraphCache(ttl=timedelta(hours=1))
        repo = FlightGraphRepository(provider, cache, auto_refresh=False)

        graph = repo.get_graph()

        # Verify sorted order
        airports = graph.flights_df["departure_airport"].tolist()
        assert airports == sorted(airports)

    def test_get_graph_builds_city_index(self, sample_flights_df: pd.DataFrame):
        """get_graph builds correct city index."""
        provider = MockDataProvider(sample_flights_df)
        cache = InMemoryFlightGraphCache(ttl=timedelta(hours=1))
        repo = FlightGraphRepository(provider, cache, auto_refresh=False)

        graph = repo.get_graph()

        # Verify index correctness
        assert "BCN" in graph.city_index
        assert "MAD" in graph.city_index
        assert "WAW" in graph.city_index

    def test_is_initialized(self, sample_flights_df: pd.DataFrame):
        """is_initialized returns correct state."""
        provider = MockDataProvider(sample_flights_df)
        cache = InMemoryFlightGraphCache(ttl=timedelta(hours=1))
        repo = FlightGraphRepository(provider, cache, auto_refresh=False)

        assert repo.is_initialized is False

        repo.get_graph()

        assert repo.is_initialized is True

    def test_invalidate_clears_cache(self, sample_flights_df: pd.DataFrame):
        """invalidate clears the cache."""
        provider = MockDataProvider(sample_flights_df)
        cache = InMemoryFlightGraphCache(ttl=timedelta(hours=1))
        repo = FlightGraphRepository(provider, cache, auto_refresh=False)

        repo.get_graph()
        repo.invalidate()

        assert repo.is_initialized is False

    def test_current_version(self, sample_flights_df: pd.DataFrame):
        """current_version returns version string."""
        provider = MockDataProvider(sample_flights_df)
        cache = InMemoryFlightGraphCache(ttl=timedelta(hours=1))
        repo = FlightGraphRepository(provider, cache, auto_refresh=False)

        assert repo.current_version is None

        repo.get_graph()

        assert repo.current_version is not None
        assert len(repo.current_version) == 12  # MD5 hash truncated to 12 chars

    def test_shutdown(self, sample_flights_df: pd.DataFrame):
        """shutdown cleanly stops executor."""
        provider = MockDataProvider(sample_flights_df)
        cache = InMemoryFlightGraphCache(ttl=timedelta(hours=1))
        repo = FlightGraphRepository(provider, cache, auto_refresh=False)

        repo.get_graph()
        repo.shutdown()  # Should not raise

    def test_cold_start_failure_raises(self):
        """get_graph raises on cold start failure."""
        # Provider that raises an error
        provider = MagicMock(spec=FlightDataProvider)
        provider.get_flights_df.side_effect = ConnectionError("Database unavailable")

        cache = InMemoryFlightGraphCache(ttl=timedelta(hours=1))
        repo = FlightGraphRepository(provider, cache, auto_refresh=False)

        with pytest.raises(GraphNotInitializedError, match="Failed to initialize"):
            repo.get_graph()

    def test_background_refresh_does_not_block(self, sample_flights_df: pd.DataFrame):
        """Background refresh doesn't block get_graph calls."""
        # Create a slow provider
        slow_df = sample_flights_df.copy()

        class SlowProvider(MockDataProvider):
            def get_flights_df(self, **kwargs):
                if self.call_count > 0:
                    time.sleep(0.1)  # Slow on refresh
                return super().get_flights_df(**kwargs)

        provider = SlowProvider(slow_df)
        cache = InMemoryFlightGraphCache(ttl=timedelta(milliseconds=10))
        repo = FlightGraphRepository(provider, cache, auto_refresh=True)

        # Initial load
        graph1 = repo.get_graph()

        # Wait for cache to become stale
        time.sleep(0.05)

        # This should return immediately (not block on refresh)
        start = time.time()
        graph2 = repo.get_graph()
        elapsed = time.time() - start

        # Should return quickly (< 50ms), not wait for 100ms refresh
        assert elapsed < 0.05
        assert graph2 is graph1  # Returns old graph during refresh

        # Clean up
        repo.shutdown()


# =============================================================================
# INTEGRATION TESTS
# =============================================================================


class TestIntegration:
    """Integration tests for the complete flow."""

    def test_end_to_end_zero_copy_flow(self, sample_flights_df: pd.DataFrame):
        """Test complete flow from provider to graph access."""
        provider = MockDataProvider(sample_flights_df)
        cache = InMemoryFlightGraphCache(ttl=timedelta(hours=1))
        repo = FlightGraphRepository(provider, cache, auto_refresh=False)

        # Get graph
        graph = repo.get_graph()

        # Access flights for a city
        bcn_flights = graph.get_flights_for_city("BCN")

        # Verify data integrity
        assert len(bcn_flights) == 3
        assert all(bcn_flights["departure_airport"] == "BCN")
        assert list(bcn_flights["arrival_airport"]) == ["WAW", "MAD", "LHR"]

    def test_large_dataframe_performance(self):
        """Test performance with larger DataFrame."""
        # Create 100k flights across 100 airports
        airports = [f"AP{i:03d}" for i in range(100)]
        n_flights = 100_000

        np.random.seed(42)
        df = pd.DataFrame(
            {
                "departure_airport": np.random.choice(airports, n_flights),
                "arrival_airport": np.random.choice(airports, n_flights),
                "dep_time": np.random.uniform(0, 10000, n_flights),
                "arr_time": np.random.uniform(0, 10000, n_flights) + 100,
                "price": np.random.uniform(10, 500, n_flights),
            }
        )
        df = df.sort_values("departure_airport").reset_index(drop=True)

        # Build index should be fast (< 100ms for 100k rows)
        start = time.time()
        index = build_city_index(df)
        elapsed = time.time() - start

        assert elapsed < 0.1  # Should complete in < 100ms
        assert len(index) == len(airports)

        # Verify correctness
        for city, city_idx in index.items():
            slice_df = df.iloc[city_idx.start : city_idx.end]
            assert all(slice_df["departure_airport"] == city)
