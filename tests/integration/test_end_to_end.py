"""
End-to-End Integration Tests for Flight Router.

These tests validate the complete stack using the real Duffel database:
- DuffelDataProvider (SQL to DataFrame)
- FlightGraphRepository (cached graph with index)
- DijkstraRouteFinder (algorithm adapter)
- RouteFinderService (orchestration)
- FindOptimalRoutes (public API)

Requirements:
- Duffel_api/flights.db must exist with flight data
- Tests verify actual routes and data integrity
"""

from datetime import datetime, timedelta
from pathlib import Path

import pytest

from src.flight_router.adapters.algorithms.dijkstra_adapter import DijkstraRouteFinder
from src.flight_router.adapters.data_providers.duffel_provider import (
    EPOCH_REFERENCE,
    DuffelDataProvider,
)
from src.flight_router.adapters.repositories.flight_graph_repo import (
    FlightGraphRepository,
    InMemoryFlightGraphCache,
)
from src.flight_router.application.find_optimal_routes import FindOptimalRoutes
from src.flight_router.schemas.route import RouteResult
from src.flight_router.services.route_finder_service import RouteFinderService

# Path to real database
DB_PATH = Path("Duffel_api/flights.db")


@pytest.fixture(scope="module")
def db_available() -> bool:
    """Check if database is available for integration tests."""
    return DB_PATH.exists()


@pytest.fixture(scope="module")
def data_provider(db_available):
    """Create DuffelDataProvider with real database."""
    if not db_available:
        pytest.skip("Database not available: Duffel_api/flights.db")
    provider = DuffelDataProvider(db_path=str(DB_PATH))
    yield provider
    provider.close()


@pytest.fixture(scope="module")
def graph_repo(data_provider):
    """Create FlightGraphRepository with in-memory cache."""
    cache = InMemoryFlightGraphCache(ttl=timedelta(hours=1))
    repo = FlightGraphRepository(
        data_provider=data_provider,
        cache=cache,
        auto_refresh=False,  # Disable auto-refresh for predictable tests
    )
    yield repo
    repo.shutdown()


@pytest.fixture(scope="module")
def route_service(graph_repo):
    """Create RouteFinderService with Dijkstra algorithm."""
    route_finder = DijkstraRouteFinder(require_defensive_copy=False)
    return RouteFinderService(graph_repo=graph_repo, route_finder=route_finder)


@pytest.fixture(scope="module")
def router(db_available):
    """Create FindOptimalRoutes public API."""
    if not db_available:
        pytest.skip("Database not available: Duffel_api/flights.db")
    router = FindOptimalRoutes(db_path=str(DB_PATH))
    yield router
    router.shutdown()


# =============================================================================
# DATA PROVIDER TESTS
# =============================================================================


class TestDuffelDataProviderIntegration:
    """Integration tests for DuffelDataProvider with real database."""

    def test_loads_flights_from_real_database(self, data_provider):
        """Should load flights from the real Duffel database."""
        df = data_provider.get_flights_df()

        assert len(df) > 0, "Database should contain flights"
        assert "departure_airport" in df.columns
        assert "arrival_airport" in df.columns
        assert "dep_time" in df.columns
        assert "arr_time" in df.columns
        assert "price" in df.columns

    def test_filters_by_origin(self, data_provider):
        """Should filter flights by origin airport."""
        df = data_provider.get_flights_df(origin="WAW")

        assert len(df) > 0, "WAW should have outgoing flights"
        assert (df["departure_airport"] == "WAW").all()

    def test_filters_by_destination(self, data_provider):
        """Should filter flights by destination airport."""
        df = data_provider.get_flights_df(destination="LHR")

        assert len(df) > 0, "LHR should have incoming flights"
        assert (df["arrival_airport"] == "LHR").all()

    def test_returns_airports(self, data_provider):
        """Should return set of all airports."""
        airports = data_provider.get_airports()

        assert len(airports) > 50, "Should have many airports"
        assert "WAW" in airports
        assert "LHR" in airports

    def test_dep_time_is_actual_timestamp(self, data_provider):
        """Should use actual departure timestamps, not hardcoded times."""
        df = data_provider.get_flights_df(origin="WAW")

        # Departure times should vary (not all at same time of day)
        dep_times = df["dep_time"].values

        # Get time-of-day variation (minutes within a day)
        day_minutes = dep_times % (24 * 60)
        unique_times = len(set(day_minutes.astype(int)))

        assert unique_times > 1, (
            "Departure times should vary throughout the day, "
            "indicating actual timestamps are used"
        )


# =============================================================================
# GRAPH REPOSITORY TESTS
# =============================================================================


class TestFlightGraphRepositoryIntegration:
    """Integration tests for FlightGraphRepository with real data."""

    def test_builds_graph_from_real_data(self, graph_repo):
        """Should build CachedFlightGraph from real database."""
        graph = graph_repo.get_graph()

        assert graph.row_count > 0
        assert len(graph.airports) > 50
        assert len(graph.city_index) > 0

    def test_index_provides_zero_copy_access(self, graph_repo):
        """City index should enable fast lookups."""
        graph = graph_repo.get_graph()

        # Get flights for WAW
        waw_flights = graph.get_flights_for_city("WAW")

        assert len(waw_flights) > 0
        assert (waw_flights["departure_airport"] == "WAW").all()

    def test_graph_contains_real_routes(self, graph_repo):
        """Graph should contain routes between major airports."""
        graph = graph_repo.get_graph()

        # WAW -> LHR should exist (common route)
        assert graph.has_city("WAW")
        assert graph.has_city("LHR")
        # Check at least some connectivity exists
        assert len(graph.routes) > 100


# =============================================================================
# ROUTE FINDER SERVICE TESTS
# =============================================================================


class TestRouteFinderServiceIntegration:
    """Integration tests for RouteFinderService with real algorithm."""

    def test_finds_routes_from_waw(self, route_service):
        """Should find round-trip routes starting and ending at Warsaw."""
        # Use wide time window to ensure we find flights
        # Note: The dijkstra algorithm finds ROUND TRIPS (start -> required -> start)
        results = route_service.find_optimal_routes(
            start_city="WAW",
            required_cities={"LHR"},
            t_min=0.0,
            t_max=float("inf"),
        )

        assert len(results) > 0, "Should find at least one route WAW -> LHR -> WAW"

        # Verify result structure (round trip: starts and ends at WAW)
        route = results[0]
        assert isinstance(route, RouteResult)
        assert route.start_city == "WAW"
        assert route.end_city == "WAW"  # Round trip returns to start
        assert "LHR" in route.visited_cities  # Required city was visited
        assert route.total_cost > 0
        assert route.num_segments >= 2  # At least: WAW->LHR, LHR->WAW

    def test_route_times_are_consistent(self, route_service):
        """Route times should be logically consistent."""
        results = route_service.find_optimal_routes(
            start_city="WAW",
            required_cities={"LHR"},
            t_min=0.0,
            t_max=float("inf"),
        )

        for route in results:
            # Arrival time should be after departure time
            assert route.arrival_time > route.departure_time

            # Each segment's arrival should be after its departure
            for segment in route.segments:
                assert segment.arr_time > segment.dep_time

            # For multi-segment routes, connections should be valid
            for i in range(len(route.segments) - 1):
                current = route.segments[i]
                next_seg = route.segments[i + 1]
                assert next_seg.dep_time >= current.arr_time, (
                    f"Connection invalid: arrives {current.arr_time}, "
                    f"departs {next_seg.dep_time}"
                )

    def test_max_stops_filter(self, route_service):
        """Should filter results by max_stops."""
        # Get all results
        all_results = route_service.find_optimal_routes(
            start_city="WAW",
            required_cities={"LHR"},
            t_min=0.0,
            t_max=float("inf"),
        )

        # Get only direct flights (0 stops = 1 segment)
        direct_only = route_service.find_optimal_routes(
            start_city="WAW",
            required_cities={"LHR"},
            t_min=0.0,
            t_max=float("inf"),
            max_stops=0,
        )

        # All direct flights should have exactly 1 segment
        for route in direct_only:
            assert route.num_segments == 1, "Direct flight should have 1 segment"

        # Direct flights should be subset of all results
        assert len(direct_only) <= len(all_results)


# =============================================================================
# PUBLIC API TESTS (FindOptimalRoutes)
# =============================================================================


class TestFindOptimalRoutesIntegration:
    """Integration tests for the public API."""

    def test_basic_search(self, router):
        """Should find round-trip routes using the public API."""
        # Note: The dijkstra algorithm finds ROUND TRIPS (start -> destinations -> start)
        results = router.search(origin="WAW", destinations={"LHR"})

        assert len(results) > 0
        assert results[0].start_city == "WAW"
        assert results[0].end_city == "WAW"  # Round trip returns to origin
        assert "LHR" in results[0].visited_cities  # Destination was visited

    def test_search_with_date_constraint(self, router):
        """Should filter by departure date."""
        # Use a date in the future (database has 2026 data)
        results = router.search(
            origin="WAW",
            destinations={"LHR"},
            departure_date=datetime(2026, 7, 1),
        )

        assert len(results) > 0

        # All results should depart after the specified date
        min_time = router.datetime_to_epoch_minutes(datetime(2026, 7, 1))
        for route in results:
            assert route.departure_time >= min_time

    def test_search_raw_interface(self, router):
        """Should work with raw epoch-based time parameters."""
        results = router.search_raw(
            start_city="WAW",
            required_cities={"LHR"},
            t_min=0.0,
            t_max=float("inf"),
        )

        assert len(results) > 0

    def test_get_available_airports(self, router):
        """Should return available airports."""
        airports = router.get_available_airports()

        assert "WAW" in airports
        assert "LHR" in airports
        assert len(airports) > 50

    def test_has_route_check(self, router):
        """Should check if direct route exists."""
        # WAW -> LHR should exist
        assert router.has_route("WAW", "LHR") is True

        # Unlikely direct route
        # (Note: may need to adjust based on actual data)
        result = router.has_route("WAW", "WAW")  # Self-loop unlikely
        assert result is False

    def test_datetime_conversion_utilities(self, router):
        """Should convert between datetime and epoch minutes."""
        dt = datetime(2026, 6, 15, 10, 30)

        # Round-trip conversion
        minutes = router.datetime_to_epoch_minutes(dt)
        converted_back = router.epoch_minutes_to_datetime(minutes)

        assert converted_back == dt

    def test_context_manager_usage(self, db_available):
        """Should work as context manager for clean shutdown."""
        if not db_available:
            pytest.skip("Database not available")

        with FindOptimalRoutes(db_path=str(DB_PATH)) as router:
            results = router.search(origin="WAW", destinations={"LHR"})
            assert len(results) > 0
        # Router should be shut down after exiting context

    def test_is_ready_property(self, router):
        """Should report ready status after initialization."""
        # First access triggers graph load
        _ = router.get_available_airports()

        assert router.is_ready is True

    def test_algorithm_name_property(self, router):
        """Should report algorithm name."""
        assert router.algorithm_name == "Multi-Criteria Dijkstra"


# =============================================================================
# DATA INTEGRITY TESTS
# =============================================================================


class TestDataIntegrity:
    """Tests verifying data integrity across the stack."""

    def test_prices_are_positive(self, router):
        """All route prices should be positive."""
        results = router.search(origin="WAW", destinations={"LHR"})

        for route in results:
            assert route.total_cost > 0
            for segment in route.segments:
                assert segment.price > 0

    def test_times_are_in_expected_range(self, router):
        """Times should be in reasonable range (not negative, not ancient)."""
        results = router.search(origin="WAW", destinations={"LHR"})

        # Epoch reference is 2024-01-01
        min_reasonable_time = 0  # At or after epoch
        max_reasonable_time = 365 * 24 * 60 * 5  # Within 5 years of epoch

        for route in results:
            assert route.departure_time >= min_reasonable_time
            assert route.departure_time <= max_reasonable_time
            assert route.arrival_time >= min_reasonable_time
            assert route.arrival_time <= max_reasonable_time

    def test_route_cities_are_valid_iata_codes(self, router):
        """All airport codes should be valid IATA format."""
        results = router.search(origin="WAW", destinations={"LHR"})

        for route in results:
            for city in route.route_cities:
                assert len(city) == 3, f"IATA code should be 3 chars: {city}"
                assert city.isupper(), f"IATA code should be uppercase: {city}"
                assert city.isalpha(), f"IATA code should be letters: {city}"

    def test_segment_airports_are_contiguous(self, router):
        """Adjacent segments should share an airport."""
        results = router.search(
            origin="WAW",
            destinations={"LHR"},
            max_stops=5,  # Allow connections to test contiguity
        )

        for route in results:
            segments = route.segments
            for i in range(len(segments) - 1):
                current = segments[i]
                next_seg = segments[i + 1]
                assert current.arrival_airport == next_seg.departure_airport, (
                    f"Segments not contiguous: {current.arrival_airport} != "
                    f"{next_seg.departure_airport}"
                )
