"""
Algorithm performance benchmarks.

Measures:
- Dijkstra routing latency with varying complexity
- Scaling behavior with number of destinations
- Immutability overhead
"""

import pytest

from src.flight_router.adapters.algorithms.dijkstra_adapter import DijkstraRouteFinder
from src.flight_router.adapters.algorithms.immutability import (
    make_defensive_copy,
    make_immutable,
)
from src.flight_router.adapters.repositories.flight_graph_repo import CachedFlightGraph


class TestAlgorithmLatency:
    """Benchmark core algorithm performance."""

    def test_algorithm_single_destination(
        self,
        benchmark,
        preloaded_graph: CachedFlightGraph,
        route_finder: DijkstraRouteFinder,
    ):
        """
        Benchmark routing: WAW -> {LHR} -> WAW (single destination).

        This is the simplest round-trip case. Uses pedantic mode
        for accurate measurement of long-running operation.
        """
        result = benchmark.pedantic(
            route_finder.find_routes,
            kwargs={
                "graph": preloaded_graph,
                "start_city": "WAW",
                "required_cities": {"LHR"},
                "t_min": 0.0,
                "t_max": float("inf"),
            },
            rounds=3,
            warmup_rounds=1,
        )

        # Sanity check - should find routes
        assert len(result) > 0, "Should find at least one route"
        assert result[0].start_city == "WAW"
        assert result[0].end_city == "WAW"

    def test_algorithm_two_destinations(
        self,
        benchmark,
        preloaded_graph: CachedFlightGraph,
        route_finder: DijkstraRouteFinder,
    ):
        """
        Benchmark routing: WAW -> {LHR, CDG} -> WAW (two destinations).

        More complex search - tests scaling behavior.
        """
        result = benchmark.pedantic(
            route_finder.find_routes,
            kwargs={
                "graph": preloaded_graph,
                "start_city": "WAW",
                "required_cities": {"LHR", "CDG"},
                "t_min": 0.0,
                "t_max": float("inf"),
            },
            rounds=2,
            warmup_rounds=1,
        )

        # May or may not find routes depending on data
        # Just verify it completes

    def test_algorithm_nearby_destination(
        self,
        benchmark,
        preloaded_graph: CachedFlightGraph,
        route_finder: DijkstraRouteFinder,
    ):
        """
        Benchmark routing to a nearby hub (potentially faster).

        WAW -> {VIE} -> WAW (Vienna is close to Warsaw).
        """
        result = benchmark.pedantic(
            route_finder.find_routes,
            kwargs={
                "graph": preloaded_graph,
                "start_city": "WAW",
                "required_cities": {"VIE"},
                "t_min": 0.0,
                "t_max": float("inf"),
            },
            rounds=3,
            warmup_rounds=1,
        )


class TestImmutabilityOverhead:
    """Benchmark overhead of immutability guardrails."""

    def test_make_immutable_overhead(
        self,
        benchmark,
        preloaded_graph: CachedFlightGraph,
    ):
        """
        Measure overhead of setting immutability flags.

        This happens on every request. Should be O(columns) not O(rows).
        """
        # Get a fresh copy to test on
        df = preloaded_graph.flights_df.copy()

        def make_immutable_fresh():
            # Reset flags before each call
            for col in df.columns:
                arr = df[col].values
                if hasattr(arr, "flags"):
                    arr.flags.writeable = True
            make_immutable(df)

        benchmark(make_immutable_fresh)

    def test_make_defensive_copy_overhead(
        self,
        benchmark,
        preloaded_graph: CachedFlightGraph,
    ):
        """
        Measure overhead of defensive copy.

        This is the expensive fallback - O(n) memory and time.
        """
        result = benchmark(make_defensive_copy, preloaded_graph.flights_df)

        # Verify it's a copy
        assert result is not preloaded_graph.flights_df


class TestAlgorithmWithDifferentOrigins:
    """Benchmark algorithm from different starting airports."""

    @pytest.mark.parametrize("origin", ["WAW", "LHR", "CDG", "FRA"])
    def test_algorithm_from_various_origins(
        self,
        benchmark,
        preloaded_graph: CachedFlightGraph,
        route_finder: DijkstraRouteFinder,
        origin: str,
    ):
        """
        Benchmark routing from different major European hubs.

        Tests if performance varies significantly by origin.
        """
        # Skip if origin not in graph
        if origin not in preloaded_graph.airports:
            pytest.skip(f"Airport {origin} not in graph")

        # Find a destination that exists
        for dest in ["LHR", "CDG", "AMS", "FRA", "VIE"]:
            if dest in preloaded_graph.airports and dest != origin:
                break
        else:
            pytest.skip("No suitable destination found")

        result = benchmark.pedantic(
            route_finder.find_routes,
            kwargs={
                "graph": preloaded_graph,
                "start_city": origin,
                "required_cities": {dest},
                "t_min": 0.0,
                "t_max": float("inf"),
            },
            rounds=2,
            warmup_rounds=1,
        )
