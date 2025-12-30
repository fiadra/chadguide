"""
Index building benchmarks.

Measures:
- build_city_index scaling behavior (should be O(n))
- Varying dataset sizes to verify linear scaling
"""

import pytest

from src.flight_router.adapters.repositories.flight_graph_repo import build_city_index


class TestIndexBuildingScaling:
    """Benchmark index building with varying data sizes."""

    def test_build_city_index_10k(
        self,
        benchmark,
        synthetic_flights_10k,
    ):
        """
        Benchmark index building with 10,000 flights.

        Smallest dataset - establishes baseline.
        """
        result = benchmark(build_city_index, synthetic_flights_10k)

        # Sanity check - should have airports
        assert len(result) > 0, "Index should contain airports"

    def test_build_city_index_50k(
        self,
        benchmark,
        synthetic_flights_50k,
    ):
        """
        Benchmark index building with 50,000 flights.

        5x baseline - time should scale ~linearly.
        """
        result = benchmark(build_city_index, synthetic_flights_50k)

        assert len(result) > 0

    def test_build_city_index_100k(
        self,
        benchmark,
        synthetic_flights_100k,
    ):
        """
        Benchmark index building with 100,000 flights.

        10x baseline - verifies O(n) scaling.
        """
        result = benchmark(build_city_index, synthetic_flights_100k)

        assert len(result) > 0

    def test_build_city_index_250k(
        self,
        benchmark,
        synthetic_flights_250k,
    ):
        """
        Benchmark index building with 250,000 flights.

        Production-scale dataset (~250k flights in real DB).
        """
        result = benchmark(build_city_index, synthetic_flights_250k)

        assert len(result) > 0


class TestIndexAccess:
    """Benchmark index-based data access patterns."""

    def test_get_flights_for_single_city(
        self,
        benchmark,
        preloaded_graph,
    ):
        """
        Benchmark O(1) city lookup via CityIndex.

        This is the hot path used by the algorithm.
        """
        # Pick a city we know exists
        city = "WAW" if "WAW" in preloaded_graph.airports else next(iter(preloaded_graph.airports))

        result = benchmark(preloaded_graph.get_flights_for_city, city)

        # Should return a DataFrame view
        assert result is not None

    def test_get_flights_for_missing_city(
        self,
        benchmark,
        preloaded_graph,
    ):
        """
        Benchmark cache miss (city not in graph).

        Should return singleton empty DataFrame (no allocation).
        """
        result = benchmark(preloaded_graph.get_flights_for_city, "XXX")

        # Should return empty DataFrame
        assert len(result) == 0
