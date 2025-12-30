"""
Cache access benchmarks.

Measures:
- Cold start latency (first graph load)
- Warm cache hit latency (should be <1ms)
- Cache miss behavior
"""

from datetime import timedelta
from pathlib import Path

import pytest

from src.flight_router.adapters.data_providers.duffel_provider import DuffelDataProvider
from src.flight_router.adapters.repositories.flight_graph_repo import (
    FlightGraphRepository,
    InMemoryFlightGraphCache,
)


class TestCacheAccess:
    """Benchmark cache access patterns."""

    def test_warm_cache_access(self, benchmark, preloaded_repo):
        """
        Measure cache hit latency.

        Expected: O(1) operation, <1ms.
        This is the critical hot path - every request hits this.
        """
        result = benchmark(preloaded_repo.get_graph)

        # Sanity check
        assert result is not None
        assert result.row_count > 0

    def test_warm_cache_access_repeated(self, benchmark, preloaded_repo):
        """
        Measure repeated cache hits to verify consistency.

        Multiple iterations should show consistent low latency.
        """
        def get_graph_10_times():
            for _ in range(10):
                preloaded_repo.get_graph()

        benchmark(get_graph_10_times)


class TestColdStart:
    """Benchmark cold start behavior."""

    def test_cold_start_latency(self, benchmark, db_path):
        """
        Measure first graph load (includes DB + sort + index build).

        This is the startup latency users experience on first request.
        Uses pedantic mode with fewer rounds since cold start is expensive.
        """
        def cold_start():
            provider = DuffelDataProvider(db_path=str(db_path))
            cache = InMemoryFlightGraphCache(ttl=timedelta(hours=1))
            repo = FlightGraphRepository(provider, cache, auto_refresh=False)
            try:
                graph = repo.get_graph()
                return graph
            finally:
                repo.shutdown()
                provider.close()

        result = benchmark.pedantic(
            cold_start,
            rounds=3,  # Fewer rounds - cold start is expensive
            warmup_rounds=0,  # No warmup for cold start measurement
        )

        # Sanity checks
        assert result is not None
        assert result.row_count > 100_000  # Real DB should have many flights


class TestCacheInvalidation:
    """Benchmark cache invalidation patterns."""

    def test_cache_set_overhead(self, benchmark, preloaded_graph):
        """
        Measure overhead of storing a graph in cache.

        This happens during background refresh.
        """
        cache = InMemoryFlightGraphCache(ttl=timedelta(hours=1))

        benchmark(cache.set, preloaded_graph)

    def test_cache_invalidate_overhead(self, benchmark):
        """
        Measure overhead of cache invalidation.

        Should be O(1) - just setting pointer to None.
        """
        cache = InMemoryFlightGraphCache(ttl=timedelta(hours=1))

        benchmark(cache.invalidate)
