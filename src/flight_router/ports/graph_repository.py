"""
Graph Repository port interface.

Defines the caching protocol for flight graphs, designed for
future migration to shared memory backends.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional, Protocol, runtime_checkable

if TYPE_CHECKING:
    from src.flight_router.adapters.repositories.flight_graph_repo import (
        CachedFlightGraph,
    )


class GraphNotInitializedError(Exception):
    """Raised when graph is accessed before first load."""

    pass


@runtime_checkable
class FlightGraphCache(Protocol):
    """
    Protocol for graph caching - designed for SHARED MEMORY migration.

    This protocol defines the interface for all cache backends:
    - Phase 1: InMemoryFlightGraphCache (per-worker, in-process)
    - Phase 2: ArrowPlasmaCache or MmapCache (shared memory, multi-worker)

    IMPORTANT: Interface designed for zero-copy - no serialize/deserialize
    in the hot path. Phase 2 backends read directly from shared memory.

    All implementations must be thread-safe for concurrent access.
    """

    def get(self) -> Optional[CachedFlightGraph]:
        """
        Get cached graph or None if miss.

        Returns:
            Cached graph if available, None otherwise.
            Phase 2: Returns view into shared memory (zero-copy).
        """
        ...

    def set(self, graph: CachedFlightGraph) -> None:
        """
        Store graph in cache.

        Args:
            graph: The CachedFlightGraph to store.
            Phase 2: Writes to shared memory segment.
        """
        ...

    def invalidate(self) -> None:
        """
        Invalidate current cache.

        Clears the cached graph, forcing a refresh on next access.
        """
        ...

    @property
    def is_stale(self) -> bool:
        """
        Check if cache needs refresh.

        Returns:
            True if cache is empty or TTL has expired.
        """
        ...
