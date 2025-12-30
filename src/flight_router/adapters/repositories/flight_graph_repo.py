"""
Flight Graph Repository - Zero-Copy Cached Graph Infrastructure.

Implements high-performance flight data caching with:
- Zero-copy index-based access via CityIndex
- Numpy-vectorized index building (GIL-free)
- Double-buffer background refresh (zero-downtime)
- Singleton empty DataFrame (zero GC overhead)
"""

from __future__ import annotations

import hashlib
import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Dict, Optional, Tuple

import numpy as np
import pandas as pd

if TYPE_CHECKING:
    from src.flight_router.ports.flight_data_provider import FlightDataProvider

from src.flight_router.ports.graph_repository import GraphNotInitializedError

logger = logging.getLogger(__name__)


# =============================================================================
# SINGLETON: Avoid allocating empty DataFrame on every cache miss
# =============================================================================

_EMPTY_FLIGHTS_DF: Optional[pd.DataFrame] = None


def _get_empty_df(columns: pd.Index) -> pd.DataFrame:
    """
    Return cached empty DataFrame singleton (zero GC overhead).

    Instead of creating a new empty DataFrame every time a city lookup
    misses, we return a singleton. This eliminates allocation overhead
    for cache misses.

    Args:
        columns: Column index to use if creating new singleton.

    Returns:
        Empty DataFrame with matching columns.
    """
    global _EMPTY_FLIGHTS_DF
    if _EMPTY_FLIGHTS_DF is None or not _EMPTY_FLIGHTS_DF.columns.equals(columns):
        _EMPTY_FLIGHTS_DF = pd.DataFrame(columns=columns)
    return _EMPTY_FLIGHTS_DF


# =============================================================================
# CITY INDEX: Zero-copy index for O(1) city-based flight access
# =============================================================================


@dataclass(frozen=True)
class CityIndex:
    """
    Zero-copy index for O(1) city-based flight access.

    Stores the start and end indices (exclusive) for flights departing
    from a specific city in the sorted DataFrame. Using iloc slicing
    with these indices returns a VIEW, not a copy.

    Attributes:
        start: Start index in sorted DataFrame (inclusive).
        end: End index in sorted DataFrame (exclusive).
    """

    start: int
    end: int

    def __post_init__(self) -> None:
        """Validate index bounds."""
        if self.start < 0:
            raise ValueError(f"start must be >= 0, got {self.start}")
        if self.end < self.start:
            raise ValueError(f"end ({self.end}) must be >= start ({self.start})")


# =============================================================================
# BUILD CITY INDEX: Numpy-vectorized for GIL-free operation
# =============================================================================


def build_city_index(df: pd.DataFrame) -> Dict[str, CityIndex]:
    """
    Build index from pre-sorted DataFrame using VECTORIZED numpy operations.

    CRITICAL: Pure Python loops hold the GIL during background refresh,
    blocking all other threads. Numpy operations release the GIL,
    enabling true concurrency during graph rebuilds.

    The algorithm:
    1. Get numpy array of departure_airport values
    2. Create boolean mask where city changes (vectorized comparison)
    3. Find indices where changes occur using np.where
    4. Build CityIndex entries from boundary positions

    Memory: O(num_airports) - just integer pairs, not data copies.
    Time: O(n) with vectorized operations (GIL-free during numpy ops).

    Args:
        df: DataFrame MUST be pre-sorted by 'departure_airport'.
            Index should be reset (0, 1, 2, ...).

    Returns:
        Dict mapping city code to CityIndex with (start, end) range.

    Example:
        >>> df = pd.DataFrame({
        ...     'departure_airport': ['BCN', 'BCN', 'WAW', 'WAW', 'WAW'],
        ...     'arrival_airport': ['WAW', 'MAD', 'BCN', 'MAD', 'LHR'],
        ...     'dep_time': [100, 200, 300, 400, 500],
        ...     'arr_time': [200, 300, 400, 500, 600],
        ...     'price': [50, 60, 70, 80, 90],
        ... })
        >>> index = build_city_index(df)
        >>> index['BCN']
        CityIndex(start=0, end=2)
        >>> index['WAW']
        CityIndex(start=2, end=5)
    """
    if df.empty:
        return {}

    # Get numpy array directly - avoids pandas overhead
    cities = df["departure_airport"].values
    n = len(cities)

    # VECTORIZED boundary detection:
    # - cities[1:] != cities[:-1] compares each element with its predecessor
    # - Prepend True for the first element (always a boundary)
    # - np.concatenate and comparison both release GIL
    change_mask = np.concatenate([[True], cities[1:] != cities[:-1]])

    # np.where returns indices where condition is True
    # This operation also releases the GIL
    change_indices = np.where(change_mask)[0]

    # Build index from boundary positions
    # This loop is O(num_airports), not O(n), so GIL impact is minimal
    index: Dict[str, CityIndex] = {}
    num_boundaries = len(change_indices)

    for i in range(num_boundaries):
        start = int(change_indices[i])
        end = int(change_indices[i + 1]) if i + 1 < num_boundaries else n
        city = str(cities[start])
        index[city] = CityIndex(start=start, end=end)

    return index


# =============================================================================
# CACHED FLIGHT GRAPH: Memory-efficient graph with zero-copy access
# =============================================================================


@dataclass
class CachedFlightGraph:
    """
    Memory-efficient flight graph with zero-copy access patterns.

    Key optimization: DataFrame is sorted by departure_airport,
    enabling slice-based access without copying data.

    The graph stores:
    - Single copy of flight data (flights_df)
    - Lightweight index (city_index) mapping cities to row ranges
    - Pre-computed metadata (airports, routes) for fast lookups

    Attributes:
        flights_df: Flight data SORTED by departure_airport, index reset.
        city_index: Dict mapping city code to CityIndex range.
        airports: All airport codes (departures and arrivals).
        routes: All (origin, destination) pairs.
        built_at: Timestamp when graph was built.
        version: Hash for cache invalidation.
        row_count: Number of flights in graph.
    """

    flights_df: pd.DataFrame
    city_index: Dict[str, CityIndex]
    airports: frozenset[str]
    routes: frozenset[Tuple[str, str]]
    built_at: datetime
    version: str
    row_count: int

    def get_flights_for_city(self, city: str) -> pd.DataFrame:
        """
        O(1) zero-copy access to flights departing from city.

        Returns a VIEW (not copy) of the underlying DataFrame via iloc.
        The view shares memory with the original - modifications would
        affect the cached data. Use immutability guardrails in adapters.

        Args:
            city: IATA airport code (e.g., 'WAW', 'BCN').

        Returns:
            DataFrame view of flights departing from city.
            Empty singleton DataFrame if city not found.
        """
        if city not in self.city_index:
            return _get_empty_df(self.flights_df.columns)

        idx = self.city_index[city]
        return self.flights_df.iloc[idx.start : idx.end]

    def get_flights_for_cities(self, cities: set[str]) -> pd.DataFrame:
        """
        Get flights for multiple cities (still zero-copy via iloc).

        For multiple cities, we collect all row indices and do a single
        iloc call. This is more efficient than concatenating results.

        Args:
            cities: Set of IATA airport codes.

        Returns:
            DataFrame view of flights departing from any of the cities.
            Empty singleton DataFrame if no cities found.
        """
        indices: list[int] = []
        for city in cities:
            if city in self.city_index:
                idx = self.city_index[city]
                indices.extend(range(idx.start, idx.end))

        if not indices:
            return _get_empty_df(self.flights_df.columns)

        return self.flights_df.iloc[indices]

    def has_city(self, city: str) -> bool:
        """Check if city has any departing flights."""
        return city in self.city_index

    def has_route(self, origin: str, destination: str) -> bool:
        """Check if direct route exists."""
        return (origin, destination) in self.routes


# =============================================================================
# IN-MEMORY CACHE: Phase 1 implementation (per-worker)
# =============================================================================


class InMemoryFlightGraphCache:
    """
    Phase 1: In-process cache (per-worker).

    Each Gunicorn/uvicorn worker has its own copy of the graph.
    Acceptable for development and single-worker deployments.

    Thread-safe for concurrent access within a single process.

    Attributes:
        _graph: Currently cached graph (or None).
        _ttl: Time-to-live for cache entries.
        _lock: Lock for thread-safe access.
    """

    def __init__(self, ttl: timedelta) -> None:
        """
        Initialize cache with TTL.

        Args:
            ttl: How long cached graph remains valid.
        """
        self._graph: Optional[CachedFlightGraph] = None
        self._ttl = ttl
        self._lock = threading.Lock()

    def get(self) -> Optional[CachedFlightGraph]:
        """Get cached graph or None if miss."""
        with self._lock:
            return self._graph

    def set(self, graph: CachedFlightGraph) -> None:
        """Store graph in cache."""
        with self._lock:
            self._graph = graph

    def invalidate(self) -> None:
        """Clear cached graph."""
        with self._lock:
            self._graph = None

    @property
    def is_stale(self) -> bool:
        """Check if cache needs refresh."""
        with self._lock:
            if self._graph is None:
                return True
            return datetime.now() - self._graph.built_at > self._ttl


# =============================================================================
# FLIGHT GRAPH REPOSITORY: Double-buffer with zero-downtime refresh
# =============================================================================


class FlightGraphRepository:
    """
    Production-grade repository with zero-downtime refresh.

    Architecture:
    - _current: Always points to valid, servable graph
    - Background thread builds new graph, then atomic swap
    - Readers NEVER block (always read _current)
    - Writers build in background, then atomic pointer swap

    Usage:
        >>> provider = DuffelDataProvider(db_path)
        >>> cache = InMemoryFlightGraphCache(ttl=timedelta(hours=1))
        >>> repo = FlightGraphRepository(provider, cache)
        >>> graph = repo.get_graph()  # Fast, never blocks after init
    """

    def __init__(
        self,
        data_provider: FlightDataProvider,
        cache: InMemoryFlightGraphCache,
        auto_refresh: bool = True,
    ) -> None:
        """
        Initialize repository with data provider and cache.

        Args:
            data_provider: Source for flight data.
            cache: Cache backend (InMemoryFlightGraphCache or Protocol impl).
            auto_refresh: If True, automatically refresh when stale.
        """
        self._provider = data_provider
        self._cache = cache
        self._auto_refresh = auto_refresh

        # Refresh coordination
        self._refresh_in_progress = False
        self._refresh_lock = threading.Lock()

        # Background executor for non-blocking refresh
        self._executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="graph-refresh"
        )

    def get_graph(self) -> CachedFlightGraph:
        """
        Get current graph - NEVER BLOCKS after initialization.

        First call (cold start) blocks while building initial graph.
        Subsequent calls return cached graph immediately, potentially
        triggering background refresh if stale.

        Returns:
            Current valid CachedFlightGraph.

        Raises:
            GraphNotInitializedError: If cold start fails.
        """
        cached = self._cache.get()

        if cached is not None:
            # Fast path: return cached graph
            # Trigger background refresh if stale (non-blocking)
            if self._auto_refresh and self._cache.is_stale:
                self._trigger_background_refresh()
            return cached

        # Cold start: must block for first load
        with self._refresh_lock:
            # Double-check after acquiring lock
            cached = self._cache.get()
            if cached is not None:
                return cached

            try:
                self._do_refresh()
            except Exception as e:
                logger.error(f"Cold start failed: {e}")
                raise GraphNotInitializedError(
                    f"Failed to initialize flight graph: {e}"
                ) from e

        result = self._cache.get()
        if result is None:
            raise GraphNotInitializedError("Graph initialization failed silently")
        return result

    def _trigger_background_refresh(self) -> None:
        """Trigger non-blocking background refresh."""
        with self._refresh_lock:
            if self._refresh_in_progress:
                return  # Already refreshing
            self._refresh_in_progress = True

        # Submit to background thread
        self._executor.submit(self._background_refresh)

    def _background_refresh(self) -> None:
        """Execute refresh in background thread."""
        try:
            new_graph = self._build_graph()

            # ATOMIC SWAP - readers see either old or new, never partial
            self._cache.set(new_graph)

            logger.info(
                f"Background refresh completed: {new_graph.row_count} flights, "
                f"{len(new_graph.airports)} airports"
            )
        except Exception as e:
            logger.error(f"Background refresh failed: {e}")
            # Keep serving old data on failure
        finally:
            self._refresh_in_progress = False

    def _do_refresh(self) -> None:
        """Blocking refresh (only for cold start)."""
        new_graph = self._build_graph()
        self._cache.set(new_graph)
        logger.info(
            f"Initial graph loaded: {new_graph.row_count} flights, "
            f"{len(new_graph.airports)} airports"
        )

    def _build_graph(self) -> CachedFlightGraph:
        """
        Build new graph from data provider.

        Steps:
        1. Fetch data from provider
        2. Sort by departure_airport for index-based access
        3. Build lightweight city index (no data copying)
        4. Extract metadata (airports, routes)
        5. Create CachedFlightGraph

        Returns:
            Newly built CachedFlightGraph.
        """
        # 1. Fetch data
        flights_df = self._provider.get_flights_df()

        # 2. Sort for index-based access (one-time cost)
        flights_df = flights_df.sort_values("departure_airport").reset_index(drop=True)

        # 3. Build lightweight index (no data copying)
        city_index = build_city_index(flights_df)

        # 4. Extract metadata
        departure_airports = set(flights_df["departure_airport"].unique())
        arrival_airports = set(flights_df["arrival_airport"].unique())
        airports = frozenset(departure_airports | arrival_airports)

        routes = frozenset(
            zip(flights_df["departure_airport"], flights_df["arrival_airport"])
        )

        # 5. Compute version hash for cache invalidation
        version = self._compute_version(flights_df)

        return CachedFlightGraph(
            flights_df=flights_df,
            city_index=city_index,
            airports=airports,
            routes=routes,
            built_at=datetime.now(),
            version=version,
            row_count=len(flights_df),
        )

    def _compute_version(self, df: pd.DataFrame) -> str:
        """Compute hash of DataFrame for version tracking."""
        # Use shape and sample of data for fast hashing
        content = f"{len(df)}:{df.columns.tolist()}"
        if len(df) > 0:
            # Include first and last row for change detection
            content += f":{df.iloc[0].to_dict()}:{df.iloc[-1].to_dict()}"
        return hashlib.md5(content.encode()).hexdigest()[:12]

    def force_refresh(self) -> None:
        """Force immediate background refresh."""
        self._trigger_background_refresh()

    def invalidate(self) -> None:
        """Invalidate cache and force refresh on next access."""
        self._cache.invalidate()

    def shutdown(self) -> None:
        """Clean shutdown of background executor."""
        self._executor.shutdown(wait=True)

    @property
    def is_initialized(self) -> bool:
        """Check if graph has been loaded at least once."""
        return self._cache.get() is not None

    @property
    def current_version(self) -> Optional[str]:
        """Get version of currently cached graph."""
        graph = self._cache.get()
        return graph.version if graph else None
