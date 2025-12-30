"""
Shared fixtures for performance benchmarks.

Key design principle: Pre-load expensive resources (database, graph) once
at module scope, then benchmark only the hot paths.
"""

from datetime import timedelta
from pathlib import Path
from typing import Generator

import numpy as np
import pandas as pd
import pytest

from src.flight_router.adapters.algorithms.dijkstra_adapter import DijkstraRouteFinder
from src.flight_router.adapters.data_providers.duffel_provider import DuffelDataProvider
from src.flight_router.adapters.repositories.flight_graph_repo import (
    CachedFlightGraph,
    FlightGraphRepository,
    InMemoryFlightGraphCache,
)

# Path to real database
DB_PATH = Path("Duffel_api/flights.db")


@pytest.fixture(scope="module")
def db_path() -> Path:
    """Return path to the real database."""
    if not DB_PATH.exists():
        pytest.skip(f"Database not found: {DB_PATH}")
    return DB_PATH


@pytest.fixture(scope="module")
def data_provider(db_path: Path) -> Generator[DuffelDataProvider, None, None]:
    """Create DuffelDataProvider with real database (module-scoped)."""
    provider = DuffelDataProvider(db_path=str(db_path))
    yield provider
    provider.close()


@pytest.fixture(scope="module")
def preloaded_cache() -> InMemoryFlightGraphCache:
    """Create a fresh cache (module-scoped)."""
    return InMemoryFlightGraphCache(ttl=timedelta(hours=24))


@pytest.fixture(scope="module")
def preloaded_repo(
    data_provider: DuffelDataProvider,
    preloaded_cache: InMemoryFlightGraphCache,
) -> Generator[FlightGraphRepository, None, None]:
    """
    Create FlightGraphRepository with pre-loaded graph (module-scoped).

    The cold start (DB read + sort + index build) happens once here.
    All subsequent benchmarks use the cached graph.
    """
    repo = FlightGraphRepository(
        data_provider=data_provider,
        cache=preloaded_cache,
        auto_refresh=False,  # Disable auto-refresh for predictable benchmarks
    )
    # Force cold start to happen during fixture setup
    _ = repo.get_graph()
    yield repo
    repo.shutdown()


@pytest.fixture(scope="module")
def preloaded_graph(preloaded_repo: FlightGraphRepository) -> CachedFlightGraph:
    """
    Get pre-loaded CachedFlightGraph (module-scoped).

    This is the key isolation mechanism: benchmarks receive
    an already-loaded graph, so we measure algorithm performance
    without I/O overhead.
    """
    return preloaded_repo.get_graph()


@pytest.fixture(scope="module")
def route_finder() -> DijkstraRouteFinder:
    """Create DijkstraRouteFinder instance (module-scoped)."""
    return DijkstraRouteFinder(require_defensive_copy=False)


# =============================================================================
# SYNTHETIC DATA GENERATORS (for scaling tests)
# =============================================================================


def generate_synthetic_flights(
    num_rows: int,
    num_airports: int = 100,
    seed: int = 42,
) -> pd.DataFrame:
    """
    Generate synthetic flight data for scaling benchmarks.

    Args:
        num_rows: Number of flight records to generate.
        num_airports: Number of unique airports.
        seed: Random seed for reproducibility.

    Returns:
        DataFrame matching CoreFlightSchema.
    """
    rng = np.random.default_rng(seed)

    # Generate airport codes (AAA, AAB, ..., ZZZ)
    airports = [
        f"{chr(65 + i // 26 // 26)}{chr(65 + i // 26 % 26)}{chr(65 + i % 26)}"
        for i in range(num_airports)
    ]

    # Generate random flight data
    departure_airports = rng.choice(airports, size=num_rows)
    arrival_airports = rng.choice(airports, size=num_rows)

    # Ensure no self-loops
    mask = departure_airports == arrival_airports
    while mask.any():
        arrival_airports[mask] = rng.choice(airports, size=mask.sum())
        mask = departure_airports == arrival_airports

    dep_times = rng.uniform(0, 1_000_000, size=num_rows)
    durations = rng.uniform(60, 600, size=num_rows)  # 1-10 hours
    prices = rng.uniform(50, 500, size=num_rows)

    return pd.DataFrame({
        "departure_airport": departure_airports,
        "arrival_airport": arrival_airports,
        "dep_time": dep_times,
        "arr_time": dep_times + durations,
        "price": prices,
    })


@pytest.fixture
def synthetic_flights_10k() -> pd.DataFrame:
    """10,000 synthetic flights."""
    df = generate_synthetic_flights(10_000)
    return df.sort_values("departure_airport").reset_index(drop=True)


@pytest.fixture
def synthetic_flights_50k() -> pd.DataFrame:
    """50,000 synthetic flights."""
    df = generate_synthetic_flights(50_000)
    return df.sort_values("departure_airport").reset_index(drop=True)


@pytest.fixture
def synthetic_flights_100k() -> pd.DataFrame:
    """100,000 synthetic flights."""
    df = generate_synthetic_flights(100_000)
    return df.sort_values("departure_airport").reset_index(drop=True)


@pytest.fixture
def synthetic_flights_250k() -> pd.DataFrame:
    """250,000 synthetic flights."""
    df = generate_synthetic_flights(250_000)
    return df.sort_values("departure_airport").reset_index(drop=True)
