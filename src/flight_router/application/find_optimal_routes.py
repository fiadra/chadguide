"""
FindOptimalRoutes Use Case - Public API for flight routing.

This module provides the main entry point for the flight routing engine.
It acts as a Facade/Factory, handling dependency initialization and
providing a clean interface for consumers.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional, Set, Union

from src.flight_router.adapters.algorithms.dijkstra_adapter import DijkstraRouteFinder
from src.flight_router.adapters.data_providers.duffel_provider import (
    EPOCH_REFERENCE,
    DuffelDataProvider,
)
from src.flight_router.adapters.repositories.flight_graph_repo import (
    FlightGraphRepository,
    InMemoryFlightGraphCache,
)
from src.flight_router.ports.flight_data_provider import FlightDataProvider
from src.flight_router.ports.route_finder import RouteFinder
from src.flight_router.schemas.route import RouteResult
from src.flight_router.services.flight_data_expander_service import (
    FlightDataExpanderService,
)
from src.flight_router.services.route_finder_service import RouteFinderService

logger = logging.getLogger(__name__)

# Default configuration
DEFAULT_DB_PATH = "Duffel_api/flights.db"
DEFAULT_CACHE_TTL = timedelta(hours=1)


class FindOptimalRoutes:
    """
    Public API for finding optimal flight routes.

    This is the main entry point for consumers of the flight routing engine.
    It handles dependency initialization with sensible defaults and provides
    a clean, simple interface for route searches.

    Example usage:
        >>> router = FindOptimalRoutes()
        >>> results = router.search(
        ...     origin="WAW",
        ...     destinations={"BCN", "MAD"},
        ...     departure_date=datetime(2024, 6, 15),
        ... )
        >>> for route in results:
        ...     print(f"Route: {route.route_cities}, Cost: {route.total_cost}")

    Attributes:
        _service: Underlying RouteFinderService.
        _graph_repo: Flight graph repository (for shutdown).
    """

    def __init__(
        self,
        db_path: Optional[Union[str, Path]] = None,
        data_provider: Optional[FlightDataProvider] = None,
        route_finder: Optional[RouteFinder] = None,
        cache_ttl: timedelta = DEFAULT_CACHE_TTL,
        enable_date_extrapolation: bool = True,
    ) -> None:
        """
        Initialize the route finder with optional custom dependencies.

        Args:
            db_path: Path to SQLite database. Defaults to Duffel_api/flights.db.
            data_provider: Custom data provider. If None, uses DuffelDataProvider.
            route_finder: Custom algorithm. If None, uses DijkstraRouteFinder.
            cache_ttl: Cache time-to-live. Defaults to 1 hour.
            enable_date_extrapolation: If True, enables searching for flights
                on dates outside the base data week by extrapolating weekly
                patterns. Defaults to True.
        """
        # Initialize data provider
        if data_provider is not None:
            self._data_provider = data_provider
        else:
            db_path = db_path or DEFAULT_DB_PATH
            self._data_provider = DuffelDataProvider(db_path=str(db_path))

        # Initialize cache and repository
        self._cache = InMemoryFlightGraphCache(ttl=cache_ttl)
        self._graph_repo = FlightGraphRepository(
            data_provider=self._data_provider,
            cache=self._cache,
            auto_refresh=True,
        )

        # Initialize date expander if enabled
        self._data_expander = None
        if enable_date_extrapolation:
            self._data_expander = FlightDataExpanderService()
            logger.info("Date extrapolation enabled (base week: 2026-07-13 to 2026-07-19)")

        # Initialize algorithm
        if route_finder is not None:
            self._route_finder = route_finder
        else:
            self._route_finder = DijkstraRouteFinder(
                require_defensive_copy=False,
                data_expander=self._data_expander,
            )

        # Create the service
        self._service = RouteFinderService(
            graph_repo=self._graph_repo,
            route_finder=self._route_finder,
        )

        logger.info(
            "FindOptimalRoutes initialized with %s algorithm",
            self._route_finder.name,
        )

    def search(
        self,
        origin: str,
        destinations: Optional[Set[str]] = None,
        departure_date: Optional[datetime] = None,
        return_date: Optional[datetime] = None,
        max_stops: Optional[int] = None,
        max_price: Optional[float] = None,
        min_stay_hours: Optional[float] = None,
    ) -> List[RouteResult]:
        """
        Search for optimal routes between airports.

        This is the primary search method using human-friendly datetime inputs.

        Args:
            origin: Origin airport IATA code (e.g., 'WAW').
            destinations: Set of airports to visit. If None, returns all
                Pareto-optimal routes from origin.
            departure_date: Earliest departure date/time.
            return_date: Latest return date/time.
            max_stops: Maximum number of intermediate stops.
            max_price: Maximum total price.

        Returns:
            List of Pareto-optimal RouteResult objects, sorted by total cost.

        Example:
            >>> router = FindOptimalRoutes()
            >>> results = router.search(
            ...     origin="WAW",
            ...     destinations={"BCN"},
            ...     departure_date=datetime(2024, 6, 15),
            ...     max_stops=1,
            ... )
        """
        return self._service.search_with_datetime(
            start_city=origin,
            required_cities=destinations,
            departure_after=departure_date,
            arrival_before=return_date,
            max_stops=max_stops,
            max_price=max_price,
            min_stay_hours=min_stay_hours,
        )

    def search_raw(
        self,
        start_city: str,
        required_cities: Optional[Set[str]] = None,
        t_min: float = 0.0,
        t_max: float = float("inf"),
        max_stops: Optional[int] = None,
        max_price: Optional[float] = None,
        min_stay_hours: Optional[float] = None,
    ) -> List[RouteResult]:
        """
        Search for routes using raw epoch-based time parameters.

        Lower-level interface for advanced use cases requiring direct
        control over time parameters.

        Args:
            start_city: Origin airport IATA code.
            required_cities: Set of airports that must be visited.
            t_min: Earliest departure time (minutes since epoch).
            t_max: Latest arrival time (minutes since epoch).
            max_stops: Maximum intermediate stops.
            max_price: Maximum total price.

        Returns:
            List of Pareto-optimal RouteResult objects.
        """
        return self._service.find_optimal_routes(
            start_city=start_city,
            required_cities=required_cities,
            t_min=t_min,
            t_max=t_max,
            max_stops=max_stops,
            max_price=max_price,
            min_stay_hours=min_stay_hours,
        )

    def get_available_airports(self) -> frozenset[str]:
        """
        Get all airports available in the flight database.

        Returns:
            Frozenset of IATA airport codes.
        """
        graph = self._graph_repo.get_graph()
        return graph.airports

    def has_route(self, origin: str, destination: str) -> bool:
        """
        Check if a direct route exists between two airports.

        Args:
            origin: Origin airport IATA code.
            destination: Destination airport IATA code.

        Returns:
            True if a direct flight exists, False otherwise.
        """
        graph = self._graph_repo.get_graph()
        return graph.has_route(origin, destination)

    @staticmethod
    def datetime_to_epoch_minutes(dt: datetime) -> float:
        """
        Convert datetime to epoch minutes (minutes since reference epoch).

        Utility method for consumers who need to work with raw time values.

        Args:
            dt: Datetime to convert.

        Returns:
            Minutes since epoch reference (2024-01-01 00:00:00).
        """
        delta = dt - EPOCH_REFERENCE
        return delta.total_seconds() / 60

    @staticmethod
    def epoch_minutes_to_datetime(minutes: float) -> datetime:
        """
        Convert epoch minutes back to datetime.

        Args:
            minutes: Minutes since epoch reference.

        Returns:
            Corresponding datetime.
        """
        return EPOCH_REFERENCE + timedelta(minutes=minutes)

    @property
    def is_ready(self) -> bool:
        """Check if the router is ready to handle requests."""
        return self._service.is_ready

    @property
    def algorithm_name(self) -> str:
        """Get the name of the routing algorithm being used."""
        return self._service.algorithm_name

    def refresh_data(self) -> None:
        """Force a refresh of the flight data cache."""
        self._graph_repo.force_refresh()

    def shutdown(self) -> None:
        """
        Clean shutdown of the router.

        Closes database connections and stops background threads.
        Should be called when the router is no longer needed.
        """
        self._graph_repo.shutdown()
        if hasattr(self._data_provider, "close"):
            self._data_provider.close()
        logger.info("FindOptimalRoutes shutdown complete")

    def __enter__(self) -> "FindOptimalRoutes":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit with cleanup."""
        self.shutdown()
