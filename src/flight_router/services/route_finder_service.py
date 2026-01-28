"""
Route Finder Service - Domain orchestrator for flight routing.

Coordinates the interaction between:
- FlightGraphRepository (cached flight data)
- RouteFinder (algorithm adapter)
- TravelConstraints (validated search parameters)
"""

from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import TYPE_CHECKING, List, Optional, Set

from src.flight_router.schemas.constraints import TravelConstraints
from src.flight_router.schemas.route import RouteResult

if TYPE_CHECKING:
    from src.flight_router.adapters.repositories.flight_graph_repo import (
        FlightGraphRepository,
    )
    from src.flight_router.ports.route_finder import RouteFinder

logger = logging.getLogger(__name__)


class RouteFinderService:
    """
    Domain service for finding optimal flight routes.

    Orchestrates the routing process:
    1. Validates and normalizes input constraints
    2. Retrieves cached flight graph (non-blocking)
    3. Delegates route finding to algorithm adapter
    4. Logs performance metrics

    This service is stateless and thread-safe.

    Attributes:
        _graph_repo: Repository providing cached flight graphs.
        _route_finder: Algorithm adapter for route finding.
    """

    def __init__(
        self,
        graph_repo: FlightGraphRepository,
        route_finder: RouteFinder,
    ) -> None:
        """
        Initialize the route finder service.

        Args:
            graph_repo: Repository for cached flight graph access.
            route_finder: Algorithm adapter (e.g., DijkstraRouteFinder).
        """
        self._graph_repo = graph_repo
        self._route_finder = route_finder

    def find_optimal_routes(
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
        Find Pareto-optimal routes matching the given constraints.

        This is the main entry point for route searches. It:
        1. Creates and validates TravelConstraints
        2. Gets the cached flight graph
        3. Calls the algorithm adapter
        4. Returns filtered results

        Args:
            start_city: Origin airport IATA code (e.g., 'WAW').
            required_cities: Set of airports that must be visited.
            t_min: Earliest departure time (minutes since epoch).
            t_max: Latest arrival time (minutes since epoch).
            max_stops: Maximum intermediate stops (None = unlimited).
            max_price: Maximum total price (None = unlimited).

        Returns:
            List of Pareto-optimal RouteResult objects.

        Raises:
            ValueError: If constraints are invalid.
            GraphNotInitializedError: If graph cannot be loaded.
        """
        start_time = time.perf_counter()

        # 1. Validate and create immutable constraints
        constraints = TravelConstraints.create(
            start_city=start_city,
            required_cities=required_cities,
            t_min=t_min,
            t_max=t_max,
            max_stops=max_stops,
            max_price=max_price,
            min_stay_hours=min_stay_hours,
        )

        logger.debug(
            "Search constraints: start=%s, required=%s, t_min=%.0f, t_max=%.0f",
            constraints.start_city,
            constraints.required_cities,
            constraints.t_min,
            constraints.t_max,
        )

        # 2. Get cached flight graph (non-blocking after cold start)
        graph_start = time.perf_counter()
        graph = self._graph_repo.get_graph()
        graph_time = time.perf_counter() - graph_start

        logger.debug(
            "Graph retrieved in %.3fms (%d flights, %d airports)",
            graph_time * 1000,
            graph.row_count,
            len(graph.airports),
        )

        # 3. Delegate to algorithm adapter
        min_stay_minutes = (constraints.min_stay_hours or 0.0) * 60

        algo_start = time.perf_counter()
        results = self._route_finder.find_routes(
            graph=graph,
            start_city=constraints.start_city,
            required_cities=set(constraints.required_cities),
            t_min=constraints.t_min,
            t_max=constraints.t_max,
            min_stay_minutes=min_stay_minutes,
        )
        algo_time = time.perf_counter() - algo_start

        # 4. Apply post-filtering (max_stops, max_price)
        filtered_results = self._apply_post_filters(results, constraints)

        total_time = time.perf_counter() - start_time

        # Log performance metrics
        logger.info(
            "Route search completed: %d results (filtered from %d) in %.3fms "
            "(graph: %.3fms, algo: %.3fms)",
            len(filtered_results),
            len(results),
            total_time * 1000,
            graph_time * 1000,
            algo_time * 1000,
        )

        return filtered_results

    def _apply_post_filters(
        self,
        results: List[RouteResult],
        constraints: TravelConstraints,
    ) -> List[RouteResult]:
        """
        Apply post-processing filters to route results.

        Some constraints are more efficiently applied after the algorithm
        runs (e.g., max_price) rather than during the search.

        Args:
            results: Raw results from algorithm.
            constraints: Search constraints with filter criteria.

        Returns:
            Filtered list of RouteResults.
        """
        filtered = results

        if constraints.max_stops is not None:
            filtered = [r for r in filtered if r.num_segments - 1 <= constraints.max_stops]

        if constraints.max_price is not None:
            filtered = [r for r in filtered if r.total_cost <= constraints.max_price]

        return filtered

    def search_with_datetime(
        self,
        start_city: str,
        required_cities: Optional[Set[str]] = None,
        departure_after: Optional[datetime] = None,
        arrival_before: Optional[datetime] = None,
        max_stops: Optional[int] = None,
        max_price: Optional[float] = None,
        min_stay_hours: Optional[float] = None,
    ) -> List[RouteResult]:
        """
        Find routes using datetime objects for time constraints.

        Convenience method that converts datetime to epoch minutes.

        Args:
            start_city: Origin airport IATA code.
            required_cities: Set of airports that must be visited.
            departure_after: Earliest departure datetime.
            arrival_before: Latest arrival datetime.
            max_stops: Maximum intermediate stops.
            max_price: Maximum total price.

        Returns:
            List of Pareto-optimal RouteResult objects.
        """
        from src.flight_router.adapters.data_providers.duffel_provider import (
            EPOCH_REFERENCE,
        )

        t_min = 0.0
        t_max = float("inf")

        if departure_after is not None:
            delta = departure_after - EPOCH_REFERENCE
            t_min = delta.total_seconds() / 60

        if arrival_before is not None:
            delta = arrival_before - EPOCH_REFERENCE
            t_max = delta.total_seconds() / 60

        return self.find_optimal_routes(
            start_city=start_city,
            required_cities=required_cities,
            t_min=t_min,
            t_max=t_max,
            max_stops=max_stops,
            max_price=max_price,
            min_stay_hours=min_stay_hours,
        )

    @property
    def algorithm_name(self) -> str:
        """Get name of the underlying algorithm."""
        return self._route_finder.name

    @property
    def is_ready(self) -> bool:
        """Check if service is ready to handle requests."""
        return self._graph_repo.is_initialized
