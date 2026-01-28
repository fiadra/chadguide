"""
Route Finder port interface.

Defines the abstract contract for routing algorithms.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, List

if TYPE_CHECKING:
    from src.flight_router.adapters.repositories.flight_graph_repo import (
        CachedFlightGraph,
    )
    from src.flight_router.schemas.route import RouteResult


class RouteFinder(ABC):
    """
    Abstract interface for route finding algorithms.

    Algorithm adapters receive the full CachedFlightGraph, not a
    pre-filtered dictionary. This preserves zero-copy access patterns.
    The algorithm handles internal data access via graph.flights_df
    or graph.get_flights_for_city().

    Implementations:
    - DijkstraRouteFinder: Multi-criteria Dijkstra with Pareto optimization
    """

    @abstractmethod
    def find_routes(
        self,
        graph: CachedFlightGraph,
        start_city: str,
        required_cities: set[str],
        t_min: float,
        t_max: float,
        min_stay_minutes: float = 0.0,
    ) -> List[RouteResult]:
        """
        Find optimal routes using the cached flight graph.

        Args:
            graph: Pre-built CachedFlightGraph with indexed access.
            start_city: Origin airport IATA code.
            required_cities: Set of airports that must be visited.
            t_min: Earliest departure time (minutes since epoch).
            t_max: Latest arrival time (minutes since epoch).
            min_stay_minutes: Minimum stay at each destination city (minutes).

        Returns:
            List of Pareto-optimal RouteResult objects.
        """
        ...

    @property
    @abstractmethod
    def name(self) -> str:
        """
        Algorithm identifier.

        Returns:
            Human-readable algorithm name.
        """
        ...
