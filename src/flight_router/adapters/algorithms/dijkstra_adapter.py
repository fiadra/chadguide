"""
Dijkstra Algorithm Adapter - Bridge between architecture and algorithm.

Wraps the dijkstra module with immutability safety and converts
Label output to RouteResult schema objects.

Uses CityIndex for O(num_airports) flights_by_city construction
instead of O(n) groupby() per request.
"""

import logging
from typing import Dict, List, Optional

import pandas as pd

from src.dijkstra.alg import dijkstra
from src.dijkstra.labels import Label
from src.dijkstra.reconstruction import reconstruct_path

from src.flight_router.adapters.algorithms.immutability import (
    make_defensive_copy,
    make_immutable,
)
from src.flight_router.adapters.repositories.flight_graph_repo import (
    CachedFlightGraph,
)
from src.flight_router.ports.route_finder import RouteFinder
from src.flight_router.schemas.route import RouteResult, RouteSegment

logger = logging.getLogger(__name__)


class DijkstraRouteFinder(RouteFinder):
    """
    Adapter for existing dijkstra module with IMMUTABILITY ENFORCEMENT.

    This adapter bridges the new architecture with the legacy dijkstra
    algorithm, ensuring:
    1. DataFrame immutability is enforced before passing to algorithm
    2. Output Labels are converted to RouteResult schema objects
    3. Cache integrity is preserved across concurrent requests

    Attributes:
        _require_copy: If True, always copy DataFrame before passing to dijkstra.
    """

    def __init__(self, require_defensive_copy: bool = False) -> None:
        """
        Initialize the Dijkstra route finder.

        Args:
            require_defensive_copy: If True, always copy DataFrame before
                passing to dijkstra. Use only if dijkstra is verified to
                mutate input. Default False (prefer immutability flag).
        """
        self._require_copy = require_defensive_copy

    @property
    def name(self) -> str:
        """Algorithm identifier."""
        return "Multi-Criteria Dijkstra"

    def find_routes(
        self,
        graph: CachedFlightGraph,
        start_city: str,
        required_cities: set[str],
        t_min: float,
        t_max: float,
    ) -> List[RouteResult]:
        """
        Find Pareto-optimal routes with IMMUTABILITY SAFETY and OPTIMIZED DATA ACCESS.

        Optimization:
        - Builds flights_by_city from CityIndex (O(num_airports), not O(n))
        - Passes pre-computed dict to dijkstra, skipping expensive groupby()
        - Uses zero-copy DataFrame views from CityIndex slices

        Args:
            graph: Pre-built CachedFlightGraph with CityIndex.
            start_city: Origin airport.
            required_cities: Must-visit airports.
            t_min, t_max: Time window in epoch minutes.

        Returns:
            List of Pareto-optimal RouteResult objects.

        Raises:
            RuntimeError: If algorithm attempts to mutate immutable DataFrame.
        """
        flights_df = graph.flights_df

        if self._require_copy:
            # DEFENSIVE COPY: Use only if dijkstra mutates input
            logger.debug("Using defensive copy for dijkstra input")
            flights_df = make_defensive_copy(flights_df)
        else:
            # IMMUTABILITY FLAG: Zero-copy, raises on mutation attempt
            flights_df = make_immutable(flights_df)

        try:
            # Build flights_by_city from CityIndex (O(num_airports))
            flights_by_city = self._build_flights_by_city_from_index(graph)

            solutions: List[Label] = dijkstra(
                flights_df=flights_df,
                start_city=start_city,
                required_cities=required_cities,
                T_min=t_min,
                T_max=t_max,
                flights_by_city=flights_by_city,
            )

            logger.debug(
                "Dijkstra found %d Pareto-optimal solutions for %s -> %s",
                len(solutions),
                start_city,
                required_cities,
            )

            # Convert Labels to RouteResults, filtering out empty solutions
            # (empty solutions occur when required_cities is empty - no travel needed)
            results = []
            for i, label in enumerate(solutions):
                _, flights = reconstruct_path(label)
                if flights:  # Only include solutions with actual flights
                    results.append(self._label_to_route_result(label, route_id=len(results)))

            return results

        except ValueError as e:
            if "read-only" in str(e):
                raise RuntimeError(
                    "Algorithm attempted to mutate shared DataFrame. "
                    "Enable require_defensive_copy=True or fix algorithm."
                ) from e
            raise

    def _build_flights_by_city_from_index(
        self, graph: CachedFlightGraph
    ) -> Dict[str, pd.DataFrame]:
        """
        Build flights_by_city dict from CityIndex using zero-copy views.

        This is O(num_airports) vs O(n) for groupby().
        For 250k flights with 100 airports: ~100 dict entries vs 250k row scan.

        Args:
            graph: CachedFlightGraph with pre-built CityIndex.

        Returns:
            Dict mapping city code to DataFrame view of departing flights.
        """
        return {
            city: graph.get_flights_for_city(city)
            for city in graph.city_index.keys()
        }

    def _label_to_route_result(self, label: Label, route_id: int) -> RouteResult:
        """
        Convert dijkstra Label to RouteResult schema object.

        Traverses the label chain (via prev pointers) to extract all
        flights, then creates RouteSegment objects for each.

        Args:
            label: Terminal Label from dijkstra algorithm.
            route_id: Unique identifier for this route.

        Returns:
            RouteResult with all segments.
        """
        # Reconstruct path from label chain
        _, flights = reconstruct_path(label)

        # Create RouteSegments from flights
        segments: List[RouteSegment] = []
        for i, flight in enumerate(flights):
            segment = RouteSegment(
                segment_index=i,
                departure_airport=str(flight["departure_airport"]),
                arrival_airport=str(flight["arrival_airport"]),
                dep_time=float(flight["dep_time"]),
                arr_time=float(flight["arr_time"]),
                price=float(flight["price"]),
                carrier_code=self._safe_get(flight, "carrier_code"),
                carrier_name=self._safe_get(flight, "carrier_name"),
            )
            segments.append(segment)

        return RouteResult.from_segments(route_id=route_id, segments=segments)

    def _safe_get(self, flight, key: str) -> Optional[str]:
        """Safely extract optional field from flight (Series or FlightRecord)."""
        val = flight.get(key)
        if val is not None and str(val) != "nan":
            return str(val)
        return None
