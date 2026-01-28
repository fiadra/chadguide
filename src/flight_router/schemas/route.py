"""
Route result schemas using Pandera.

Defines the output contract for routing algorithm results.
Standardizes the interface between algorithms and consumers.
"""

from dataclasses import dataclass, field
from typing import List, Optional, Sequence

import pandera as pa
from pandera.typing import DataFrame, Series


class RouteSegmentSchema(pa.DataFrameModel):
    """
    Schema for individual flight segments in a route.

    Each row represents one flight in a multi-leg journey.
    """

    segment_index: Series[int] = pa.Field(
        ge=0,
        description="Zero-based index of this segment in the route",
    )
    departure_airport: Series[str] = pa.Field(
        nullable=False,
        description="Departure airport IATA code",
    )
    arrival_airport: Series[str] = pa.Field(
        nullable=False,
        description="Arrival airport IATA code",
    )
    dep_time: Series[float] = pa.Field(
        ge=0,
        description="Departure time in minutes since epoch",
    )
    arr_time: Series[float] = pa.Field(
        ge=0,
        description="Arrival time in minutes since epoch",
    )
    price: Series[float] = pa.Field(
        ge=0,
        description="Segment price",
    )

    class Config:
        strict = False
        coerce = True
        name = "RouteSegmentSchema"
        ordered = True


class RouteResultSchema(pa.DataFrameModel):
    """
    Schema for complete route results.

    Each row represents a complete Pareto-optimal route solution.
    Used for batch processing of multiple route results.
    """

    route_id: Series[int] = pa.Field(
        ge=0,
        description="Unique identifier for this route",
    )
    total_cost: Series[float] = pa.Field(
        ge=0,
        description="Total route cost (sum of all segment prices)",
    )
    total_time: Series[float] = pa.Field(
        ge=0,
        description="Total route duration (end time - start time)",
    )
    num_segments: Series[int] = pa.Field(
        ge=1,
        description="Number of flight segments in route",
    )
    start_city: Series[str] = pa.Field(
        nullable=False,
        description="Origin airport IATA code",
    )
    end_city: Series[str] = pa.Field(
        nullable=False,
        description="Final destination airport IATA code",
    )
    departure_time: Series[float] = pa.Field(
        ge=0,
        description="First segment departure time",
    )
    arrival_time: Series[float] = pa.Field(
        ge=0,
        description="Last segment arrival time",
    )

    class Config:
        strict = False
        coerce = True
        name = "RouteResultSchema"


@dataclass(frozen=True)
class RouteSegment:
    """
    Immutable representation of a single flight segment.

    Used for constructing route results from algorithm output.
    """

    segment_index: int
    departure_airport: str
    arrival_airport: str
    dep_time: float
    arr_time: float
    price: float

    # Optional extended fields
    carrier_code: Optional[str] = None
    carrier_name: Optional[str] = None

    @property
    def duration(self) -> float:
        """Flight duration in minutes."""
        return self.arr_time - self.dep_time


@dataclass(frozen=True)
class RouteResult:
    """
    Immutable representation of a complete route.

    Aggregates multiple RouteSegments into a cohesive result.
    This is the primary output type from the routing algorithm.
    """

    route_id: int
    segments: tuple[RouteSegment, ...]
    visited_cities: frozenset[str]

    @property
    def total_cost(self) -> float:
        """Sum of all segment prices."""
        return sum(seg.price for seg in self.segments)

    @property
    def total_time(self) -> float:
        """Total elapsed time from first departure to last arrival (in minutes)."""
        if not self.segments:
            return 0.0
        return self.segments[-1].arr_time - self.segments[0].dep_time

    @property
    def total_flight_time(self) -> float:
        """Sum of all flight durations - actual time spent in air (in minutes)."""
        return sum(seg.duration for seg in self.segments)

    @property
    def trip_duration_days(self) -> float:
        """Trip duration in days (from first departure to last arrival)."""
        return self.total_time / (24 * 60)

    @property
    def num_segments(self) -> int:
        """Number of flight segments."""
        return len(self.segments)

    @property
    def start_city(self) -> str:
        """Origin airport."""
        if not self.segments:
            raise ValueError("Route has no segments")
        return self.segments[0].departure_airport

    @property
    def end_city(self) -> str:
        """Final destination airport."""
        if not self.segments:
            raise ValueError("Route has no segments")
        return self.segments[-1].arrival_airport

    @property
    def departure_time(self) -> float:
        """First segment departure time."""
        if not self.segments:
            raise ValueError("Route has no segments")
        return self.segments[0].dep_time

    @property
    def arrival_time(self) -> float:
        """Last segment arrival time."""
        if not self.segments:
            raise ValueError("Route has no segments")
        return self.segments[-1].arr_time

    @property
    def route_cities(self) -> List[str]:
        """Ordered list of all cities in route."""
        if not self.segments:
            return []
        cities = [self.segments[0].departure_airport]
        for seg in self.segments:
            cities.append(seg.arrival_airport)
        return cities

    @classmethod
    def from_segments(
        cls,
        route_id: int,
        segments: Sequence[RouteSegment],
    ) -> "RouteResult":
        """
        Factory method to create RouteResult from segments.

        Args:
            route_id: Unique identifier for this route.
            segments: Sequence of RouteSegment objects.

        Returns:
            Validated RouteResult instance.
        """
        if not segments:
            raise ValueError("Route must have at least one segment")

        # Extract visited cities (excluding start city for consistency with dijkstra)
        visited = frozenset(seg.arrival_airport for seg in segments)

        return cls(
            route_id=route_id,
            segments=tuple(segments),
            visited_cities=visited,
        )
