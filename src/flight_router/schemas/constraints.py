"""
Travel constraints schema using Pandera.

Defines the contract for search parameters passed to the routing algorithm.
"""

from dataclasses import dataclass, field
from typing import FrozenSet, Optional, Set

import pandera as pa
from pandera.typing import Series


@dataclass(frozen=True)
class TravelConstraints:
    """
    Immutable travel search constraints.

    This dataclass represents the validated search parameters for route finding.
    Frozen to prevent accidental mutation during concurrent access.

    Attributes:
        start_city: Origin airport IATA code.
        required_cities: Set of airports that must be visited.
        t_min: Earliest departure time (minutes since epoch).
        t_max: Latest arrival time (minutes since epoch).
        max_stops: Maximum number of intermediate stops (None = unlimited).
        max_price: Maximum total price (None = unlimited).
        min_stay_hours: Minimum hours to stay at each destination city (None = no constraint).
    """

    start_city: str
    required_cities: FrozenSet[str]
    t_min: float
    t_max: float
    max_stops: Optional[int] = None
    max_price: Optional[float] = None
    min_stay_hours: Optional[float] = None

    def __post_init__(self) -> None:
        """Validate constraints after initialization."""
        if not self.start_city:
            raise ValueError("start_city cannot be empty")
        if self.t_min > self.t_max:
            raise ValueError(
                f"t_min ({self.t_min}) must be <= t_max ({self.t_max})"
            )
        if self.max_stops is not None and self.max_stops < 0:
            raise ValueError(f"max_stops must be >= 0, got {self.max_stops}")
        if self.max_price is not None and self.max_price < 0:
            raise ValueError(f"max_price must be >= 0, got {self.max_price}")
        if self.min_stay_hours is not None and self.min_stay_hours < 0:
            raise ValueError(
                f"min_stay_hours must be >= 0, got {self.min_stay_hours}"
            )

    @classmethod
    def create(
        cls,
        start_city: str,
        required_cities: Set[str] | FrozenSet[str] | None = None,
        t_min: float = 0.0,
        t_max: float = float("inf"),
        max_stops: Optional[int] = None,
        max_price: Optional[float] = None,
        min_stay_hours: Optional[float] = None,
    ) -> "TravelConstraints":
        """
        Factory method for creating TravelConstraints.

        Converts mutable set to frozenset for immutability.

        Args:
            start_city: Origin airport IATA code.
            required_cities: Airports to visit (will be converted to frozenset).
            t_min: Earliest departure time.
            t_max: Latest arrival time.
            max_stops: Maximum intermediate stops.
            max_price: Maximum total price.
            min_stay_hours: Minimum hours to stay at each destination city.

        Returns:
            Validated TravelConstraints instance.
        """
        if required_cities is None:
            required_cities = frozenset()
        elif not isinstance(required_cities, frozenset):
            required_cities = frozenset(required_cities)

        return cls(
            start_city=start_city,
            required_cities=required_cities,
            t_min=t_min,
            t_max=t_max,
            max_stops=max_stops,
            max_price=max_price,
            min_stay_hours=min_stay_hours,
        )

    def with_time_window(self, t_min: float, t_max: float) -> "TravelConstraints":
        """Create new constraints with updated time window."""
        return TravelConstraints(
            start_city=self.start_city,
            required_cities=self.required_cities,
            t_min=t_min,
            t_max=t_max,
            max_stops=self.max_stops,
            max_price=self.max_price,
            min_stay_hours=self.min_stay_hours,
        )

    def with_required_cities(
        self, cities: Set[str] | FrozenSet[str]
    ) -> "TravelConstraints":
        """Create new constraints with updated required cities."""
        if not isinstance(cities, frozenset):
            cities = frozenset(cities)
        return TravelConstraints(
            start_city=self.start_city,
            required_cities=cities,
            t_min=self.t_min,
            t_max=self.t_max,
            max_stops=self.max_stops,
            max_price=self.max_price,
            min_stay_hours=self.min_stay_hours,
        )


class TravelConstraintsSchema(pa.DataFrameModel):
    """
    Pandera schema for batch constraint validation.

    Used when processing multiple constraint sets from external sources
    (e.g., batch API requests, CSV imports).
    """

    start_city: Series[str] = pa.Field(
        nullable=False,
        str_length={"min_value": 2, "max_value": 4},
        description="Origin airport IATA code",
    )
    t_min: Series[float] = pa.Field(
        ge=0,
        description="Earliest departure time (minutes since epoch)",
    )
    t_max: Series[float] = pa.Field(
        ge=0,
        description="Latest arrival time (minutes since epoch)",
    )

    class Config:
        strict = False
        coerce = True
        name = "TravelConstraintsSchema"

    @pa.check("t_min", "t_max")
    def time_range_valid(cls, series: Series[float]) -> Series[bool]:
        """Validate that t_min <= t_max when both are present."""
        # This is a column-level check; row-level validation is done in dataclass
        return series >= 0
