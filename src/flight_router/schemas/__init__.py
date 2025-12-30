"""
Schema definitions for Flight Router.

Pandera-validated DataFrames as the primary data contracts.
"""

from .constraints import TravelConstraints, TravelConstraintsSchema
from .flight import (
    CoreFlightSchema,
    ExtendedFlightSchema,
    ExtendedFlightDataFrame,
    FlightDataFrame,
)
from .route import RouteSegment, RouteResult, RouteSegmentSchema, RouteResultSchema

__all__ = [
    # Flight schemas
    "CoreFlightSchema",
    "ExtendedFlightSchema",
    "FlightDataFrame",
    "ExtendedFlightDataFrame",
    # Constraints
    "TravelConstraints",
    "TravelConstraintsSchema",
    # Route schemas
    "RouteSegment",
    "RouteResult",
    "RouteSegmentSchema",
    "RouteResultSchema",
]
