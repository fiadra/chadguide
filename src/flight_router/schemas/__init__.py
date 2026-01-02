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
from .validation import (
    DEFAULT_MATCHING_WEIGHTS,
    MatchingWeights,
    ValidatedRoute,
    ValidationConfig,
)

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
    # Validation schemas
    "DEFAULT_MATCHING_WEIGHTS",
    "MatchingWeights",
    "ValidatedRoute",
    "ValidationConfig",
]
