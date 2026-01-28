"""
Port interfaces for the Flight Router.

Ports define the abstract interfaces (ABCs and Protocols) that the domain
layer uses to communicate with external systems. This follows the
Ports and Adapters (Hexagonal) architecture pattern.
"""

from src.flight_router.ports.flight_data_expander import FlightDataExpander
from src.flight_router.ports.flight_data_provider import FlightDataProvider
from src.flight_router.ports.graph_repository import (
    FlightGraphCache,
    GraphNotInitializedError,
)
from src.flight_router.ports.offer_validator import (
    OfferValidator,
    RouteValidation,
    SegmentValidation,
    ValidationStatus,
)
from src.flight_router.ports.route_finder import RouteFinder

__all__ = [
    "FlightDataExpander",
    "FlightDataProvider",
    "FlightGraphCache",
    "GraphNotInitializedError",
    "OfferValidator",
    "RouteFinder",
    "RouteValidation",
    "SegmentValidation",
    "ValidationStatus",
]
