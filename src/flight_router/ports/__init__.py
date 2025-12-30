"""
Port interfaces for the Flight Router.

Ports define the abstract interfaces (ABCs and Protocols) that the domain
layer uses to communicate with external systems. This follows the
Ports and Adapters (Hexagonal) architecture pattern.
"""

from src.flight_router.ports.flight_data_provider import FlightDataProvider
from src.flight_router.ports.graph_repository import (
    FlightGraphCache,
    GraphNotInitializedError,
)
from src.flight_router.ports.route_finder import RouteFinder

__all__ = [
    "FlightDataProvider",
    "FlightGraphCache",
    "GraphNotInitializedError",
    "RouteFinder",
]
