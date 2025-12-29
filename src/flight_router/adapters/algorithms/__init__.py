"""
Algorithm adapters for flight routing.
"""

from src.flight_router.adapters.algorithms.dijkstra_adapter import (
    DijkstraRouteFinder,
)
from src.flight_router.adapters.algorithms.immutability import (
    make_defensive_copy,
    make_immutable,
)

__all__ = [
    "DijkstraRouteFinder",
    "make_defensive_copy",
    "make_immutable",
]
