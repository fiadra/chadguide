"""
Application layer for the Flight Router.

This layer provides the public API for the flight routing engine.
It acts as a facade, handling dependency initialization and providing
a simple interface for consumers.
"""

from src.flight_router.application.find_optimal_routes import FindOptimalRoutes

__all__ = ["FindOptimalRoutes"]
