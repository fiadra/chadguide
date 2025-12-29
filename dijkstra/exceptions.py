"""
Custom exceptions for the dijkstra module.

Provides a hierarchy of exceptions for clear error handling
and debugging of route-finding operations.
"""


class DijkstraError(Exception):
    """Base exception for all dijkstra module errors."""

    pass


class ValidationError(DijkstraError):
    """Base exception for input validation errors."""

    pass


class EmptyFlightsError(ValidationError):
    """Raised when flights DataFrame is empty."""

    def __init__(self, message: str = "Flights DataFrame is empty") -> None:
        super().__init__(message)


class InvalidAirportError(ValidationError):
    """Raised when an airport code is not found in the flight data."""

    def __init__(self, airport: str, context: str = "data") -> None:
        self.airport = airport
        message = f"Airport '{airport}' not found in {context}"
        super().__init__(message)


class InvalidTimeRangeError(ValidationError):
    """Raised when time range parameters are invalid."""

    def __init__(self, t_min: float, t_max: float) -> None:
        self.t_min = t_min
        self.t_max = t_max
        message = f"Invalid time range: T_min ({t_min}) must be <= T_max ({t_max})"
        super().__init__(message)


class MissingColumnsError(ValidationError):
    """Raised when required DataFrame columns are missing."""

    def __init__(self, missing: set[str]) -> None:
        self.missing = missing
        columns_str = ", ".join(sorted(missing))
        message = f"Missing required columns: {columns_str}"
        super().__init__(message)
