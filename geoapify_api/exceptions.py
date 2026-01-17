"""
Custom exceptions for the geoapify_api module.

Provides a hierarchy of exceptions for clear error handling
and debugging of json-fetching operations.
"""

class GeoapifyError(Exception):
    """Base exception for all Geoapify client errors."""
    pass


class GeoapifyAPIError(GeoapifyError):
    """Raised when the Geoapify API returns an HTTP error or invalid response."""
    def __init__(self, status_code: int, message: str = ""):
        self.status_code = status_code
        self.message = message or f"Geoapify API returned status code {status_code}"
        super().__init__(self.message)


class GeoapifyCityNotFoundError(GeoapifyError):
    """Raised when a city's coordinates cannot be found via the Geoapify API."""
    def __init__(self, city: str):
        self.city = city
        self.message = f"Could not fetch place coordinates for city '{city}'"
        super().__init__(self.message)


class GeoapifyInvalidParameterError(GeoapifyError):
    """Raised when an invalid parameter is passed to the Geoapify client."""
    def __init__(self, parameter_name: str, message: str = ""):
        self.parameter_name = parameter_name
        self.message = message or f"Invalid parameter: {parameter_name}"
        super().__init__(self.message)
