"""
Flight Data Provider port interface.

Defines the abstract contract for data sources that provide flight data.
Implementations handle the specifics of different data backends (SQL, API, etc.).
"""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional, Set

from src.flight_router.schemas.flight import FlightDataFrame


class FlightDataProvider(ABC):
    """
    Abstract interface for flight data providers.

    Data providers return validated DataFrames directly - no object creation.
    Schema validation happens at the boundary (in the provider), not per-row.

    Implementations:
    - DuffelDataProvider: SQL → DataFrame from Duffel SQLite database
    - MockDataProvider: In-memory DataFrame for testing
    """

    @abstractmethod
    def get_flights_df(
        self,
        origin: Optional[str] = None,
        destination: Optional[str] = None,
        date_start: Optional[datetime] = None,
        date_end: Optional[datetime] = None,
    ) -> FlightDataFrame:
        """
        Return flights as a validated DataFrame.

        Schema validation (CoreFlightSchema) happens here at the boundary,
        not per-row or per-object. Extra columns are preserved for
        forward compatibility.

        Args:
            origin: Filter by departure airport IATA code (optional).
            destination: Filter by arrival airport IATA code (optional).
            date_start: Filter by earliest departure datetime (optional).
            date_end: Filter by latest departure datetime (optional).

        Returns:
            DataFrame validated against CoreFlightSchema.
            May contain additional columns beyond the core schema.

        Raises:
            pandera.errors.SchemaError: If data fails validation.
            ConnectionError: If data source is unavailable.
        """
        ...

    @abstractmethod
    def get_airports(self) -> Set[str]:
        """
        Return all airport IATA codes available in the data source.

        Returns:
            Set of 3-letter IATA airport codes.
        """
        ...

    @property
    @abstractmethod
    def name(self) -> str:
        """
        Human-readable name of this data provider.

        Returns:
            Provider identifier (e.g., "Duffel SQLite", "Mock Provider").
        """
        ...

    @property
    def is_available(self) -> bool:
        """
        Check if the data source is currently available.

        Default implementation returns True. Override for providers
        that need connection health checks.

        Returns:
            True if data source is accessible.
        """
        return True
