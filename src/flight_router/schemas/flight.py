"""
Flight data schemas using Pandera.

Defines the core contract for flight data flowing through the system.
Schema validation happens at layer boundaries only, not per-row.
"""

from typing import Optional

import pandera as pa
from pandera.typing import DataFrame, Series


class CoreFlightSchema(pa.DataFrameModel):
    """
    Immutable core contract - algorithm requirements.

    These are the minimum fields required by the dijkstra routing algorithm.
    All providers must supply these fields. Extra columns are allowed and
    preserved (via strict="filter").
    """

    departure_airport: Series[str] = pa.Field(
        nullable=False,
        description="Departure airport IATA code (e.g., 'WAW', 'BCN')",
    )
    arrival_airport: Series[str] = pa.Field(
        nullable=False,
        description="Arrival airport IATA code",
    )
    dep_time: Series[float] = pa.Field(
        ge=0,
        description="Departure time in minutes since epoch",
    )
    arr_time: Series[float] = pa.Field(
        ge=0,
        description="Arrival time in minutes since epoch",
    )
    price: Series[float] = pa.Field(
        ge=0,
        description="Flight price in base currency",
    )

    class Config:
        # CRITICAL: strict=False allows extra columns to pass through unchanged
        # This enables forward compatibility - providers can add fields
        # without breaking the core algorithm contract
        # Note: strict="filter" would REMOVE extra columns, which we don't want
        strict = False
        coerce = True
        name = "CoreFlightSchema"
        description = "Core flight data required by routing algorithms"


class ExtendedFlightSchema(CoreFlightSchema):
    """
    Extended fields - optional, for filtering/display.

    Inherits all core fields and adds optional metadata fields.
    These fields are nullable since not all providers may supply them.

    Extension workflow (adding a new field):
    1. Add field here with nullable=True
    2. Update data provider to include the column
    3. Algorithm continues working (uses CoreFlightSchema)
    4. New filtering/display logic can access the extended field
    5. No changes to domain layer or algorithm adapter needed
    """

    # Carrier information
    carrier_code: Optional[Series[str]] = pa.Field(
        nullable=True,
        description="Airline IATA code (e.g., 'LO', 'FR')",
    )
    carrier_name: Optional[Series[str]] = pa.Field(
        nullable=True,
        description="Full airline name",
    )

    # Terminal information
    terminal_origin: Optional[Series[str]] = pa.Field(
        nullable=True,
        description="Departure terminal",
    )
    terminal_dest: Optional[Series[str]] = pa.Field(
        nullable=True,
        description="Arrival terminal",
    )

    # Transfer and baggage
    transfer_time_mins: Optional[Series[float]] = pa.Field(
        nullable=True,
        ge=0,
        description="Minimum transfer time in minutes",
    )
    baggage_included: Optional[Series[float]] = pa.Field(
        nullable=True,
        ge=0,
        description="Number of included baggage pieces (stored as float for NaN support)",
    )

    # Original datetime strings (for display)
    scheduled_departure: Optional[Series[str]] = pa.Field(
        nullable=True,
        description="Original departure datetime string",
    )
    scheduled_arrival: Optional[Series[str]] = pa.Field(
        nullable=True,
        description="Original arrival datetime string",
    )

    class Config:
        strict = False
        coerce = True
        name = "ExtendedFlightSchema"
        description = "Extended flight data with optional metadata"


# Type aliases for clarity in function signatures
FlightDataFrame = DataFrame[CoreFlightSchema]
ExtendedFlightDataFrame = DataFrame[ExtendedFlightSchema]
