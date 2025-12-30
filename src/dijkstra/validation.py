"""
Input validation for the dijkstra module.

Provides validation functions that check inputs before algorithm execution,
ensuring fail-fast behavior with clear error messages.
"""

from typing import Set

import pandas as pd

from .exceptions import (
    EmptyFlightsError,
    InvalidAirportError,
    InvalidTimeRangeError,
    MissingColumnsError,
)

# Required columns for the flights DataFrame
REQUIRED_COLUMNS: Set[str] = {
    "departure_airport",
    "arrival_airport",
    "dep_time",
    "arr_time",
    "price",
}


def validate_flights_df(flights_df: pd.DataFrame) -> None:
    """
    Validate the flights DataFrame structure and content.

    Args:
        flights_df: DataFrame to validate.

    Raises:
        EmptyFlightsError: If DataFrame is empty.
        MissingColumnsError: If required columns are missing.
    """
    if flights_df.empty:
        raise EmptyFlightsError()

    existing_columns = set(flights_df.columns)
    missing_columns = REQUIRED_COLUMNS - existing_columns

    if missing_columns:
        raise MissingColumnsError(missing_columns)


def validate_airport_exists(
    airport: str,
    flights_df: pd.DataFrame,
    context: str = "flights data",
) -> None:
    """
    Validate that an airport exists in the flights data.

    Args:
        airport: Airport IATA code to validate.
        flights_df: DataFrame containing flight data.
        context: Description for error message.

    Raises:
        InvalidAirportError: If airport is not found.
    """
    all_airports = set(flights_df["departure_airport"]) | set(
        flights_df["arrival_airport"]
    )

    if airport not in all_airports:
        raise InvalidAirportError(airport, context)


def validate_required_cities(
    required_cities: Set[str],
    flights_df: pd.DataFrame,
) -> None:
    """
    Validate that all required cities exist in the flights data.

    Args:
        required_cities: Set of airport codes that must be visited.
        flights_df: DataFrame containing flight data.

    Raises:
        InvalidAirportError: If any required city is not found.
    """
    all_airports = set(flights_df["departure_airport"]) | set(
        flights_df["arrival_airport"]
    )

    for city in required_cities:
        if city not in all_airports:
            raise InvalidAirportError(city, "required cities")


def validate_time_range(t_min: float, t_max: float) -> None:
    """
    Validate the time range parameters.

    Args:
        t_min: Earliest start time.
        t_max: Latest end time.

    Raises:
        InvalidTimeRangeError: If T_min > T_max.
    """
    if t_min > t_max:
        raise InvalidTimeRangeError(t_min, t_max)


def validate_dijkstra_inputs(
    flights_df: pd.DataFrame,
    start_city: str,
    required_cities: Set[str],
    t_min: float,
    t_max: float,
) -> None:
    """
    Validate all inputs for the dijkstra algorithm.

    This is the main validation entry point that performs all checks
    in the correct order for fail-fast behavior.

    Args:
        flights_df: DataFrame with flight data.
        start_city: Starting airport code.
        required_cities: Set of airports to visit.
        t_min: Earliest start time.
        t_max: Latest end time.

    Raises:
        EmptyFlightsError: If flights DataFrame is empty.
        MissingColumnsError: If required columns are missing.
        InvalidAirportError: If start_city or required cities not found.
        InvalidTimeRangeError: If T_min > T_max.
    """
    # 1. Validate DataFrame structure first (cheapest check)
    validate_flights_df(flights_df)

    # 2. Validate time range (cheap check, no DataFrame scan)
    validate_time_range(t_min, t_max)

    # 3. Validate airports exist (requires DataFrame scan)
    validate_airport_exists(start_city, flights_df, "start city")
    validate_required_cities(required_cities, flights_df)
