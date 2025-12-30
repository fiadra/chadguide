"""
Duffel Data Provider - SQL to DataFrame adapter.

Fetches flight data from the Duffel SQLite database and transforms it
into CoreFlightSchema-compliant DataFrames.
"""

import logging
import re
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Optional, Set

import numpy as np
import pandas as pd

from src.flight_router.ports.flight_data_provider import FlightDataProvider
from src.flight_router.schemas.flight import CoreFlightSchema, FlightDataFrame

logger = logging.getLogger(__name__)

# Reference epoch for time calculations (midnight UTC, Jan 1, 2024)
# Using a fixed epoch allows consistent time comparisons across sessions
EPOCH_REFERENCE = datetime(2024, 1, 1, 0, 0, 0)


def parse_duration_to_minutes_vectorized(durations: pd.Series) -> pd.Series:
    """
    Vectorized ISO 8601 duration parsing.

    Converts duration strings like 'PT2H30M' to total minutes.
    Optimized for batch processing with pandas str methods.

    Args:
        durations: Series of ISO 8601 duration strings.

    Returns:
        Series of duration in minutes (float for NaN support).
        Returns NaN for null/empty inputs.

    Examples:
        >>> s = pd.Series(['PT2H30M', 'PT1H', 'PT45M', None])
        >>> parse_duration_to_minutes_vectorized(s)
        0    150.0
        1     60.0
        2     45.0
        3      NaN
    """
    # Track which values are null/empty
    is_null = durations.isna() | (durations == "")

    # Extract hours using vectorized regex
    hours = durations.str.extract(r"(\d+)H", expand=False)
    hours = pd.to_numeric(hours, errors="coerce").fillna(0)

    # Extract minutes using vectorized regex
    minutes = durations.str.extract(r"(\d+)M", expand=False)
    minutes = pd.to_numeric(minutes, errors="coerce").fillna(0)

    result = hours * 60 + minutes

    # Set null inputs to NaN
    result[is_null] = np.nan

    return result


def datetime_to_epoch_minutes(dt_series: pd.Series) -> pd.Series:
    """
    Convert datetime series to minutes since reference epoch.

    Args:
        dt_series: Series of datetime objects or strings.

    Returns:
        Series of float values representing minutes since epoch.
    """
    # Convert to datetime if strings
    if dt_series.dtype == object:
        dt_series = pd.to_datetime(dt_series, errors="coerce")

    # Calculate minutes since epoch
    delta = dt_series - pd.Timestamp(EPOCH_REFERENCE)
    return delta.dt.total_seconds() / 60


class DuffelDataProvider(FlightDataProvider):
    """
    Data provider for Duffel SQLite database.

    Reads flight data from the flights_static and flight_quotes tables,
    joins them, and transforms to CoreFlightSchema format.

    Attributes:
        db_path: Path to the SQLite database file.
        _conn: SQLite connection (lazy initialized).
    """

    def __init__(self, db_path: str = "Duffel_api/flights.db") -> None:
        """
        Initialize the Duffel data provider.

        Args:
            db_path: Path to SQLite database file.
        """
        self._db_path = Path(db_path)
        self._conn: Optional[sqlite3.Connection] = None

    def _get_connection(self) -> sqlite3.Connection:
        """Get or create database connection."""
        if self._conn is None:
            if not self._db_path.exists():
                raise FileNotFoundError(f"Database not found: {self._db_path}")
            self._conn = sqlite3.connect(str(self._db_path))
        return self._conn

    def get_flights_df(
        self,
        origin: Optional[str] = None,
        destination: Optional[str] = None,
        date_start: Optional[datetime] = None,
        date_end: Optional[datetime] = None,
    ) -> FlightDataFrame:
        """
        Fetch flights from database and transform to CoreFlightSchema.

        Args:
            origin: Filter by departure airport (optional).
            destination: Filter by arrival airport (optional).
            date_start: Filter by earliest departure date (optional).
            date_end: Filter by latest departure date (optional).

        Returns:
            DataFrame validated against CoreFlightSchema.
        """
        conn = self._get_connection()

        # Build SQL query with optional filters
        query = """
            SELECT
                fs.origin_iata,
                fs.dest_iata,
                fs.duration_iso,
                fs.carrier_code,
                fs.carrier_name,
                fq.price_amount,
                fq.departure_date,
                fq.baggage_checked
            FROM flights_static fs
            JOIN flight_quotes fq ON fs.route_id = fq.flight_static_id
            WHERE 1=1
        """
        params: list = []

        if origin:
            query += " AND fs.origin_iata = ?"
            params.append(origin)

        if destination:
            query += " AND fs.dest_iata = ?"
            params.append(destination)

        if date_start:
            query += " AND fq.departure_date >= ?"
            params.append(date_start.strftime("%Y-%m-%d"))

        if date_end:
            query += " AND fq.departure_date <= ?"
            params.append(date_end.strftime("%Y-%m-%d"))

        logger.debug("Executing query: %s with params: %s", query, params)

        # Use pandas read_sql for efficient loading
        df = pd.read_sql(query, conn, params=params)

        if df.empty:
            logger.warning("No flights found matching criteria")
            # Return empty DataFrame with correct schema
            return pd.DataFrame(
                columns=[
                    "departure_airport",
                    "arrival_airport",
                    "dep_time",
                    "arr_time",
                    "price",
                    "carrier_code",
                    "carrier_name",
                    "baggage_included",
                    "scheduled_departure",
                ]
            )

        # Transform to CoreFlightSchema format
        transformed = self._transform_to_schema(df)

        # Validate against schema
        validated = CoreFlightSchema.validate(transformed)

        logger.info(
            "Loaded %d flights from Duffel database",
            len(validated),
        )

        return validated

    def _transform_to_schema(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform raw database columns to CoreFlightSchema format.

        Mapping:
        - origin_iata -> departure_airport
        - dest_iata -> arrival_airport
        - departure_date (ISO timestamp) -> dep_time (minutes since epoch)
        - dep_time + duration_iso -> arr_time
        - price_amount -> price

        The departure_date column contains full ISO timestamps (e.g., '2026-07-13T09:17:00'),
        which are converted directly to minutes since epoch for accurate routing.

        Args:
            df: Raw DataFrame from SQL query.

        Returns:
            Transformed DataFrame matching CoreFlightSchema.
        """
        result = pd.DataFrame()

        # Core fields (required by schema)
        result["departure_airport"] = df["origin_iata"]
        result["arrival_airport"] = df["dest_iata"]

        # Parse duration from ISO format (e.g., 'PT2H30M' -> 150.0)
        duration_minutes = parse_duration_to_minutes_vectorized(df["duration_iso"])

        # Convert departure_date (full ISO timestamp) to epoch minutes
        # The database stores full timestamps like '2026-07-13T09:17:00'
        departure_timestamps = pd.to_datetime(df["departure_date"], errors="coerce")
        dep_time_minutes = datetime_to_epoch_minutes(departure_timestamps)

        result["dep_time"] = dep_time_minutes
        result["arr_time"] = dep_time_minutes + duration_minutes
        result["price"] = df["price_amount"].astype(float)

        # Extended fields (optional, for filtering/display)
        result["carrier_code"] = df["carrier_code"]
        result["carrier_name"] = df["carrier_name"]
        result["baggage_included"] = df["baggage_checked"].astype(float)
        result["scheduled_departure"] = df["departure_date"].astype(str)

        return result

    def get_airports(self) -> Set[str]:
        """
        Get all airport codes in the database.

        Returns:
            Set of IATA airport codes.
        """
        conn = self._get_connection()

        query = """
            SELECT DISTINCT origin_iata FROM flights_static
            UNION
            SELECT DISTINCT dest_iata FROM flights_static
        """
        df = pd.read_sql(query, conn)

        airports = set(df.iloc[:, 0].dropna().unique())
        logger.debug("Found %d airports in database", len(airports))

        return airports

    @property
    def name(self) -> str:
        """Human-readable provider name."""
        return "Duffel SQLite"

    @property
    def is_available(self) -> bool:
        """Check if database is accessible."""
        return self._db_path.exists()

    def close(self) -> None:
        """Close database connection."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None
            logger.debug("Database connection closed")
