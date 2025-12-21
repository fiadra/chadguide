"""
Database module for flight data persistence.

This module provides a Database class for managing SQLite database operations
including table creation, route storage, quote storage, and scan status tracking.
"""

import logging
import sqlite3
from datetime import datetime
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


class Database:
    """
    SQLite database manager for flight data.

    This class handles all database operations including creating tables,
    saving flight routes and quotes, and tracking scan completion status.

    Attributes:
        conn: The SQLite database connection object.

    Example:
        >>> db = Database("Duffel_api/flights.db")
        >>> db.save_route(static_record, "2024-07-15")
        >>> db.save_quote(quote_record)
    """

    def __init__(self, db_name: str = "Duffel_api/flights.db") -> None:
        """
        Initialize the database connection and create tables.

        Args:
            db_name: Path to the SQLite database file.
                Defaults to "Duffel_api/flights.db".
        """
        logger.debug("Connecting to database: %s", db_name)
        self.conn: sqlite3.Connection = sqlite3.connect(db_name)
        self._create_tables()

    def _create_tables(self) -> None:
        """
        Create database tables if they do not exist.

        Creates three tables:
            - flights_static: Static route and aircraft information
            - flight_quotes: Price quotes and baggage information
            - route_scan_status: Scan completion tracking
        """
        cursor = self.conn.cursor()

        # Static flight information table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS flights_static (
                route_id TEXT PRIMARY KEY,
                carrier_code TEXT,
                carrier_name TEXT,
                flight_number TEXT,
                origin_iata TEXT,
                dest_iata TEXT,
                dest_city_code TEXT,
                duration_iso TEXT,
                origin_lat REAL,
                origin_lon REAL,
                dest_lat REAL,
                dest_lon REAL,
                aircraft_model TEXT,
                has_wifi BOOLEAN,
                has_power BOOLEAN,
                seat_pitch TEXT,
                legroom TEXT,
                co2_kg REAL,
                logo_url TEXT,
                is_non_stop BOOLEAN,
                operating_days TEXT
            )
        ''')

        # Price quotes table (dynamic data: price, baggage, departure date)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS flight_quotes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                flight_static_id TEXT,
                price_amount REAL,
                currency TEXT,
                fare_brand TEXT,
                baggage_checked INTEGER,
                baggage_carryon INTEGER,
                departure_date DATE,
                scanned_at DATETIME
            )
        ''')

        # Scan status tracking table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS route_scan_status (
                origin TEXT,
                destination TEXT,
                status TEXT,
                last_scanned_at DATETIME,
                total_flights_found INTEGER,
                PRIMARY KEY (origin, destination)
            )
        ''')

        self.conn.commit()
        logger.debug("Database tables created/verified")

    def save_route(
        self,
        record: Dict[str, Any],
        scan_date_str: str
    ) -> None:
        """
        Save or update a static flight route record.

        If the route already exists, updates the record and merges the
        operating days. If new, inserts the record with the current
        day of week.

        Args:
            record: A dictionary containing the static flight data.
                Must include a 'route_id' key.
            scan_date_str: The scan date in ISO format (YYYY-MM-DD).
                Used to determine the day of week for operating days.
        """
        cursor = self.conn.cursor()

        # Calculate day of week (1-7, Monday=1)
        scan_date = datetime.strptime(scan_date_str, "%Y-%m-%d")
        current_day_num: str = str(scan_date.isoweekday())

        route_id: str = record['route_id']
        cursor.execute(
            "SELECT operating_days FROM flights_static WHERE route_id = ?",
            (route_id,)
        )
        existing_row: Optional[tuple] = cursor.fetchone()

        if existing_row is None:
            # Insert new route
            record['operating_days'] = current_day_num
            columns = ', '.join(record.keys())
            placeholders = ', '.join(['?'] * len(record))
            sql = f"INSERT INTO flights_static ({columns}) VALUES ({placeholders})"
            cursor.execute(sql, list(record.values()))
            logger.debug("Inserted new route: %s", route_id)
        else:
            # Update existing route and merge operating days
            existing_days_str: Optional[str] = existing_row[0]
            days_set = set(existing_days_str.split(',')) if existing_days_str else set()
            days_set.add(current_day_num)
            sorted_days = sorted(list(days_set))
            record['operating_days'] = ",".join(sorted_days)

            set_clause = ', '.join([f"{key} = ?" for key in record.keys()])
            sql = f"UPDATE flights_static SET {set_clause} WHERE route_id = ?"
            values = list(record.values()) + [route_id]
            cursor.execute(sql, values)
            logger.debug("Updated route: %s", route_id)

        self.conn.commit()

    def save_quote(self, record: Dict[str, Any]) -> None:
        """
        Save a flight price quote record.

        Args:
            record: A dictionary containing the quote data including
                price, currency, baggage allowance, and departure date.
        """
        cursor = self.conn.cursor()

        columns = ', '.join(record.keys())
        placeholders = ', '.join(['?'] * len(record))

        sql = f"INSERT INTO flight_quotes ({columns}) VALUES ({placeholders})"
        cursor.execute(sql, list(record.values()))
        self.conn.commit()

        logger.debug(
            "Saved quote for flight: %s",
            record.get('flight_static_id', 'unknown')
        )

    def is_route_fully_scanned(self, origin: str, destination: str) -> bool:
        """
        Check if a route has been completely scanned.

        Args:
            origin: The IATA code of the departure airport.
            destination: The IATA code of the arrival airport.

        Returns:
            True if the route has been marked as completed, False otherwise.
        """
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT 1 FROM route_scan_status
            WHERE origin = ? AND destination = ? AND status = 'COMPLETED'
        ''', (origin, destination))
        return cursor.fetchone() is not None

    def mark_route_completed(
        self,
        origin: str,
        destination: str,
        total_found: int
    ) -> None:
        """
        Mark a route as completely scanned.

        Args:
            origin: The IATA code of the departure airport.
            destination: The IATA code of the arrival airport.
            total_found: The total number of flight offers found for this route.
        """
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO route_scan_status
            (origin, destination, status, last_scanned_at, total_flights_found)
            VALUES (?, ?, 'COMPLETED', ?, ?)
        ''', (origin, destination, datetime.now(), total_found))
        self.conn.commit()

        logger.info(
            "Route %s -> %s marked as completed with %d flights",
            origin, destination, total_found
        )

    def close(self) -> None:
        """Close the database connection."""
        self.conn.close()
        logger.debug("Database connection closed")
