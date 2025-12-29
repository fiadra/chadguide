"""
Tests for load_flights module.

Tests SQLite database loading and datetime normalization.
"""

import sqlite3
import tempfile
from pathlib import Path

import pandas as pd
import pytest

from dijkstra.load_flights import load_flights


# -------------------------
# Fixtures
# -------------------------


@pytest.fixture
def temp_db():
    """Create a temporary SQLite database with test flight data."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test_flights.db"
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Create flights table
        cursor.execute("""
            CREATE TABLE flights (
                departure_airport TEXT,
                arrival_airport TEXT,
                scheduled_departure TEXT,
                scheduled_arrival TEXT,
                price REAL
            )
        """)

        # Insert test data
        test_flights = [
            ("WAW", "BCN", "2026-07-15 08:00:00", "2026-07-15 11:30:00", 99.50),
            ("BCN", "WAW", "2026-07-18 14:00:00", "2026-07-18 17:30:00", 120.00),
            ("WAW", "FCO", "2026-07-15 10:00:00", "2026-07-15 12:30:00", 85.00),
        ]

        cursor.executemany(
            """
            INSERT INTO flights VALUES (?, ?, ?, ?, ?)
            """,
            test_flights,
        )

        conn.commit()
        conn.close()

        yield str(db_path)


@pytest.fixture
def empty_db():
    """Create an empty SQLite database with flights table."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "empty_flights.db"
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE flights (
                departure_airport TEXT,
                arrival_airport TEXT,
                scheduled_departure TEXT,
                scheduled_arrival TEXT,
                price REAL
            )
        """)

        conn.commit()
        conn.close()

        yield str(db_path)


# -------------------------
# Basic loading tests
# -------------------------


def test_load_flights_returns_dataframe(temp_db):
    """Test that load_flights returns a pandas DataFrame."""
    result = load_flights(temp_db)

    assert isinstance(result, pd.DataFrame)


def test_load_flights_has_expected_columns(temp_db):
    """Test that the returned DataFrame has all expected columns."""
    result = load_flights(temp_db)

    expected_columns = {
        "departure_airport",
        "arrival_airport",
        "scheduled_departure",
        "scheduled_arrival",
        "price",
        "dep_time",
        "arr_time",
    }
    assert set(result.columns) == expected_columns


def test_load_flights_correct_row_count(temp_db):
    """Test that all rows are loaded from the database."""
    result = load_flights(temp_db)

    assert len(result) == 3


def test_load_flights_empty_database(empty_db):
    """Test that empty database returns empty DataFrame."""
    result = load_flights(empty_db)

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 0


# -------------------------
# Data integrity tests
# -------------------------


def test_load_flights_airport_codes_preserved(temp_db):
    """Test that airport codes are loaded correctly."""
    result = load_flights(temp_db)

    departure_airports = set(result["departure_airport"])
    arrival_airports = set(result["arrival_airport"])

    assert departure_airports == {"WAW", "BCN"}
    assert arrival_airports == {"BCN", "WAW", "FCO"}


def test_load_flights_prices_preserved(temp_db):
    """Test that prices are loaded correctly."""
    result = load_flights(temp_db)

    prices = set(result["price"])
    assert prices == {99.50, 120.00, 85.00}


# -------------------------
# Datetime conversion tests
# -------------------------


def test_load_flights_scheduled_departure_is_datetime(temp_db):
    """Test that scheduled_departure is converted to datetime."""
    result = load_flights(temp_db)

    assert pd.api.types.is_datetime64_any_dtype(result["scheduled_departure"])


def test_load_flights_scheduled_arrival_is_datetime(temp_db):
    """Test that scheduled_arrival is converted to datetime."""
    result = load_flights(temp_db)

    assert pd.api.types.is_datetime64_any_dtype(result["scheduled_arrival"])


def test_load_flights_dep_time_is_numeric(temp_db):
    """Test that dep_time is converted to numeric (minutes since epoch)."""
    result = load_flights(temp_db)

    assert pd.api.types.is_numeric_dtype(result["dep_time"])


def test_load_flights_arr_time_is_numeric(temp_db):
    """Test that arr_time is converted to numeric (minutes since epoch)."""
    result = load_flights(temp_db)

    assert pd.api.types.is_numeric_dtype(result["arr_time"])


def test_load_flights_arr_time_greater_than_dep_time(temp_db):
    """Test that arrival time is after departure time for all flights."""
    result = load_flights(temp_db)

    assert (result["arr_time"] > result["dep_time"]).all()


def test_load_flights_time_conversion_accuracy(temp_db):
    """Test that time conversion to minutes is accurate."""
    result = load_flights(temp_db)

    # First flight: 2026-07-15 08:00:00 to 2026-07-15 11:30:00
    # Duration should be 3.5 hours = 210 minutes
    first_flight = result.iloc[0]
    duration_minutes = first_flight["arr_time"] - first_flight["dep_time"]

    assert duration_minutes == 210.0


# -------------------------
# Error handling tests
# -------------------------


def test_load_flights_nonexistent_db_raises_error():
    """Test that nonexistent database raises an error."""
    with pytest.raises(Exception):  # sqlite3.OperationalError
        load_flights("/nonexistent/path/to/flights.db")


def test_load_flights_invalid_db_raises_error():
    """Test that invalid database file raises an error."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        f.write(b"invalid database content")
        f.flush()

        with pytest.raises(Exception):  # sqlite3.DatabaseError
            load_flights(f.name)
