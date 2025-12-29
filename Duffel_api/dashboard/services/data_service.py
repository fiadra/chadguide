"""
Data service module for loading and preprocessing flight data.

Provides functions for fetching and transforming flight data
from the SQLite database for dashboard visualization.
"""

import logging
import sqlite3
from typing import Optional

import pandas as pd
import streamlit as st

from dashboard.config import DashboardConfig
from dashboard.services.route_service import parse_duration_to_minutes

logger = logging.getLogger(__name__)

__all__ = [
    "load_flight_data",
    "get_cached_flight_data",
]


def _build_query() -> str:
    """Build the SQL query for joining quotes with static data."""
    return """
    SELECT
        q.price_amount, q.currency, q.departure_date, q.fare_brand,
        q.baggage_checked, q.baggage_carryon,
        s.*
    FROM flight_quotes q
    LEFT JOIN flights_static s ON q.flight_static_id = s.route_id
    """


def _apply_transformations(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply data type conversions and calculate derived columns.

    Args:
        df: Raw DataFrame from database query.

    Returns:
        Transformed DataFrame with additional computed columns.
    """
    if df.empty:
        return df

    config = DashboardConfig.data

    # Date transformations
    df["departure_date"] = pd.to_datetime(df["departure_date"])
    df["day_of_week"] = df["departure_date"].dt.day_name()
    df["day_num"] = df["departure_date"].dt.weekday  # 0=Monday

    # Boolean conversions
    df["has_wifi"] = df["has_wifi"].astype(bool)
    df["has_power"] = df["has_power"].astype(bool)

    # Numeric conversions
    df["co2_kg"] = pd.to_numeric(df["co2_kg"], errors="coerce").fillna(0)

    # Parse seat pitch (extract numeric value from text like "30 inches")
    df["seat_pitch_num"] = pd.to_numeric(
        df["seat_pitch"].astype(str).str.extract(r"(\d+)")[0], errors="coerce"
    ).fillna(config.default_seat_pitch)

    # Calculate comfort score (0-4 points)
    # +2 for seat pitch >= 30 inches, +1 for WiFi, +1 for power
    df["comfort_score"] = (
        (df["seat_pitch_num"] >= config.seat_pitch_threshold).astype(int) * 2
        + df["has_wifi"].astype(int)
        + df["has_power"].astype(int)
    )

    # Parse duration from ISO format (PT2H30M -> 150 minutes)
    if "duration_iso" in df.columns:
        df["duration_minutes"] = df["duration_iso"].apply(parse_duration_to_minutes)
    else:
        df["duration_minutes"] = None

    # Boolean for direct flights
    if "is_non_stop" in df.columns:
        df["is_direct"] = df["is_non_stop"].astype(bool)
    else:
        df["is_direct"] = True  # Default to direct

    return df


def load_flight_data(db_path: Optional[str] = None) -> pd.DataFrame:
    """
    Load and preprocess flight data from the database.

    Args:
        db_path: Path to SQLite database. Uses default from config if not specified.

    Returns:
        Preprocessed DataFrame with flight data and computed columns.
        Returns empty DataFrame if database is unavailable.
    """
    if db_path is None:
        db_path = DashboardConfig.data.database_path

    try:
        logger.info("Loading data from database: %s", db_path)
        conn = sqlite3.connect(db_path)
        df = pd.read_sql(_build_query(), conn)
        conn.close()

        if df.empty:
            logger.warning("Database returned no records")
            return pd.DataFrame()

        df = _apply_transformations(df)
        logger.info("Loaded %d records from database", len(df))
        return df

    except (sqlite3.Error, OSError) as error:
        logger.exception("Database error: %s", error)
        return pd.DataFrame()


@st.cache_data(ttl=3600)
def get_cached_flight_data() -> pd.DataFrame:
    """
    Load flight data with Streamlit caching.

    This is the primary entry point for loading data in the dashboard.
    Uses Streamlit's cache_data decorator for performance.
    Cache expires after 1 hour (ttl=3600 seconds) to pick up new data.

    Returns:
        Cached DataFrame with flight data.
    """
    return load_flight_data()
