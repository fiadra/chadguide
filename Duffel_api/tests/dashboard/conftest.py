"""
Pytest fixtures for dashboard tests.

Provides common test data and fixtures used across dashboard test modules.
"""

from datetime import date, datetime

import pandas as pd
import pytest


@pytest.fixture
def sample_flight_data() -> pd.DataFrame:
    """Create sample flight data for testing."""
    return pd.DataFrame(
        {
            "origin_iata": ["WAW", "WAW", "WAW", "KRK", "KRK"],
            "dest_iata": ["BCN", "BCN", "PAR", "BCN", "ROM"],
            "carrier_name": ["Ryanair", "Vueling", "Air France", "Ryanair", "LOT"],
            "carrier_code": ["FR", "VY", "AF", "FR", "LO"],
            "price_amount": [50.0, 100.0, 150.0, 75.0, 200.0],
            "currency": ["EUR", "EUR", "EUR", "EUR", "EUR"],
            "departure_date": pd.to_datetime(
                [
                    "2024-07-15",
                    "2024-07-16",
                    "2024-07-17",
                    "2024-07-15",
                    "2024-07-18",
                ]
            ),
            "day_of_week": ["Monday", "Tuesday", "Wednesday", "Monday", "Thursday"],
            "day_num": [0, 1, 2, 0, 3],
            "has_wifi": [False, True, True, False, True],
            "has_power": [False, True, True, False, True],
            "baggage_checked": [0, 1, 2, 0, 1],
            "baggage_carryon": [1, 1, 1, 1, 1],
            "seat_pitch": ["29 inches", "31 inches", "32 inches", "29 inches", "30 inches"],
            "seat_pitch_num": [29, 31, 32, 29, 30],
            "comfort_score": [0, 4, 4, 0, 4],
            "co2_kg": [85.0, 90.0, 120.0, 85.0, 150.0],
            "aircraft_model": [
                "Boeing 737",
                "Airbus A320",
                "Airbus A321",
                "Boeing 737",
                "Embraer E195",
            ],
            "origin_lat": [52.17, 52.17, 52.17, 50.08, 50.08],
            "origin_lon": [20.97, 20.97, 20.97, 19.78, 19.78],
            "dest_lat": [41.30, 41.30, 48.86, 41.30, 41.90],
            "dest_lon": [2.08, 2.08, 2.35, 2.08, 12.50],
            "flight_number": ["1234", "5678", "9012", "3456", "7890"],
            "is_direct": [True, True, False, True, True],
            "duration_minutes": [150, 155, 180, 160, 200],
        }
    )


@pytest.fixture
def empty_flight_data() -> pd.DataFrame:
    """Create an empty DataFrame with expected columns."""
    return pd.DataFrame(
        columns=[
            "origin_iata",
            "dest_iata",
            "carrier_name",
            "carrier_code",
            "price_amount",
            "currency",
            "departure_date",
            "day_of_week",
            "day_num",
            "has_wifi",
            "has_power",
            "baggage_checked",
            "baggage_carryon",
            "seat_pitch",
            "seat_pitch_num",
            "comfort_score",
            "co2_kg",
            "aircraft_model",
            "origin_lat",
            "origin_lon",
            "dest_lat",
            "dest_lon",
            "flight_number",
            "is_direct",
            "duration_minutes",
        ]
    )
