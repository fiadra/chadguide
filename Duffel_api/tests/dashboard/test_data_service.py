"""Tests for data service module."""

import pandas as pd
import pytest

from dashboard.services.data_service import _apply_transformations


class TestApplyTransformations:
    """Tests for the _apply_transformations function."""

    def test_empty_dataframe_returns_empty(self) -> None:
        """Test that empty DataFrame is returned unchanged."""
        df = pd.DataFrame()
        result = _apply_transformations(df)
        assert result.empty

    def test_datetime_conversion(self) -> None:
        """Test that departure_date is converted to datetime."""
        df = pd.DataFrame(
            {
                "departure_date": ["2024-07-15T10:00:00"],
                "has_wifi": [1],
                "has_power": [1],
                "co2_kg": ["100"],
                "seat_pitch": ["30 inches"],
            }
        )

        result = _apply_transformations(df)

        assert pd.api.types.is_datetime64_any_dtype(result["departure_date"])

    def test_day_of_week_extraction(self) -> None:
        """Test that day_of_week is correctly extracted."""
        df = pd.DataFrame(
            {
                "departure_date": ["2024-07-15T10:00:00"],  # Monday
                "has_wifi": [1],
                "has_power": [1],
                "co2_kg": ["100"],
                "seat_pitch": ["30 inches"],
            }
        )

        result = _apply_transformations(df)

        assert result.iloc[0]["day_of_week"] == "Monday"
        assert result.iloc[0]["day_num"] == 0

    def test_boolean_conversion(self) -> None:
        """Test that has_wifi and has_power are converted to boolean."""
        df = pd.DataFrame(
            {
                "departure_date": ["2024-07-15T10:00:00"],
                "has_wifi": [1],
                "has_power": [0],
                "co2_kg": ["100"],
                "seat_pitch": ["30 inches"],
            }
        )

        result = _apply_transformations(df)

        assert result.iloc[0]["has_wifi"] == True
        assert result.iloc[0]["has_power"] == False

    def test_seat_pitch_parsing(self) -> None:
        """Test that seat_pitch numeric value is extracted."""
        df = pd.DataFrame(
            {
                "departure_date": ["2024-07-15T10:00:00"],
                "has_wifi": [1],
                "has_power": [1],
                "co2_kg": ["100"],
                "seat_pitch": ["32 inches"],
            }
        )

        result = _apply_transformations(df)

        assert result.iloc[0]["seat_pitch_num"] == 32

    def test_seat_pitch_default_value(self) -> None:
        """Test that invalid seat_pitch uses default value."""
        df = pd.DataFrame(
            {
                "departure_date": ["2024-07-15T10:00:00"],
                "has_wifi": [1],
                "has_power": [1],
                "co2_kg": ["100"],
                "seat_pitch": ["unknown"],
            }
        )

        result = _apply_transformations(df)

        assert result.iloc[0]["seat_pitch_num"] == 29  # Default value

    def test_comfort_score_calculation_max(self) -> None:
        """Test comfort score calculation with all features."""
        df = pd.DataFrame(
            {
                "departure_date": ["2024-07-15T10:00:00"],
                "has_wifi": [1],
                "has_power": [1],
                "co2_kg": ["100"],
                "seat_pitch": ["32 inches"],  # >= 30, so +2
            }
        )

        result = _apply_transformations(df)

        # seat_pitch >= 30 (+2) + wifi (+1) + power (+1) = 4
        assert result.iloc[0]["comfort_score"] == 4

    def test_comfort_score_calculation_min(self) -> None:
        """Test comfort score calculation with no features."""
        df = pd.DataFrame(
            {
                "departure_date": ["2024-07-15T10:00:00"],
                "has_wifi": [0],
                "has_power": [0],
                "co2_kg": ["100"],
                "seat_pitch": ["28 inches"],  # < 30, so +0
            }
        )

        result = _apply_transformations(df)

        assert result.iloc[0]["comfort_score"] == 0

    def test_co2_kg_conversion(self) -> None:
        """Test that co2_kg is converted to numeric."""
        df = pd.DataFrame(
            {
                "departure_date": ["2024-07-15T10:00:00"],
                "has_wifi": [1],
                "has_power": [1],
                "co2_kg": ["150.5"],
                "seat_pitch": ["30 inches"],
            }
        )

        result = _apply_transformations(df)

        assert result.iloc[0]["co2_kg"] == 150.5

    def test_co2_kg_invalid_defaults_to_zero(self) -> None:
        """Test that invalid co2_kg defaults to 0."""
        df = pd.DataFrame(
            {
                "departure_date": ["2024-07-15T10:00:00"],
                "has_wifi": [1],
                "has_power": [1],
                "co2_kg": ["invalid"],
                "seat_pitch": ["30 inches"],
            }
        )

        result = _apply_transformations(df)

        assert result.iloc[0]["co2_kg"] == 0
