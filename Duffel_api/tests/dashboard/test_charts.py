"""Tests for chart creation functions."""

import pandas as pd
import plotly.graph_objects as go
import pytest

from dashboard.charts.price_charts import (
    create_price_by_day_figure,
    create_price_calendar_figure,
    create_price_distribution_figure,
)
from dashboard.charts.quality_charts import (
    create_eco_chart_figure,
    create_value_matrix_figure,
)
from dashboard.charts.map_chart import create_route_map_figure


class TestPriceCharts:
    """Tests for price chart creation functions."""

    def test_price_by_day_returns_figure(
        self, sample_flight_data: pd.DataFrame
    ) -> None:
        """Test that price_by_day returns a Plotly figure."""
        result = create_price_by_day_figure(sample_flight_data)

        assert isinstance(result, go.Figure)

    def test_price_by_day_empty_returns_none(
        self, empty_flight_data: pd.DataFrame
    ) -> None:
        """Test that empty data returns None."""
        result = create_price_by_day_figure(empty_flight_data)

        assert result is None

    def test_price_distribution_returns_figure(
        self, sample_flight_data: pd.DataFrame
    ) -> None:
        """Test that price_distribution returns a Plotly figure."""
        result = create_price_distribution_figure(sample_flight_data)

        assert isinstance(result, go.Figure)

    def test_price_distribution_empty_returns_none(
        self, empty_flight_data: pd.DataFrame
    ) -> None:
        """Test that empty data returns None."""
        result = create_price_distribution_figure(empty_flight_data)

        assert result is None

    def test_price_calendar_returns_figure(
        self, sample_flight_data: pd.DataFrame
    ) -> None:
        """Test that price_calendar returns a Plotly figure."""
        result = create_price_calendar_figure(sample_flight_data)

        assert isinstance(result, go.Figure)

    def test_price_calendar_empty_returns_none(
        self, empty_flight_data: pd.DataFrame
    ) -> None:
        """Test that empty data returns None."""
        result = create_price_calendar_figure(empty_flight_data)

        assert result is None


class TestQualityCharts:
    """Tests for quality/comfort chart creation functions."""

    def test_value_matrix_returns_figure(
        self, sample_flight_data: pd.DataFrame
    ) -> None:
        """Test that value_matrix returns a Plotly figure."""
        result = create_value_matrix_figure(sample_flight_data)

        assert isinstance(result, go.Figure)

    def test_value_matrix_empty_returns_none(
        self, empty_flight_data: pd.DataFrame
    ) -> None:
        """Test that empty data returns None."""
        result = create_value_matrix_figure(empty_flight_data)

        assert result is None

    def test_eco_chart_returns_figure(self, sample_flight_data: pd.DataFrame) -> None:
        """Test that eco_chart returns a Plotly figure."""
        result = create_eco_chart_figure(sample_flight_data)

        assert isinstance(result, go.Figure)

    def test_eco_chart_empty_returns_none(
        self, empty_flight_data: pd.DataFrame
    ) -> None:
        """Test that empty data returns None."""
        result = create_eco_chart_figure(empty_flight_data)

        assert result is None


class TestMapChart:
    """Tests for map chart creation function."""

    def test_route_map_returns_figure(self, sample_flight_data: pd.DataFrame) -> None:
        """Test that route_map returns a Plotly figure."""
        result = create_route_map_figure(sample_flight_data)

        assert isinstance(result, go.Figure)

    def test_route_map_empty_returns_none(
        self, empty_flight_data: pd.DataFrame
    ) -> None:
        """Test that empty data returns None."""
        result = create_route_map_figure(empty_flight_data)

        assert result is None

    def test_route_map_has_traces(self, sample_flight_data: pd.DataFrame) -> None:
        """Test that route map has expected traces (routes + markers)."""
        result = create_route_map_figure(sample_flight_data)

        # Should have route lines, destination markers, and origin marker
        assert len(result.data) > 0
