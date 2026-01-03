"""
Tests for RouteValidationService.

Tests cover:
- Status aggregation (worst-status-wins)
- Confidence averaging
- Price change detection
- Integration with validator
"""

from datetime import date
from typing import List
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.flight_router.ports.offer_validator import (
    RouteValidation,
    SegmentValidation,
    ValidationStatus,
)
from src.flight_router.schemas.route import RouteResult, RouteSegment
from src.flight_router.schemas.validation import ValidatedRoute, ValidationConfig
from src.flight_router.services.route_validation_service import (
    RouteValidationService,
    aggregate_route_status,
)


# =============================================================================
# FIXTURES
# =============================================================================


@pytest.fixture
def sample_segments() -> tuple[RouteSegment, ...]:
    """Create sample route segments."""
    return (
        RouteSegment(
            segment_index=0,
            departure_airport="WAW",
            arrival_airport="BCN",
            dep_time=100.0,
            arr_time=200.0,
            price=150.0,
            carrier_code="LO",
        ),
        RouteSegment(
            segment_index=1,
            departure_airport="BCN",
            arrival_airport="MAD",
            dep_time=250.0,
            arr_time=320.0,
            price=75.0,
            carrier_code="VY",
        ),
    )


@pytest.fixture
def sample_route(sample_segments: tuple[RouteSegment, ...]) -> RouteResult:
    """Create a sample RouteResult."""
    return RouteResult(
        route_id=1,
        segments=sample_segments,
        visited_cities=frozenset({"BCN", "MAD"}),
    )


@pytest.fixture
def mock_validator() -> AsyncMock:
    """Create a mock OfferValidator."""
    validator = AsyncMock()
    validator.name = "Mock Validator"
    return validator


# =============================================================================
# STATUS AGGREGATION TESTS
# =============================================================================


class TestStatusAggregation:
    """Tests for worst-status-wins aggregation logic."""

    def test_all_confirmed_returns_confirmed(self):
        """All CONFIRMED segments produce CONFIRMED route."""
        statuses = [
            ValidationStatus.CONFIRMED,
            ValidationStatus.CONFIRMED,
            ValidationStatus.CONFIRMED,
        ]
        result = aggregate_route_status(statuses)
        assert result == ValidationStatus.CONFIRMED

    def test_any_unavailable_returns_unavailable(self):
        """Any UNAVAILABLE segment produces UNAVAILABLE route."""
        statuses = [
            ValidationStatus.CONFIRMED,
            ValidationStatus.UNAVAILABLE,
            ValidationStatus.CONFIRMED,
        ]
        result = aggregate_route_status(statuses)
        assert result == ValidationStatus.UNAVAILABLE

    def test_any_api_error_returns_api_error(self):
        """Any API_ERROR segment produces API_ERROR route."""
        statuses = [
            ValidationStatus.CONFIRMED,
            ValidationStatus.PRICE_CHANGED,
            ValidationStatus.API_ERROR,
        ]
        result = aggregate_route_status(statuses)
        assert result == ValidationStatus.API_ERROR

    def test_price_changed_beats_confirmed(self):
        """PRICE_CHANGED beats CONFIRMED."""
        statuses = [
            ValidationStatus.CONFIRMED,
            ValidationStatus.PRICE_CHANGED,
        ]
        result = aggregate_route_status(statuses)
        assert result == ValidationStatus.PRICE_CHANGED

    def test_api_error_beats_unavailable(self):
        """API_ERROR beats UNAVAILABLE."""
        statuses = [
            ValidationStatus.UNAVAILABLE,
            ValidationStatus.API_ERROR,
        ]
        result = aggregate_route_status(statuses)
        assert result == ValidationStatus.API_ERROR

    def test_unavailable_beats_price_changed(self):
        """UNAVAILABLE beats PRICE_CHANGED."""
        statuses = [
            ValidationStatus.PRICE_CHANGED,
            ValidationStatus.UNAVAILABLE,
        ]
        result = aggregate_route_status(statuses)
        assert result == ValidationStatus.UNAVAILABLE


# =============================================================================
# ROUTE VALIDATION SERVICE TESTS
# =============================================================================


class TestRouteValidationService:
    """Tests for RouteValidationService."""

    @pytest.mark.anyio
    async def test_validate_route_calls_validator(
        self,
        sample_route: RouteResult,
        mock_validator: AsyncMock,
    ):
        """validate_route calls validator.validate_segments."""
        # Setup mock response
        mock_validator.validate_segments.return_value = [
            SegmentValidation(
                segment_index=0,
                status=ValidationStatus.CONFIRMED,
                confidence=90.0,
                cached_price=150.0,
                live_price=152.0,
            ),
            SegmentValidation(
                segment_index=1,
                status=ValidationStatus.CONFIRMED,
                confidence=85.0,
                cached_price=75.0,
                live_price=76.0,
            ),
        ]

        service = RouteValidationService(mock_validator)
        result = await service.validate_route(sample_route, date(2024, 7, 15))

        # Verify validator was called
        mock_validator.validate_segments.assert_called_once()
        assert result.is_validated is True

    @pytest.mark.anyio
    async def test_aggregates_confidence(
        self,
        sample_route: RouteResult,
        mock_validator: AsyncMock,
    ):
        """Confidence is averaged across segments."""
        mock_validator.validate_segments.return_value = [
            SegmentValidation(
                segment_index=0,
                status=ValidationStatus.CONFIRMED,
                confidence=80.0,
                cached_price=150.0,
                live_price=150.0,
            ),
            SegmentValidation(
                segment_index=1,
                status=ValidationStatus.CONFIRMED,
                confidence=100.0,
                cached_price=75.0,
                live_price=75.0,
            ),
        ]

        service = RouteValidationService(mock_validator)
        result = await service.validate_route(sample_route, date(2024, 7, 15))

        assert result.validation is not None
        assert result.validation.average_confidence == 90.0  # (80 + 100) / 2

    @pytest.mark.anyio
    async def test_aggregates_prices(
        self,
        sample_route: RouteResult,
        mock_validator: AsyncMock,
    ):
        """Prices are summed correctly."""
        mock_validator.validate_segments.return_value = [
            SegmentValidation(
                segment_index=0,
                status=ValidationStatus.CONFIRMED,
                confidence=90.0,
                cached_price=150.0,
                live_price=155.0,
            ),
            SegmentValidation(
                segment_index=1,
                status=ValidationStatus.CONFIRMED,
                confidence=90.0,
                cached_price=75.0,
                live_price=80.0,
            ),
        ]

        service = RouteValidationService(mock_validator)
        result = await service.validate_route(sample_route, date(2024, 7, 15))

        assert result.validation is not None
        assert result.validation.total_cached_price == 225.0  # 150 + 75
        assert result.validation.total_live_price == 235.0  # 155 + 80

    @pytest.mark.anyio
    async def test_live_price_none_if_any_missing(
        self,
        sample_route: RouteResult,
        mock_validator: AsyncMock,
    ):
        """total_live_price is None if any segment missing live price."""
        mock_validator.validate_segments.return_value = [
            SegmentValidation(
                segment_index=0,
                status=ValidationStatus.CONFIRMED,
                confidence=90.0,
                cached_price=150.0,
                live_price=155.0,
            ),
            SegmentValidation(
                segment_index=1,
                status=ValidationStatus.UNAVAILABLE,
                confidence=0.0,
                cached_price=75.0,
                live_price=None,  # Missing
            ),
        ]

        service = RouteValidationService(mock_validator)
        result = await service.validate_route(sample_route, date(2024, 7, 15))

        assert result.validation is not None
        assert result.validation.total_cached_price == 225.0
        assert result.validation.total_live_price is None

    @pytest.mark.anyio
    async def test_validate_routes_top_n(
        self,
        sample_segments: tuple[RouteSegment, ...],
        mock_validator: AsyncMock,
    ):
        """validate_routes only validates top N routes."""
        routes = [
            RouteResult(
                route_id=i,
                segments=sample_segments,
                visited_cities=frozenset({"BCN", "MAD"}),
            )
            for i in range(5)
        ]

        mock_validator.validate_segments.return_value = [
            SegmentValidation(
                segment_index=0,
                status=ValidationStatus.CONFIRMED,
                confidence=90.0,
                cached_price=150.0,
                live_price=150.0,
            ),
            SegmentValidation(
                segment_index=1,
                status=ValidationStatus.CONFIRMED,
                confidence=90.0,
                cached_price=75.0,
                live_price=75.0,
            ),
        ]

        service = RouteValidationService(mock_validator)
        results = await service.validate_routes(
            routes, date(2024, 7, 15), validate_top_n=2
        )

        # Should have 5 results
        assert len(results) == 5

        # First 2 should be validated
        assert results[0].is_validated is True
        assert results[1].is_validated is True

        # Last 3 should NOT be validated
        assert results[2].is_validated is False
        assert results[3].is_validated is False
        assert results[4].is_validated is False

    @pytest.mark.anyio
    async def test_empty_route_returns_none_validation(
        self,
        mock_validator: AsyncMock,
    ):
        """Route with no segments returns None validation."""
        empty_route = RouteResult(
            route_id=1,
            segments=(),
            visited_cities=frozenset(),
        )

        service = RouteValidationService(mock_validator)

        # This should raise ValueError since RouteResult requires segments
        # But for robustness, let's test the service handles it
        result = await service.validate_route(empty_route, date(2024, 7, 15))

        assert result.validation is None


# =============================================================================
# VALIDATED ROUTE TESTS
# =============================================================================


class TestValidatedRoute:
    """Tests for ValidatedRoute dataclass."""

    def test_is_validated_false_when_no_validation(
        self, sample_route: RouteResult
    ):
        """is_validated is False when validation is None."""
        validated = ValidatedRoute(route=sample_route, validation=None)
        assert validated.is_validated is False

    def test_is_validated_true_when_has_validation(
        self, sample_route: RouteResult
    ):
        """is_validated is True when validation present."""
        validation = RouteValidation(
            route_id=1,
            status=ValidationStatus.CONFIRMED,
            segments=(),
            total_cached_price=225.0,
            total_live_price=230.0,
            average_confidence=90.0,
            validation_time_ms=1500.0,
        )
        validated = ValidatedRoute(route=sample_route, validation=validation)
        assert validated.is_validated is True

    def test_is_bookable_confirmed(self, sample_route: RouteResult):
        """is_bookable True for CONFIRMED status."""
        validation = RouteValidation(
            route_id=1,
            status=ValidationStatus.CONFIRMED,
            segments=(),
            total_cached_price=225.0,
            total_live_price=230.0,
            average_confidence=90.0,
            validation_time_ms=1500.0,
        )
        validated = ValidatedRoute(route=sample_route, validation=validation)
        assert validated.is_bookable is True

    def test_is_bookable_price_changed(self, sample_route: RouteResult):
        """is_bookable True for PRICE_CHANGED status."""
        validation = RouteValidation(
            route_id=1,
            status=ValidationStatus.PRICE_CHANGED,
            segments=(),
            total_cached_price=225.0,
            total_live_price=280.0,
            average_confidence=85.0,
            validation_time_ms=1500.0,
        )
        validated = ValidatedRoute(route=sample_route, validation=validation)
        assert validated.is_bookable is True

    def test_is_bookable_unavailable(self, sample_route: RouteResult):
        """is_bookable False for UNAVAILABLE status."""
        validation = RouteValidation(
            route_id=1,
            status=ValidationStatus.UNAVAILABLE,
            segments=(),
            total_cached_price=225.0,
            total_live_price=None,
            average_confidence=20.0,
            validation_time_ms=1500.0,
        )
        validated = ValidatedRoute(route=sample_route, validation=validation)
        assert validated.is_bookable is False

    def test_total_price_uses_live_when_available(
        self, sample_route: RouteResult
    ):
        """total_price uses live price when available."""
        validation = RouteValidation(
            route_id=1,
            status=ValidationStatus.CONFIRMED,
            segments=(),
            total_cached_price=225.0,
            total_live_price=230.0,
            average_confidence=90.0,
            validation_time_ms=1500.0,
        )
        validated = ValidatedRoute(route=sample_route, validation=validation)
        assert validated.total_price == 230.0

    def test_total_price_falls_back_to_cached(
        self, sample_route: RouteResult
    ):
        """total_price falls back to cached when live unavailable."""
        validation = RouteValidation(
            route_id=1,
            status=ValidationStatus.UNAVAILABLE,
            segments=(),
            total_cached_price=225.0,
            total_live_price=None,
            average_confidence=20.0,
            validation_time_ms=1500.0,
        )
        validated = ValidatedRoute(route=sample_route, validation=validation)
        assert validated.total_price == 225.0

    def test_total_price_unvalidated_uses_route_cost(
        self, sample_route: RouteResult
    ):
        """Unvalidated route uses route.total_cost."""
        validated = ValidatedRoute(route=sample_route, validation=None)
        assert validated.total_price == sample_route.total_cost

    def test_price_confidence_levels(self, sample_route: RouteResult):
        """price_confidence returns correct levels."""
        # Unvalidated
        unvalidated = ValidatedRoute(route=sample_route, validation=None)
        assert unvalidated.price_confidence == "unvalidated"

        # Confirmed -> high
        confirmed = ValidatedRoute(
            route=sample_route,
            validation=RouteValidation(
                route_id=1,
                status=ValidationStatus.CONFIRMED,
                segments=(),
                total_cached_price=225.0,
                total_live_price=225.0,
                average_confidence=95.0,
                validation_time_ms=1500.0,
            ),
        )
        assert confirmed.price_confidence == "high"

        # Price changed -> medium
        price_changed = ValidatedRoute(
            route=sample_route,
            validation=RouteValidation(
                route_id=1,
                status=ValidationStatus.PRICE_CHANGED,
                segments=(),
                total_cached_price=225.0,
                total_live_price=280.0,
                average_confidence=85.0,
                validation_time_ms=1500.0,
            ),
        )
        assert price_changed.price_confidence == "medium"

        # Unavailable -> low
        unavailable = ValidatedRoute(
            route=sample_route,
            validation=RouteValidation(
                route_id=1,
                status=ValidationStatus.UNAVAILABLE,
                segments=(),
                total_cached_price=225.0,
                total_live_price=None,
                average_confidence=10.0,
                validation_time_ms=1500.0,
            ),
        )
        assert unavailable.price_confidence == "low"
