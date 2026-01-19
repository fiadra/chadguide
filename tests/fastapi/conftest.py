"""
Fixtures for FastAPI endpoint tests.

Provides shared test data and mocks for SSE streaming endpoint tests.
"""

import pytest
from unittest.mock import AsyncMock, MagicMock
from typing import Optional

from src.flight_router.schemas.route import RouteResult, RouteSegment
from src.flight_router.schemas.validation import ValidatedRoute
from src.flight_router.ports.offer_validator import (
    RouteValidation,
    SegmentValidation,
    ValidationStatus,
)


@pytest.fixture
def anyio_backend():
    """Use asyncio backend for async tests."""
    return "asyncio"


@pytest.fixture
def sample_route_segments() -> tuple[RouteSegment, ...]:
    """Create sample route segments for WAW → BCN → WAW."""
    return (
        RouteSegment(
            segment_index=0,
            departure_airport="WAW",
            arrival_airport="BCN",
            dep_time=600.0,
            arr_time=780.0,
            price=150.0,
            carrier_code="LO",
            carrier_name="LOT Polish Airlines",
        ),
        RouteSegment(
            segment_index=1,
            departure_airport="BCN",
            arrival_airport="WAW",
            dep_time=900.0,
            arr_time=1080.0,
            price=140.0,
            carrier_code="VY",
            carrier_name="Vueling",
        ),
    )


@pytest.fixture
def sample_route_result(sample_route_segments) -> RouteResult:
    """Create a sample RouteResult."""
    return RouteResult(
        route_id=1,
        segments=sample_route_segments,
        visited_cities=frozenset({"BCN"}),
    )


@pytest.fixture
def sample_segment_validations() -> tuple[SegmentValidation, ...]:
    """Create sample segment validations."""
    return (
        SegmentValidation(
            segment_index=0,
            status=ValidationStatus.CONFIRMED,
            confidence=90.0,
            cached_price=150.0,
            live_price=152.0,
            offer_id="off_001",
        ),
        SegmentValidation(
            segment_index=1,
            status=ValidationStatus.CONFIRMED,
            confidence=85.0,
            cached_price=140.0,
            live_price=143.0,
            offer_id="off_002",
        ),
    )


@pytest.fixture
def sample_route_validation(sample_segment_validations) -> RouteValidation:
    """Create a sample RouteValidation with CONFIRMED status."""
    return RouteValidation(
        route_id=1,
        status=ValidationStatus.CONFIRMED,
        segments=sample_segment_validations,
        total_cached_price=290.0,
        total_live_price=295.0,
        average_confidence=87.5,
        validation_time_ms=1500.0,
    )


@pytest.fixture
def sample_validated_route_bookable(
    sample_route_result, sample_route_validation
) -> ValidatedRoute:
    """Create a validated route that is bookable (CONFIRMED status)."""
    return ValidatedRoute(
        route=sample_route_result,
        validation=sample_route_validation,
    )


@pytest.fixture
def sample_validated_route_price_changed(sample_route_result) -> ValidatedRoute:
    """Create a validated route with PRICE_CHANGED status (still bookable)."""
    validation = RouteValidation(
        route_id=1,
        status=ValidationStatus.PRICE_CHANGED,
        segments=(
            SegmentValidation(
                segment_index=0,
                status=ValidationStatus.PRICE_CHANGED,
                confidence=80.0,
                cached_price=150.0,
                live_price=180.0,  # 20% increase
                offer_id="off_003",
            ),
        ),
        total_cached_price=150.0,
        total_live_price=180.0,
        average_confidence=80.0,
        validation_time_ms=1200.0,
    )
    return ValidatedRoute(route=sample_route_result, validation=validation)


@pytest.fixture
def sample_validated_route_unavailable(sample_route_result) -> ValidatedRoute:
    """Create a validated route that is NOT bookable (UNAVAILABLE status)."""
    validation = RouteValidation(
        route_id=1,
        status=ValidationStatus.UNAVAILABLE,
        segments=(
            SegmentValidation(
                segment_index=0,
                status=ValidationStatus.UNAVAILABLE,
                confidence=20.0,
                cached_price=150.0,
                live_price=None,
                error_message="Flight not found in live search",
            ),
        ),
        total_cached_price=150.0,
        total_live_price=None,
        average_confidence=20.0,
        validation_time_ms=800.0,
    )
    return ValidatedRoute(route=sample_route_result, validation=validation)


@pytest.fixture
def mock_router() -> MagicMock:
    """Create a mock FindOptimalRoutes router."""
    router = MagicMock()
    router.search = MagicMock(return_value=[])
    return router


@pytest.fixture
def mock_validation_service() -> AsyncMock:
    """Create a mock RouteValidationService."""
    service = AsyncMock()
    service.validate_routes = AsyncMock(return_value=[])
    return service


def create_multiple_routes(
    base_segments: tuple[RouteSegment, ...],
    count: int,
) -> list[RouteResult]:
    """Helper to create multiple route results with different IDs."""
    return [
        RouteResult(
            route_id=i,
            segments=base_segments,
            visited_cities=frozenset({"BCN"}),
        )
        for i in range(count)
    ]


def create_validated_routes_with_prices(
    routes: list[RouteResult],
    prices: list[float],
    statuses: Optional[list[ValidationStatus]] = None,
) -> list[ValidatedRoute]:
    """Helper to create validated routes with specific prices and statuses."""
    if statuses is None:
        statuses = [ValidationStatus.CONFIRMED] * len(routes)

    validated = []
    for route, price, status in zip(routes, prices, statuses):
        is_bookable = status in (ValidationStatus.CONFIRMED, ValidationStatus.PRICE_CHANGED)
        validation = RouteValidation(
            route_id=route.route_id,
            status=status,
            segments=(),
            total_cached_price=price,
            total_live_price=price if is_bookable else None,
            average_confidence=90.0 if is_bookable else 20.0,
            validation_time_ms=100.0,
        )
        validated.append(ValidatedRoute(route=route, validation=validation))
    return validated
