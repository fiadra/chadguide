"""
Tests for /search and /search/stream FastAPI endpoints.

Tests cover:
- SSE streaming stage events (routing, validating, complete)
- Route validation integration
- Bookable route filtering and sorting
- Fallback behavior when no bookable routes
- Response format validation
"""

import json
import pytest
from datetime import datetime
from unittest.mock import patch, MagicMock, AsyncMock

from src.flight_router.schemas.route import RouteResult, RouteSegment
from src.flight_router.schemas.validation import ValidatedRoute
from src.flight_router.ports.offer_validator import (
    RouteValidation,
    ValidationStatus,
)

from .conftest import create_multiple_routes, create_validated_routes_with_prices


# =============================================================================
# HELPER: Collect SSE events from async generator
# =============================================================================

async def collect_sse_events(generator):
    """Collect all events from the search_with_validation generator."""
    events = []
    async for event in generator:
        events.append(event)
    return events


def create_search_request(origin="WAW", destinations=None, departure_date=None):
    """Create a mock SearchRequest object."""
    from pydantic import BaseModel
    from typing import Set
    from datetime import datetime as dt

    class MockSearchRequest(BaseModel):
        origin: str
        destinations: Set[str]
        departure_date: dt
        return_date: dt = None

    return MockSearchRequest(
        origin=origin,
        destinations=destinations or {"BCN"},
        departure_date=departure_date or dt(2026, 7, 1),
    )


# =============================================================================
# SSE STREAMING GENERATOR TESTS
# =============================================================================


class TestSearchWithValidationGenerator:
    """Tests for the search_with_validation async generator function."""

    @pytest.mark.anyio
    async def test_generator_emits_routing_stage_first(
        self, sample_route_result, sample_validated_route_bookable
    ):
        """Generator emits 'routing' stage event first."""
        mock_router = MagicMock()
        mock_router.search.return_value = [sample_route_result]

        mock_service = AsyncMock()
        mock_service.validate_routes.return_value = [sample_validated_route_bookable]

        with patch("src.fastapi.flights_api.router", mock_router), \
             patch("src.fastapi.flights_api.validation_service", mock_service):
            from src.fastapi.flights_api import search_with_validation

            request = create_search_request()
            events = await collect_sse_events(search_with_validation(request))

        # First event should be routing stage
        assert len(events) > 0
        assert events[0]["event"] == "stage"
        assert events[0]["data"] == "routing"

    @pytest.mark.anyio
    async def test_generator_emits_validating_stage_after_routing(
        self, sample_route_result, sample_validated_route_bookable
    ):
        """Generator emits 'validating' stage after routing completes."""
        mock_router = MagicMock()
        mock_router.search.return_value = [sample_route_result]

        mock_service = AsyncMock()
        mock_service.validate_routes.return_value = [sample_validated_route_bookable]

        with patch("src.fastapi.flights_api.router", mock_router), \
             patch("src.fastapi.flights_api.validation_service", mock_service):
            from src.fastapi.flights_api import search_with_validation

            request = create_search_request()
            events = await collect_sse_events(search_with_validation(request))

        # Extract stage events
        stage_events = [e for e in events if e["event"] == "stage"]
        stage_data = [e["data"] for e in stage_events]

        assert "routing" in stage_data
        assert "validating" in stage_data
        assert stage_data.index("routing") < stage_data.index("validating")

    @pytest.mark.anyio
    async def test_generator_emits_complete_with_routes(
        self, sample_route_result, sample_validated_route_bookable
    ):
        """Generator emits 'complete' event with route data."""
        mock_router = MagicMock()
        mock_router.search.return_value = [sample_route_result]

        mock_service = AsyncMock()
        mock_service.validate_routes.return_value = [sample_validated_route_bookable]

        with patch("src.fastapi.flights_api.router", mock_router), \
             patch("src.fastapi.flights_api.validation_service", mock_service):
            from src.fastapi.flights_api import search_with_validation

            request = create_search_request()
            events = await collect_sse_events(search_with_validation(request))

        # Find complete event
        complete_events = [e for e in events if e["event"] == "complete"]
        assert len(complete_events) == 1

        complete_data = json.loads(complete_events[0]["data"])
        assert "routes" in complete_data
        assert len(complete_data["routes"]) > 0

    @pytest.mark.anyio
    async def test_generator_validates_top_5_routes_only(
        self, sample_route_segments
    ):
        """Generator validates only top 5 routes (validate_top_n=5)."""
        # Create 10 routes
        routes = create_multiple_routes(sample_route_segments, 10)

        mock_router = MagicMock()
        mock_router.search.return_value = routes

        mock_service = AsyncMock()
        mock_service.validate_routes.return_value = []

        with patch("src.fastapi.flights_api.router", mock_router), \
             patch("src.fastapi.flights_api.validation_service", mock_service):
            from src.fastapi.flights_api import search_with_validation

            request = create_search_request()
            await collect_sse_events(search_with_validation(request))

        # Verify validate_top_n=5 was passed
        mock_service.validate_routes.assert_called_once()
        call_args = mock_service.validate_routes.call_args
        assert call_args.kwargs.get("validate_top_n") == 5

    @pytest.mark.anyio
    async def test_generator_returns_top_2_bookable_routes_sorted_by_price(
        self, sample_route_segments
    ):
        """Generator filters to top 2 bookable routes sorted by price."""
        routes = create_multiple_routes(sample_route_segments, 5)

        # Create validated routes: some bookable, some not, different prices
        validated = create_validated_routes_with_prices(
            routes,
            prices=[300.0, 250.0, 200.0, 150.0, 400.0],
            statuses=[
                ValidationStatus.CONFIRMED,      # Bookable, price 300
                ValidationStatus.UNAVAILABLE,    # Not bookable
                ValidationStatus.PRICE_CHANGED,  # Bookable, price 200
                ValidationStatus.CONFIRMED,      # Bookable, price 150 (cheapest)
                ValidationStatus.API_ERROR,      # Not bookable
            ],
        )

        mock_router = MagicMock()
        mock_router.search.return_value = routes

        mock_service = AsyncMock()
        mock_service.validate_routes.return_value = validated

        with patch("src.fastapi.flights_api.router", mock_router), \
             patch("src.fastapi.flights_api.validation_service", mock_service):
            from src.fastapi.flights_api import search_with_validation

            request = create_search_request()
            events = await collect_sse_events(search_with_validation(request))

        complete_events = [e for e in events if e["event"] == "complete"]
        complete_data = json.loads(complete_events[0]["data"])

        # Should return 2 routes
        assert len(complete_data["routes"]) == 2
        # Sorted by price: 150 (route 3), 200 (route 2)
        assert complete_data["routes"][0]["total_cost"] == 150.0
        assert complete_data["routes"][1]["total_cost"] == 200.0

    @pytest.mark.anyio
    async def test_generator_fallback_when_no_bookable_routes(
        self, sample_route_segments
    ):
        """Generator falls back to top 2 unvalidated when no bookable."""
        routes = create_multiple_routes(sample_route_segments, 3)

        # All routes unavailable (not bookable)
        validated = create_validated_routes_with_prices(
            routes,
            prices=[200.0, 210.0, 220.0],
            statuses=[ValidationStatus.UNAVAILABLE] * 3,
        )

        mock_router = MagicMock()
        mock_router.search.return_value = routes

        mock_service = AsyncMock()
        mock_service.validate_routes.return_value = validated

        with patch("src.fastapi.flights_api.router", mock_router), \
             patch("src.fastapi.flights_api.validation_service", mock_service):
            from src.fastapi.flights_api import search_with_validation

            request = create_search_request()
            events = await collect_sse_events(search_with_validation(request))

        complete_events = [e for e in events if e["event"] == "complete"]
        complete_data = json.loads(complete_events[0]["data"])

        # Should return top 2 as fallback
        assert len(complete_data["routes"]) == 2

    @pytest.mark.anyio
    async def test_generator_returns_error_when_no_routes_found(self):
        """Generator returns error in complete event when no routes."""
        mock_router = MagicMock()
        mock_router.search.return_value = []

        with patch("src.fastapi.flights_api.router", mock_router):
            from src.fastapi.flights_api import search_with_validation

            request = create_search_request(destinations={"XXX"})
            events = await collect_sse_events(search_with_validation(request))

        complete_events = [e for e in events if e["event"] == "complete"]
        complete_data = json.loads(complete_events[0]["data"])

        assert complete_data["routes"] == []
        assert "error" in complete_data
        assert "No routes found" in complete_data["error"]

    @pytest.mark.anyio
    async def test_generator_response_format_matches_schema(
        self, sample_route_result, sample_validated_route_bookable
    ):
        """Generator complete event data matches RouteResultSchema format."""
        mock_router = MagicMock()
        mock_router.search.return_value = [sample_route_result]

        mock_service = AsyncMock()
        mock_service.validate_routes.return_value = [sample_validated_route_bookable]

        with patch("src.fastapi.flights_api.router", mock_router), \
             patch("src.fastapi.flights_api.validation_service", mock_service):
            from src.fastapi.flights_api import search_with_validation

            request = create_search_request()
            events = await collect_sse_events(search_with_validation(request))

        complete_events = [e for e in events if e["event"] == "complete"]
        complete_data = json.loads(complete_events[0]["data"])
        route = complete_data["routes"][0]

        # Verify route schema
        assert "route_id" in route
        assert "segments" in route
        assert "total_cost" in route
        assert "total_time" in route
        assert "route_cities" in route
        assert "num_segments" in route

        # Verify segment schema
        seg = route["segments"][0]
        assert "segment_index" in seg
        assert "departure_airport" in seg
        assert "arrival_airport" in seg
        assert "dep_time" in seg
        assert "arr_time" in seg
        assert "price" in seg
        assert "duration" in seg
        assert "carrier_code" in seg


# =============================================================================
# SYNCHRONOUS /SEARCH ENDPOINT TESTS
# =============================================================================


class TestSearchEndpoint:
    """Tests for the synchronous /search POST endpoint."""

    @pytest.mark.anyio
    async def test_search_returns_routes_successfully(
        self, sample_route_result
    ):
        """POST /search returns routes when found."""
        mock_router = MagicMock()
        mock_router.search.return_value = [sample_route_result]

        with patch("src.fastapi.flights_api.router", mock_router):
            from src.fastapi.flights_api import app
            from httpx import AsyncClient, ASGITransport

            async with AsyncClient(
                transport=ASGITransport(app=app),
                base_url="http://test"
            ) as client:
                response = await client.post("/search", json={
                    "origin": "WAW",
                    "destinations": ["BCN"],
                    "departure_date": "2026-07-01T00:00:00",
                })

        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["route_id"] == 1

    @pytest.mark.anyio
    async def test_search_returns_404_when_no_routes(self):
        """POST /search returns 404 when no routes found."""
        mock_router = MagicMock()
        mock_router.search.return_value = []

        with patch("src.fastapi.flights_api.router", mock_router):
            from src.fastapi.flights_api import app
            from httpx import AsyncClient, ASGITransport

            async with AsyncClient(
                transport=ASGITransport(app=app),
                base_url="http://test"
            ) as client:
                response = await client.post("/search", json={
                    "origin": "WAW",
                    "destinations": ["XXX"],
                    "departure_date": "2026-07-01T00:00:00",
                })

        assert response.status_code == 404
        assert "No routes found" in response.json()["detail"]

    @pytest.mark.anyio
    async def test_search_validates_request_body(self):
        """POST /search validates required fields."""
        from src.fastapi.flights_api import app
        from httpx import AsyncClient, ASGITransport

        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test"
        ) as client:
            # Missing required fields
            response = await client.post("/search", json={
                "origin": "WAW",
                # missing destinations and departure_date
            })

        assert response.status_code == 422  # Validation error
