"""
Route Validation Service - Orchestrates live validation of route results.

Coordinates the validation of Dijkstra-generated routes against live API
data. Handles virtual interlining (validating each segment separately)
and aggregates results using worst-status-wins logic.
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import date
from typing import TYPE_CHECKING, List, Optional

from src.flight_router.ports.offer_validator import (
    RouteValidation,
    SegmentValidation,
    ValidationStatus,
)
from src.flight_router.schemas.route import RouteResult
from src.flight_router.schemas.validation import ValidatedRoute, ValidationConfig

if TYPE_CHECKING:
    from src.flight_router.ports.offer_validator import OfferValidator

logger = logging.getLogger(__name__)


class RouteValidationService:
    """
    Domain service for validating routes against live flight APIs.

    Orchestrates the validation process:
    1. Extracts segments from RouteResult
    2. Validates each segment via OfferValidator (parallel)
    3. Aggregates segment results into RouteValidation
    4. Returns ValidatedRoute with combined data

    Virtual Interlining Strategy:
        Each segment is validated as an independent one-way ticket.
        This reflects reality - Duffel doesn't guarantee connecting
        flights will be available as a bundle.

    Aggregation Rules:
        - Route status = worst segment status
        - Route confidence = average segment confidence
        - Route bookable = all segments bookable

    Attributes:
        _validator: Offer validator for live API access.
        _config: Validation configuration.
    """

    def __init__(
        self,
        validator: OfferValidator,
        config: Optional[ValidationConfig] = None,
    ) -> None:
        """
        Initialize the validation service.

        Args:
            validator: Offer validator implementation (e.g., DuffelOfferValidator).
            config: Validation configuration. If None, uses defaults.
        """
        self._validator = validator
        self._config = config or ValidationConfig()

    async def validate_route(
        self,
        route: RouteResult,
        departure_date: date,
    ) -> ValidatedRoute:
        """
        Validate a single route against live API.

        Validates all segments in parallel and aggregates results.

        Args:
            route: Route result from Dijkstra algorithm.
            departure_date: Date of departure for validation search.

        Returns:
            ValidatedRoute with validation results.
        """
        start_time = time.perf_counter()

        if not route.segments:
            logger.warning("Route %d has no segments to validate", route.route_id)
            return ValidatedRoute(route=route, validation=None)

        # Validate all segments in parallel
        segment_validations = await self._validator.validate_segments(
            list(route.segments),
            departure_date,
        )

        # Aggregate results
        route_validation = self._aggregate_validations(
            route_id=route.route_id,
            segment_validations=segment_validations,
            elapsed_ms=(time.perf_counter() - start_time) * 1000,
        )

        logger.debug(
            "Route %d validated: %s (%.0f%% confidence) in %.0fms",
            route.route_id,
            route_validation.status.value,
            route_validation.average_confidence,
            route_validation.validation_time_ms,
        )

        return ValidatedRoute(route=route, validation=route_validation)

    async def validate_routes(
        self,
        routes: List[RouteResult],
        departure_date: date,
        validate_top_n: Optional[int] = None,
    ) -> List[ValidatedRoute]:
        """
        Validate multiple routes.

        Validates routes sequentially to avoid overwhelming the API.
        Use validate_top_n to limit validation to best N routes.

        Args:
            routes: List of route results to validate.
            departure_date: Date of departure for validation search.
            validate_top_n: If set, only validate top N routes.

        Returns:
            List of ValidatedRoute objects. Unvalidated routes
            have validation=None.
        """
        if not routes:
            return []

        start_time = time.perf_counter()

        # Determine which routes to validate
        to_validate = routes[:validate_top_n] if validate_top_n else routes
        to_skip = routes[validate_top_n:] if validate_top_n else []

        # Validate selected routes
        validated: List[ValidatedRoute] = []
        for route in to_validate:
            result = await self.validate_route(route, departure_date)
            validated.append(result)

        # Add unvalidated routes
        for route in to_skip:
            validated.append(ValidatedRoute(route=route, validation=None))

        total_time = time.perf_counter() - start_time
        logger.info(
            "Validated %d/%d routes in %.0fms",
            len(to_validate),
            len(routes),
            total_time * 1000,
        )

        return validated

    async def validate_route_on_demand(
        self,
        route: RouteResult,
        departure_date: date,
    ) -> ValidatedRoute:
        """
        On-demand validation for a single route.

        Use this for lazy validation when user selects a specific route.

        Args:
            route: Route to validate.
            departure_date: Date of departure.

        Returns:
            ValidatedRoute with validation results.
        """
        return await self.validate_route(route, departure_date)

    def _aggregate_validations(
        self,
        route_id: int,
        segment_validations: List[SegmentValidation],
        elapsed_ms: float,
    ) -> RouteValidation:
        """
        Aggregate segment validations into route-level result.

        Uses worst-status-wins logic for route status.

        Args:
            route_id: Route identifier.
            segment_validations: Individual segment results.
            elapsed_ms: Total validation time.

        Returns:
            Aggregated RouteValidation.
        """
        if not segment_validations:
            return RouteValidation(
                route_id=route_id,
                status=ValidationStatus.API_ERROR,
                segments=(),
                total_cached_price=0.0,
                total_live_price=None,
                average_confidence=0.0,
                validation_time_ms=elapsed_ms,
            )

        # Aggregate status (worst wins)
        status = self._aggregate_status(segment_validations)

        # Calculate totals
        total_cached = sum(sv.cached_price for sv in segment_validations)

        # Only calculate live total if ALL segments have live prices
        all_have_live = all(sv.live_price is not None for sv in segment_validations)
        total_live = (
            sum(sv.live_price for sv in segment_validations)
            if all_have_live
            else None
        )

        # Average confidence
        avg_confidence = sum(sv.confidence for sv in segment_validations) / len(
            segment_validations
        )

        return RouteValidation(
            route_id=route_id,
            status=status,
            segments=tuple(segment_validations),
            total_cached_price=total_cached,
            total_live_price=total_live,
            average_confidence=avg_confidence,
            validation_time_ms=elapsed_ms,
        )

    def _aggregate_status(
        self,
        segment_validations: List[SegmentValidation],
    ) -> ValidationStatus:
        """
        Aggregate segment statuses using worst-status-wins.

        Priority (worst to best):
        1. API_ERROR - validation failed
        2. UNAVAILABLE - flight not found
        3. PRICE_CHANGED - price differs significantly
        4. CONFIRMED - ready to book

        Args:
            segment_validations: Segment validation results.

        Returns:
            Aggregated status (worst of all segments).
        """
        statuses = [sv.status for sv in segment_validations]

        # Worst status wins
        if ValidationStatus.API_ERROR in statuses:
            return ValidationStatus.API_ERROR
        if ValidationStatus.UNAVAILABLE in statuses:
            return ValidationStatus.UNAVAILABLE
        if ValidationStatus.PRICE_CHANGED in statuses:
            return ValidationStatus.PRICE_CHANGED
        return ValidationStatus.CONFIRMED

    @property
    def validator_name(self) -> str:
        """Get name of the underlying validator."""
        return self._validator.name


def aggregate_route_status(
    segment_statuses: List[ValidationStatus],
) -> ValidationStatus:
    """
    Standalone function for aggregating statuses.

    Provided for use outside the service context.

    Args:
        segment_statuses: List of segment statuses.

    Returns:
        Aggregated status (worst wins).
    """
    if ValidationStatus.API_ERROR in segment_statuses:
        return ValidationStatus.API_ERROR
    if ValidationStatus.UNAVAILABLE in segment_statuses:
        return ValidationStatus.UNAVAILABLE
    if ValidationStatus.PRICE_CHANGED in segment_statuses:
        return ValidationStatus.PRICE_CHANGED
    return ValidationStatus.CONFIRMED
