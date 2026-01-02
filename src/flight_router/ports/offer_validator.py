"""
Offer Validator port interface.

Defines the abstract contract for validating flight offers against live APIs.
This port enables decoupling the routing algorithm from the validation mechanism.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date
from enum import Enum
from typing import TYPE_CHECKING, List, Optional

if TYPE_CHECKING:
    from src.flight_router.schemas.route import RouteSegment


class ValidationStatus(Enum):
    """
    Status of flight offer validation.

    Represents the outcome of validating a cached flight against live API data.
    Used to determine if a route can be confidently offered to users.
    """

    CONFIRMED = "confirmed"
    """Price within tolerance (±5%), ready to book."""

    PRICE_CHANGED = "price_changed"
    """Flight exists but price differs by >5%, show warning."""

    UNAVAILABLE = "unavailable"
    """Flight not found in live search, schedule may have changed."""

    API_ERROR = "api_error"
    """Validation failed due to API error, retry later."""


@dataclass(frozen=True)
class SegmentValidation:
    """
    Validation result for a single flight segment.

    Immutable record of the validation outcome for one segment,
    including the confidence score and any price changes detected.

    Attributes:
        segment_index: Zero-based index of this segment in the route.
        status: Validation outcome status.
        confidence: Match confidence score (0-100%).
        cached_price: Original price from cached data.
        live_price: Current price from live API (if found).
        offer_id: Live offer ID for booking (if available).
        error_message: Error details if validation failed.
    """

    segment_index: int
    status: ValidationStatus
    confidence: float
    cached_price: float
    live_price: Optional[float] = None
    offer_id: Optional[str] = None
    error_message: Optional[str] = None

    @property
    def price_change_percent(self) -> Optional[float]:
        """Calculate percentage price change from cached to live."""
        if self.live_price is None or self.cached_price == 0:
            return None
        return ((self.live_price - self.cached_price) / self.cached_price) * 100


@dataclass(frozen=True)
class RouteValidation:
    """
    Aggregated validation result for a complete route.

    Combines validation results from all segments to provide
    an overall route validation status using worst-status-wins logic.

    Attributes:
        route_id: Identifier of the validated route.
        status: Aggregated status (worst of all segments).
        segments: Individual segment validation results.
        total_cached_price: Sum of cached segment prices.
        total_live_price: Sum of live segment prices (if all found).
        average_confidence: Average confidence across segments.
        validation_time_ms: Total time taken for validation.
    """

    route_id: int
    status: ValidationStatus
    segments: tuple[SegmentValidation, ...]
    total_cached_price: float
    total_live_price: Optional[float]
    average_confidence: float
    validation_time_ms: float

    @property
    def total_price_change_percent(self) -> Optional[float]:
        """Calculate total percentage price change."""
        if self.total_live_price is None or self.total_cached_price == 0:
            return None
        return (
            (self.total_live_price - self.total_cached_price)
            / self.total_cached_price
        ) * 100

    @property
    def is_bookable(self) -> bool:
        """Check if route can be confidently booked."""
        return self.status in (
            ValidationStatus.CONFIRMED,
            ValidationStatus.PRICE_CHANGED,
        )


class OfferValidator(ABC):
    """
    Abstract interface for flight offer validation.

    Implementations connect to live flight APIs to validate
    cached flight data before presenting to users.

    The validator handles:
    - Re-searching for flights on the live API
    - Matching live offers to cached segments
    - Calculating confidence scores
    - Rate limiting and error handling

    Implementations:
    - DuffelOfferValidator: Validates against Duffel API
    """

    @abstractmethod
    async def validate_segment(
        self,
        segment: RouteSegment,
        departure_date: date,
    ) -> SegmentValidation:
        """
        Validate a single flight segment against live API.

        Searches for the flight on the live API and attempts to match
        it with the cached segment data. Returns validation result
        with confidence score.

        Args:
            segment: Cached flight segment to validate.
            departure_date: Date of departure for the search.

        Returns:
            SegmentValidation with status and confidence.
        """
        ...

    @abstractmethod
    async def validate_segments(
        self,
        segments: List[RouteSegment],
        departure_date: date,
    ) -> List[SegmentValidation]:
        """
        Validate multiple segments with parallel execution.

        Validates all segments concurrently (respecting rate limits)
        and returns results in the same order as input.

        Args:
            segments: List of cached segments to validate.
            departure_date: Date of departure for the search.

        Returns:
            List of SegmentValidation in same order as input.
        """
        ...

    @property
    @abstractmethod
    def name(self) -> str:
        """
        Validator identifier.

        Returns:
            Human-readable validator name (e.g., "Duffel API").
        """
        ...
