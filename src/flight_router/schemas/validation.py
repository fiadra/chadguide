"""
Validation schemas for route validation results.

Defines dataclasses for validation configuration and combined
route + validation result types.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from src.flight_router.ports.offer_validator import RouteValidation
    from src.flight_router.schemas.route import RouteResult


@dataclass(frozen=True)
class ValidationConfig:
    """
    Configuration for route validation behavior.

    Controls thresholds, timeouts, and rate limiting for the
    validation process. Empirically tuned based on PoC results.

    Attributes:
        price_confirmed_threshold: Max % price change for CONFIRMED status.
        price_changed_threshold: Max % price change before UNAVAILABLE.
        min_confidence_threshold: Min confidence score (0-100) to accept match.
        max_concurrent_requests: Semaphore limit for parallel validation.
        request_timeout_ms: Per-request timeout in milliseconds.
        cache_ttl_minutes: Time-to-live for validation cache entries.
        max_retries: Max retry attempts on rate limit errors.
        backoff_multiplier: Exponential backoff multiplier for retries.
    """

    price_confirmed_threshold: float = 5.0
    price_changed_threshold: float = 25.0
    min_confidence_threshold: float = 30.0
    max_concurrent_requests: int = 3
    request_timeout_ms: int = 10000
    cache_ttl_minutes: int = 10
    max_retries: int = 5
    backoff_multiplier: float = 2.0


# Scoring weights for offer matching algorithm
# Validated in PoC: 90% success rate, 70% average confidence
@dataclass(frozen=True)
class MatchingWeights:
    """
    Weights for the offer matching algorithm.

    Used to calculate confidence scores when matching live offers
    to cached flight segments. PoC-validated values.

    Attributes:
        non_stop: Points for non-stop flight.
        carrier_match: Points for exact carrier match.
        hour_exact: Points for exact departure hour match.
        hour_close: Points for departure within ±1 hour.
        price_exact: Points for price within 5%.
        price_close: Points for price within 25%.
        carrier_mismatch_penalty: Penalty for carrier mismatch.
        hour_outside_penalty: Penalty for departure >1 hour different.
        price_outside_penalty: Penalty for price >25% different.
        per_stop_penalty: Penalty per additional stop.
    """

    non_stop: float = 20.0
    carrier_match: float = 50.0
    hour_exact: float = 30.0
    hour_close: float = 20.0
    price_exact: float = 30.0
    price_close: float = 15.0
    carrier_mismatch_penalty: float = -20.0
    hour_outside_penalty: float = -30.0
    price_outside_penalty: float = -50.0
    per_stop_penalty: float = -10.0

    @property
    def max_score(self) -> float:
        """Maximum achievable score (for confidence calculation)."""
        return self.non_stop + self.carrier_match + self.hour_exact + self.price_exact


# Default matching weights (PoC-validated)
DEFAULT_MATCHING_WEIGHTS = MatchingWeights()


@dataclass(frozen=True)
class ValidatedRoute:
    """
    Combined route result with live validation status.

    Wraps a RouteResult with its validation outcome, providing
    a unified response type for the API layer.

    Attributes:
        route: The original route result from Dijkstra.
        validation: Live validation result (or None if not validated).
        is_validated: Whether validation was performed.
    """

    route: RouteResult
    validation: Optional[RouteValidation] = None

    @property
    def is_validated(self) -> bool:
        """Check if this route has been validated."""
        return self.validation is not None

    @property
    def is_bookable(self) -> bool:
        """Check if route can be confidently booked."""
        if self.validation is None:
            return False
        return self.validation.is_bookable

    @property
    def total_price(self) -> float:
        """Get total price (live if available, otherwise cached)."""
        if self.validation and self.validation.total_live_price is not None:
            return self.validation.total_live_price
        return self.route.total_cost

    @property
    def price_confidence(self) -> str:
        """Human-readable price confidence indicator."""
        if not self.validation:
            return "unvalidated"
        if self.validation.status.value == "confirmed":
            return "high"
        if self.validation.status.value == "price_changed":
            return "medium"
        return "low"
