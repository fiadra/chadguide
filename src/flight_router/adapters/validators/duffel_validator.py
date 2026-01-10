"""
Duffel Offer Validator - Validates flight segments against Duffel API.

Implements the re-search strategy to validate cached flight data
against live Duffel API offers. Uses async HTTP client with rate
limiting and exponential backoff.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Dict, List, Optional

import httpx

from src.flight_router.ports.offer_validator import (
    OfferValidator,
    SegmentValidation,
    ValidationStatus,
)
from src.flight_router.schemas.route import RouteSegment
from src.flight_router.schemas.validation import (
    DEFAULT_MATCHING_WEIGHTS,
    MatchingWeights,
    ValidationConfig,
)

logger = logging.getLogger(__name__)

# Duffel API constants
DUFFEL_API_URL = "https://api.duffel.com"
DUFFEL_TEST_API_URL = "https://api.duffel.com"  # Same URL, uses test mode via token


@dataclass
class OfferMatch:
    """Result of matching a live offer to a cached segment."""

    offer_id: str
    price: float
    currency: str
    carrier_code: str
    departure_time: datetime
    arrival_time: datetime
    score: float
    num_stops: int

    @property
    def confidence(self) -> float:
        """Convert score to 0-100 confidence percentage."""
        max_score = DEFAULT_MATCHING_WEIGHTS.max_score
        return max(0.0, min(100.0, (self.score / max_score) * 100))


class DuffelOfferValidator(OfferValidator):
    """
    Validates flight segments against the Duffel API.

    Uses the re-search strategy (POST /offer_requests) to find live
    offers and matches them to cached segments. Implements rate limiting
    with exponential backoff.

    Attributes:
        _api_token: Duffel API access token.
        _client: Async HTTP client.
        _config: Validation configuration.
        _semaphore: Concurrency limiter.
        _weights: Matching algorithm weights.
    """

    def __init__(
        self,
        api_token: Optional[str] = None,
        config: Optional[ValidationConfig] = None,
        weights: Optional[MatchingWeights] = None,
    ) -> None:
        """
        Initialize the Duffel validator.

        Args:
            api_token: Duffel API token. If None, reads from DUFFEL_ACCESS_TOKEN env.
            config: Validation config. If None, uses defaults.
            weights: Matching weights. If None, uses PoC-validated defaults.
        """
        self._api_token = api_token or os.environ.get("DUFFEL_ACCESS_TOKEN", "")
        if not self._api_token:
            logger.warning("DUFFEL_ACCESS_TOKEN not set - validation will fail")

        self._config = config or ValidationConfig()
        self._weights = weights or DEFAULT_MATCHING_WEIGHTS
        self._semaphore = asyncio.Semaphore(self._config.max_concurrent_requests)

        # Lazy-initialized client
        self._client: Optional[httpx.AsyncClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create async HTTP client."""
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                base_url=DUFFEL_API_URL,
                headers={
                    "Authorization": f"Bearer {self._api_token}",
                    "Duffel-Version": "v2",
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                },
                timeout=httpx.Timeout(
                    self._config.request_timeout_ms / 1000.0,
                    connect=5.0,
                ),
            )
        return self._client

    async def close(self) -> None:
        """Close the HTTP client."""
        if self._client and not self._client.is_closed:
            await self._client.aclose()
            self._client = None

    @property
    def name(self) -> str:
        """Validator identifier."""
        return "Duffel API"

    async def validate_segment(
        self,
        segment: RouteSegment,
        departure_date: date,
    ) -> SegmentValidation:
        """
        Validate a single flight segment against live Duffel API.

        Searches Duffel API and matches live offers to the cached
        segment using scoring algorithm.

        Args:
            segment: Cached flight segment to validate.
            departure_date: Date of departure for the search.

        Returns:
            SegmentValidation with status and confidence.
        """
        start_time = time.time()
        try:
            async with self._semaphore:
                result = await self._validate_segment_impl(segment, departure_date)
        except Exception as e:
            logger.error(
                "Validation failed for %s->%s: %s",
                segment.departure_airport,
                segment.arrival_airport,
                e,
            )
            result = SegmentValidation(
                segment_index=segment.segment_index,
                status=ValidationStatus.API_ERROR,
                confidence=0.0,
                cached_price=segment.price,
                error_message=str(e),
            )

        elapsed_ms = (time.time() - start_time) * 1000
        logger.debug(
            "Validated %s->%s in %.0fms: %s (%.0f%% confidence)",
            segment.departure_airport,
            segment.arrival_airport,
            elapsed_ms,
            result.status.value,
            result.confidence,
        )

        return result

    async def _validate_segment_impl(
        self,
        segment: RouteSegment,
        departure_date: date,
    ) -> SegmentValidation:
        """
        Internal implementation of segment validation.

        Searches Duffel and matches offers to segment.
        """
        # Build offer request
        offers = await self._search_offers(
            origin=segment.departure_airport,
            destination=segment.arrival_airport,
            departure_date=departure_date,
        )

        if not offers:
            return SegmentValidation(
                segment_index=segment.segment_index,
                status=ValidationStatus.UNAVAILABLE,
                confidence=0.0,
                cached_price=segment.price,
                error_message="No offers found for route",
            )

        # Check for "ZZ" placeholder carrier (route doesn't exist)
        if all(self._is_placeholder_offer(o) for o in offers):
            return SegmentValidation(
                segment_index=segment.segment_index,
                status=ValidationStatus.UNAVAILABLE,
                confidence=0.0,
                cached_price=segment.price,
                error_message="Route not served by real airlines",
            )

        # Match best offer to segment
        best_match = self._find_best_match(segment, offers, departure_date)

        if best_match is None or best_match.confidence < self._config.min_confidence_threshold:
            return SegmentValidation(
                segment_index=segment.segment_index,
                status=ValidationStatus.UNAVAILABLE,
                confidence=best_match.confidence if best_match else 0.0,
                cached_price=segment.price,
                error_message="No matching offer found above confidence threshold",
            )

        # Determine status based on price change
        price_change_pct = abs(
            (best_match.price - segment.price) / segment.price * 100
        ) if segment.price > 0 else 0.0

        if price_change_pct <= self._config.price_confirmed_threshold:
            status = ValidationStatus.CONFIRMED
        elif price_change_pct <= self._config.price_changed_threshold:
            status = ValidationStatus.PRICE_CHANGED
        else:
            status = ValidationStatus.UNAVAILABLE

        return SegmentValidation(
            segment_index=segment.segment_index,
            status=status,
            confidence=best_match.confidence,
            cached_price=segment.price,
            live_price=best_match.price,
            offer_id=best_match.offer_id,
        )

    async def _search_offers(
        self,
        origin: str,
        destination: str,
        departure_date: date,
    ) -> List[Dict[str, Any]]:
        """
        Search Duffel API for offers.

        Implements retry logic with exponential backoff for rate limits.
        """
        client = await self._get_client()

        payload = {
            "data": {
                "slices": [
                    {
                        "origin": origin,
                        "destination": destination,
                        "departure_date": departure_date.isoformat(),
                    }
                ],
                "passengers": [{"type": "adult"}],
                "cabin_class": "economy",
            }
        }

        retries = 0
        backoff = 1.0

        while retries <= self._config.max_retries:
            try:
                response = await client.post(
                    "/air/offer_requests",
                    json=payload,
                    params={"return_offers": "true"},
                )

                if response.status_code in (200, 201):
                    data = response.json()
                    return data.get("data", {}).get("offers", [])

                if response.status_code == 429:
                    # Rate limited - back off and retry
                    retries += 1
                    logger.warning(
                        "Rate limited by Duffel, retry %d/%d in %.1fs",
                        retries,
                        self._config.max_retries,
                        backoff,
                    )
                    await asyncio.sleep(backoff)
                    backoff *= self._config.backoff_multiplier
                    continue

                # Other error
                logger.error(
                    "Duffel API error: %d %s",
                    response.status_code,
                    response.text[:200],
                )
                return []

            except httpx.TimeoutException:
                retries += 1
                if retries > self._config.max_retries:
                    logger.error("Duffel API timeout after %d retries", retries)
                    raise
                await asyncio.sleep(backoff)
                backoff *= self._config.backoff_multiplier

        return []

    def _is_placeholder_offer(self, offer: Dict[str, Any]) -> bool:
        """Check if offer is from placeholder carrier (ZZ)."""
        try:
            slices = offer.get("slices", [])
            for slice_data in slices:
                for segment in slice_data.get("segments", []):
                    carrier = segment.get("operating_carrier", {})
                    if carrier.get("iata_code") == "ZZ":
                        return True
            return False
        except (KeyError, TypeError):
            return False

    def _find_best_match(
        self,
        segment: RouteSegment,
        offers: List[Dict[str, Any]],
        departure_date: date,
    ) -> Optional[OfferMatch]:
        """
        Find the best matching offer for a segment.

        Scores each offer based on carrier, timing, and price match.
        Returns the highest-scoring offer above minimum threshold.
        """
        best_match: Optional[OfferMatch] = None
        best_score = float("-inf")

        # Extract expected departure hour from segment
        expected_hour = int((segment.dep_time % 1440) / 60)
        expected_carrier = segment.carrier_code

        for offer in offers:
            try:
                match = self._score_offer(
                    offer,
                    segment,
                    expected_carrier,
                    expected_hour,
                )

                if match and match.score > best_score:
                    best_score = match.score
                    best_match = match

            except (KeyError, TypeError, ValueError) as e:
                logger.debug("Error scoring offer: %s", e)
                continue

        return best_match

    def _score_offer(
        self,
        offer: Dict[str, Any],
        segment: RouteSegment,
        expected_carrier: Optional[str],
        expected_hour: int,
    ) -> Optional[OfferMatch]:
        """
        Score a single offer against segment.

        Returns OfferMatch with calculated score, or None if offer
        cannot be matched (wrong route, etc.).
        """
        try:
            # Extract offer details
            offer_id = offer["id"]
            price = float(offer["total_amount"])
            currency = offer["total_currency"]

            # Get first slice (we search for one-way)
            slices = offer.get("slices", [])
            if not slices:
                return None

            first_slice = slices[0]
            segments_data = first_slice.get("segments", [])
            if not segments_data:
                return None

            # For now, prefer non-stop flights
            num_stops = len(segments_data) - 1
            first_segment = segments_data[0]

            # Extract carrier and timing
            carrier = first_segment.get("operating_carrier", {})
            carrier_code = carrier.get("iata_code", "")

            dep_time_str = first_segment.get("departing_at", "")
            arr_time_str = segments_data[-1].get("arriving_at", "")

            dep_time = datetime.fromisoformat(dep_time_str.replace("Z", "+00:00"))
            arr_time = datetime.fromisoformat(arr_time_str.replace("Z", "+00:00"))

            offer_hour = dep_time.hour

            # Calculate score
            score = 0.0

            # Non-stop bonus
            if num_stops == 0:
                score += self._weights.non_stop

            # Carrier match
            if expected_carrier:
                if carrier_code == expected_carrier:
                    score += self._weights.carrier_match
                else:
                    score += self._weights.carrier_mismatch_penalty

            # Hour match
            hour_diff = abs(offer_hour - expected_hour)
            if hour_diff == 0:
                score += self._weights.hour_exact
            elif hour_diff <= 1:
                score += self._weights.hour_close
            else:
                score += self._weights.hour_outside_penalty

            # Price match
            if segment.price > 0:
                price_diff_pct = abs(price - segment.price) / segment.price * 100
                if price_diff_pct <= 5:
                    score += self._weights.price_exact
                elif price_diff_pct <= 25:
                    score += self._weights.price_close
                else:
                    score += self._weights.price_outside_penalty

            # Stop penalty
            score += num_stops * self._weights.per_stop_penalty

            return OfferMatch(
                offer_id=offer_id,
                price=price,
                currency=currency,
                carrier_code=carrier_code,
                departure_time=dep_time,
                arrival_time=arr_time,
                score=score,
                num_stops=num_stops,
            )

        except (KeyError, TypeError, ValueError) as e:
            logger.debug("Error parsing offer: %s", e)
            return None

    async def validate_segments(
        self,
        segments: List[RouteSegment],
        departure_date: date,
    ) -> List[SegmentValidation]:
        """
        Validate multiple segments with parallel execution.

        Uses semaphore to limit concurrency per configuration.
        Results are returned in the same order as input.

        Args:
            segments: List of cached segments to validate.
            departure_date: Date of departure for the search.

        Returns:
            List of SegmentValidation in same order as input.
        """
        if not segments:
            return []

        # Create tasks for parallel execution
        tasks = [
            self.validate_segment(segment, departure_date) for segment in segments
        ]

        # Execute in parallel (semaphore limits concurrency internally)
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Convert exceptions to API_ERROR results
        validated_results: List[SegmentValidation] = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                validated_results.append(
                    SegmentValidation(
                        segment_index=segments[i].segment_index,
                        status=ValidationStatus.API_ERROR,
                        confidence=0.0,
                        cached_price=segments[i].price,
                        error_message=str(result),
                    )
                )
            else:
                validated_results.append(result)

        return validated_results
