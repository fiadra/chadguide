"""
Tests for DuffelOfferValidator adapter.

Tests cover:
- Offer scoring algorithm
- Rate limiting with exponential backoff
- ZZ placeholder carrier detection
- Error handling (timeouts, API errors)
- Segment validation logic
- Parallel segment validation
"""

from datetime import date, datetime
from unittest.mock import patch

import httpx
import pytest
import respx

from src.flight_router.adapters.validators.duffel_validator import (
    DuffelOfferValidator,
    OfferMatch,
    DUFFEL_API_URL,
)
from src.flight_router.ports.offer_validator import ValidationStatus
from src.flight_router.schemas.route import RouteSegment
from src.flight_router.schemas.validation import (
    DEFAULT_MATCHING_WEIGHTS,
    MatchingWeights,
    ValidationConfig,
)


# =============================================================================
# FIXTURES
# =============================================================================


@pytest.fixture
def anyio_backend():
    """Use asyncio backend."""
    return "asyncio"


@pytest.fixture
def sample_segment() -> RouteSegment:
    """Create a sample route segment for testing."""
    return RouteSegment(
        segment_index=0,
        departure_airport="WAW",
        arrival_airport="BCN",
        dep_time=600.0,  # 10:00 (600 minutes = 10 hours)
        arr_time=720.0,  # 12:00
        price=150.0,
        carrier_code="LO",
    )


@pytest.fixture
def validator() -> DuffelOfferValidator:
    """Create a DuffelOfferValidator with test token."""
    return DuffelOfferValidator(
        api_token="test_token",
        config=ValidationConfig(
            max_retries=2,
            request_timeout_ms=5000,
            min_confidence_threshold=30.0,
        ),
    )


@pytest.fixture
def sample_duffel_response() -> dict:
    """Create a sample Duffel API response with offers."""
    return {
        "data": {
            "offers": [
                {
                    "id": "off_123",
                    "total_amount": "152.00",
                    "total_currency": "EUR",
                    "slices": [
                        {
                            "segments": [
                                {
                                    "operating_carrier": {"iata_code": "LO"},
                                    "departing_at": "2024-07-15T10:00:00Z",
                                    "arriving_at": "2024-07-15T12:30:00Z",
                                }
                            ]
                        }
                    ],
                }
            ]
        }
    }


@pytest.fixture
def multi_offer_response() -> dict:
    """Create response with multiple offers for scoring tests."""
    return {
        "data": {
            "offers": [
                # Perfect match - same carrier, same hour, price within 5%
                {
                    "id": "off_perfect",
                    "total_amount": "151.00",
                    "total_currency": "EUR",
                    "slices": [
                        {
                            "segments": [
                                {
                                    "operating_carrier": {"iata_code": "LO"},
                                    "departing_at": "2024-07-15T10:00:00Z",
                                    "arriving_at": "2024-07-15T12:30:00Z",
                                }
                            ]
                        }
                    ],
                },
                # Good match - same carrier, 1 hour off, price within 25%
                {
                    "id": "off_good",
                    "total_amount": "170.00",
                    "total_currency": "EUR",
                    "slices": [
                        {
                            "segments": [
                                {
                                    "operating_carrier": {"iata_code": "LO"},
                                    "departing_at": "2024-07-15T11:00:00Z",
                                    "arriving_at": "2024-07-15T13:30:00Z",
                                }
                            ]
                        }
                    ],
                },
                # Poor match - different carrier, different time, high price
                {
                    "id": "off_poor",
                    "total_amount": "250.00",
                    "total_currency": "EUR",
                    "slices": [
                        {
                            "segments": [
                                {
                                    "operating_carrier": {"iata_code": "FR"},
                                    "departing_at": "2024-07-15T15:00:00Z",
                                    "arriving_at": "2024-07-15T17:30:00Z",
                                }
                            ]
                        }
                    ],
                },
            ]
        }
    }


@pytest.fixture
def zz_carrier_response() -> dict:
    """Create response with ZZ placeholder carrier."""
    return {
        "data": {
            "offers": [
                {
                    "id": "off_zz",
                    "total_amount": "100.00",
                    "total_currency": "EUR",
                    "slices": [
                        {
                            "segments": [
                                {
                                    "operating_carrier": {"iata_code": "ZZ"},
                                    "departing_at": "2024-07-15T10:00:00Z",
                                    "arriving_at": "2024-07-15T12:00:00Z",
                                }
                            ]
                        }
                    ],
                }
            ]
        }
    }


# =============================================================================
# OFFER SCORING TESTS
# =============================================================================


class TestOfferScoring:
    """Tests for the offer matching/scoring algorithm."""

    def test_perfect_match_gets_highest_score(
        self,
        validator: DuffelOfferValidator,
        sample_segment: RouteSegment,
        multi_offer_response: dict,
    ):
        """Perfect match (carrier, time, price) gets highest score."""
        offers = multi_offer_response["data"]["offers"]

        best_match = validator._find_best_match(
            sample_segment,
            offers,
            date(2024, 7, 15),
        )

        assert best_match is not None
        assert best_match.offer_id == "off_perfect"
        assert best_match.confidence > 80.0  # High confidence

    def test_non_stop_bonus_applied(
        self,
        validator: DuffelOfferValidator,
        sample_segment: RouteSegment,
    ):
        """Non-stop flights get bonus points."""
        non_stop_offer = {
            "id": "off_nonstop",
            "total_amount": "150.00",
            "total_currency": "EUR",
            "slices": [
                {
                    "segments": [
                        {
                            "operating_carrier": {"iata_code": "LO"},
                            "departing_at": "2024-07-15T10:00:00Z",
                            "arriving_at": "2024-07-15T12:00:00Z",
                        }
                    ]
                }
            ],
        }

        one_stop_offer = {
            "id": "off_onestop",
            "total_amount": "150.00",
            "total_currency": "EUR",
            "slices": [
                {
                    "segments": [
                        {
                            "operating_carrier": {"iata_code": "LO"},
                            "departing_at": "2024-07-15T10:00:00Z",
                            "arriving_at": "2024-07-15T11:00:00Z",
                        },
                        {
                            "operating_carrier": {"iata_code": "LO"},
                            "departing_at": "2024-07-15T11:30:00Z",
                            "arriving_at": "2024-07-15T12:30:00Z",
                        },
                    ]
                }
            ],
        }

        non_stop_match = validator._score_offer(
            non_stop_offer, sample_segment, "LO", 10
        )
        one_stop_match = validator._score_offer(
            one_stop_offer, sample_segment, "LO", 10
        )

        assert non_stop_match is not None
        assert one_stop_match is not None
        assert non_stop_match.score > one_stop_match.score
        assert non_stop_match.num_stops == 0
        assert one_stop_match.num_stops == 1

    def test_carrier_match_bonus(
        self,
        validator: DuffelOfferValidator,
        sample_segment: RouteSegment,
    ):
        """Matching carrier gets bonus, mismatch gets penalty."""
        same_carrier = {
            "id": "off_same",
            "total_amount": "150.00",
            "total_currency": "EUR",
            "slices": [
                {
                    "segments": [
                        {
                            "operating_carrier": {"iata_code": "LO"},
                            "departing_at": "2024-07-15T10:00:00Z",
                            "arriving_at": "2024-07-15T12:00:00Z",
                        }
                    ]
                }
            ],
        }

        diff_carrier = {
            "id": "off_diff",
            "total_amount": "150.00",
            "total_currency": "EUR",
            "slices": [
                {
                    "segments": [
                        {
                            "operating_carrier": {"iata_code": "FR"},
                            "departing_at": "2024-07-15T10:00:00Z",
                            "arriving_at": "2024-07-15T12:00:00Z",
                        }
                    ]
                }
            ],
        }

        same_match = validator._score_offer(same_carrier, sample_segment, "LO", 10)
        diff_match = validator._score_offer(diff_carrier, sample_segment, "LO", 10)

        assert same_match is not None
        assert diff_match is not None
        # Carrier match vs mismatch should differ by carrier_match - carrier_mismatch_penalty
        expected_diff = (
            DEFAULT_MATCHING_WEIGHTS.carrier_match
            - DEFAULT_MATCHING_WEIGHTS.carrier_mismatch_penalty
        )
        assert same_match.score - diff_match.score == expected_diff

    def test_hour_matching_tiers(
        self,
        validator: DuffelOfferValidator,
        sample_segment: RouteSegment,
    ):
        """Hour matching has three tiers: exact, close (±1), outside."""
        # Exact hour (10:00)
        exact = {
            "id": "exact",
            "total_amount": "150.00",
            "total_currency": "EUR",
            "slices": [
                {
                    "segments": [
                        {
                            "operating_carrier": {"iata_code": "LO"},
                            "departing_at": "2024-07-15T10:00:00Z",
                            "arriving_at": "2024-07-15T12:00:00Z",
                        }
                    ]
                }
            ],
        }

        # Close hour (11:00, 1 hour off)
        close = {
            "id": "close",
            "total_amount": "150.00",
            "total_currency": "EUR",
            "slices": [
                {
                    "segments": [
                        {
                            "operating_carrier": {"iata_code": "LO"},
                            "departing_at": "2024-07-15T11:00:00Z",
                            "arriving_at": "2024-07-15T13:00:00Z",
                        }
                    ]
                }
            ],
        }

        # Outside (14:00, 4 hours off)
        outside = {
            "id": "outside",
            "total_amount": "150.00",
            "total_currency": "EUR",
            "slices": [
                {
                    "segments": [
                        {
                            "operating_carrier": {"iata_code": "LO"},
                            "departing_at": "2024-07-15T14:00:00Z",
                            "arriving_at": "2024-07-15T16:00:00Z",
                        }
                    ]
                }
            ],
        }

        exact_match = validator._score_offer(exact, sample_segment, "LO", 10)
        close_match = validator._score_offer(close, sample_segment, "LO", 10)
        outside_match = validator._score_offer(outside, sample_segment, "LO", 10)

        assert exact_match.score > close_match.score > outside_match.score

    def test_price_matching_tiers(
        self,
        validator: DuffelOfferValidator,
        sample_segment: RouteSegment,
    ):
        """Price matching has tiers: exact (≤5%), close (≤25%), outside."""
        base_offer = {
            "total_currency": "EUR",
            "slices": [
                {
                    "segments": [
                        {
                            "operating_carrier": {"iata_code": "LO"},
                            "departing_at": "2024-07-15T10:00:00Z",
                            "arriving_at": "2024-07-15T12:00:00Z",
                        }
                    ]
                }
            ],
        }

        # Exact price (within 5% of 150)
        exact = {**base_offer, "id": "exact", "total_amount": "152.00"}

        # Close price (within 25% of 150)
        close = {**base_offer, "id": "close", "total_amount": "180.00"}

        # Outside price (>25% of 150)
        outside = {**base_offer, "id": "outside", "total_amount": "250.00"}

        exact_match = validator._score_offer(exact, sample_segment, "LO", 10)
        close_match = validator._score_offer(close, sample_segment, "LO", 10)
        outside_match = validator._score_offer(outside, sample_segment, "LO", 10)

        assert exact_match.score > close_match.score > outside_match.score

    def test_offer_match_confidence_calculation(self):
        """OfferMatch.confidence converts score to 0-100 percentage."""
        max_score = DEFAULT_MATCHING_WEIGHTS.max_score

        # Perfect score
        perfect = OfferMatch(
            offer_id="test",
            price=100.0,
            currency="EUR",
            carrier_code="LO",
            departure_time=datetime.now(),
            arrival_time=datetime.now(),
            score=max_score,
            num_stops=0,
        )
        assert perfect.confidence == 100.0

        # Half score
        half = OfferMatch(
            offer_id="test",
            price=100.0,
            currency="EUR",
            carrier_code="LO",
            departure_time=datetime.now(),
            arrival_time=datetime.now(),
            score=max_score / 2,
            num_stops=0,
        )
        assert half.confidence == 50.0

        # Negative score clamped to 0
        negative = OfferMatch(
            offer_id="test",
            price=100.0,
            currency="EUR",
            carrier_code="LO",
            departure_time=datetime.now(),
            arrival_time=datetime.now(),
            score=-50.0,
            num_stops=0,
        )
        assert negative.confidence == 0.0


# =============================================================================
# ZZ PLACEHOLDER CARRIER TESTS
# =============================================================================


class TestPlaceholderCarrierDetection:
    """Tests for ZZ placeholder carrier detection."""

    def test_detects_zz_carrier(self, validator: DuffelOfferValidator):
        """ZZ carrier is detected as placeholder."""
        zz_offer = {
            "slices": [
                {
                    "segments": [
                        {"operating_carrier": {"iata_code": "ZZ"}}
                    ]
                }
            ]
        }
        assert validator._is_placeholder_offer(zz_offer) is True

    def test_real_carrier_not_placeholder(self, validator: DuffelOfferValidator):
        """Real carriers are not flagged as placeholder."""
        real_offer = {
            "slices": [
                {
                    "segments": [
                        {"operating_carrier": {"iata_code": "LO"}}
                    ]
                }
            ]
        }
        assert validator._is_placeholder_offer(real_offer) is False

    def test_zz_in_any_segment_is_placeholder(self, validator: DuffelOfferValidator):
        """ZZ in any segment marks entire offer as placeholder."""
        mixed_offer = {
            "slices": [
                {
                    "segments": [
                        {"operating_carrier": {"iata_code": "LO"}},
                        {"operating_carrier": {"iata_code": "ZZ"}},
                    ]
                }
            ]
        }
        assert validator._is_placeholder_offer(mixed_offer) is True

    def test_malformed_offer_not_placeholder(self, validator: DuffelOfferValidator):
        """Malformed offers don't crash, return False."""
        malformed_offers = [
            {},
            {"slices": None},
            {"slices": []},
            {"slices": [{"segments": None}]},
        ]
        for offer in malformed_offers:
            assert validator._is_placeholder_offer(offer) is False

    @pytest.mark.anyio
    @respx.mock
    async def test_all_zz_offers_returns_unavailable(
        self,
        validator: DuffelOfferValidator,
        sample_segment: RouteSegment,
        zz_carrier_response: dict,
    ):
        """Route with only ZZ offers returns UNAVAILABLE status."""
        respx.post(f"{DUFFEL_API_URL}/air/offer_requests").mock(
            return_value=httpx.Response(200, json=zz_carrier_response)
        )

        result = await validator.validate_segment(sample_segment, date(2024, 7, 15))

        assert result.status == ValidationStatus.UNAVAILABLE
        assert "not served by real airlines" in result.error_message


# =============================================================================
# RATE LIMITING AND RETRY TESTS
# =============================================================================


class TestRateLimitingAndRetries:
    """Tests for rate limiting with exponential backoff."""

    @pytest.mark.anyio
    @respx.mock
    async def test_retries_on_429(
        self,
        validator: DuffelOfferValidator,
        sample_segment: RouteSegment,
        sample_duffel_response: dict,
    ):
        """Validator retries on 429 rate limit response."""
        route = respx.post(f"{DUFFEL_API_URL}/air/offer_requests")

        # First call returns 429, second returns success
        route.side_effect = [
            httpx.Response(429, json={"error": "rate limited"}),
            httpx.Response(200, json=sample_duffel_response),
        ]

        with patch("asyncio.sleep", return_value=None):
            result = await validator.validate_segment(sample_segment, date(2024, 7, 15))

        assert result.status == ValidationStatus.CONFIRMED
        assert route.call_count == 2

    @pytest.mark.anyio
    @respx.mock
    async def test_gives_up_after_max_retries(
        self,
        sample_segment: RouteSegment,
    ):
        """Validator gives up after max_retries attempts."""
        validator = DuffelOfferValidator(
            api_token="test_token",
            config=ValidationConfig(max_retries=2),
        )

        respx.post(f"{DUFFEL_API_URL}/air/offer_requests").mock(
            return_value=httpx.Response(429, json={"error": "rate limited"})
        )

        with patch("asyncio.sleep", return_value=None):
            result = await validator.validate_segment(sample_segment, date(2024, 7, 15))

        # Should return UNAVAILABLE after exhausting retries (empty offers)
        assert result.status == ValidationStatus.UNAVAILABLE

    @pytest.mark.anyio
    @respx.mock
    async def test_exponential_backoff_timing(
        self,
        sample_segment: RouteSegment,
        sample_duffel_response: dict,
    ):
        """Backoff multiplier is applied correctly."""
        validator = DuffelOfferValidator(
            api_token="test_token",
            config=ValidationConfig(max_retries=3, backoff_multiplier=2.0),
        )

        route = respx.post(f"{DUFFEL_API_URL}/air/offer_requests")
        route.side_effect = [
            httpx.Response(429),
            httpx.Response(429),
            httpx.Response(200, json=sample_duffel_response),
        ]

        sleep_times = []

        async def mock_sleep(duration):
            sleep_times.append(duration)

        with patch("asyncio.sleep", side_effect=mock_sleep):
            await validator.validate_segment(sample_segment, date(2024, 7, 15))

        # Backoff: 1.0, then 2.0 (1.0 * 2.0)
        assert sleep_times == [1.0, 2.0]


# =============================================================================
# ERROR HANDLING TESTS
# =============================================================================


class TestErrorHandling:
    """Tests for error handling scenarios."""

    @pytest.mark.anyio
    @respx.mock
    async def test_api_error_on_500(
        self,
        validator: DuffelOfferValidator,
        sample_segment: RouteSegment,
    ):
        """500 error returns empty offers (UNAVAILABLE)."""
        respx.post(f"{DUFFEL_API_URL}/air/offer_requests").mock(
            return_value=httpx.Response(500, json={"error": "internal error"})
        )

        result = await validator.validate_segment(sample_segment, date(2024, 7, 15))

        assert result.status == ValidationStatus.UNAVAILABLE

    @pytest.mark.anyio
    @respx.mock
    async def test_timeout_returns_api_error(
        self,
        sample_segment: RouteSegment,
    ):
        """Timeout after max retries returns API_ERROR."""
        validator = DuffelOfferValidator(
            api_token="test_token",
            config=ValidationConfig(max_retries=1, request_timeout_ms=100),
        )

        respx.post(f"{DUFFEL_API_URL}/air/offer_requests").mock(
            side_effect=httpx.TimeoutException("Connection timed out")
        )

        with patch("asyncio.sleep", return_value=None):
            result = await validator.validate_segment(sample_segment, date(2024, 7, 15))

        assert result.status == ValidationStatus.API_ERROR
        assert "timed out" in result.error_message.lower()

    @pytest.mark.anyio
    @respx.mock
    async def test_no_offers_returns_unavailable(
        self,
        validator: DuffelOfferValidator,
        sample_segment: RouteSegment,
    ):
        """Empty offers list returns UNAVAILABLE."""
        respx.post(f"{DUFFEL_API_URL}/air/offer_requests").mock(
            return_value=httpx.Response(200, json={"data": {"offers": []}})
        )

        result = await validator.validate_segment(sample_segment, date(2024, 7, 15))

        assert result.status == ValidationStatus.UNAVAILABLE
        assert "No offers found" in result.error_message

    @pytest.mark.anyio
    @respx.mock
    async def test_low_confidence_match_returns_unavailable(
        self,
        sample_segment: RouteSegment,
    ):
        """Match below min_confidence_threshold returns UNAVAILABLE."""
        validator = DuffelOfferValidator(
            api_token="test_token",
            config=ValidationConfig(min_confidence_threshold=90.0),
        )

        # Offer with very different characteristics (low score)
        poor_match_response = {
            "data": {
                "offers": [
                    {
                        "id": "off_poor",
                        "total_amount": "500.00",  # Way off price
                        "total_currency": "EUR",
                        "slices": [
                            {
                                "segments": [
                                    {
                                        "operating_carrier": {"iata_code": "XX"},
                                        "departing_at": "2024-07-15T20:00:00Z",
                                        "arriving_at": "2024-07-15T23:00:00Z",
                                    },
                                    {
                                        "operating_carrier": {"iata_code": "XX"},
                                        "departing_at": "2024-07-16T01:00:00Z",
                                        "arriving_at": "2024-07-16T04:00:00Z",
                                    },
                                ]
                            }
                        ],
                    }
                ]
            }
        }

        respx.post(f"{DUFFEL_API_URL}/air/offer_requests").mock(
            return_value=httpx.Response(200, json=poor_match_response)
        )

        result = await validator.validate_segment(sample_segment, date(2024, 7, 15))

        assert result.status == ValidationStatus.UNAVAILABLE
        assert "confidence threshold" in result.error_message


# =============================================================================
# SEGMENT VALIDATION TESTS
# =============================================================================


class TestSegmentValidation:
    """Tests for segment validation logic."""

    @pytest.mark.anyio
    @respx.mock
    async def test_confirmed_when_price_within_threshold(
        self,
        validator: DuffelOfferValidator,
        sample_segment: RouteSegment,
        sample_duffel_response: dict,
    ):
        """Price within 5% returns CONFIRMED status."""
        respx.post(f"{DUFFEL_API_URL}/air/offer_requests").mock(
            return_value=httpx.Response(200, json=sample_duffel_response)
        )

        result = await validator.validate_segment(sample_segment, date(2024, 7, 15))

        assert result.status == ValidationStatus.CONFIRMED
        assert result.live_price == 152.0
        assert result.cached_price == 150.0
        assert result.offer_id == "off_123"

    @pytest.mark.anyio
    @respx.mock
    async def test_price_changed_when_significant_difference(
        self,
        sample_segment: RouteSegment,
    ):
        """Price change >5% but ≤25% returns PRICE_CHANGED."""
        validator = DuffelOfferValidator(
            api_token="test_token",
            config=ValidationConfig(
                price_confirmed_threshold=5.0,
                price_changed_threshold=25.0,
            ),
        )

        # 15% price increase (150 -> 172.50)
        response = {
            "data": {
                "offers": [
                    {
                        "id": "off_changed",
                        "total_amount": "172.50",
                        "total_currency": "EUR",
                        "slices": [
                            {
                                "segments": [
                                    {
                                        "operating_carrier": {"iata_code": "LO"},
                                        "departing_at": "2024-07-15T10:00:00Z",
                                        "arriving_at": "2024-07-15T12:00:00Z",
                                    }
                                ]
                            }
                        ],
                    }
                ]
            }
        }

        respx.post(f"{DUFFEL_API_URL}/air/offer_requests").mock(
            return_value=httpx.Response(200, json=response)
        )

        result = await validator.validate_segment(sample_segment, date(2024, 7, 15))

        assert result.status == ValidationStatus.PRICE_CHANGED
        assert result.live_price == 172.50

    @pytest.mark.anyio
    @respx.mock
    async def test_unavailable_when_price_too_different(
        self,
        sample_segment: RouteSegment,
    ):
        """Price change >25% returns UNAVAILABLE."""
        validator = DuffelOfferValidator(
            api_token="test_token",
            config=ValidationConfig(price_changed_threshold=25.0),
        )

        # 50% price increase
        response = {
            "data": {
                "offers": [
                    {
                        "id": "off_expensive",
                        "total_amount": "225.00",
                        "total_currency": "EUR",
                        "slices": [
                            {
                                "segments": [
                                    {
                                        "operating_carrier": {"iata_code": "LO"},
                                        "departing_at": "2024-07-15T10:00:00Z",
                                        "arriving_at": "2024-07-15T12:00:00Z",
                                    }
                                ]
                            }
                        ],
                    }
                ]
            }
        }

        respx.post(f"{DUFFEL_API_URL}/air/offer_requests").mock(
            return_value=httpx.Response(200, json=response)
        )

        result = await validator.validate_segment(sample_segment, date(2024, 7, 15))

        assert result.status == ValidationStatus.UNAVAILABLE


# =============================================================================
# PARALLEL VALIDATION TESTS
# =============================================================================


class TestParallelValidation:
    """Tests for validate_segments parallel execution."""

    @pytest.mark.anyio
    @respx.mock
    async def test_validates_multiple_segments_in_parallel(
        self,
        validator: DuffelOfferValidator,
        sample_duffel_response: dict,
    ):
        """Multiple segments are validated and results returned in order."""
        segments = [
            RouteSegment(
                segment_index=0,
                departure_airport="WAW",
                arrival_airport="BCN",
                dep_time=600.0,
                arr_time=720.0,
                price=150.0,
                carrier_code="LO",
            ),
            RouteSegment(
                segment_index=1,
                departure_airport="BCN",
                arrival_airport="MAD",
                dep_time=780.0,
                arr_time=840.0,
                price=75.0,
                carrier_code="VY",
            ),
        ]

        respx.post(f"{DUFFEL_API_URL}/air/offer_requests").mock(
            return_value=httpx.Response(200, json=sample_duffel_response)
        )

        results = await validator.validate_segments(segments, date(2024, 7, 15))

        assert len(results) == 2
        assert results[0].segment_index == 0
        assert results[1].segment_index == 1

    @pytest.mark.anyio
    @respx.mock
    async def test_exception_in_one_segment_doesnt_break_others(
        self,
        validator: DuffelOfferValidator,
    ):
        """Exception in one segment returns API_ERROR, others succeed."""
        segments = [
            RouteSegment(
                segment_index=0,
                departure_airport="WAW",
                arrival_airport="BCN",
                dep_time=600.0,
                arr_time=720.0,
                price=150.0,
                carrier_code="LO",
            ),
            RouteSegment(
                segment_index=1,
                departure_airport="BCN",
                arrival_airport="MAD",
                dep_time=780.0,  # 13:00
                arr_time=840.0,
                price=100.0,
                carrier_code="VY",
            ),
        ]

        # Response that matches the second segment (VY carrier, 13:00 departure)
        second_segment_response = {
            "data": {
                "offers": [
                    {
                        "id": "off_vy",
                        "total_amount": "102.00",
                        "total_currency": "EUR",
                        "slices": [
                            {
                                "segments": [
                                    {
                                        "operating_carrier": {"iata_code": "VY"},
                                        "departing_at": "2024-07-15T13:00:00Z",
                                        "arriving_at": "2024-07-15T14:00:00Z",
                                    }
                                ]
                            }
                        ],
                    }
                ]
            }
        }

        call_count = 0

        def mock_response(request):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise httpx.ConnectError("Connection failed")
            return httpx.Response(200, json=second_segment_response)

        respx.post(f"{DUFFEL_API_URL}/air/offer_requests").mock(
            side_effect=mock_response
        )

        results = await validator.validate_segments(segments, date(2024, 7, 15))

        assert len(results) == 2
        # First fails
        assert results[0].status == ValidationStatus.API_ERROR
        # Second succeeds
        assert results[1].status == ValidationStatus.CONFIRMED

    @pytest.mark.anyio
    async def test_empty_segments_returns_empty_list(
        self,
        validator: DuffelOfferValidator,
    ):
        """Empty segments list returns empty results."""
        results = await validator.validate_segments([], date(2024, 7, 15))
        assert results == []


# =============================================================================
# VALIDATOR PROPERTIES TESTS
# =============================================================================


class TestValidatorProperties:
    """Tests for validator properties and configuration."""

    def test_name_property(self, validator: DuffelOfferValidator):
        """Validator has correct name."""
        assert validator.name == "Duffel API"

    def test_missing_token_logs_warning(self, caplog):
        """Missing API token logs warning."""
        with patch.dict("os.environ", {}, clear=True):
            DuffelOfferValidator(api_token="")

        # Warning should be logged
        assert any("DUFFEL_ACCESS_TOKEN" in record.message for record in caplog.records)

    def test_custom_config_applied(self):
        """Custom configuration is applied."""
        config = ValidationConfig(
            max_concurrent_requests=5,
            request_timeout_ms=15000,
        )
        validator = DuffelOfferValidator(api_token="test", config=config)

        assert validator._config.max_concurrent_requests == 5
        assert validator._config.request_timeout_ms == 15000

    def test_custom_weights_applied(self):
        """Custom matching weights are applied."""
        weights = MatchingWeights(carrier_match=100.0)
        validator = DuffelOfferValidator(api_token="test", weights=weights)

        assert validator._weights.carrier_match == 100.0
