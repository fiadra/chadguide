"""
API client module for the Duffel Flight API.

This module provides functions for communicating with the Duffel API
to fetch flight offers and related data.
"""

import logging
from typing import Any, Dict, List

import requests

from core.config import Config

logger = logging.getLogger(__name__)


def fetch_flight_offers(
    origin: str,
    destination: str,
    date: str
) -> List[Dict[str, Any]]:
    """
    Fetch flight offers from the Duffel API for a given route and date.

    Sends a POST request to the Duffel API to retrieve available flight
    offers for a one-way trip with a single adult passenger in economy
    class. Only non-stop flights are requested.

    Args:
        origin: The IATA code of the departure airport (e.g., "WAW").
        destination: The IATA code of the arrival airport (e.g., "LON").
        date: The departure date in ISO format (YYYY-MM-DD).

    Returns:
        A list of offer dictionaries containing flight details and pricing.
        Returns an empty list if no offers are available.

    Raises:
        requests.exceptions.HTTPError: If the API returns an error status
            code (4xx or 5xx). Common errors include:
            - 429: Rate limit exceeded
            - 401: Invalid API token
            - 500: Server error
        requests.exceptions.RequestException: For network-related errors
            such as connection timeouts or DNS failures.

    Example:
        >>> offers = fetch_flight_offers("WAW", "BCN", "2024-07-15")
        >>> if offers:
        ...     print(f"Found {len(offers)} offers")
    """
    payload: Dict[str, Any] = {
        "data": {
            "slices": [{
                "origin": origin,
                "destination": destination,
                "departure_date": date
            }],
            "passengers": [{"type": "adult"}],
            "cabin_class": "economy",
            "max_connections": 0
        }
    }

    params: Dict[str, str] = {
        "return_offers": "true",
        "supplier_timeout": str(Config.TIMEOUT)
    }

    logger.debug(
        "Fetching offers for %s -> %s on %s",
        origin, destination, date
    )

    response = requests.post(
        Config.API_URL,
        headers=Config.HEADERS,
        params=params,
        json=payload
    )

    # Raise exception for 4xx/5xx status codes
    response.raise_for_status()

    offers = response.json().get('data', {}).get('offers', [])
    logger.debug("Received %d offers", len(offers))

    return offers
