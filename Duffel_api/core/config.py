"""
Configuration module for the Flight Scanner application.

This module handles loading environment variables and provides
centralized configuration for API access and application settings.
"""

import os
from typing import Dict, Optional

from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


class Config:
    """
    Application configuration class.

    This class provides centralized access to configuration values
    including API credentials, endpoints, and request settings.

    Attributes:
        API_TOKEN: The Duffel API authentication token.
        API_URL: The base URL for the Duffel API offers endpoint.
        TIMEOUT: Request timeout in milliseconds.
        HEADERS: HTTP headers for API requests.

    Raises:
        ValueError: If the API token is not found in environment variables.
    """

    API_TOKEN: Optional[str] = os.getenv("DUFFEL_API_TOKEN")
    API_URL: str = "https://api.duffel.com/air/offer_requests"
    TIMEOUT: int = 8000  # milliseconds

    HEADERS: Dict[str, str] = {
        "Authorization": f"Bearer {API_TOKEN}",
        "Duffel-Version": "v2",
        "Content-Type": "application/json",
        "Accept-Encoding": "gzip"
    }

    if not API_TOKEN:
        raise ValueError(
            "API token not found. Please check your .env file and ensure "
            "DUFFEL_API_TOKEN is set."
        )
