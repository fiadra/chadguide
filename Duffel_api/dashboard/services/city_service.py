"""
City service for IATA code to city name lookups.

Provides functions for converting airport codes to human-readable
city names for better user experience.
"""

import json
import logging
from pathlib import Path
from typing import Dict, Optional

logger = logging.getLogger(__name__)

__all__ = [
    "get_city_name",
    "get_city_with_code",
    "get_country",
    "format_origin_destination",
]

# Cache for city data
_city_data: Optional[Dict[str, Dict[str, str]]] = None


def _load_city_data() -> Dict[str, Dict[str, str]]:
    """
    Load city data from JSON file.

    Returns:
        Dictionary mapping IATA codes to city info.
    """
    global _city_data

    if _city_data is not None:
        return _city_data

    data_path = Path(__file__).parent.parent / "data" / "city_names.json"

    try:
        with open(data_path, "r", encoding="utf-8") as f:
            _city_data = json.load(f)
            logger.debug("Loaded %d city names from %s", len(_city_data), data_path)
    except FileNotFoundError:
        logger.warning("City names file not found: %s", data_path)
        _city_data = {}
    except json.JSONDecodeError as e:
        logger.error("Invalid JSON in city names file: %s", e)
        _city_data = {}

    return _city_data


def get_city_name(iata_code: str) -> str:
    """
    Get city name for an IATA airport code.

    Args:
        iata_code: Three-letter IATA airport code (e.g., "BCN").

    Returns:
        City name (e.g., "Barcelona") or the code itself if not found.

    Examples:
        >>> get_city_name("BCN")
        "Barcelona"
        >>> get_city_name("XXX")
        "XXX"
    """
    data = _load_city_data()
    city_info = data.get(iata_code.upper())

    if city_info:
        return city_info.get("city", iata_code)

    return iata_code


def get_city_with_code(iata_code: str) -> str:
    """
    Get city name with IATA code in parentheses.

    Args:
        iata_code: Three-letter IATA airport code.

    Returns:
        Formatted string like "Barcelona (BCN)".

    Examples:
        >>> get_city_with_code("BCN")
        "Barcelona (BCN)"
        >>> get_city_with_code("XXX")
        "XXX"
    """
    city = get_city_name(iata_code)

    if city == iata_code:
        return iata_code

    return f"{city} ({iata_code})"


def get_country(iata_code: str) -> Optional[str]:
    """
    Get country name for an IATA airport code.

    Args:
        iata_code: Three-letter IATA airport code.

    Returns:
        Country name or None if not found.
    """
    data = _load_city_data()
    city_info = data.get(iata_code.upper())

    if city_info:
        return city_info.get("country")

    return None


def format_origin_destination(origin: str, dest: str) -> str:
    """
    Format origin and destination for display.

    Args:
        origin: Origin IATA code.
        dest: Destination IATA code.

    Returns:
        Formatted route string like "Warsaw → Barcelona".
    """
    origin_city = get_city_name(origin)
    dest_city = get_city_name(dest)

    return f"{origin_city} → {dest_city}"
