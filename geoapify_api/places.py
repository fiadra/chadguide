from typing import List, Dict, Any
from .client import GeoapifyClient
from .geocoding import get_place_id


def fetch_amenities(
    client: GeoapifyClient,
    city: str,
    categories: List[str],
    limit: int = 3,
) -> Dict[str, Any]:
    """
    Fetch amenities within a city's administrative boundary.
    """
    place_id = get_place_id(client, city)
    if not place_id:
        raise ValueError(f"Could not fetch place ID for city '{city}'")

    features = []

    for category in categories:
        data = client.get(
            "/v2/places",
            {
                "categories": category,
                "filter": f"place:{place_id}",
                "lang": "en",
                "limit": limit,
            },
        )

        features.extend(data.get("features", []))

    return {
        "type": "FeatureCollection",
        "features": features,
    }


def fetch_hotels(
    client: GeoapifyClient,
    city: str,
    *,
    limit: int = 3,
) -> Dict[str, Any]:
    """
    Fetch hotels within a city's administrative boundary.
    """
    return fetch_amenities(
        client=client,
        city=city,
        categories=["accommodation.hotel"],
        limit=limit,
    )
