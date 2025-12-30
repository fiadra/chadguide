import requests
from typing import List, Dict, Any, Optional
from dijkstra.labels import Label
import os


api_key = os.getenv("GEOAPIFY_API_KEY")
if not api_key:
    raise RuntimeError("GEOAPIFY_API_KEY environment variable not set")


def get_place_id(city: str) -> Optional[str]:
    url = "https://api.geoapify.com/v1/geocode/search"
    params = {
        "text": city,
        "type": "city",
        "limit": 1,
        "apiKey": api_key,
    }

    response = requests.get(url, params=params)
    response.raise_for_status()

    data = response.json()

    if not data["features"]:
        return None

    return data["features"][0]["properties"]["place_id"]


def create_url(place_id: str, category: str, limit: int = 3) -> str:
    return (
        "https://api.geoapify.com/v2/places"
        f"?categories={category}"
        f"&filter=place:{place_id}"
        "&lang=en"
        f"&limit={limit}"
        "&apiKey={api_key}"
    )


def fetch_data(label: Label, categories: List[str]) -> Dict[str, Any]:
    place_id = get_place_id(label.city, api_key)
    if place_id is None:
        raise ValueError(f"Could not fetch Geoapify place ID for city '{label.city}'")
    combined_results = []

    for category in categories:
        url = create_url(place_id, category)
        response = requests.get(url)
        response.raise_for_status()  # fails fast if request is bad

        data = response.json()

        # Geoapify returns features inside "features"
        if "features" in data:
            combined_results.extend(data["features"])

    # Return a combined GeoJSON-like structure
    return {
        "type": "FeatureCollection",
        "features": combined_results
    }
