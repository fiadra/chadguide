import requests
from typing import Dict, Any, List, Optional, Tuple
from config import radius, get_api_key
from exceptions import (
    GeoapifyAPIError,
    GeoapifyCityNotFoundError,
    GeoapifyInvalidParameterError,
)

GeoJSONFeatureCollection = Dict[str, Any]


class GeoapifyClient:
    BASE_URL = "https://api.geoapify.com"

    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize the Geoapify client with an API key.

        Args:
            api_key (Optional[str]): Your Geoapify API key. If None, will use config.get_api_key().
        """
        self.api_key = api_key or get_api_key()
        if not self.api_key:
            raise GeoapifyInvalidParameterError(
                "api_key", "API key must be provided or configured"
            )
        self.session = requests.Session()

    def get(self, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Perform a GET request to a Geoapify API endpoint.

        Args:
            path (str): The API endpoint path (e.g., "/v2/places").
            params (Dict[str, Any]): Query parameters.

        Returns:
            Dict[str, Any]: Parsed JSON response.

        Raises:
            GeoapifyAPIError: If the API returns a non-2xx response.
        """
        params["apiKey"] = self.api_key
        url = f"{self.BASE_URL}{path}"

        try:
            response = self.session.get(url, params=params, timeout=5)
            response.raise_for_status()
        except requests.HTTPError as e:
            raise GeoapifyAPIError(response.status_code, str(e))
        except requests.RequestException as e:
            # Catch network-level errors
            raise GeoapifyAPIError(-1, f"Network error: {str(e)}")

        try:
            return response.json()
        except ValueError as e:
            raise GeoapifyAPIError(
                response.status_code, f"Invalid JSON response: {str(e)}"
            )

    def get_place_coords(self, city: str) -> Tuple[float, float]:
        """
        Get the longitude and latitude of a city.

        Args:
            city (str): City name.

        Returns:
            Tuple[float, float]: (longitude, latitude)

        Raises:
            GeoapifyCityNotFoundError: If the city cannot be found.
            GeoapifyAPIError: If the API call fails.
        """
        data = self.get(
            "/v1/geocode/search",
            {
                "text": city,
                "type": "city",
                "limit": 1,
            },
        )

        features = data.get("features", [])
        if not features:
            raise GeoapifyCityNotFoundError(city)

        props = features[0].get("properties", {})
        lon, lat = props.get("lon"), props.get("lat")
        if lon is None or lat is None:
            raise GeoapifyCityNotFoundError(city)

        return lon, lat

    def fetch_amenities(
        self,
        city: str,
        categories: List[str],
        limit: Optional[int] = 3,
    ) -> GeoJSONFeatureCollection:
        """
        Fetch amenities within a city's vicinity.

        Args:
            city (str): City name.
            categories (List[str]): List of category strings (e.g., ["restaurant", "park"]).
            limit (int, optional): Maximum results per category.

        Returns:
            GeoJSONFeatureCollection: GeoJSON FeatureCollection of amenities.

        Raises:
            GeoapifyCityNotFoundError: If the city cannot be located.
            GeoapifyAPIError: If an API call fails.
            GeoapifyInvalidParameterError: If categories is empty.
        """
        if not categories:
            raise GeoapifyInvalidParameterError(
                "categories", "At least one category must be provided"
            )

        longitude, latitude = self.get_place_coords(city)

        features = []

        for category in categories:
            data = self.get(
                "/v2/places",
                {
                    "categories": category,
                    "filter": f"circle:{longitude},{latitude},{radius}",
                    "bias": f"proximity:{longitude},{latitude}",
                    "lang": "en",
                    "limit": limit,
                },
            )
            features.extend(data.get("features", []))

        return {
            "type": "FeatureCollection",
            "features": features,
        }
