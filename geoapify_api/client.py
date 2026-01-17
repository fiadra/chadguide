import requests
from typing import Dict, Any, List, Optional
from config import radius, get_api_key

GeoJSONFeatureCollection = Dict[str, Any]


class GeoapifyClient:
    # Base URL of the Geoapify API
    BASE_URL = "https://api.geoapify.com"

    def __init__(self, api_key: Optional[str]):
        """
        Initialize the Geoapify client with an API key.

        Args:
            api_key (str): Your Geoapify API key.
        """
        if api_key is None:
            self.api_key = get_api_key()  # will raise if missing
        else:
            self.api_key = api_key

        self.session = requests.Session()

    def get(self, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Perform a GET request to a Geoapify API endpoint.

        Args:
            path (str): The API endpoint path (e.g., "/v2/places").
            params (Dict[str, Any]): Query parameters to include in the request.

        Returns:
            Dict[str, Any]: The JSON response parsed into a Python dictionary.

        Raises:
            requests.HTTPError: If the HTTP request fails (non-2xx response).
        """
        # Add the API key to the query parameters for authentication
        params["apiKey"] = self.api_key

        # Construct the full URL by combining the base URL and the endpoint path
        url = f"{self.BASE_URL}{path}"

        # Send the GET request using the session
        response = self.session.get(url, params=params, timeout=5)

        # Raise an exception if the response status code indicates an error
        response.raise_for_status()

        # Return the response body as a Python dictionary
        return response.json()

    def get_place_coords(self, city: str) -> Optional[str]:
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
            return None

        return features[0]["properties"]["lon"], features[0]["properties"]["lat"]

    def fetch_amenities(
        self,
        city: str,
        categories: List[str],
        limit: int = 3,
    ) -> GeoJSONFeatureCollection:
        """
        Fetch amenities (places) within a city's administrative boundary.

        This method performs multiple API calls to Geoapify for each category
        and combines the results into a single GeoJSON FeatureCollection.

        Args:
            city (str): The name of the city to search in.
            categories (List[str]): A list of categories to fetch (e.g., ["restaurant", "park"]).
            limit (int, optional): Maximum number of results per category. Defaults to 3.

        Returns:
            Dict[str, Any]: A GeoJSON-style FeatureCollection containing the fetched amenities.

        Raises:
            ValueError: If the place ID for the city cannot be fetched.
        """
        # Return early if `categories` is empty
        if not categories:
            return {"type": "FeatureCollection", "features": []}

        longitude, latitude = self.get_place_coords(city)

        # If we cannot find a place ID, raise an exception
        if longitude is None or latitude is None:
            raise ValueError(f"Could not fetch place coordinates for city '{city}'")

        # Initialize an empty list to store all features (amenities) across categories
        features = []

        # Loop through each requested category
        for category in categories:
            # Make a GET request to the Geoapify /v2/places endpoint
            # The request filters results by the city's place_id
            # The limit parameter restricts how many results are returned
            # The language parameter ensures results are in English
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

            # Extract the "features" list from the API response and add it to our master list
            # If the "features" key is missing, default to an empty list
            features.extend(data.get("features", []))

        # Return all the fetched features in a standard GeoJSON FeatureCollection format
        return {
            "type": "FeatureCollection",
            "features": features,
        }
