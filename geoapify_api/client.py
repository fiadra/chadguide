import logging
from typing import Dict, Any, List, Optional, Tuple
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pydantic import BaseModel, Field
from config import radius, get_api_key
from exceptions import (
    GeoapifyAPIError,
    GeoapifyCityNotFoundError,
    GeoapifyInvalidParameterError,
)

# -------------------------------
# Pydantic Models for structured data
# -------------------------------

class Properties(BaseModel):
    lon: float
    lat: float
    name: Optional[str] = None
    address_line1: Optional[str] = None
    categories: Optional[List[str]] = None

class Feature(BaseModel):
    type: str
    properties: Properties
    geometry: Optional[Dict[str, Any]] = None

class FeatureCollection(BaseModel):
    type: str = "FeatureCollection"
    features: List[Feature] = Field(default_factory=list)


# -------------------------------
# Geoapify Client
# -------------------------------

class GeoapifyClient:
    BASE_URL = "https://api.geoapify.com"

    def __init__(self, api_key: Optional[str] = None, retries: int = 3, backoff_factor: float = 0.3):
        """
        Initialize the Geoapify client.

        Args:
            api_key (Optional[str]): Geoapify API key. Fallback to config.get_api_key().
            retries (int): Number of retry attempts for failed requests.
            backoff_factor (float): Backoff factor for retries.
        """
        self.api_key = api_key or get_api_key()
        if not self.api_key:
            raise GeoapifyInvalidParameterError("api_key", "API key must be provided or configured")

        # Configure logging
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
        self.logger.addHandler(handler)

        # Session with retry logic
        self.session = requests.Session()
        retry_strategy = Retry(
            total=retries,
            backoff_factor=backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    # -------------------------------
    # GET request with error handling
    # -------------------------------
    def get(self, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Perform a GET request to a Geoapify API endpoint with retries and logging.
        Raises GeoapifyAPIError if the request fails.

        Args:
            path (str): API endpoint path.
            params (dict): Query parameters.

        Returns:
            dict: JSON response.
        """
        params["apiKey"] = self.api_key
        url = f"{self.BASE_URL}{path}"
        self.logger.info(f"Requesting {url} with params {params}")

        try:
            response = self.session.get(url, params=params, timeout=5)
            response.raise_for_status()
        except requests.HTTPError as e:
            payload = None
            try:
                payload = response.json()
            except Exception:
                pass
            self.logger.error(f"HTTPError {response.status_code}: {e} | Response: {payload}")
            raise GeoapifyAPIError(response.status_code, str(e), payload)
        except requests.RequestException as e:
            raise GeoapifyAPIError(-1, f"Network error: {e}")
            self.logger.error(f"RequestException: {e}")

        try:
            return response.json()
        except ValueError as e:
            self.logger.error(f"Invalid JSON response: {e}")
            raise GeoapifyAPIError(response.status_code, f"Invalid JSON response: {e}")

    # -------------------------------
    # Get city coordinates
    # -------------------------------
    def get_place_coords(self, city: str) -> Tuple[float, float]:
        data = self.get(
            "/v1/geocode/search",
            {"text": city, "type": "city", "limit": 1},
        )

        features = data.get("features", [])
        if not features:
            self.logger.warning(f"City '{city}' not found")
            raise GeoapifyCityNotFoundError(city)

        props = features[0].get("properties", {})
        lon, lat = props.get("lon"), props.get("lat")
        if lon is None or lat is None:
            self.logger.warning(f"Coordinates not found for city '{city}'")
            raise GeoapifyCityNotFoundError(city)

        self.logger.info(f"Found coordinates for {city}: ({lon}, {lat})")
        return lon, lat

    # -------------------------------
    # Fetch amenities
    # -------------------------------
    def fetch_amenities(
        self,
        city: str,
        categories: List[str],
        limit: Optional[int] = 3,
    ) -> FeatureCollection:
        if not categories:
            raise GeoapifyInvalidParameterError("categories", "At least one category must be provided")

        longitude, latitude = self.get_place_coords(city)
        features: List[Feature] = []

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
            # Convert API features into Pydantic models
            for f in data.get("features", []):
                try:
                    feature = Feature(**f)
                    features.append(feature)
                except Exception as e:
                    self.logger.warning(f"Failed to parse feature: {e} | Data: {f}")

        self.logger.info(f"Fetched {len(features)} features for city '{city}'")
        return FeatureCollection(features=features)
