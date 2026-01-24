Geoapify API
===================

This module provides the `GeoapifyClient` class, a Python client for interacting
with the [Geoapify API](https://www.geoapify.com/). It allows querying for
geographical data such as city place IDs and points of interest (amenities) within
a given administrative boundary.

Classes
-------
GeoapifyClient(api_key: Optional[str])
    A client to interact with Geoapify endpoints. Supports:
    - Fetching place IDs for cities.
    - Fetching amenities (restaurants, parks, shops, etc.) within a city.
    - Returning results as GeoJSON FeatureCollections.

Type Aliases
------------
GeoJSONFeatureCollection : Dict[str, Any]
    A dictionary representing a GeoJSON FeatureCollection, as returned by the
    Geoapify API.

Usage Example
-------------
```python
from geoapify_api.client import GeoapifyClient

# Initialize client with an API key
client = GeoapifyClient(api_key="YOUR_API_KEY")

# Get the place ID for Paris
place_id = client.get_place_id("Paris")
print(place_id)  # e.g., "12345"

# Fetch amenities in Paris
amenities = client.fetch_amenities(
    city="Paris",
    categories=["restaurant", "park"],
    limit=5
)

print(amenities["type"])          # "FeatureCollection"
print(len(amenities["features"])) # Number of amenities returned
```
