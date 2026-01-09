from typing import Optional
from .client import GeoapifyClient

def get_place_id(client: GeoapifyClient, city: str) -> Optional[str]:
    data = client.get(
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

    return features[0]["properties"]["place_id"]
