import requests
from typing import Dict, Any

class GeoapifyClient:
    BASE_URL = "https://api.geoapify.com"

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.session = requests.Session()

    def get(self, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
        params["apiKey"] = self.api_key
        url = f"{self.BASE_URL}{path}"

        response = self.session.get(url, params=params)
        response.raise_for_status()
        return response.json()
