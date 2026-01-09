import os

def get_api_key() -> str:
    api_key = os.getenv("GEOAPIFY_API_KEY")
    if not api_key:
        raise RuntimeError("GEOAPIFY_API_KEY environment variable not set")
    return api_key
