"""
Utility functions for the Flight Scanner application.

This module provides helper functions for safe data extraction
and other common operations used throughout the application.
"""

from typing import Any, Dict, Optional


def safe_get(
    data: Optional[Dict[str, Any]],
    path: str,
    default: Any = None
) -> Any:
    """
    Safely extract a value from a nested dictionary using dot notation.

    This function traverses a nested dictionary structure using a dot-separated
    path string and returns the value at the specified path. If any key in the
    path does not exist or if the data is not a dictionary, the default value
    is returned.

    Args:
        data: The dictionary to extract the value from. Can be None or a
            nested dictionary structure.
        path: A dot-separated string representing the path to the desired
            value. For example, "user.profile.name" would access
            data["user"]["profile"]["name"].
        default: The value to return if the path does not exist or if any
            intermediate value is not a dictionary. Defaults to None.

    Returns:
        The value at the specified path if it exists, otherwise the default
        value.

    Examples:
        >>> data = {"user": {"name": "John", "age": 30}}
        >>> safe_get(data, "user.name")
        'John'
        >>> safe_get(data, "user.email", "N/A")
        'N/A'
        >>> safe_get(None, "any.path", "default")
        'default'
    """
    keys = path.split('.')
    for key in keys:
        if data and isinstance(data, dict):
            data = data.get(key)
        else:
            return default
    return data if data is not None else default
