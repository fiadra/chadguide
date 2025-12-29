"""
Immutability utilities for DataFrame protection.

Provides functions to enforce read-only access to shared DataFrames,
preventing cache corruption from legacy algorithms that might mutate data.
"""

import numpy as np
import pandas as pd


def make_immutable(df: pd.DataFrame) -> pd.DataFrame:
    """
    Make DataFrame read-only to prevent accidental mutation.

    Sets the writeable flag to False on all underlying numpy arrays.
    Any attempt to modify the DataFrame will raise ValueError.

    CRITICAL: Call this before passing shared data to untrusted/legacy code.
    This is a zero-copy operation - no data duplication occurs.

    Args:
        df: DataFrame to make immutable.

    Returns:
        The same DataFrame with read-only arrays.

    Raises:
        ValueError: If any code attempts to modify the DataFrame.

    Example:
        >>> df = pd.DataFrame({'a': [1, 2, 3]})
        >>> df = make_immutable(df)
        >>> df['a'] = [4, 5, 6]  # Raises ValueError
    """
    for col in df.columns:
        arr = df[col].values
        if isinstance(arr, np.ndarray) and arr.flags.writeable:
            arr.flags.writeable = False

    return df


def make_defensive_copy(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create a mutable copy for algorithms that REQUIRE mutation.

    Use this ONLY when:
    1. Algorithm is verified to require in-place modification
    2. Performance cost of copy is acceptable
    3. Immutability flag causes failures

    WARNING: O(n) memory and time cost!

    Args:
        df: DataFrame to copy.

    Returns:
        New mutable DataFrame with copied data.

    Example:
        >>> immutable_df = make_immutable(original_df)
        >>> mutable_df = make_defensive_copy(immutable_df)
        >>> mutable_df['new_col'] = 'works'  # OK
    """
    return df.copy(deep=True)


def is_immutable(df: pd.DataFrame) -> bool:
    """
    Check if DataFrame is immutable (all arrays are read-only).

    Args:
        df: DataFrame to check.

    Returns:
        True if all underlying arrays are read-only.
    """
    for col in df.columns:
        arr = df[col].values
        if isinstance(arr, np.ndarray) and arr.flags.writeable:
            return False
    return True
