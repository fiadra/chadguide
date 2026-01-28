"""
Flight Data Expander port interface.

Defines the abstract contract for expanding flight data from a base week
to cover arbitrary date ranges. This enables searching for flights on dates
outside the original data range by assuming weekly periodicity.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import List

import pandas as pd


class FlightDataExpander(ABC):
    """
    Abstract interface for expanding flight data across date ranges.

    The base flight database contains data for a single representative week.
    Implementations of this port create virtual copies of this data for
    other weeks, allowing searches across arbitrary date ranges.

    The expansion process:
    1. Calculate which weeks are needed to cover [t_min, t_max]
    2. For each needed week, create a copy of base data with shifted times
    3. Optionally filter by operating_days (which days of week the flight runs)

    Implementations:
    - FlightDataExpanderService: Standard week-based expansion
    """

    @abstractmethod
    def expand_for_date_range(
        self,
        flights_df: pd.DataFrame,
        t_min: float,
        t_max: float,
    ) -> pd.DataFrame:
        """
        Expand flight data to cover the specified date range.

        Creates copies of the base week data for all weeks that overlap
        with [t_min, t_max]. Each copy has dep_time and arr_time shifted
        by the appropriate number of days.

        Args:
            flights_df: Base flight data (typically one week).
            t_min: Start of date range (epoch minutes).
            t_max: End of date range (epoch minutes).

        Returns:
            DataFrame with original data plus shifted copies for each
            needed week. If range fits within base week, returns original.
        """
        ...

    @abstractmethod
    def get_week_offsets_for_range(
        self,
        t_min: float,
        t_max: float,
    ) -> List[int]:
        """
        Calculate week offsets needed to cover a date range.

        Determines how many weeks before and after the base week are
        required to fully cover the requested range.

        Args:
            t_min: Start of date range (epoch minutes).
            t_max: End of date range (epoch minutes).

        Returns:
            List of day offsets, e.g., [-7, 0, 7, 14] means one week
            before base, base week, and two weeks after.
        """
        ...

    @property
    @abstractmethod
    def base_week_start_minutes(self) -> float:
        """
        Start of base week in epoch minutes.

        Returns:
            Epoch minutes for the first moment of the base week.
        """
        ...

    @property
    @abstractmethod
    def base_week_end_minutes(self) -> float:
        """
        End of base week in epoch minutes.

        Returns:
            Epoch minutes for the last moment of the base week.
        """
        ...
