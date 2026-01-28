"""
Flight Data Expander Service - Weekly extrapolation of flight data.

Expands flight data from a base week to cover arbitrary date ranges,
enabling searches across dates outside the original data collection period.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import List

import pandas as pd

from src.flight_router.ports.flight_data_expander import FlightDataExpander

logger = logging.getLogger(__name__)

# Reference epoch (must match EPOCH_REFERENCE in duffel_provider.py)
EPOCH_REFERENCE = datetime(2024, 1, 1, 0, 0, 0)


class FlightDataExpanderService(FlightDataExpander):
    """
    Service for expanding flight data across date ranges.

    Takes flight data from a representative base week and creates
    virtual copies for other weeks, allowing searches on arbitrary dates.

    The expansion process:
    1. Determine which weeks overlap with the requested [t_min, t_max]
    2. For each needed week, copy base data with shifted dep_time/arr_time
    3. Return concatenated DataFrame covering the full range

    Attributes:
        BASE_WEEK_START: Start of base data week (Monday).
        BASE_WEEK_END: End of base data week (Sunday).
    """

    # Base week in database: 2026-07-13 (Mon) to 2026-07-19 (Sun)
    BASE_WEEK_START = datetime(2026, 7, 13, 0, 0, 0)
    BASE_WEEK_END = datetime(2026, 7, 19, 23, 59, 59)

    # Time constants
    MINUTES_PER_DAY = 24 * 60
    MINUTES_PER_WEEK = 7 * MINUTES_PER_DAY

    def __init__(self) -> None:
        """Initialize the expander service."""
        self._base_start_minutes = self._to_epoch_minutes(self.BASE_WEEK_START)
        self._base_end_minutes = self._to_epoch_minutes(self.BASE_WEEK_END)

    def _to_epoch_minutes(self, dt: datetime) -> float:
        """Convert datetime to minutes since epoch reference."""
        delta = dt - EPOCH_REFERENCE
        return delta.total_seconds() / 60

    def _from_epoch_minutes(self, minutes: float) -> datetime:
        """Convert epoch minutes back to datetime."""
        return EPOCH_REFERENCE + timedelta(minutes=minutes)

    @property
    def base_week_start_minutes(self) -> float:
        """Start of base week in epoch minutes."""
        return self._base_start_minutes

    @property
    def base_week_end_minutes(self) -> float:
        """End of base week in epoch minutes."""
        return self._base_end_minutes

    def get_week_offsets_for_range(
        self,
        t_min: float,
        t_max: float,
    ) -> List[int]:
        """
        Calculate day offsets needed to cover the date range.

        Determines how many weeks before and after the base week are
        required. Returns offsets in days (multiples of 7).

        Args:
            t_min: Start of search range (epoch minutes).
            t_max: End of search range (epoch minutes).

        Returns:
            Sorted list of day offsets, e.g., [-7, 0, 7, 14].
            Always includes 0 (base week) if range overlaps with it.

        Example:
            Base week: 2026-07-13 to 2026-07-19
            Range: 2026-07-20 to 2026-08-10

            Returns: [7, 14, 21, 28] (4 weeks after base)
        """
        # How many weeks before base week start?
        if t_min < self._base_start_minutes:
            weeks_before = int(
                (self._base_start_minutes - t_min) / self.MINUTES_PER_WEEK
            ) + 1
        else:
            weeks_before = 0

        # How many weeks after base week end?
        if t_max > self._base_end_minutes:
            weeks_after = int(
                (t_max - self._base_end_minutes) / self.MINUTES_PER_WEEK
            ) + 1
        else:
            weeks_after = 0

        # Generate offsets (in days)
        offsets = []
        for w in range(-weeks_before, weeks_after + 1):
            offset_days = w * 7
            offsets.append(offset_days)

        # Filter: only include offsets where the week overlaps with [t_min, t_max]
        filtered_offsets = []
        for offset_days in offsets:
            offset_minutes = offset_days * self.MINUTES_PER_DAY
            week_start = self._base_start_minutes + offset_minutes
            week_end = self._base_end_minutes + offset_minutes

            # Check if this week overlaps with the search range
            if week_end >= t_min and week_start <= t_max:
                filtered_offsets.append(offset_days)

        logger.debug(
            "Week offsets for range [%.0f, %.0f]: %s",
            t_min,
            t_max,
            filtered_offsets,
        )

        return sorted(filtered_offsets)

    def expand_for_date_range(
        self,
        flights_df: pd.DataFrame,
        t_min: float,
        t_max: float,
    ) -> pd.DataFrame:
        """
        Expand flight data to cover the requested date range.

        Creates copies of base week data for each needed week,
        shifting dep_time and arr_time appropriately.

        Args:
            flights_df: Base flight data (one week).
            t_min: Start of search range (epoch minutes).
            t_max: End of search range (epoch minutes).

        Returns:
            DataFrame with data for all needed weeks.
            If range fits within base week, returns original unchanged.
        """
        if flights_df.empty:
            logger.warning("Empty flights DataFrame, nothing to expand")
            return flights_df

        offsets = self.get_week_offsets_for_range(t_min, t_max)

        # Fast path: no expansion needed (range within base week)
        if len(offsets) == 1 and offsets[0] == 0:
            logger.debug("No expansion needed, range within base week")
            return flights_df

        logger.info(
            "Expanding flight data for %d weeks (offsets: %s)",
            len(offsets),
            offsets,
        )

        expanded_dfs = []

        for offset_days in offsets:
            offset_minutes = offset_days * self.MINUTES_PER_DAY

            if offset_days == 0:
                # Base week - use original data
                expanded_dfs.append(flights_df)
            else:
                # Create copy with shifted times
                df_copy = flights_df.copy()
                df_copy["dep_time"] = df_copy["dep_time"] + offset_minutes
                df_copy["arr_time"] = df_copy["arr_time"] + offset_minutes

                # Update scheduled_departure if present (for debugging)
                if "scheduled_departure" in df_copy.columns:
                    # Shift datetime strings by offset_days
                    try:
                        original_dates = pd.to_datetime(
                            df_copy["scheduled_departure"], errors="coerce"
                        )
                        shifted_dates = original_dates + pd.Timedelta(days=offset_days)
                        df_copy["scheduled_departure"] = shifted_dates.dt.strftime(
                            "%Y-%m-%dT%H:%M:%S"
                        )
                    except Exception:
                        # If conversion fails, just keep original
                        pass

                expanded_dfs.append(df_copy)

        # Concatenate all weeks
        result = pd.concat(expanded_dfs, ignore_index=True)

        logger.info(
            "Expanded from %d to %d flights (%d weeks)",
            len(flights_df),
            len(result),
            len(offsets),
        )

        return result
