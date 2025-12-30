"""
Multi-criteria Dijkstra algorithm for finding Pareto-optimal routes.

Performance optimizations:
- FlightRecord dataclass replaces pd.Series (10x memory reduction)
- CityFlightArrays enables vectorized filtering (eliminates iterrows overhead)
- Pre-extracted numpy arrays provide O(1) column access
"""

import heapq
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Dict, List, Set

import numpy as np
import pandas as pd

from .dominance import dominates, pareto_filter
from .labels import Label
from .validation import validate_dijkstra_inputs


@dataclass(frozen=True, slots=True)
class FlightRecord:
    """
    Lightweight flight record for algorithm iteration.

    Provides dict-like access for compatibility with path reconstruction.
    Memory footprint: ~200 bytes vs ~2KB for pd.Series.
    """

    departure_airport: str
    arrival_airport: str
    dep_time: float
    arr_time: float
    price: float
    _extra: tuple = ()  # tuple of (key, value) pairs for extended schema

    def __getitem__(self, key: str) -> Any:
        """Dict-like access for reconstruction compatibility."""
        if key == "departure_airport":
            return self.departure_airport
        elif key == "arrival_airport":
            return self.arrival_airport
        elif key == "dep_time":
            return self.dep_time
        elif key == "arr_time":
            return self.arr_time
        elif key == "price":
            return self.price
        else:
            for k, v in self._extra:
                if k == key:
                    return v
            raise KeyError(key)

    def get(self, key: str, default: Any = None) -> Any:
        """Dict-like get method."""
        try:
            return self[key]
        except KeyError:
            return default


class CityFlightArrays:
    """
    Pre-extracted numpy arrays for a city's outbound flights.

    Enables O(1) column access and vectorized feasibility filtering
    without creating pd.Series objects during iteration.
    """

    __slots__ = (
        "arr_airport",
        "dep_time",
        "arr_time",
        "price",
        "extra_cols",
        "extra_arrays",
        "n",
    )

    def __init__(self, df: pd.DataFrame) -> None:
        self.n = len(df)
        if self.n == 0:
            self.arr_airport = np.array([], dtype=object)
            self.dep_time = np.array([], dtype=np.float64)
            self.arr_time = np.array([], dtype=np.float64)
            self.price = np.array([], dtype=np.float64)
            self.extra_cols: List[str] = []
            self.extra_arrays: List[np.ndarray] = []
        else:
            self.arr_airport = df["arrival_airport"].values
            self.dep_time = df["dep_time"].values
            self.arr_time = df["arr_time"].values
            self.price = df["price"].values
            core_cols = {
                "departure_airport",
                "arrival_airport",
                "dep_time",
                "arr_time",
                "price",
            }
            self.extra_cols = [c for c in df.columns if c not in core_cols]
            self.extra_arrays = [df[c].values for c in self.extra_cols]

    def get_feasible_indices(self, current_time: float, t_max: float) -> np.ndarray:
        """Return indices of flights departing after current_time and arriving before t_max."""
        if self.n == 0:
            return np.array([], dtype=np.intp)
        mask = (self.dep_time >= current_time) & (self.arr_time <= t_max)
        return np.nonzero(mask)[0]

    def make_flight_record(self, idx: int, dep_airport: str) -> FlightRecord:
        """Create FlightRecord at given index."""
        extra = tuple(
            (c, self.extra_arrays[i][idx]) for i, c in enumerate(self.extra_cols)
        )
        return FlightRecord(
            departure_airport=dep_airport,
            arrival_airport=str(self.arr_airport[idx]),
            dep_time=float(self.dep_time[idx]),
            arr_time=float(self.arr_time[idx]),
            price=float(self.price[idx]),
            _extra=extra,
        )


def try_insert_label(labels_at_state: List[Label], new_label: Label) -> bool:
    """
    Try to insert new_label into labels for the same state.

    Returns True if label was inserted (not dominated),
    False if dominated and discarded.
    """
    to_remove = []

    for existing in labels_at_state:
        if dominates(existing, new_label):
            return False
        if dominates(new_label, existing):
            to_remove.append(existing)

    for r in to_remove:
        labels_at_state.remove(r)

    labels_at_state.append(new_label)
    return True


def dijkstra(
    flights_df: pd.DataFrame,
    start_city: str,
    required_cities: Set[str],
    T_min: float,
    T_max: float,
    flights_by_city: Dict[str, pd.DataFrame],
) -> List[Label]:
    """
    Multi-criteria Dijkstra algorithm for finding Pareto-optimal routes.

    Args:
        flights_df: DataFrame with flights for input validation.
        start_city: Starting airport IATA code.
        required_cities: Set of airport codes that must be visited.
        T_min: Earliest departure time (minutes since epoch).
        T_max: Latest arrival time (minutes since epoch).
        flights_by_city: Pre-computed dict mapping departure city to DataFrame.

    Returns:
        List of Pareto-optimal Label objects representing complete routes.

    Raises:
        EmptyFlightsError: If flights DataFrame is empty.
        MissingColumnsError: If required columns are missing.
        InvalidAirportError: If start_city or required cities not found.
        InvalidTimeRangeError: If T_min > T_max.
    """
    validate_dijkstra_inputs(flights_df, start_city, required_cities, T_min, T_max)

    # Pre-extract numpy arrays for each city
    city_arrays: Dict[str, CityFlightArrays] = {
        city: CityFlightArrays(df) for city, df in flights_by_city.items()
    }

    # State: (city, visited_set_as_frozenset) -> list of non-dominated labels
    labels: Dict[tuple, List[Label]] = defaultdict(list)
    pq: List[tuple[float, float, Label]] = []

    start_label = Label(city=start_city, time=T_min, visited=set(), cost=0.0)
    labels[(start_city, frozenset())].append(start_label)
    heapq.heappush(pq, (0.0, T_min, start_label))

    solutions: List[Label] = []

    while pq:
        curr_cost, curr_time, label = heapq.heappop(pq)

        if label.time > T_max:
            continue

        if label.city == start_city and label.visited == required_cities:
            solutions.append(label)
            continue

        city = label.city
        if city not in city_arrays:
            continue

        arrays = city_arrays[city]
        feasible_idx = arrays.get_feasible_indices(label.time, T_max)

        for idx in feasible_idx:
            arr_airport = str(arrays.arr_airport[idx])
            arr_time = float(arrays.arr_time[idx])
            price = float(arrays.price[idx])

            flight = arrays.make_flight_record(idx, city)

            new_cost = label.cost + price
            new_visited = set(label.visited)
            if arr_airport in required_cities:
                new_visited.add(arr_airport)

            new_label = Label(
                city=arr_airport,
                time=arr_time,
                visited=new_visited,
                cost=new_cost,
                prev=label,
                flight=flight,
            )

            key = (arr_airport, frozenset(new_visited))

            if try_insert_label(labels[key], new_label):
                heapq.heappush(pq, (new_cost, arr_time, new_label))

    return pareto_filter(solutions)
