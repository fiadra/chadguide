"""
Tests for min_stay_minutes feature in dijkstra algorithm.

Verifies that the algorithm enforces minimum stay time at destination cities.
"""

import pytest
import pandas as pd

from src.dijkstra.alg import dijkstra
from src.dijkstra.reconstruction import reconstruct_path


def make_flights_df(flights: list[dict]) -> pd.DataFrame:
    """Create a flights DataFrame from a list of flight dicts."""
    return pd.DataFrame(flights)


def make_flights_by_city(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Build flights_by_city dict from DataFrame."""
    return {city: group for city, group in df.groupby("departure_airport")}


# Base time: 0 = midnight day 1
# 60 = 1:00, 120 = 2:00, ..., 1440 = midnight day 2

FLIGHTS = [
    # WAW -> BCN: departs 08:00, arrives 11:00 (day 1)
    {"departure_airport": "WAW", "arrival_airport": "BCN", "dep_time": 480, "arr_time": 660, "price": 100},
    # BCN -> WAW: departs 13:00 (day 1) - only 2h after arrival
    {"departure_airport": "BCN", "arrival_airport": "WAW", "dep_time": 780, "arr_time": 960, "price": 100},
    # BCN -> WAW: departs 14:00 (day 2) - 27h after arrival
    {"departure_airport": "BCN", "arrival_airport": "WAW", "dep_time": 2280, "arr_time": 2460, "price": 120},
]


class TestMinStayInAlgorithm:
    """Tests for min_stay_minutes parameter in dijkstra()."""

    def _run(self, flights, start, required, t_min, t_max, min_stay_minutes=0.0):
        df = make_flights_df(flights)
        by_city = make_flights_by_city(df)
        return dijkstra(
            flights_df=df,
            start_city=start,
            required_cities=required,
            T_min=t_min,
            T_max=t_max,
            flights_by_city=by_city,
            min_stay_minutes=min_stay_minutes,
        )

    def test_no_min_stay_finds_short_layover(self):
        """Without min_stay, the 2h layover route in BCN is valid."""
        solutions = self._run(FLIGHTS, "WAW", {"BCN"}, 0, 3000)
        assert len(solutions) >= 1
        # Should find the cheap route with 2h layover
        costs = [s.cost for s in solutions]
        assert 200 in costs  # 100 + 100

    def test_min_stay_excludes_short_layover(self):
        """With min_stay=12h, the 2h layover route is excluded."""
        solutions = self._run(FLIGHTS, "WAW", {"BCN"}, 0, 3000, min_stay_minutes=720)
        # Only the day-2 return should be valid (27h layover)
        for sol in solutions:
            _, flights = reconstruct_path(sol)
            assert len(flights) == 2
            # Second flight must be the late one (dep_time=2280)
            assert flights[1]["dep_time"] == 2280

    def test_min_stay_zero_equals_no_constraint(self):
        """min_stay=0 behaves the same as no constraint."""
        solutions_none = self._run(FLIGHTS, "WAW", {"BCN"}, 0, 3000, min_stay_minutes=0.0)
        solutions_default = self._run(FLIGHTS, "WAW", {"BCN"}, 0, 3000)
        assert len(solutions_none) == len(solutions_default)

    def test_min_stay_start_city_not_affected(self):
        """Departure from start city is not delayed by min_stay."""
        # If start_city were affected, no flights would depart (min_stay > earliest dep)
        solutions = self._run(FLIGHTS, "WAW", {"BCN"}, 0, 3000, min_stay_minutes=720)
        assert len(solutions) >= 1

    def test_min_stay_only_affects_required_cities(self):
        """Transit cities (not in required_cities) are not affected by min_stay."""
        # WAW -> BCN -> MAD -> WAW
        # BCN is transit (not required), MAD is required
        transit_flights = [
            {"departure_airport": "WAW", "arrival_airport": "BCN", "dep_time": 480, "arr_time": 660, "price": 50},
            # BCN -> MAD: departs 1h after arrival at BCN (transit, should be OK)
            {"departure_airport": "BCN", "arrival_airport": "MAD", "dep_time": 720, "arr_time": 840, "price": 50},
            # MAD -> WAW: departs 24h after arrival at MAD (meets min_stay)
            {"departure_airport": "MAD", "arrival_airport": "WAW", "dep_time": 2280, "arr_time": 2460, "price": 80},
        ]
        solutions = self._run(transit_flights, "WAW", {"MAD"}, 0, 3000, min_stay_minutes=720)
        # Should find route: BCN is transit so 1h layover is fine
        assert len(solutions) >= 1

    def test_no_solutions_when_min_stay_too_large(self):
        """If min_stay is too large for the time window, no solutions found."""
        # T_max is too tight for a 24h stay in BCN
        solutions = self._run(FLIGHTS, "WAW", {"BCN"}, 0, 1000, min_stay_minutes=1440)
        assert len(solutions) == 0
