import pandas as pd
import pytest

from src.dijkstra.alg import dijkstra


# -------------------------
# Fixtures
# -------------------------

@pytest.fixture
def time_limits():
    return 0, 100


def build_flights_by_city(df):
    """Build flights_by_city dict from DataFrame (test helper)."""
    return {city: group for city, group in df.groupby("departure_airport")}


@pytest.fixture
def run_dijkstra():
    def _run(flights_df, start_city, required_cities, T_min, T_max):
        flights_by_city = build_flights_by_city(flights_df)
        return dijkstra(
            flights_df,
            start_city=start_city,
            required_cities=required_cities,
            T_min=T_min,
            T_max=T_max,
            flights_by_city=flights_by_city,
        )

    return _run


def flights_df_from_list(data):
    return pd.DataFrame(
        data,
        columns=[
            "departure_airport",
            "arrival_airport",
            "dep_time",
            "arr_time",
            "price",
        ],
    )


# -------------------------
# Tests
# -------------------------

@pytest.mark.parametrize(
    "data,required,expected_len",
    [
        (
            # two Pareto-optimal solutions
            [
                ["A", "B", 10, 20, 1],
                ["B", "D", 20, 50, 1],
                ["D", "A", 50, 80, 1],
                ["A", "C", 1, 2, 100],
                ["C", "D", 3, 4, 100],
                ["D", "A", 5, 6, 100],
            ],
            {"D"},
            2,
        ),
        (
            # single Pareto-optimal solution
            [
                ["A", "B", 1, 2, 1],
                ["B", "D", 3, 4, 1],
                ["D", "A", 5, 6, 1],
                ["A", "C", 10, 20, 100],
                ["C", "D", 20, 50, 100],
                ["D", "A", 50, 80, 100],
            ],
            {"D"},
            1,
        ),
        (
            # second flight departs too early
            [
                ["A", "B", 1, 3, 1],
                ["B", "A", 2, 4, 1],
            ],
            {"B"},
            0,
        ),
        (
            # invalid times (depart before arrival)
            [
                ["A", "B", 2, 3, 1],
                ["B", "A", 1, 4, 1],
            ],
            {"B"},
            0,
        ),
    ],
)
def test_dijkstra(
    time_limits,
    run_dijkstra,
    data,
    required,
    expected_len,
):
    T_min, T_max = time_limits
    flights_df = flights_df_from_list(data)

    solutions = run_dijkstra(
        flights_df,
        start_city="A",
        required_cities=required,
        T_min=T_min,
        T_max=T_max,
    )

    assert len(solutions) == expected_len
