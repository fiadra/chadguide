import pandas as pd
import pytest
from dijkstra.alg import dijkstra


@pytest.fixture
def time_limits():
    """Return T_min and T_max as named dictionary."""
    return {"T_min": 0, "T_max": 100}


def flights_df_from_list(data):
    """Helper to convert list of flights into DataFrame."""
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


def run_dijkstra(flights_df, start_city, required_cities, T_min, T_max):
    """Helper to run the dijkstra algorithm."""
    return dijkstra(
        flights_df,
        start_city=start_city,
        required_cities=required_cities,
        T_min=T_min,
        T_max=T_max,
    )


def test_two_pareto_solutions(time_limits):
    """There are two Pareto-optimal solutions, one cheaper, one faster."""
    T_min = time_limits["T_min"]
    T_max = time_limits["T_max"]

    data = [
        ["A", "B", 10, 20, 1],
        ["B", "D", 20, 50, 1],
        ["D", "A", 50, 80, 1],
        ["A", "C", 1, 2, 100],
        ["C", "D", 3, 4, 100],
        ["D", "A", 5, 6, 100],
    ]

    flights_df = flights_df_from_list(data)
    solutions = run_dijkstra(
        flights_df, start_city="A", required_cities={"D"}, T_min=T_min, T_max=T_max
    )

    assert len(solutions) == 2


def test_one_pareto_solution(time_limits):
    """There is only one Pareto-optimal solution."""
    T_min = time_limits["T_min"]
    T_max = time_limits["T_max"]

    data = [
        ["A", "B", 1, 2, 1],
        ["B", "D", 3, 4, 1],
        ["D", "A", 5, 6, 1],
        ["A", "C", 10, 20, 100],
        ["C", "D", 20, 50, 100],
        ["D", "A", 50, 80, 100],
    ]

    flights_df = flights_df_from_list(data)
    solutions = run_dijkstra(
        flights_df, start_city="A", required_cities={"D"}, T_min=T_min, T_max=T_max
    )

    assert len(solutions) == 1


def test_no_solution_due_to_early_departure(time_limits):
    """Second flight departs too early; no solution possible."""
    T_min = time_limits["T_min"]
    T_max = time_limits["T_max"]

    data = [
        ["A", "B", 1, 3, 1],
        ["B", "A", 2, 4, 1],
    ]

    flights_df = flights_df_from_list(data)
    solutions = run_dijkstra(
        flights_df, start_city="A", required_cities={"B"}, T_min=T_min, T_max=T_max
    )

    assert len(solutions) == 0


def test_no_solution_due_to_invalid_times(time_limits):
    """Second flight departs before arrival; no solution possible."""
    T_min = time_limits["T_min"]
    T_max = time_limits["T_max"]

    data = [
        ["A", "B", 2, 3, 1],
        ["B", "A", 1, 4, 1],
    ]

    flights_df = flights_df_from_list(data)
    solutions = run_dijkstra(
        flights_df, start_city="A", required_cities={"B"}, T_min=T_min, T_max=T_max
    )

    assert len(solutions) == 0
