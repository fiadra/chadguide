import heapq
from collections import defaultdict
from typing import List, Dict, Set
from labels import Label
from dominance import dominates, pareto_filter
import pandas as pd


def build_flights_by_city(flights_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """
    Groups flights by departure airport.
    """
    return {city: group for city, group in flights_df.groupby("departure_airport")}


def create_new_label(label: Label, flight: pd.Series, required_cities: Set[str]) -> Label:
    """
    Generate a new label after taking a flight.
    """
    new_city: str = flight["arrival_airport"]
    new_time: float = flight["arr_time"]
    new_cost: float = label.cost + flight["price"]
    new_visited = set(label.visited)

    if new_city in required_cities:
        new_visited.add(new_city)

    return Label(
        city=new_city,
        time=new_time,
        visited=new_visited,
        cost=new_cost,
        prev=label,
        flight=flight,
    )


def filter_feasible_flights(
    outgoing: pd.DataFrame, current_time: float, T_max: float
) -> pd.DataFrame:
    """
    Return only flights that depart after current time and arrive before T_max.
    """
    return outgoing[
        (outgoing["dep_time"] >= current_time) & (outgoing["arr_time"] <= T_max)
    ]


def try_insert_label(
    labels_at_state: list[Label],
    new_label: Label,
) -> bool:
    """
    Try to insert 'new_label' into a list of labels for the same state.

    Returns True if the label should be kept (and inserted),
    False if it is dominated and should be discarded.
    """
    to_remove = []

    for existing in labels_at_state:
        if dominates(existing, new_label):
            return False  # new label is dominated
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
    T_min: int,
    T_max: int,
) -> List[Label]:
    """
    Multi-criteria Dijkstra algorithm for finding Pareto-optimal routes.

    Args:
        flights_df: DataFrame with flights (columns: departure_airport, arrival_airport,
                                            dep_time, arr_time, price)
        start_city: starting airport code
        required_cities: set of airport codes to visit
        T_min: earliest start time (float)
        T_max: latest end time (float)

    Returns:
        List of Pareto-optimal Label objects representing complete routes.
    """
    flights_by_city = build_flights_by_city(flights_df)
    labels: Dict[tuple[str, int], List[Label]] = defaultdict(list)
    pq: List[tuple[float, float, Label]] = []

    start_label = Label(city=start_city, time=T_min, visited=set(), cost=0.0)
    labels[(start_city, 0)].append(start_label)
    heapq.heappush(pq, (0.0, T_min, start_label))

    solutions: List[Label] = []

    while pq:
        curr_cost, curr_time, label = heapq.heappop(pq)

        if label.time > T_max:
            continue

        if label.city == start_city and label.visited == required_cities:
            solutions.append(label)
            continue

        if label.city not in flights_by_city:
            continue

        outgoing = flights_by_city[label.city]
        feasible_flights = filter_feasible_flights(outgoing, label.time, T_max)

        for _, flight in feasible_flights.iterrows():
            new_label = create_new_label(label, flight, required_cities)
            key = (new_label.city, frozenset(new_label.visited))

            if try_insert_label(labels[key], new_label):
                heapq.heappush(pq, (new_label.cost, new_label.time, new_label))

    return pareto_filter(solutions)
