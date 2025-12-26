import heapq
from collections import defaultdict
from typing import List, Dict
from labels import Label
from dominance import dominates, pareto_filter
import pandas as pd


def build_flights_by_city(flights_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """
    Groups flights by departure airport.
    """
    return {city: group for city, group in flights_df.groupby("departure_airport")}


def create_new_label(
    label: Label, flight: pd.Series, required_cities: Dict[str, int]
) -> Label:
    """
    Generate a new label after taking a flight.
    """
    new_city: str = flight["arrival_airport"]
    new_time: float = flight["arr_time"]
    new_cost: float = label.cost + flight["price"]
    new_mask: int = label.visited_mask

    if new_city in required_cities:
        new_mask |= 1 << required_cities[new_city]

    return Label(city=new_city, time=new_time, visited_mask=new_mask, cost=new_cost)


def filter_feasible_flights(
    outgoing: pd.DataFrame, current_time: float, T_max: float
) -> pd.DataFrame:
    """
    Return only flights that depart after current time and arrive before T_max.
    """
    return outgoing[
        (outgoing["dep_time"] >= current_time) & (outgoing["arr_time"] <= T_max)
    ]


def dijkstra(
    flights_df: pd.DataFrame,
    start_city: str,
    required_cities: Dict[str, int],
    T_min: int,
    T_max: int,
) -> List[Label]:

    k = len(required_cities)
    ALL_VISITED = (1 << k) - 1

    flights_by_city = build_flights_by_city(flights_df)
    labels: Dict[tuple[str, int], List[Label]] = defaultdict(list)
    pq: List[tuple[float, float, Label]] = []

    start_label = Label(city=start_city, time=T_min, visited_mask=0, cost=0.0)
    labels[(start_city, 0)].append(start_label)
    heapq.heappush(pq, (0.0, T_min, start_label))

    solutions: List[Label] = []

    while pq:
        curr_cost, curr_time, label = heapq.heappop(pq)

        if label.time > T_max:
            continue

        if label.city == start_city and label.visited_mask == ALL_VISITED:
            solutions.append(label)
            continue

        if label.city not in flights_by_city:
            continue

        outgoing = flights_by_city[label.city]
        feasible_flights = filter_feasible_flights(outgoing, label.time, T_max)

        for _, flight in feasible_flights.iterrows():
            new_label = create_new_label(label, flight, required_cities)
            key = (new_label.city, new_label.visited_mask)

            dominated = False
            to_remove: List[Label] = []

            for existing in labels[key]:
                if dominates(existing, new_label):
                    dominated = True
                    break
                if dominates(new_label, existing):
                    to_remove.append(existing)

            if dominated:
                continue

            for r in to_remove:
                labels[key].remove(r)

            labels[key].append(new_label)
            heapq.heappush(pq, (new_label.cost, new_label.time, new_label))

    return pareto_filter(solutions)
