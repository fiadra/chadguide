from .labels import Label
from typing import List, Tuple, Optional
import pandas as pd


def reconstruct_path(label: Label) -> Tuple[List[Label], List[pd.Series]]:
    """
    Reconstruct path (labels and flights) from a terminal label.

    Returns:
        path: ordered list of Labels from start to end
        flights: ordered list of flights taken
    """
    path: List[Label] = []
    flights: List[pd.Series] = []

    curr: Optional[Label] = label
    while curr is not None:
        path.append(curr)
        if curr.flight is not None:
            flights.append(curr.flight)
        curr = curr.prev

    path.reverse()
    flights.reverse()

    return path, flights


def print_sols(solutions: List[Label]):
    """
    Print all the solutions.

    Returns:
        Nothing.
    """
    for i, sol in enumerate(solutions, 1):
        path, flights = reconstruct_path(sol)

        print(f"\nSolution {i}")
        print(f"Total cost: {sol.cost}")
        print("Route:")

        for f in flights:
            print(
                f"{f['departure_airport']} -> {f['arrival_airport']} "
                f"({f['dep_time']} → {f['arr_time']}, ${f['price']})"
            )
