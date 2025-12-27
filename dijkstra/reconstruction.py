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
