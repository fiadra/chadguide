from dataclasses import dataclass, field
from typing import Set, Optional
import pandas as pd


@dataclass(frozen=True, eq=False)
class Label:
    """
    Represents a state in the Dijkstra search space.

    Each Label tracks:
    - Current position (city)
    - Arrival time at that position
    - Set of visited cities
    - Total cost accumulated
    - Chain back to previous label (for path reconstruction)
    - The flight that led to this state

    Note: eq=False is used to prevent auto-generated __eq__ from comparing
    pd.Series fields, which causes "ambiguous truth value" errors.
    """
    city: str
    time: float
    visited: Set[str]
    cost: float
    prev: Optional["Label"] = None
    flight: Optional[pd.Series] = field(default=None, compare=False)

    def __eq__(self, other: object) -> bool:
        """Identity-based equality for heap operations."""
        return self is other

    def __hash__(self) -> int:
        """Identity-based hash for consistent behavior with __eq__."""
        return id(self)

    def __lt__(self, other: "Label") -> bool:
        """
        Comparison for heapq tiebreaking.

        When (cost, time) are equal, heapq tries to compare Labels directly.
        This method provides a consistent (but arbitrary) ordering to prevent
        TypeError. The actual ordering is meaningless since the algorithm
        only cares about cost and time.
        """
        # Compare by id() for consistent arbitrary ordering
        return id(self) < id(other)
