from dataclasses import dataclass


@dataclass(frozen=True)
class Label:
    city: str
    time: float
    visited_mask: int
    cost: float
