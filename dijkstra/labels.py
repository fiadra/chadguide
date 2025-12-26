from dataclasses import dataclass
from typing import Set


@dataclass(frozen=True)
class Label:
    city: str
    time: float
    visited: Set[str]
    cost: float
