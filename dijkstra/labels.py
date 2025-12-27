from dataclasses import dataclass
from typing import Set, Optional
import pandas as pd


@dataclass(frozen=True)
class Label:
    city: str
    time: float
    visited: Set[str]
    cost: float
    prev: Optional["Label"] = None
    flight: Optional[pd.Series] = None
