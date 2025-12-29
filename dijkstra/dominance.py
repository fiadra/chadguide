from collections import defaultdict
from typing import Dict, List, Tuple

from .labels import Label


def dominates(l1: Label, l2: Label) -> bool:
    """
    Check whether one label dominates the other.

    l1 dominates l2 if they have the same city and visited set,
    l1 is no worse in both time and cost, and strictly better in at least one.
    """
    return (
        l1.city == l2.city
        and l1.visited == l2.visited
        and l1.time <= l2.time
        and l1.cost <= l2.cost
        and (l1.time < l2.time or l1.cost < l2.cost)
    )


def pareto_filter(labels: List[Label]) -> List[Label]:
    """
    Keep only Pareto-optimal labels.

    Optimized O(n log n) algorithm using sorting.

    For labels with the same (city, visited) state, dominance reduces to
    a 2D Pareto front problem on (cost, time). We solve this by:
    1. Grouping labels by (city, frozenset(visited))
    2. Sorting each group by (cost, time) ascending
    3. Keeping labels with strictly decreasing time (Pareto-optimal)

    Args:
        labels: List of labels to filter.

    Returns:
        List of Pareto-optimal labels.
    """
    if not labels:
        return []

    # Group labels by (city, frozenset(visited))
    groups: Dict[Tuple[str, frozenset], List[Label]] = defaultdict(list)
    for label in labels:
        key = (label.city, frozenset(label.visited))
        groups[key].append(label)

    result: List[Label] = []

    for group in groups.values():
        # Sort by cost (primary), then time (secondary) - both ascending
        sorted_labels = sorted(group, key=lambda l: (l.cost, l.time))

        # Keep labels with strictly decreasing time
        # After sorting by cost, a label is Pareto-optimal iff
        # its time is less than all previously seen labels
        min_time = float("inf")
        for label in sorted_labels:
            if label.time < min_time:
                result.append(label)
                min_time = label.time

    return result
