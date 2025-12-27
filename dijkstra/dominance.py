from .labels import Label
from typing import List


def dominates(l1: Label, l2: Label) -> bool:
    """
    Check whether one label dominates the other.
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
    Keep only Pareto-optimal labels using the 'dominates' function.
    """
    pareto = []

    for label in labels:
        dominated = False
        to_remove = []

        for p in pareto:
            if dominates(p, label):
                # Existing label dominates this one -> discard it
                dominated = True
                break
            if dominates(label, p):
                # This label dominates existing one ->
                # mark existing for removal
                to_remove.append(p)

        if dominated:
            continue

        # Remove dominated labels
        for r in to_remove:
            pareto.remove(r)

        pareto.append(label)

    return pareto
