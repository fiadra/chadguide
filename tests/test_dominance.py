import pytest
from dijkstra.dominance import dominates, pareto_filter
from dijkstra.labels import Label


def test_dominates_strictly_better_time_and_cost():
    l1 = Label(city="A", time=5, cost=100, visited={"B"})
    l2 = Label(city="A", time=6, cost=120, visited={"B"})
    assert dominates(l1, l2) is True


def test_dominates_different_visited_false():
    l1 = Label(city="A", time=5, cost=100, visited={"B"})
    l2 = Label(city="A", time=6, cost=120, visited={"C"})
    assert dominates(l1, l2) is False


def test_dominates_worse_time_false():
    l1 = Label(city="A", time=7, cost=100, visited={"B"})
    l2 = Label(city="A", time=6, cost=120, visited={"B"})
    assert dominates(l1, l2) is False


def test_dominates_equal_labels_false():
    l1 = Label(city="A", time=5, cost=100, visited={"B"})
    l2 = Label(city="A", time=5, cost=100, visited={"B"})
    assert dominates(l1, l2) is False


def test_dominates_better_cost_only():
    l1 = Label(city="A", time=5, cost=90, visited={"B"})
    l2 = Label(city="A", time=5, cost=100, visited={"B"})
    assert dominates(l1, l2) is True


def test_pareto_filter_removes_dominated():
    labels = [
        Label("A", 5, {"B"}, 100),
        Label("A", 6, {"B"}, 120),  # dominated
        Label("A", 4, {"B"}, 130),  # tradeoff
    ]

    result = pareto_filter(labels)

    assert len(result) == 2
    assert {label.cost for label in result} == {100, 130}


def test_pareto_filter_keeps_tradeoffs():
    labels = [
        Label("A", 5, {"B"}, 100),
        Label("A", 4, {"B"}, 130),
        Label("A", 6, {"B"}, 90),
    ]

    result = pareto_filter(labels)

    assert len(result) == 3


def test_pareto_filter_order_independent():
    labels1 = [
        Label("A", 5, {"B"}, 100),
        Label("A", 6, {"B"}, 120),
    ]
    labels2 = list(reversed(labels1))

    r1 = pareto_filter(labels1)
    r2 = pareto_filter(labels2)

    assert len(r1) == len(r2)
    assert {label.cost for label in r1} == {label.cost for label in r2}


def test_pareto_filter_empty():
    assert pareto_filter([]) == []
