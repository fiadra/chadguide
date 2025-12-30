import pytest

from src.dijkstra.dominance import dominates, pareto_filter
from src.dijkstra.labels import Label


# -------------------------
# dominates tests
# -------------------------

@pytest.mark.parametrize(
    "l1,l2,expected",
    [
        # strictly better time and cost
        (
            Label("A", 5, {"B"}, 100),
            Label("A", 6, {"B"}, 120),
            True,
        ),
        # different visited sets -> no dominance
        (
            Label("A", 5, {"B"}, 100),
            Label("A", 6, {"C"}, 120),
            False,
        ),
        # worse time -> no dominance
        (
            Label("A", 7, {"B"}, 100),
            Label("A", 6, {"B"}, 120),
            False,
        ),
        # equal labels -> no dominance
        (
            Label("A", 5, {"B"}, 100),
            Label("A", 5, {"B"}, 100),
            False,
        ),
        # better cost only
        (
            Label("A", 5, {"B"}, 90),
            Label("A", 5, {"B"}, 100),
            True,
        ),
    ],
)
def test_dominates(l1, l2, expected):
    assert dominates(l1, l2) is expected


# -------------------------
# pareto_filter tests
# -------------------------

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


@pytest.mark.parametrize(
    "labels",
    [
        [
            Label("A", 5, {"B"}, 100),
            Label("A", 6, {"B"}, 120),
        ],
        [
            Label("A", 6, {"B"}, 120),
            Label("A", 5, {"B"}, 100),
        ],
    ],
)
def test_pareto_filter_order_independent(labels):
    result = pareto_filter(labels)
    assert len(result) == 1
    assert result[0].cost == 100


def test_pareto_filter_empty():
    assert pareto_filter([]) == []
