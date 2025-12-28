import pandas as pd
import pytest

from dijkstra.prune import (
    extract_countries,
    extract_airports,
    build_reachable_airports,
    prune_flights_df,
    prune_flights,
)


# -------------------------
# Fixtures
# -------------------------

@pytest.fixture
def airports_df():
    return pd.DataFrame(
        [
            ["JFK", "USA"],
            ["ABC", "USA"],
            ["DEF", "USA"],
            ["PAP", "Poland"],
            ["IEZ", "Poland"],
            ["IEB", "Poland"],
            ["ALL", "Poland"],
            ["AAA", "Kazakhstan"],
            ["BBB", "Kazakhstan"],
            ["CCC", "Kazakhstan"],
        ],
        columns=["airport", "country"],
    )


@pytest.fixture
def flights_df():
    return pd.DataFrame(
        [
            ["JFK", "ABC"],
            ["ABC", "DEF"],
            ["DEF", "PAP"],
            ["PAP", "IEZ"],
            ["IEZ", "PAP"],
            ["PAP", "IEB"],
            ["IEB", "BBB"],
            ["ALL", "PAP"],
            ["AAA", "BBB"],
            ["BBB", "CCC"],
            ["CCC", "JFK"],
            ["IEZ", "JFK"],
        ],
        columns=["departure_airport", "arrival_airport"],
    )


# -------------------------
# Extraction tests
# -------------------------

def test_extract_countries(airports_df):
    countries = extract_countries("JFK", {"ABC", "PAP"}, airports_df)
    assert countries == {"USA", "Poland"}


def test_extract_airports(airports_df):
    airports = extract_airports({"USA", "Poland"}, airports_df)
    assert airports == {"JFK", "ABC", "DEF", "PAP", "IEZ", "IEB", "ALL"}


# -------------------------
# Reachability tests
# -------------------------

@pytest.mark.parametrize(
    "sources,max_dist,expected",
    [
        (
            {"PAP", "CCC"},
            1,
            {"PAP", "CCC", "DEF", "IEZ", "IEB", "ALL", "BBB", "JFK"},
        ),
        (
            {"PAP"},
            2,
            {"JFK", "ABC", "DEF", "PAP", "IEZ", "IEB", "ALL", "BBB"},
        ),
        (
            {"AAA"},
            3,
            {"JFK", "PAP", "IEB", "AAA", "BBB", "CCC"},
        ),
    ],
)
def test_build_reachable_airports(
    flights_df,
    sources,
    max_dist,
    expected,
):
    reachable = build_reachable_airports(flights_df, sources, max_dist)
    assert reachable == expected


# -------------------------
# prune_flights_df tests
# -------------------------

@pytest.mark.parametrize(
    "sources,max_dist,expected_rows",
    [
        (
            {"PAP", "CCC"},
            1,
            [
                ["DEF", "PAP"],
                ["PAP", "IEZ"],
                ["IEZ", "PAP"],
                ["PAP", "IEB"],
                ["IEB", "BBB"],
                ["ALL", "PAP"],
                ["BBB", "CCC"],
                ["CCC", "JFK"],
                ["IEZ", "JFK"],
            ],
        ),
        (
            {"PAP"},
            2,
            [
                ["JFK", "ABC"],
                ["ABC", "DEF"],
                ["DEF", "PAP"],
                ["PAP", "IEZ"],
                ["IEZ", "PAP"],
                ["PAP", "IEB"],
                ["IEB", "BBB"],
                ["ALL", "PAP"],
                ["IEZ", "JFK"],
            ],
        ),
        (
            {"AAA"},
            3,
            [
                ["PAP", "IEB"],
                ["IEB", "BBB"],
                ["AAA", "BBB"],
                ["BBB", "CCC"],
                ["CCC", "JFK"],
            ],
        ),
    ],
)
def test_prune_flights_df(
    flights_df,
    sources,
    max_dist,
    expected_rows,
):
    reachable = build_reachable_airports(flights_df, sources, max_dist)
    result = prune_flights_df(flights_df, reachable)

    expected = pd.DataFrame(
        expected_rows,
        columns=["departure_airport", "arrival_airport"],
    )

    pd.testing.assert_frame_equal(
        result.reset_index(drop=True),
        expected.reset_index(drop=True),
    )


# -------------------------
# Integration tests
# -------------------------

@pytest.mark.parametrize(
    "start,required,max_dist,expected_airports",
    [
        (
            "AAA",
            {"JFK"},
            1,
            {"AAA", "BBB", "CCC", "JFK", "ABC", "DEF", "PAP", "IEZ", "IEB"},
        ),
        (
            "JFK",
            {"ABC"},
            2,
            {"BBB", "CCC", "JFK", "ABC", "DEF", "PAP", "IEZ", "IEB", "ALL"},
        ),
    ],
)
def test_prune_flights(
    flights_df,
    airports_df,
    start,
    required,
    max_dist,
    expected_airports,
):
    pruned_df = prune_flights(
        flights_df,
        airports_df,
        start,
        required,
        max_dist,
    )

    airports = (
        set(pruned_df["departure_airport"])
        | set(pruned_df["arrival_airport"])
    )

    assert airports == expected_airports
