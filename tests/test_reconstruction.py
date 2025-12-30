"""
Tests for reconstruction module.

Tests path reconstruction from Label chains and solution printing.
"""

import io
import sys

import pandas as pd
import pytest

from src.dijkstra.labels import Label
from src.dijkstra.reconstruction import reconstruct_path, print_sols


# -------------------------
# Fixtures
# -------------------------


@pytest.fixture
def flight_a_to_b():
    """Create a sample flight from A to B."""
    return pd.Series({
        "departure_airport": "A",
        "arrival_airport": "B",
        "dep_time": 100,
        "arr_time": 200,
        "price": 50,
    })


@pytest.fixture
def flight_b_to_c():
    """Create a sample flight from B to C."""
    return pd.Series({
        "departure_airport": "B",
        "arrival_airport": "C",
        "dep_time": 250,
        "arr_time": 350,
        "price": 75,
    })


@pytest.fixture
def flight_c_to_a():
    """Create a sample flight from C to A."""
    return pd.Series({
        "departure_airport": "C",
        "arrival_airport": "A",
        "dep_time": 400,
        "arr_time": 500,
        "price": 60,
    })


@pytest.fixture
def single_label():
    """Create a single label (start state, no previous)."""
    return Label(
        city="A",
        time=0,
        visited=set(),
        cost=0,
        prev=None,
        flight=None,
    )


@pytest.fixture
def two_label_chain(flight_a_to_b):
    """Create a chain of 2 labels (A -> B)."""
    start = Label(
        city="A",
        time=0,
        visited=set(),
        cost=0,
        prev=None,
        flight=None,
    )
    end = Label(
        city="B",
        time=200,
        visited={"B"},
        cost=50,
        prev=start,
        flight=flight_a_to_b,
    )
    return end


@pytest.fixture
def three_label_chain(flight_a_to_b, flight_b_to_c):
    """Create a chain of 3 labels (A -> B -> C)."""
    start = Label(
        city="A",
        time=0,
        visited=set(),
        cost=0,
        prev=None,
        flight=None,
    )
    middle = Label(
        city="B",
        time=200,
        visited={"B"},
        cost=50,
        prev=start,
        flight=flight_a_to_b,
    )
    end = Label(
        city="C",
        time=350,
        visited={"B", "C"},
        cost=125,
        prev=middle,
        flight=flight_b_to_c,
    )
    return end


@pytest.fixture
def round_trip_chain(flight_a_to_b, flight_b_to_c, flight_c_to_a):
    """Create a round trip chain (A -> B -> C -> A)."""
    start = Label(
        city="A",
        time=0,
        visited=set(),
        cost=0,
        prev=None,
        flight=None,
    )
    after_b = Label(
        city="B",
        time=200,
        visited={"B"},
        cost=50,
        prev=start,
        flight=flight_a_to_b,
    )
    after_c = Label(
        city="C",
        time=350,
        visited={"B", "C"},
        cost=125,
        prev=after_b,
        flight=flight_b_to_c,
    )
    back_to_a = Label(
        city="A",
        time=500,
        visited={"B", "C"},
        cost=185,
        prev=after_c,
        flight=flight_c_to_a,
    )
    return back_to_a


# -------------------------
# reconstruct_path tests
# -------------------------


class TestReconstructPath:
    """Tests for reconstruct_path function."""

    def test_returns_tuple(self, single_label):
        """Test that reconstruct_path returns a tuple."""
        result = reconstruct_path(single_label)

        assert isinstance(result, tuple)
        assert len(result) == 2

    def test_single_label_path(self, single_label):
        """Test path reconstruction for single label."""
        path, flights = reconstruct_path(single_label)

        assert len(path) == 1
        assert path[0] == single_label
        assert len(flights) == 0

    def test_two_label_path(self, two_label_chain):
        """Test path reconstruction for two labels."""
        path, flights = reconstruct_path(two_label_chain)

        assert len(path) == 2
        assert path[0].city == "A"
        assert path[1].city == "B"
        assert len(flights) == 1

    def test_three_label_path(self, three_label_chain):
        """Test path reconstruction for three labels."""
        path, flights = reconstruct_path(three_label_chain)

        assert len(path) == 3
        assert [label.city for label in path] == ["A", "B", "C"]
        assert len(flights) == 2

    def test_round_trip_path(self, round_trip_chain):
        """Test path reconstruction for round trip."""
        path, flights = reconstruct_path(round_trip_chain)

        assert len(path) == 4
        assert [label.city for label in path] == ["A", "B", "C", "A"]
        assert len(flights) == 3

    def test_path_order_is_correct(self, three_label_chain):
        """Test that path is in forward order (start to end)."""
        path, flights = reconstruct_path(three_label_chain)

        # First label should have lowest time
        assert path[0].time < path[1].time < path[2].time

        # First label should have no prev
        assert path[0].prev is None

        # Last label should be the input label
        assert path[-1] == three_label_chain

    def test_flights_order_matches_path(self, three_label_chain, flight_a_to_b, flight_b_to_c):
        """Test that flights are in correct order."""
        path, flights = reconstruct_path(three_label_chain)

        assert flights[0]["departure_airport"] == "A"
        assert flights[0]["arrival_airport"] == "B"
        assert flights[1]["departure_airport"] == "B"
        assert flights[1]["arrival_airport"] == "C"

    def test_flights_contain_expected_data(self, two_label_chain):
        """Test that flight data is preserved."""
        path, flights = reconstruct_path(two_label_chain)

        flight = flights[0]
        assert flight["departure_airport"] == "A"
        assert flight["arrival_airport"] == "B"
        assert flight["dep_time"] == 100
        assert flight["arr_time"] == 200
        assert flight["price"] == 50


# -------------------------
# print_sols tests
# -------------------------


class TestPrintSols:
    """Tests for print_sols function."""

    def test_prints_solution_header(self, two_label_chain):
        """Test that solution header is printed."""
        captured = io.StringIO()
        sys.stdout = captured

        try:
            print_sols([two_label_chain])
        finally:
            sys.stdout = sys.__stdout__

        output = captured.getvalue()
        assert "Solution 1" in output

    def test_prints_total_cost(self, two_label_chain):
        """Test that total cost is printed."""
        captured = io.StringIO()
        sys.stdout = captured

        try:
            print_sols([two_label_chain])
        finally:
            sys.stdout = sys.__stdout__

        output = captured.getvalue()
        assert "Total cost: 50" in output

    def test_prints_route(self, two_label_chain):
        """Test that route label is printed."""
        captured = io.StringIO()
        sys.stdout = captured

        try:
            print_sols([two_label_chain])
        finally:
            sys.stdout = sys.__stdout__

        output = captured.getvalue()
        assert "Route:" in output

    def test_prints_flight_details(self, two_label_chain):
        """Test that flight details are printed."""
        captured = io.StringIO()
        sys.stdout = captured

        try:
            print_sols([two_label_chain])
        finally:
            sys.stdout = sys.__stdout__

        output = captured.getvalue()
        assert "A -> B" in output
        assert "100" in output  # dep_time
        assert "200" in output  # arr_time
        assert "$50" in output  # price

    def test_prints_multiple_solutions(self, two_label_chain, three_label_chain):
        """Test that multiple solutions are printed."""
        captured = io.StringIO()
        sys.stdout = captured

        try:
            print_sols([two_label_chain, three_label_chain])
        finally:
            sys.stdout = sys.__stdout__

        output = captured.getvalue()
        assert "Solution 1" in output
        assert "Solution 2" in output

    def test_empty_solutions_list(self):
        """Test that empty solutions list produces no output."""
        captured = io.StringIO()
        sys.stdout = captured

        try:
            print_sols([])
        finally:
            sys.stdout = sys.__stdout__

        output = captured.getvalue()
        assert output == ""

    def test_multiple_flights_in_solution(self, round_trip_chain):
        """Test that all flights in a solution are printed."""
        captured = io.StringIO()
        sys.stdout = captured

        try:
            print_sols([round_trip_chain])
        finally:
            sys.stdout = sys.__stdout__

        output = captured.getvalue()
        assert "A -> B" in output
        assert "B -> C" in output
        assert "C -> A" in output
