from typing import List, Set, Optional
import pandas as pd


def extract_countries(
    start_city: str, required_cities: Set[str], airports_df: pd.DataFrame
) -> Set[str]:
    """
    Return all countries in which given airports are located.
    """
    airports = required_cities | {start_city}

    return set(
        airports_df.loc[
            airports_df["airport"].isin(airports),
            "country",
        ]
    )


def extract_airports(
    countries: Set[str], airports_df: pd.DataFrame
) -> Set[str]:
    """
    Return all airports in given countries.
    """
    return set(
        airports_df.loc[
            airports_df["country"].isin(countries),
            "airport",
        ]
    )


def build_reachable_airports(
    flights_df: pd.DataFrame, sources: List[str], max_dist: int = 2
) -> Set[str]:
    """
    Vectorized computation of airports reachable from sources within max_dist hops.
    """
    reachable = set(sources)
    frontier = set(sources)

    for _ in range(max_dist):
        if not frontier:
            break

        # Select flights where either endpoint is in the frontier
        mask = (
            flights_df["departure_airport"].isin(frontier)
            | flights_df["arrival_airport"].isin(frontier)
        )
        neighbors = (
            set(flights_df.loc[mask, "departure_airport"])
            | set(flights_df.loc[mask, "arrival_airport"])
        )

        # Remove already visited airports
        neighbors -= reachable

        if not neighbors:
            break

        reachable.update(neighbors)
        frontier = neighbors  # move to the next layer

    return reachable


def prune_flights_df(
    flights_df: pd.DataFrame, reachable: Set[str]
) -> pd.DataFrame:
    """
    Keep only flights where both airports are in the reachable set.
    """
    return flights_df[
        flights_df["departure_airport"].isin(reachable)
        & flights_df["arrival_airport"].isin(reachable)
    ]


def prune_flights(
    flights_df: pd.DataFrame,
    airports_df: pd.DataFrame,
    start_city: str,
    required_cities: Set[str],
    max_dist: Optional[int] = 2,
) -> pd.DataFrame:
    """
    Full pipeline: prune flights based on reachability from source airports.
    """
    countries = extract_countries(start_city, required_cities, airports_df)
    sources = extract_airports(countries, airports_df)
    reachable = build_reachable_airports(flights_df, sources, max_dist)
    return prune_flights_df(flights_df, reachable)
