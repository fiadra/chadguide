from typing import Set, Optional
import pandas as pd


def build_reachable_airports(
    flights_df: pd.DataFrame, sources: Set[str], max_dist: int = 2
) -> Set[str]:
    """
    Vectorized computation of airports reachable from sources within max_dist hops.
    """
    reachable = sources
    frontier = sources

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
    start_city: str,
    required_cities: Set[str],
    max_dist: Optional[int] = 2,
) -> pd.DataFrame:
    """
    Full pipeline: prune flights based on reachability from source airports.
    """
    sources = required_cities | {start_city}
    reachable = build_reachable_airports(flights_df, sources, max_dist)
    return prune_flights_df(flights_df, reachable)
