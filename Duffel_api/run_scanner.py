"""
Flight Scanner - Main Entry Point.

This script orchestrates the scanning of flight routes between European cities
using the Duffel API. It iterates through all city pairs, fetches flight offers
for a week, and stores the results in a SQLite database.

Usage:
    python run_scanner.py
"""

import itertools
import json
import logging
import sys
import time
from datetime import datetime, timedelta
from typing import List, Tuple

import requests

from core.api import fetch_flight_offers
from core.database import Database
from core.parser import parse_offer_to_records

# Module-level logger
logger = logging.getLogger(__name__)


def setup_logging() -> None:
    """
    Configure logging to output to both console and file.

    Sets up the root logger with INFO level, formatting with timestamps,
    and handlers for both stdout and a log file (scanner.log).
    """
    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"

    # Create formatter
    formatter = logging.Formatter(log_format, datefmt=date_format)

    # Get root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    # File handler
    file_handler = logging.FileHandler("Duffel_api/scanner.log", encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)


def load_cities() -> List[str]:
    """
    Load the list of European city IATA codes from configuration file.

    Returns:
        A list of IATA airport codes (e.g., ["WAW", "BCN", "CDG"]).

    Raises:
        SystemExit: If the cities configuration file is not found.
    """
    try:
        with open('Duffel_api/data/europe_cities.json', 'r', encoding='utf-8') as file:
            cities: List[str] = json.load(file)
            logger.info("Loaded %d cities from configuration", len(cities))
            return cities
    except FileNotFoundError:
        logger.critical("Cities file not found: data/europe_cities.json")
        sys.exit(1)


def get_week_dates(start_date_str: str) -> List[str]:
    """
    Generate a list of dates for a 7-day period starting from the given date.

    Args:
        start_date_str: The start date in ISO format (YYYY-MM-DD).

    Returns:
        A list of 7 date strings in ISO format, starting from the given date.

    Example:
        >>> get_week_dates("2024-07-15")
        ['2024-07-15', '2024-07-16', ..., '2024-07-21']
    """
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    return [
        (start_date + timedelta(days=i)).strftime("%Y-%m-%d")
        for i in range(7)
    ]


def scan_route_for_week(
    db: Database,
    origin: str,
    destination: str,
    week_dates: List[str]
) -> Tuple[int, bool]:
    """
    Scan a single route for all dates in the given week.

    Args:
        db: The database instance for storing results.
        origin: The IATA code of the departure airport.
        destination: The IATA code of the arrival airport.
        week_dates: List of dates to scan in ISO format.

    Returns:
        A tuple containing:
            - Total number of non-stop offers found for the route.
            - Boolean indicating if all days were scanned successfully.
    """
    total_offers: int = 0
    all_days_successful: bool = True
    consecutive_rate_limit_errors: int = 0

    for current_date in week_dates:
        try:
            offers = fetch_flight_offers(origin, destination, current_date)

            # Reset rate limit counter on success
            consecutive_rate_limit_errors = 0

            if offers:
                daily_offers: int = 0
                for offer in offers:
                    static_data, quote_data = parse_offer_to_records(offer)

                    if static_data['is_non_stop']:
                        db.save_route(static_data, current_date)
                        db.save_quote(quote_data)
                        daily_offers += 1

                if daily_offers > 0:
                    logger.info(
                        "   %s: +%d offers saved",
                        current_date, daily_offers
                    )
                    total_offers += daily_offers

            # Standard delay to respect API rate limits
            time.sleep(1)

        except requests.exceptions.HTTPError as error:
            status_code = error.response.status_code

            if status_code == 429:
                # Rate limit - implement exponential backoff
                consecutive_rate_limit_errors += 1
                wait_time = 5 * consecutive_rate_limit_errors

                logger.warning(
                    "   %s: Rate limit (429). Waiting %ds...",
                    current_date, wait_time
                )
                time.sleep(wait_time)

                all_days_successful = False

                # Abort route if too many consecutive rate limit errors
                if consecutive_rate_limit_errors >= 3:
                    logger.error(
                        "Too many rate limit errors. Aborting route %s -> %s",
                        origin, destination
                    )
                    break
            else:
                # Other HTTP errors (500, etc.)
                logger.error(
                    "   API error %d for %s: %s",
                    status_code, current_date, error
                )
                all_days_successful = False

        except KeyboardInterrupt:
            logger.info("Scan interrupted by user")
            raise

        except Exception as error:
            logger.exception(
                "   Unexpected error for %s: %s",
                current_date, error
            )
            all_days_successful = False

    return total_offers, all_days_successful


def main() -> None:
    """
    Main entry point for the flight scanner.

    Loads city configurations, iterates through all route pairs,
    and scans each route for a week of flight data.
    """
    setup_logging()
    logger.info("Flight Scanner starting")

    start_date: str = "2026-07-13"

    cities = load_cities()
    db = Database()

    route_pairs: List[Tuple[str, str]] = list(itertools.permutations(cities, 2))
    week_dates = get_week_dates(start_date)

    logger.info("Route pairs to scan: %d", len(route_pairs))
    logger.info(
        "Scan range: %s - %s (7 days)",
        week_dates[0], week_dates[-1]
    )

    try:
        for index, (origin, destination) in enumerate(route_pairs):
            # Skip already completed routes
            if db.is_route_fully_scanned(origin, destination):
                continue

            logger.info(
                "[%d/%d] Scanning week for: %s -> %s",
                index + 1, len(route_pairs), origin, destination
            )

            total_offers, all_successful = scan_route_for_week(
                db, origin, destination, week_dates
            )

            if all_successful:
                db.mark_route_completed(origin, destination, total_offers)
                logger.info(
                    "Completed %s -> %s. Total offers: %d",
                    origin, destination, total_offers
                )
            else:
                logger.warning(
                    "Skipped marking %s -> %s as completed (errors occurred)",
                    origin, destination
                )

    except KeyboardInterrupt:
        logger.info("Scanner stopped by user")
        sys.exit(0)

    finally:
        db.close()
        logger.info("Flight Scanner finished")


if __name__ == "__main__":
    main()
