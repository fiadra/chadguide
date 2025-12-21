"""
Parser module for converting Duffel API responses to database records.

This module provides functions for transforming raw flight offer data
from the Duffel API into structured records suitable for database storage.
"""

from datetime import datetime
from typing import Any, Dict, List, Tuple, TypedDict, Optional

from core.utils import safe_get


class StaticFlightRecord(TypedDict):
    """Type definition for static flight route data."""

    route_id: str
    carrier_code: Optional[str]
    carrier_name: Optional[str]
    flight_number: Optional[str]
    origin_iata: Optional[str]
    dest_iata: Optional[str]
    dest_city_code: Optional[str]
    duration_iso: Optional[str]
    origin_lat: Optional[float]
    origin_lon: Optional[float]
    dest_lat: Optional[float]
    dest_lon: Optional[float]
    aircraft_model: Optional[str]
    has_wifi: bool
    has_power: bool
    seat_pitch: Optional[str]
    legroom: Optional[str]
    co2_kg: float
    logo_url: Optional[str]
    is_non_stop: bool


class QuoteRecord(TypedDict):
    """Type definition for flight price quote data."""

    flight_static_id: str
    price_amount: float
    currency: Optional[str]
    fare_brand: Optional[str]
    baggage_checked: int
    baggage_carryon: int
    departure_date: Optional[str]
    scanned_at: str


def parse_offer_to_records(
    offer: Dict[str, Any]
) -> Tuple[StaticFlightRecord, QuoteRecord]:
    """
    Parse a raw flight offer into database records.

    Converts a single flight offer from the Duffel API response into two
    separate records: a static flight record containing route and aircraft
    information, and a quote record containing pricing and baggage details.

    Args:
        offer: A dictionary containing the raw offer data from the Duffel API.
            Expected to contain 'slices', 'passengers', and pricing information.

    Returns:
        A tuple containing:
            - StaticFlightRecord: Static route information including carrier,
              airports, aircraft, and amenities.
            - QuoteRecord: Dynamic pricing information including fare,
              baggage allowance, and scan timestamp.

    Note:
        This function assumes single-slice, single-segment offers (non-stop
        flights). Multi-segment offers are not fully supported.

    Example:
        >>> static_record, quote_record = parse_offer_to_records(offer_data)
        >>> print(f"Route: {static_record['route_id']}")
        >>> print(f"Price: {quote_record['price_amount']} {quote_record['currency']}")
    """
    # Extract first slice and segment (assumes non-stop flight)
    slice_data: Dict[str, Any] = offer['slices'][0]
    segment: Dict[str, Any] = slice_data['segments'][0]
    passenger: Dict[str, Any] = offer['passengers'][0]

    # Build unique route identifier
    carrier_code: Optional[str] = safe_get(segment, 'operating_carrier.iata_code')
    flight_number: Optional[str] = safe_get(segment, 'operating_carrier_flight_number')
    origin_iata: Optional[str] = safe_get(segment, 'origin.iata_code')
    dest_iata: Optional[str] = safe_get(segment, 'destination.iata_code')

    unique_route_id: str = f"{carrier_code}{flight_number}-{origin_iata}-{dest_iata}"

    # Build static flight record
    static_record: StaticFlightRecord = {
        "route_id": unique_route_id,
        "carrier_code": carrier_code,
        "carrier_name": safe_get(segment, 'operating_carrier.name'),
        "flight_number": flight_number,
        "origin_iata": origin_iata,
        "dest_iata": dest_iata,
        "dest_city_code": safe_get(segment, 'destination.iata_city_code'),
        "duration_iso": safe_get(segment, 'duration'),
        "origin_lat": safe_get(segment, 'origin.latitude'),
        "origin_lon": safe_get(segment, 'origin.longitude'),
        "dest_lat": safe_get(segment, 'destination.latitude'),
        "dest_lon": safe_get(segment, 'destination.longitude'),
        "aircraft_model": safe_get(segment, 'aircraft.name'),
        "has_wifi": safe_get(
            segment,
            'passengers.0.cabin.amenities.wifi.available',
            False
        ),
        "has_power": safe_get(
            segment,
            'passengers.0.cabin.amenities.power.available',
            False
        ),
        "seat_pitch": safe_get(segment, 'passengers.0.cabin.amenities.seat.pitch'),
        "legroom": safe_get(segment, 'passengers.0.cabin.amenities.seat.legroom'),
        "co2_kg": float(offer.get('total_emissions_kg') or 0),
        "logo_url": safe_get(segment, 'operating_carrier.logo_symbol_url'),
        "is_non_stop": len(segment.get('stops') or []) == 0
    }

    # Calculate baggage counts
    baggages: List[Dict[str, Any]] = safe_get(passenger, 'baggages') or []
    checked_bags: int = sum(1 for bag in baggages if bag.get('type') == 'checked')
    carry_on_bags: int = sum(1 for bag in baggages if bag.get('type') == 'carry_on')

    # Build quote record
    quote_record: QuoteRecord = {
        "flight_static_id": unique_route_id,
        "price_amount": float(offer.get('total_amount', 0)),
        "currency": offer.get('total_currency'),
        "fare_brand": safe_get(slice_data, 'fare_brand_name'),
        "baggage_checked": checked_bags,
        "baggage_carryon": carry_on_bags,
        "departure_date": safe_get(segment, 'departing_at'),
        "scanned_at": datetime.now().isoformat()
    }

    return static_record, quote_record
