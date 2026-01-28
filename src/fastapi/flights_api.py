import asyncio
import json
from pathlib import Path
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, ConfigDict
from datetime import datetime
from typing import Set, List, Optional
from dotenv import load_dotenv

from sse_starlette.sse import EventSourceResponse

# Load environment variables from .env file
load_dotenv()

from src.flight_router.application import FindOptimalRoutes
from src.flight_router.schemas.route import RouteSegment, RouteResult
from src.flight_router.adapters.validators.duffel_validator import DuffelOfferValidator
from src.flight_router.services.route_validation_service import RouteValidationService
import random

DEMO_DB = Path("src/flight_router/examples/data/demo_flights.db")
router = FindOptimalRoutes(db_path=DEMO_DB)

# Validation service (uses Duffel API)
duffel_validator = DuffelOfferValidator()
validation_service = RouteValidationService(duffel_validator)

app = FastAPI(title="Flight Routing API")

# Enable CORS for frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:3001", "http://127.0.0.1:3000", "http://127.0.0.1:3001"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Serve local attraction images
app.mount("/images", StaticFiles(directory="data/images"), name="images")


# --- Pydantic Schemas (The JSON Contract) ---
# We define these so the API includes the @property fields in the response.


class RouteSegmentSchema(BaseModel):
    model_config = ConfigDict(from_attributes=True)  # Allows reading from dataclasses

    segment_index: int
    departure_airport: str
    arrival_airport: str
    dep_time: float
    arr_time: float
    price: float
    duration: float  # This captures the @property
    carrier_code: Optional[str] = None
    carrier_name: Optional[str] = None


class RouteResultSchema(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    route_id: int
    segments: List[RouteSegmentSchema]
    total_cost: float  # Captures @property
    total_time: float  # Captures @property - elapsed time from first departure to last arrival
    total_flight_time: float  # Captures @property - sum of flight durations (time in air)
    trip_duration_days: float  # Captures @property - trip duration in days
    route_cities: List[str]  # Captures @property
    num_segments: int  # Captures @property


# --- API Endpoints ---


class SearchRequest(BaseModel):
    origin: str
    destinations: Set[str]
    departure_date: datetime
    return_date: Optional[datetime] = None


@app.post("/search", response_model=List[RouteResultSchema])
async def find_routes(request: SearchRequest):
    # Call your Dijkstra implementation
    # results will be a list of RouteResult dataclass objects
    results = router.search(
        origin=request.origin,
        destinations=request.destinations,
        departure_date=request.departure_date,
        return_date=request.return_date,
    )

    if not results:
        raise HTTPException(status_code=404, detail="No routes found")

    return results


@app.post("/search/stream")
async def find_routes_stream(request: SearchRequest):
    """
    SSE streaming endpoint with validation.

    Emits stage events during processing:
    - routing: Dijkstra search started
    - validating: Route validation started
    - complete: Final results (top 2 bookable routes)
    """
    return EventSourceResponse(search_with_validation(request))


async def search_with_validation(request: SearchRequest):
    """Generator that yields SSE events during search + validation."""

    # Stage 1: Routing
    yield {"event": "stage", "data": "routing"}

    # Run Dijkstra in executor (it's synchronous)
    loop = asyncio.get_event_loop()
    routes = await loop.run_in_executor(
        None,
        lambda: router.search(
            origin=request.origin,
            destinations=request.destinations,
            departure_date=request.departure_date,
            return_date=request.return_date,
        )
    )

    if not routes:
        yield {
            "event": "complete",
            "data": json.dumps({"routes": [], "error": "No routes found"})
        }
        return

    # Stage 2: Validating
    yield {"event": "stage", "data": "validating"}

    # Validate routes
    departure_date = request.departure_date.date()
    validated = await validation_service.validate_routes(
        routes,
        departure_date,
        validate_top_n=5,  # Only validate top 5 to save time
    )

    # Filter: bookable routes, sorted by price, top 2
    bookable = [v for v in validated if v.is_bookable]
    bookable.sort(key=lambda v: v.total_price)
    top_routes = bookable[:2]

    # Fallback: if no bookable routes, return top 2 unvalidated routes
    # This ensures users see results even if Duffel API doesn't have matching flights
    if not top_routes:
        top_routes = validated[:2]

    # Convert to schema-compatible dicts (just the route, no validation details)
    results = []
    for v in top_routes:
        route = v.route
        results.append({
            "route_id": route.route_id,
            "segments": [
                {
                    "segment_index": s.segment_index,
                    "departure_airport": s.departure_airport,
                    "arrival_airport": s.arrival_airport,
                    "dep_time": s.dep_time,
                    "arr_time": s.arr_time,
                    "price": s.price,
                    "duration": s.duration,
                    "carrier_code": s.carrier_code,
                    "carrier_name": s.carrier_name,
                }
                for s in route.segments
            ],
            "total_cost": v.total_price,  # Use validated price
            "total_time": route.total_time,
            "total_flight_time": route.total_flight_time,
            "trip_duration_days": route.trip_duration_days,
            "route_cities": list(route.route_cities),
            "num_segments": route.num_segments,
        })

    # Stage 3: Complete
    yield {"event": "complete", "data": json.dumps({"routes": results})}


# Load pre-fetched attractions data (no API calls at runtime)
ATTRACTIONS_FILE = Path("data/attractions.json")
_attractions_cache: dict = None


def _load_attractions() -> dict:
    """Load attractions from JSON file (cached in memory)."""
    global _attractions_cache
    if _attractions_cache is None:
        if ATTRACTIONS_FILE.exists():
            with open(ATTRACTIONS_FILE, "r", encoding="utf-8") as f:
                _attractions_cache = json.load(f)
        else:
            _attractions_cache = {}
    return _attractions_cache


@app.get("/api/attractions")
async def get_attractions(cities: str, limit: int = 5):
    """
    Get tourist attractions for the specified cities.

    Data is pre-fetched and stored in a JSON file.
    No external API calls are made at runtime.

    Args:
        cities: Comma-separated list of city names (e.g., "Barcelona,Madrid")
        limit: Max attractions per city (default 5)

    Returns:
        Dict mapping city names to lists of attractions.
    """
    city_list = [c.strip() for c in cities.split(",") if c.strip()]

    if not city_list:
        return {}

    all_attractions = _load_attractions()
    result = {}

    for city in city_list:
        city_data = all_attractions.get(city, [])

        if city_data:
            # Randomly select up to `limit` attractions
            selected = random.sample(city_data, min(limit, len(city_data)))
            result[city] = selected
        else:
            result[city] = []

    return result
