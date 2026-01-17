from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, ConfigDict
from datetime import datetime
from typing import Set, List, Optional

from src.flight_router.application import FindOptimalRoutes
from src.flight_router.schemas.routes import RouteSegment, RouteResult

DEMO_DB = Path("src/flight_router/examples/data/demo_flights.db")
router = FlightRouter(db_path=DEMO_DB)

app = FastAPI(title="Flight Routing API")


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
    total_time: float  # Captures @property
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
