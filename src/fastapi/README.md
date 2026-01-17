# Flight Routing API Bridge

This project provides a high-performance FastAPI bridge for a Dijkstra-based flight routing algorithm. It enables a web frontend to request optimized multi-city itineraries, handling data validation, asynchronous processing, and the serialization of complex Python dataclasses.

---

## 1. System Architecture

The API acts as an intermediary layer that translates web-standard JSON into the strict types required by the Dijkstra implementation.

* **FastAPI:** Handles the HTTP layer and request routing.
* **Pydantic:** Validates incoming flight parameters and serializes algorithm output.
* **Routing Engine:** A Dijkstra-based implementation that processes flight graphs to find optimal paths.
* **Poetry:** Manages the environment and ensures dependency consistency across development and production.

---

## 2. Core Data Models

### Algorithm Output (Dataclasses)
The core logic uses `RouteSegment` and `RouteResult` dataclasses. These are `frozen=True` to ensure immutability during calculation.

### API Schema (Pydantic)
To expose the `@property` methods (like `total_cost` and `duration`) to the website, we use Pydantic models with `from_attributes=True`. This allows the API to "read" properties from the dataclasses and include them in the final JSON response.

---

## 3. API Reference

### Search Flight Routes
This endpoint is the primary interface for the Dijkstra routing engine. It calculates the most efficient paths based on cost and time constraints.

**Endpoint:** `POST /search`

**Request Body (JSON):**
| Field | Type | Description | Example |
| :--- | :--- | :--- | :--- |
| `origin` | `string` | IATA code of the starting city. | `"WAW"` |
| `destinations` | `array[string]` | Set of cities to be visited. | `["LHR", "CDG"]` |
| `departure_date` | `string (ISO)` | The date of departure. | `"2026-07-13T00:00:00"` |
| `return_date` | `string (ISO)` | Optional return date. | `"2026-07-19T00:00:00"` |

**Response Example (200 OK):**
The response returns an array of `RouteResult` objects. Note that computed properties like `total_cost` are included automatically.

```json
[
  {
    "route_id": 42,
    "total_cost": 299.99,
    "total_time": 320.0,
    "route_cities": ["WAW", "LHR"],
    "num_segments": 1,
    "segments": [
      {
        "segment_index": 0,
        "departure_airport": "WAW",
        "arrival_airport": "LHR",
        "dep_time": 1720857600.0,
        "arr_time": 1720868400.0,
        "price": 299.99,
        "duration": 180.0,
        "carrier_code": "LO",
        "carrier_name": "LOT Polish Airlines"
      }
    ]
  }
]
```

---

## 4. Deployment

### Production Stack
To ensure high availability and performance, the application should be deployed using a multi-layered stack.

1.  **Nginx:** Acts as a reverse proxy and handles SSL termination.
2.  **Gunicorn:** Manages the worker processes to ensure the app stays alive and handles concurrent requests.
3.  **Uvicorn Workers:** High-performance ASGI workers that execute the FastAPI code.

### Deployment Commands

**Using Poetry (Bare Metal/VPS):**
```bash
# Start Gunicorn with 4 Uvicorn workers
poetry run gunicorn -w 4 -k uvicorn.workers.UvicornWorker main:app --bind 0.0.0.0:8000
