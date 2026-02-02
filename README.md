# ChadGuide

**Multi-city flight route optimizer that finds the cheapest way to visit multiple European cities in a single trip.**

![Python 3.10+](https://img.shields.io/badge/Python-3.10%2B-blue)
![FastAPI](https://img.shields.io/badge/FastAPI-0.128-009688)
![Tailwind CSS](https://img.shields.io/badge/Tailwind_CSS-3-38bdf8)

## What is this?

Planning a multi-city trip means checking every possible city order — a problem that grows exponentially. ChadGuide finds the optimal route and validates prices against live airline data.

## Features

- **Pareto-optimal routing** — custom Dijkstra finds the cheapest multi-city order considering both cost and travel time
- **Live price validation** — Duffel API confirms real-time availability and pricing
- **Real-time progress** — SSE streaming with an interactive loading map animation
- **Tourist attractions** — curated points of interest with images for each destination city
- **Compare & browse** — sort, filter, and compare routes with links to airline websites

## How it works

1. **Pick cities** — choose your origin and the destinations you want to visit
2. **We optimize** — the algorithm finds Pareto-optimal routes, then Duffel validates live prices
3. **Browse & compare** — view results with maps and timelines, then visit airline websites to book

## Prerequisites

- **Python 3.10+**
- **[Poetry](https://python-poetry.org/docs/#installation)** — dependency manager
- **[Duffel API token](https://duffel.com)** — free test account for flight validation
- **[Geoapify API key](https://www.geoapify.com)** *(optional)* — only needed if fetching new attraction data

## Quick Start

```bash
# Clone the repository
git clone <repo-url>
cd chadguide

# Install dependencies
poetry install

# Configure environment
cp .env.example .env
# Edit .env and add:
#   DUFFEL_ACCESS_TOKEN=duffel_test_...
#   GEOAPIFY_API_KEY=...         (optional)

# The flight database (SQLite, ~89k flights) is included in the repo.
# To refresh it with current data, run: poetry run python Duffel_api/run_scanner.py

# Start the backend
poetry run uvicorn src.fastapi.flights_api:app --reload --port 8000

# In a separate terminal — serve the frontend
cd web && python -m http.server 3000
```

Open [http://localhost:3000](http://localhost:3000) in your browser. API docs are available at [http://localhost:8000/docs](http://localhost:8000/docs).

## Architecture

```
Frontend (Vanilla JS + Tailwind CSS)
    │
    │  SSE stream (/search/stream)
    ▼
FastAPI Backend
    ├── Dijkstra Algorithm    — Pareto-optimal multi-city routing
    ├── Duffel Validator      — live price confirmation (async)
    ├── Flight Graph          — SQLite DB 
    └── Attractions API       — Geoapify data + local images
```

**Key design decisions:**

- **Hexagonal architecture** — ports & adapters pattern for testability
- **Schema-first** — Pandera-validated DataFrames, no silent data corruption
- **Event-driven UI** — loading animation driven by backend SSE stage signals

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Backend | Python 3.10+, FastAPI |
| Algorithm | Custom Pareto-optimal Dijkstra |
| Data processing | NumPy, Pandas, Pandera |
| Validation | Duffel API (httpx async, semaphore-controlled) |
| Frontend | Vanilla JS, Tailwind CSS 3, Font Awesome 6 |
| Maps | Leaflet.js 1.9 |
| Database | SQLite (flight offers, ~89k flights / 148 airports) |
| Streaming | Server-Sent Events (SSE-Starlette) |

## Project Structure

```
├── src/
│   ├── dijkstra/            # Core routing algorithm
│   ├── fastapi/             # API server + SSE streaming
│   └── flight_router/       # Domain logic (hexagonal architecture)
│       ├── application/     # Public API (FindOptimalRoutes)
│       ├── services/        # Route validation orchestration
│       ├── adapters/        # Dijkstra, Duffel validator, data providers
│       ├── ports/           # Abstract interfaces
│       └── schemas/         # Pandera models, dataclasses
├── web/                     # Frontend (HTML + JS, no build step)
│   ├── index.html           # Search form
│   ├── results.html         # Route results display
│   └── js/                  # Modules (city-selector, api-client, etc.)
├── data/                    # Attractions JSON + images (127 cities)
├── scripts/                 # Data collection (attractions, images)
├── tests/                   # Unit, integration, performance tests
└── Duffel_api/              # Flight database scanner + Streamlit dashboard
```

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/search` | Find all Pareto-optimal routes (no validation) |
| `POST` | `/search/stream` | Find routes + validate with SSE progress events |
| `GET` | `/api/attractions?cities=London,Paris` | Tourist attractions for given cities |

The `/search/stream` endpoint emits SSE events: `stage` (routing → validating) and `complete` (final results).

## Configuration

Create a `.env` file in the project root:

```bash
# Required — Duffel flight data API
# Get a free test token at https://duffel.com
DUFFEL_ACCESS_TOKEN=duffel_test_...

# Optional — Geoapify places API (for fetching new attractions)
# Get a free key at https://www.geoapify.com
GEOAPIFY_API_KEY=...
```

## Development

```bash
# Run all tests
poetry run pytest

# Run with coverage
poetry run pytest --cov=src/flight_router --cov-report=html

# Build/update flight database
poetry run python Duffel_api/run_scanner.py

# Fetch attraction data
poetry run python scripts/fetch_attractions.py

# Download attraction images
poetry run python scripts/fetch_attraction_images.py
```

## Limitations

- **Static flight data** — the demo database is a snapshot (~89k flights, 148 European airports), not a live feed. Use the scanner to refresh.
- **Duffel API test mode** — test mode has rate limits and prices may differ from those on airline websites. Treat displayed prices as estimates useful for comparing routes, not as exact booking quotes.
- **Algorithm runtime** — scales with the number of destinations. 3+ cities can take 15-30 seconds on the demo dataset.
- **Attraction images** — sourced from Google Images and may not persist indefinitely.

## Authors

**Paweł Kauf** · **Filip Rabiega**

University project — multi-city flight route optimization.
