from dijkstra import dijkstra
from load_flights import load_flights
from datetime import datetime, timedelta
from reconstruction import reconstruct_path

flights_df = load_flights()

epoch = datetime(1970, 1, 1)
T_min = (datetime.now() - epoch).total_seconds() / 60
T_max = (datetime.now() + timedelta(days=7) - epoch).total_seconds() / 60

solutions = dijkstra(
    flights_df,
    start_city="JFK",
    required_cities={"LAX", "ATL"},
    T_min=T_min,
    T_max=T_max,
)

for i, sol in enumerate(solutions, 1):
    path, flights = reconstruct_path(sol)

    print(f"\nSolution {i}")
    print(f"Total cost: {sol.cost}")
    print("Route:")

    for f in flights:
        print(
            f"{f['departure_airport']} -> {f['arrival_airport']} "
            f"({f['dep_time']} → {f['arr_time']}, ${f['price']})"
        )
