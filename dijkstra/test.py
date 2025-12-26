from dijkstra import dijkstra
from load_flights import load_flights
from datetime import datetime, timedelta

flights_df = load_flights()

epoch = datetime(1970, 1, 1)
T_min = (datetime.now() - epoch).total_seconds() / 60
T_max = (datetime.now() + timedelta(days=7) - epoch).total_seconds() / 60

solutions = dijkstra(
    flights_df,
    start_city="JFK",
    required_cities={"LAX": 0, "ATL": 1, "LHR": 2, "CDG": 3, "FRA": 4},
    T_min=T_min,
    T_max=T_max,
)

for s in solutions:
    print(f"Cost={s.cost:.2f}, ReturnTime={s.time}")
