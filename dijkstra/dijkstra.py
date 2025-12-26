from labels import Label
from dominance import dominates, pareto_filter
import heapq
from collections import defaultdict


def dijkstra(flights_df, start_city, required_cities, T_min, T_max):
    k = len(required_cities)
    ALL_VISITED = (1 << k) - 1

    flights_by_city = {
        city: group for city, group in flights_df.groupby("departure_airport")
    }

    labels = defaultdict(list)
    pq = []

    start_label = Label(city=start_city, time=T_min, visited_mask=0, cost=0.0)

    labels[(start_city, 0)].append(start_label)
    heapq.heappush(pq, (0.0, T_min, start_label))

    # Collect all feasible complete solutions
    solutions = []

    while pq:
        curr_cost, curr_time, label = heapq.heappop(pq)

        # Hard time cutoff
        if label.time > T_max:
            continue

        # Goal state
        if (
            label.city == start_city
            and label.visited_mask == ALL_VISITED
            and label.time <= T_max
        ):
            solutions.append(label)
            continue

        if label.city not in flights_by_city:
            continue

        outgoing = flights_by_city[label.city]

        feasible_flights = outgoing[
            (outgoing["dep_time"] >= label.time) & (outgoing["arr_time"] <= T_max)
        ]

        for _, flight in feasible_flights.iterrows():
            new_city = flight["arrival_airport"]
            new_time = flight["arr_time"]
            new_cost = label.cost + flight["price"]
            new_mask = label.visited_mask

            if new_city in required_cities:
                new_mask |= 1 << required_cities[new_city]

            new_label = Label(
                city=new_city, time=new_time, visited_mask=new_mask, cost=new_cost
            )

            key = (new_city, new_mask)

            dominated = False
            to_remove = []

            for existing in labels[key]:
                if dominates(existing, new_label):
                    dominated = True
                    break
                if dominates(new_label, existing):
                    to_remove.append(existing)

            if dominated:
                continue

            for r in to_remove:
                labels[key].remove(r)

            labels[key].append(new_label)
            heapq.heappush(pq, (new_cost, new_time, new_label))

    # Keep only Pareto-optimal complete solutions
    return pareto_filter(solutions)
