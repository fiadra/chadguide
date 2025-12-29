import random
import csv
import sqlite3
from datetime import datetime, timedelta

# -----------------------------
# Configuration
# -----------------------------
START_DATE = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
MONTHS = 3
FLIGHTS_PER_DAY_RANGE = (500, 1000)
random.seed(2137)

AIRPORTS = [
    ("ATL", "Atlanta"),
    ("LAX", "Los Angeles"),
    ("ORD", "Chicago"),
    ("DFW", "Dallas"),
    ("DEN", "Denver"),
    ("JFK", "New York"),
    ("SFO", "San Francisco"),
    ("SEA", "Seattle"),
    ("LAS", "Las Vegas"),
    ("MCO", "Orlando"),
    ("CLT", "Charlotte"),
    ("EWR", "Newark"),
    ("PHX", "Phoenix"),
    ("MIA", "Miami"),
    ("IAH", "Houston"),
    ("BOS", "Boston"),
    ("MSP", "Minneapolis"),
    ("FLL", "Fort Lauderdale"),
    ("DTW", "Detroit"),
    ("PHL", "Philadelphia"),
    ("LGA", "New York LaGuardia"),
    ("BWI", "Baltimore"),
    ("SAN", "San Diego"),
    ("TPA", "Tampa"),
    ("HNL", "Honolulu"),
    ("PDX", "Portland"),
    ("IAD", "Washington D.C. Dulles"),
    ("MDW", "Chicago Midway"),
    ("SLC", "Salt Lake City"),
    ("HOU", "Houston Hobby"),
    ("OAK", "Oakland"),
    ("RDU", "Raleigh-Durham"),
    ("STL", "St. Louis"),
    ("SMF", "Sacramento"),
    ("SAN", "San Diego"),
    ("MCI", "Kansas City"),
    ("CLE", "Cleveland"),
    ("PIT", "Pittsburgh"),
    ("SJC", "San Jose"),
    ("AUS", "Austin"),
    ("SAT", "San Antonio"),
    ("IND", "Indianapolis"),
    ("CMH", "Columbus"),
    ("RNO", "Reno"),
    ("BUF", "Buffalo"),
    ("ONT", "Ontario"),
    ("PDX", "Portland"),
    ("JAX", "Jacksonville"),
    ("BNA", "Nashville"),
    ("MEM", "Memphis"),
    ("OAK", "Oakland"),
    ("LHR", "London"),
    ("CDG", "Paris"),
    ("FRA", "Frankfurt"),
    ("MAD", "Madrid"),
    ("BCN", "Barcelona"),
    ("AMS", "Amsterdam"),
    ("FCO", "Rome"),
    ("MXP", "Milan"),
    ("DUB", "Dublin"),
    ("CPH", "Copenhagen"),
    ("OSL", "Oslo"),
    ("STO", "Stockholm"),
    ("HEL", "Helsinki"),
    ("VIE", "Vienna"),
    ("ZRH", "Zurich"),
    ("BRU", "Brussels"),
    ("ATH", "Athens"),
    ("LIS", "Lisbon"),
    ("PRG", "Prague"),
    ("WAW", "Warsaw"),
    ("MUC", "Munich"),
    ("TXL", "Berlin"),
    ("SXF", "Berlin-Schönefeld"),
    ("LGW", "London-Gatwick"),
    ("LGG", "Liège"),
    ("ORY", "Paris-Orly"),
    ("NCE", "Nice"),
    ("GVA", "Geneva"),
    ("BSL", "Basel"),
    ("STN", "London-Stansted"),
    ("EDI", "Edinburgh"),
    ("GLA", "Glasgow"),
    ("DUS", "Düsseldorf"),
    ("HAM", "Hamburg"),
    ("CGN", "Cologne"),
    ("FCO2", "Rome-Ciampino"),
    ("NAP", "Naples"),
    ("VCE", "Venice"),
    ("SVO", "Moscow-Sheremetyevo"),
    ("LED", "Saint Petersburg"),
    ("KBP", "Kyiv-Boryspil"),
    ("SOF", "Sofia"),
    ("RIX", "Riga"),
    ("TLL", "Tallinn"),
    ("VNO", "Vilnius"),
    ("BUD", "Budapest"),
]

AIRLINES = {
    "AA": "American Airlines",
    "DL": "Delta Air Lines",
    "UA": "United Airlines",
    "WN": "Southwest Airlines",
    "B6": "JetBlue",
}

AIRCRAFT = [
    ("A320", 2.5),
    ("A321", 3.0),
    ("B737", 2.5),
    ("B738", 2.7),
    ("B787", 5.5),
]

CSV_FILE = "flights.csv"
DB_FILE = "flights.db"


# -----------------------------
# Helper functions
# -----------------------------
def random_time():
    hour = random.randint(5, 23)
    minute = random.choice([0, 15, 30, 45])
    return timedelta(hours=hour, minutes=minute)


def random_flight_duration():
    aircraft, base_hours = random.choice(AIRCRAFT)
    duration = timedelta(hours=base_hours + random.uniform(-0.5, 0.8))
    return aircraft, duration


def random_airports():
    return random.sample(AIRPORTS, 2)


def random_price():
    """Generate a random flight price in USD"""
    return round(random.uniform(50, 1000), 2)


# -----------------------------
# Generate schedule
# -----------------------------
schedule = []
flight_id = 1
end_date = START_DATE + timedelta(days=MONTHS * 30)

current_date = START_DATE
while current_date < end_date:
    flights_today = random.randint(*FLIGHTS_PER_DAY_RANGE)

    for _ in range(flights_today):
        dep_airport, arr_airport = random_airports()
        airline_code, airline_name = random.choice(list(AIRLINES.items()))
        aircraft, duration = random_flight_duration()
        price = random_price()

        departure_time = current_date + random_time()
        arrival_time = departure_time + duration

        flight_number = f"{airline_code}{random.randint(100, 9999)}"

        schedule.append(
            (
                flight_id,
                flight_number,
                airline_name,
                dep_airport[0],
                arr_airport[0],
                departure_time.isoformat(),
                arrival_time.isoformat(),
                aircraft,
                price,
            )
        )

        flight_id += 1

    current_date += timedelta(days=1)

# -----------------------------
# Write CSV
# -----------------------------
with open(CSV_FILE, "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(
        [
            "id",
            "flight_number",
            "airline",
            "departure_airport",
            "arrival_airport",
            "scheduled_departure",
            "scheduled_arrival",
            "aircraft",
            "price",
        ]
    )
    writer.writerows(schedule)

# -----------------------------
# SQLite setup
# -----------------------------
conn = sqlite3.connect(DB_FILE)
cur = conn.cursor()

cur.execute(
    """
CREATE TABLE IF NOT EXISTS flights (
    id INTEGER PRIMARY KEY,
    flight_number TEXT,
    airline TEXT,
    departure_airport TEXT,
    arrival_airport TEXT,
    scheduled_departure TEXT,
    scheduled_arrival TEXT,
    aircraft TEXT,
    price REAL
);
"""
)

cur.execute(
    "CREATE INDEX IF NOT EXISTS idx_departure_airport ON flights(departure_airport);"
)
cur.execute(
    "CREATE INDEX IF NOT EXISTS idx_departure_time ON flights(scheduled_departure);"
)

# -----------------------------
# Insert data
# -----------------------------
cur.executemany(
    """
INSERT OR IGNORE INTO flights (
    id,
    flight_number,
    airline,
    departure_airport,
    arrival_airport,
    scheduled_departure,
    scheduled_arrival,
    aircraft,
    price
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
""",
    schedule,
)

conn.commit()
conn.close()

print(f"Generated {len(schedule)} flights")
print(f"Saved to {CSV_FILE} and SQLite database '{DB_FILE}'")
