import sqlite3
import pandas as pd


def load_flights(db_path="dijkstra/flights.db"):
    conn = sqlite3.connect(db_path)

    df = pd.read_sql_query(
        """
        SELECT
            departure_airport,
            arrival_airport,
            scheduled_departure,
            scheduled_arrival,
            price
        FROM flights
        """,
        conn,
    )

    conn.close()

    # Convert times to pandas datetime
    df["scheduled_departure"] = pd.to_datetime(df["scheduled_departure"])
    df["scheduled_arrival"] = pd.to_datetime(df["scheduled_arrival"])

    # Convert times to numeric (minutes since epoch)
    epoch = pd.Timestamp("1970-01-01")
    df["dep_time"] = (df["scheduled_departure"] - epoch).dt.total_seconds() / 60
    df["arr_time"] = (df["scheduled_arrival"] - epoch).dt.total_seconds() / 60

    return df
