import json
import os
import time
import pandas as pd
from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "nyc-taxi-stream")
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
PARQUET_PATH = os.getenv("PARQUET_PATH")
if not PARQUET_PATH:
    raise RuntimeError("PARQUET_PATH is not set. Copy .env.example to .env and set your parquet file path.")
SLEEP_SECONDS = float(os.getenv("PRODUCER_SLEEP_SECONDS", "0.05"))


def load_and_clean_data(parquet_path: str) -> pd.DataFrame:
    print(f"Loading parquet file from: {parquet_path}...")
    columns = [
        "tpep_pickup_datetime",
        "PULocationID",
        "fare_amount",
        "trip_distance",
        "passenger_count",
    ]
    df = pd.read_parquet(parquet_path, columns=columns)

    # Filter out invalid rows
    df = df.dropna(subset=columns)
    df = df[
        (df["fare_amount"] > 0)
        & (df["trip_distance"] > 0)
        & (df["passenger_count"] >= 1)
    ]

    # Ensure datetime type and sort chronologically
    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df = df.sort_values(by="tpep_pickup_datetime").reset_index(drop=True)

    # Filter out corrupted timestamps — enforce strict January 2023 date range
    df = df[df["tpep_pickup_datetime"] >= "2023-01-01"]
    df = df[df["tpep_pickup_datetime"] < "2023-02-01"]

    print(f"Loaded {len(df):,} valid trips. Ready to stream.")
    return df


def run_producer():
    df = load_and_clean_data(PARQUET_PATH)

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    print(f"Streaming to Kafka topic '{KAFKA_TOPIC}' on {BOOTSTRAP_SERVERS}...")
    sent_count = 0

    try:
        for row in df.itertuples(index=False):
            message = {
                "tpep_pickup_datetime": row.tpep_pickup_datetime.isoformat(),
                "PULocationID": int(row.PULocationID),
                "fare_amount": float(row.fare_amount),
                "trip_distance": float(row.trip_distance),
                "passenger_count": int(row.passenger_count),
            }
            producer.send(KAFKA_TOPIC, value=message)
            sent_count += 1

            if sent_count % 100 == 0:
                print(
                    f"[Producer] Sent {sent_count:,} records | Current timestamp: {message['tpep_pickup_datetime']}"
                )

            time.sleep(SLEEP_SECONDS)

    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        producer.flush()
        producer.close()
        print(f"Producer closed. Total messages sent: {sent_count:,}")


if __name__ == "__main__":
    run_producer()
