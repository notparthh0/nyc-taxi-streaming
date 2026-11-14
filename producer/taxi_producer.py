"""
Replay cleaned NYC taxi trips into Kafka in pickup-time order.

    python producer/taxi_producer.py --month 2023-01 --days 1 --rate 1000
    python producer/taxi_producer.py --month 2023-01 --days 1 --rate 1000 --inject-spikes 20

Source: the batch pipeline's cleaned lake
(../nyc-taxi-aws/data/lake/trips_cleaned/year=YYYY/month=MM/), so the cleaning
rules live in one place. --source FILE.parquet replays any Parquet file instead.

--inject-spikes N adds N known demand spikes (extra trips in one zone for one
5-minute window) and saves them to data/injected_spikes.json. That file is the
ground truth for measuring how many spikes the stream detector catches.

Each message: key = pickup zone (so one zone's trips stay in order on one
partition), value = JSON with a stable event_id (for de-duplication) and
produced_at_ms (for end-to-end latency).
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
DEFAULT_LAKE_DIR = ROOT.parent / "nyc-taxi-aws" / "data" / "lake" / "trips_cleaned"
COLUMNS = ["tpep_pickup_datetime", "PULocationID", "fare_amount", "trip_distance", "passenger_count"]
WINDOW = timedelta(minutes=5)
FLUSH_ZONE = 0  # pseudo-zone for the final watermark-advancing event; never a real TLC zone


def month_partition(lake_dir: Path, month: str) -> Path:
    year, mm = month.split("-")
    return Path(lake_dir) / f"year={year}" / f"month={mm}"


def load_trips(source: Path, start: datetime, end: datetime) -> pd.DataFrame:
    df = pd.read_parquet(source, columns=COLUMNS)
    df = df.dropna(subset=COLUMNS)
    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df = df[(df["tpep_pickup_datetime"] >= start) & (df["tpep_pickup_datetime"] < end)]
    df = df.sort_values(["tpep_pickup_datetime", "PULocationID", "fare_amount"], kind="stable")
    return df.reset_index(drop=True)


def inject_spikes(df: pd.DataFrame, n: int, start: datetime, end: datetime, seed: int = 7):
    """Add n known spikes. Returns (new_df, truth). Spikes avoid the first hour (the detector
    needs history) and the last 15 minutes (those windows may not close before the replay ends)."""
    if n <= 0 or df.empty:
        return df, []
    rng = np.random.default_rng(seed)
    slots = int((end - start) / WINDOW)
    per_zone = df.groupby("PULocationID").size() / slots    # average trips per 5-minute window
    zones = per_zone[per_zone >= 1.0]                       # zones with at least ~1 trip per window
    first, last = start + timedelta(hours=1), end - timedelta(minutes=15)
    candidate_windows = pd.date_range(first, last - WINDOW, freq="5min")
    if zones.empty or len(candidate_windows) == 0:
        return df, []

    chosen, truth, extra_rows = set(), [], []
    while len(truth) < n and len(chosen) < len(zones) * len(candidate_windows):
        zone = int(rng.choice(zones.index.to_numpy()))
        window_start = pd.Timestamp(rng.choice(candidate_windows)).to_pydatetime()
        if (zone, window_start) in chosen:
            continue
        chosen.add((zone, window_start))
        extra = max(10, int(round(3 * zones[zone])))
        donors = df[df["PULocationID"] == zone].sample(extra, replace=True, random_state=int(rng.integers(1 << 31)))
        offsets = rng.uniform(0, WINDOW.total_seconds(), size=extra)
        spike = donors.copy()
        spike["tpep_pickup_datetime"] = [window_start + timedelta(seconds=float(s)) for s in offsets]
        spike["event_id"] = [f"inj-{len(truth)}-{j}" for j in range(extra)]
        extra_rows.append(spike)
        truth.append({"zone": zone, "window_start": window_start.strftime("%Y-%m-%d %H:%M:%S"),
                      "extra_trips": extra, "zone_mean_per_window": round(float(zones[zone]), 2)})
    out = pd.concat([df, *extra_rows], ignore_index=True)
    out = out.sort_values("tpep_pickup_datetime", kind="stable").reset_index(drop=True)
    return out, truth


def to_message(row, produced_at_ms: int) -> dict:
    return {
        "event_id": row.event_id,
        "tpep_pickup_datetime": row.tpep_pickup_datetime.strftime("%Y-%m-%dT%H:%M:%S"),
        "PULocationID": int(row.PULocationID),
        "fare_amount": float(row.fare_amount),
        "trip_distance": float(row.trip_distance),
        "passenger_count": int(row.passenger_count),
        "produced_at_ms": produced_at_ms,
    }


def build_events(source: Path, start: datetime, end: datetime, n_spikes: int, seed: int, id_prefix: str):
    df = load_trips(source, start, end)
    df["event_id"] = [f"{id_prefix}-{i}" for i in range(len(df))]
    return inject_spikes(df, n_spikes, start, end, seed)


def send(events: pd.DataFrame, bootstrap: str, topic: str, rate: float, flush_event: bool = True) -> dict:
    from kafka import KafkaProducer

    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        acks="all",
        linger_ms=5,
        key_serializer=lambda k: str(k).encode(),
        value_serializer=lambda v: json.dumps(v).encode(),
    )
    t0 = time.time()
    sent = 0
    try:
        for row in events.itertuples(index=False):
            if rate > 0 and sent % 50 == 0:
                ahead = t0 + sent / rate - time.time()
                if ahead > 0:
                    time.sleep(ahead)
            producer.send(topic, key=int(row.PULocationID), value=to_message(row, int(time.time() * 1000)))
            sent += 1
            if sent % 20000 == 0:
                print(f"[producer] {sent:,} sent | event time {row.tpep_pickup_datetime}", flush=True)
        if flush_event and len(events):
            # One pseudo-event 30 minutes after the last trip moves the event-time watermark
            # forward, so the final real windows close and get emitted.
            last = events["tpep_pickup_datetime"].max().to_pydatetime()
            producer.send(topic, key=FLUSH_ZONE, value={
                "event_id": f"flush-{last:%Y%m%d%H%M%S}",
                "tpep_pickup_datetime": (last + timedelta(minutes=30)).strftime("%Y-%m-%dT%H:%M:%S"),
                "PULocationID": FLUSH_ZONE, "fare_amount": 0.0, "trip_distance": 0.0,
                "passenger_count": 0, "produced_at_ms": int(time.time() * 1000)})
    finally:
        producer.flush()
        producer.close()
    seconds = time.time() - t0
    return {"sent": sent, "seconds": round(seconds, 2), "achieved_rate": round(sent / seconds, 1) if seconds else None}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--month", help="replay this month from the batch lake, e.g. 2023-01")
    parser.add_argument("--lake-dir", type=Path, default=Path(os.getenv("LAKE_DIR", DEFAULT_LAKE_DIR)))
    parser.add_argument("--source", type=Path, help="replay this Parquet file/folder instead of the lake")
    parser.add_argument("--start", help="first pickup to replay, e.g. 2023-01-02 (default: start of month)")
    parser.add_argument("--days", type=float, default=1.0, help="how many days of trips to replay")
    parser.add_argument("--rate", type=float, default=1000, help="events per second (0 = as fast as possible)")
    parser.add_argument("--inject-spikes", type=int, default=0)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "nyc-taxi-stream"))
    parser.add_argument("--dry-run", action="store_true", help="build the events and print stats only")
    args = parser.parse_args(argv)

    if args.source:
        source, id_prefix = args.source, args.source.stem
        default_start = None
    elif args.month:
        source, id_prefix = month_partition(args.lake_dir, args.month), args.month
        default_start = datetime.strptime(args.month + "-01", "%Y-%m-%d")
        if not source.exists():
            parser.error(f"{source} not found: run the batch pipeline for {args.month} first "
                         f"(cd ../nyc-taxi-aws && make month MONTH={args.month})")
    else:
        parser.error("pass --month (replay from the batch lake) or --source FILE")

    start = datetime.fromisoformat(args.start) if args.start else default_start
    if start is None:
        parser.error("--start is required with --source")
    end = start + timedelta(days=args.days)

    events, truth = build_events(source, start, end, args.inject_spikes, args.seed, id_prefix)
    print(f"{len(events):,} events from {start:%Y-%m-%d %H:%M} to {end:%Y-%m-%d %H:%M} "
          f"({len(truth)} injected spikes) -> topic {args.topic}", flush=True)

    DATA_DIR.mkdir(exist_ok=True)
    (DATA_DIR / "injected_spikes.json").write_text(json.dumps(truth, indent=2))
    if args.dry_run:
        return 0

    stats = send(events, args.bootstrap, args.topic, args.rate)
    stats.update({"events": len(events), "injected_spikes": len(truth), "target_rate": args.rate,
                  "start": str(start), "end": str(end)})
    (DATA_DIR / "producer_run.json").write_text(json.dumps(stats, indent=2))
    print(f"[producer] done: {json.dumps(stats)}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
