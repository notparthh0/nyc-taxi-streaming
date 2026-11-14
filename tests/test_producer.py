from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pytest

from producer.taxi_producer import build_events, to_message

START = datetime(2023, 1, 2)


@pytest.fixture
def lake_file(tmp_path):
    rng = np.random.default_rng(0)
    n = 6000
    df = pd.DataFrame({
        "tpep_pickup_datetime": [START + timedelta(seconds=int(s)) for s in rng.integers(0, 6 * 3600, n)],
        "PULocationID": rng.choice([132, 161, 237, 48], n).astype("int32"),
        "fare_amount": rng.uniform(5, 50, n),
        "trip_distance": rng.uniform(0.5, 15, n),
        "passenger_count": rng.integers(1, 4, n).astype("int32"),
    })
    df.loc[0, "tpep_pickup_datetime"] = START - timedelta(days=1)   # outside the replay range
    path = tmp_path / "trips.parquet"
    df.to_parquet(path, index=False)
    return path


def test_events_are_in_time_order_with_unique_ids(lake_file):
    events, truth = build_events(lake_file, START, START + timedelta(hours=6), 0, 7, "2023-01")
    assert events["tpep_pickup_datetime"].is_monotonic_increasing
    assert events["event_id"].is_unique
    assert len(events) == 5999 and truth == []


def test_injected_spikes_are_real_extra_trips_inside_their_window(lake_file):
    end = START + timedelta(hours=6)
    base, _ = build_events(lake_file, START, end, 0, 7, "2023-01")
    events, truth = build_events(lake_file, START, end, 5, 7, "2023-01")
    assert len(truth) == 5
    assert len(events) == len(base) + sum(t["extra_trips"] for t in truth)
    for t in truth:
        window_start = datetime.strptime(t["window_start"], "%Y-%m-%d %H:%M:%S")
        assert window_start >= START + timedelta(hours=1)            # leaves an hour of history
        assert window_start + timedelta(minutes=5) <= end - timedelta(minutes=15)
        injected = events[events["event_id"].str.startswith("inj-")]
        in_window = injected[(injected["PULocationID"] == t["zone"])
                             & (injected["tpep_pickup_datetime"] >= window_start)
                             & (injected["tpep_pickup_datetime"] < window_start + timedelta(minutes=5))]
        assert len(in_window) >= t["extra_trips"]


def test_injection_is_reproducible_with_the_same_seed(lake_file):
    end = START + timedelta(hours=6)
    _, a = build_events(lake_file, START, end, 5, 7, "2023-01")
    _, b = build_events(lake_file, START, end, 5, 7, "2023-01")
    assert a == b


def test_message_shape(lake_file):
    events, _ = build_events(lake_file, START, START + timedelta(hours=6), 0, 7, "2023-01")
    message = to_message(next(events.itertuples(index=False)), produced_at_ms=123)
    assert set(message) == {"event_id", "tpep_pickup_datetime", "PULocationID", "fare_amount",
                            "trip_distance", "passenger_count", "produced_at_ms"}
    assert message["produced_at_ms"] == 123 and isinstance(message["PULocationID"], int)
