"""The Spark windowing logic, run on a static table shaped like Kafka records (no broker needed)."""

import json

import pytest

pytest.importorskip("pyspark")
from streaming.spark_stream import build_spark, windowed_counts  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    session = build_spark("windowing-tests", with_kafka=False, with_lakehouse=False)
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def kafka_rows(spark, events):
    return spark.createDataFrame([(json.dumps(e).encode(),) for e in events], ["value"])


def event(event_id, ts, zone, fare=10.0, produced=1000):
    return {"event_id": event_id, "tpep_pickup_datetime": ts, "PULocationID": zone,
            "fare_amount": fare, "trip_distance": 2.0, "passenger_count": 1, "produced_at_ms": produced}


def test_counts_trips_per_zone_per_5_minute_window(spark):
    events = [
        event("a", "2023-01-02T08:00:10", 132),
        event("b", "2023-01-02T08:04:59", 132),
        event("c", "2023-01-02T08:05:00", 132),   # next window
        event("d", "2023-01-02T08:01:00", 48),
    ]
    out = {(r.window_start, r.zone): r.trips for r in windowed_counts(kafka_rows(spark, events)).collect()}
    assert out == {("2023-01-02 08:00:00", 132): 2, ("2023-01-02 08:05:00", 132): 1,
                   ("2023-01-02 08:00:00", 48): 1}


def test_duplicate_messages_are_counted_once(spark):
    events = [event("a", "2023-01-02T08:00:10", 132)] * 3 + [event("b", "2023-01-02T08:00:20", 132)]
    rows = windowed_counts(kafka_rows(spark, events)).collect()
    assert [(r.zone, r.trips) for r in rows] == [(132, 2)]


def test_malformed_messages_are_dropped(spark):
    good = event("a", "2023-01-02T08:00:10", 132)
    bad_time = event("b", "not-a-time", 132)
    no_zone = {k: v for k, v in event("c", "2023-01-02T08:00:10", 132).items() if k != "PULocationID"}
    df = spark.createDataFrame([(json.dumps(good).encode(),), (json.dumps(bad_time).encode(),),
                                (json.dumps(no_zone).encode(),), (b"{not json",)], ["value"])
    rows = windowed_counts(df).collect()
    assert [(r.zone, r.trips) for r in rows] == [(132, 1)]


def test_latency_inputs_and_fare_are_aggregated(spark):
    events = [event("a", "2023-01-02T08:00:10", 132, fare=10.0, produced=1000),
              event("b", "2023-01-02T08:02:00", 132, fare=20.0, produced=5000)]
    row = windowed_counts(kafka_rows(spark, events)).collect()[0]
    assert row.avg_fare == 15.0 and row.last_produced_ms == 5000
