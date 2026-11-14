"""The Iceberg sink: MERGE from the SQLite results, and replaying a batch changes nothing."""

from datetime import datetime, timedelta

import pytest

pytest.importorskip("pyspark")
from streaming.lakehouse import SPIKES_TABLE, WINDOWS_TABLE, ensure_tables  # noqa: E402
from streaming.processor import process_batch  # noqa: E402
from streaming.spark_stream import build_spark, sync_to_iceberg  # noqa: E402
from streaming.store import Store  # noqa: E402

T0 = datetime(2023, 1, 2, 8, 0)


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    session = build_spark("lakehouse-tests", with_kafka=False, warehouse=tmp_path_factory.mktemp("warehouse"))
    session.sparkContext.setLogLevel("ERROR")
    ensure_tables(session)
    yield session
    session.stop()


def rows_for(i, counts):
    ws = (T0 + timedelta(minutes=5 * i)).strftime("%Y-%m-%d %H:%M:%S")
    return [{"window_start": ws, "zone": z, "trips": n, "avg_fare": 10.0, "last_produced_ms": 1} for z, n in counts.items()]


def test_merge_is_idempotent_so_replays_are_exactly_once(spark, tmp_path, monkeypatch):
    monkeypatch.setenv("GEMINI_API_KEY", "")
    store = Store(tmp_path / "s.sqlite")
    for i in range(5):
        rows = rows_for(i, {132: 45 if i == 4 else 10, 48: 6})
        process_batch(store, rows, batch_id=i)
        sync_to_iceberg(spark, store, sorted({r["window_start"] for r in rows}))
    # Spark replays the last batch after a crash: SQLite skips it, Iceberg MERGEs it again.
    replay = rows_for(4, {132: 45, 48: 6})
    assert process_batch(store, replay, batch_id=4)["skipped"] is True
    sync_to_iceberg(spark, store, [replay[0]["window_start"]])

    assert spark.table(WINDOWS_TABLE).count() == 10
    assert spark.sql(f"SELECT SUM(trips) FROM {WINDOWS_TABLE}").first()[0] == 4 * 16 + 51
    spikes = spark.table(SPIKES_TABLE).collect()
    assert [(r.zone, r.trips, r.detector) for r in spikes] == [(132, 45, "rolling_hour")]
