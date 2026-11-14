import json
from datetime import datetime, timedelta

import pytest

from streaming.processor import process_batch, write_dashboard_snapshots
from streaming.store import Store

T0 = datetime(2023, 1, 2, 8, 0)


def window(i):
    return (T0 + timedelta(minutes=5 * i)).strftime("%Y-%m-%d %H:%M:%S")


def rows_for(i, counts):
    return [{"window_start": window(i), "zone": zone, "trips": trips, "avg_fare": 15.0,
             "last_produced_ms": 1_000} for zone, trips in counts.items()]


@pytest.fixture
def store(tmp_path):
    s = Store(tmp_path / "stream.sqlite")
    yield s
    s.close()


def no_llm(spike, label):  # explain_fn stand-in; never called because Gemini is not configured
    raise AssertionError("LLM should not be called in tests")


def test_warm_up_then_detects_a_spike(store, monkeypatch):
    monkeypatch.setenv("GEMINI_API_KEY", "")
    for i in range(4):  # 4 quiet windows: 132 has 10 trips, 48 has 6
        process_batch(store, rows_for(i, {132: 10, 48: 6}), batch_id=i, explain_fn=no_llm)
    result = process_batch(store, rows_for(4, {132: 45, 48: 6}), batch_id=4, explain_fn=no_llm)
    assert result["anomalies"] == 1
    zone, trips, explanation = store.conn.execute("SELECT zone, trips, explanation FROM anomalies").fetchone()
    assert (zone, trips) == (132, 45)
    assert explanation.startswith("JFK Airport (Queens): 45 pickups")  # factual fallback when Gemini is off


def test_missing_windows_count_as_zero_in_history(store):
    process_batch(store, rows_for(0, {132: 10, 7: 8}), batch_id=0, explain_fn=no_llm)
    process_batch(store, rows_for(1, {132: 10}), batch_id=1, explain_fn=no_llm)  # zone 7 silent
    history = store.history(datetime.strptime(window(2), "%Y-%m-%d %H:%M:%S"), [7, 132])
    assert history == {7: [8, 0], 132: [10, 10]}


def test_replayed_batch_is_skipped_so_results_stay_exactly_once(store):
    rows = rows_for(0, {132: 10})
    assert process_batch(store, rows, batch_id=0, explain_fn=no_llm)["skipped"] is False
    assert process_batch(store, rows, batch_id=0, explain_fn=no_llm)["skipped"] is True
    assert store.conn.execute("SELECT COUNT(*), SUM(trips) FROM zone_windows").fetchone() == (1, 10)


def test_crash_mid_batch_leaves_nothing_behind(store):
    rows = rows_for(0, {132: 10})

    class Boom(Exception):
        pass

    def crash(*_):
        raise Boom()

    store.upsert_windows = crash  # simulate a failure while writing
    with pytest.raises(Boom):
        process_batch(store, rows, batch_id=0, explain_fn=no_llm)
    assert store.conn.execute("SELECT COUNT(*) FROM batches").fetchone() == (0,)
    del store.upsert_windows       # restore the real method, then Spark's replay succeeds
    assert process_batch(store, rows, batch_id=0, explain_fn=no_llm)["skipped"] is False


def test_windows_in_one_batch_use_earlier_ones_as_history(store, monkeypatch):
    monkeypatch.setenv("GEMINI_API_KEY", "")
    rows = [r for i in range(4) for r in rows_for(i, {132: 10})] + rows_for(4, {132: 45})
    result = process_batch(store, rows, batch_id=0, explain_fn=no_llm)
    assert result["windows"] == 5 and result["anomalies"] == 1


def test_dashboard_snapshots_keep_the_v1_format(store, tmp_path, monkeypatch):
    monkeypatch.setenv("GEMINI_API_KEY", "")
    for i in range(4):
        process_batch(store, rows_for(i, {132: 10, 48: 6}), batch_id=i, explain_fn=no_llm)
    process_batch(store, rows_for(4, {132: 45, 48: 6}), batch_id=4, explain_fn=no_llm)
    write_dashboard_snapshots(store, tmp_path)
    metrics = json.loads((tmp_path / "metrics.json").read_text())
    anomalies = json.loads((tmp_path / "anomalies.json").read_text())
    assert metrics["total_records_processed"] == 4 * 16 + 51
    assert metrics["anomalies_detected"] == 1
    assert metrics["top_zones"][0] == {"zone": "132", "trips": 45}
    assert metrics["last_window_time"] == "2023-01-02 08:20 - 08:25"
    assert set(anomalies[0]) >= {"zone_id", "window", "observed_count", "baseline_mean", "explanation"}
