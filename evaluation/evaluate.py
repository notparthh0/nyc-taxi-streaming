"""
Score a streaming run.

    python evaluation/evaluate.py

Reads what the run produced (data/stream.sqlite, data/injected_spikes.json,
data/stream_progress.jsonl, data/producer_run.json) and reports:

  * Detection, scored against the injected spikes as labels: recall, precision
    and F1. Precision counts every alert on a non-injected window as a false
    alarm. Some of those may be real demand surges, so precision is a strict lower bound.
  * Both detectors on the same stream, recomputed offline from the stored window
    counts with the same code: the seasonal profile (default) vs the v1
    rolling-hour rule (the baseline), at several thresholds.
  * Latency: time from the last trip of a window being produced to the window's
    result being written (p50/p95/max). This includes waiting for the watermark.
  * Throughput and late data, from Spark's own progress reports.
"""

import json
import statistics
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from streaming.lakehouse import warehouse_path  # noqa: E402
from streaming.seasonal import RollingDetector, SeasonalDetector, iceberg_source  # noqa: E402
from streaming.store import TS_FORMAT, Store  # noqa: E402

DATA_DIR = ROOT / "data"


def percentile(values, q):
    values = sorted(values)
    if not values:
        return None
    index = (len(values) - 1) * q
    lo, hi = int(index), min(int(index) + 1, len(values) - 1)
    return values[lo] + (values[hi] - values[lo]) * (index - lo)


def offline_alerts(store: Store, detector):
    """Re-run a detector over every stored window, oldest first."""
    alerts = set()
    windows = [w for (w,) in store.conn.execute("SELECT DISTINCT window_start FROM zone_windows ORDER BY 1")]
    for window_start in windows:
        current = dict(store.conn.execute(
            "SELECT zone, trips FROM zone_windows WHERE window_start = ?", (window_start,)).fetchall())
        for spike in detector.detect(store, datetime.strptime(window_start, TS_FORMAT), current):
            alerts.add((spike.zone, window_start))
    return alerts


def score(alerts, truth):
    caught = alerts & truth
    recall = len(caught) / len(truth) if truth else None
    precision = len(caught) / len(alerts) if alerts else None
    f1 = 2 * precision * recall / (precision + recall) if precision and recall else 0.0
    return {"injected": len(truth), "caught": len(caught), "alerts": len(alerts),
            "recall": round(recall, 3) if recall is not None else None,
            "precision_strict": round(precision, 3) if precision is not None else None,
            "f1": round(f1, 3)}


def main() -> int:
    store = Store(DATA_DIR / "stream.sqlite")
    truth_file = DATA_DIR / "injected_spikes.json"
    truth = {(t["zone"], t["window_start"]) for t in json.loads(truth_file.read_text())} if truth_file.exists() else set()

    live = {(zone, ws) for zone, ws in store.conn.execute("SELECT zone, window_start FROM anomalies")}
    windows, zone_rows, trips = store.conn.execute(
        "SELECT COUNT(DISTINCT window_start), COUNT(*), COALESCE(SUM(trips), 0) FROM zone_windows").fetchone()
    latencies = [ms for (ms,) in store.conn.execute(
        "SELECT processed_at_ms - last_produced_ms FROM zone_windows WHERE last_produced_ms IS NOT NULL")]

    progress = []
    progress_file = DATA_DIR / "stream_progress.jsonl"
    if progress_file.exists():
        progress = [json.loads(line) for line in progress_file.read_text().splitlines() if line.strip()]
    busy = [p for p in progress if (p.get("input_rows") or 0) > 0]
    producer_file = DATA_DIR / "producer_run.json"
    producer = json.loads(producer_file.read_text()) if producer_file.exists() else {}

    source = iceberg_source(warehouse_path())
    sweep = {}
    for k in (2.0, 2.5, 3.0):
        sweep[f"rolling_hour (baseline) k={k}"] = score(offline_alerts(store, RollingDetector(k, 5)), truth)
    for k in (2.0, 3.0, 4.0):
        sweep[f"seasonal_4w k={k}"] = score(offline_alerts(store, SeasonalDetector(source, k, 5)), truth)

    report = {
        "stream": {"windows": windows, "zone_windows": zone_rows, "trips_counted": trips},
        "labelled_zone_windows": zone_rows,
        "detection_live": score(live, truth),
        "detection_compare_offline": sweep,
        "latency_ms": {
            "p50": round(percentile(latencies, 0.5)) if latencies else None,
            "p95": round(percentile(latencies, 0.95)) if latencies else None,
            "max": max(latencies) if latencies else None,
        },
        "spark": {
            "micro_batches_with_data": len(busy),
            "mean_processed_rows_per_s": round(statistics.mean(p["processed_rows_per_s"] for p in busy), 1) if busy else None,
            "p95_trigger_ms": round(percentile([p["trigger_ms"] for p in busy if p.get("trigger_ms")], 0.95) or 0),
            "rows_dropped_by_watermark": sum(p.get("rows_dropped_by_watermark") or 0 for p in progress),
        },
        "producer": producer,
    }
    (DATA_DIR / "evaluation.json").write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))
    store.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
