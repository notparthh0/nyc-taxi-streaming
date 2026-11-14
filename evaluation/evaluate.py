"""
Score a streaming run.

    python evaluation/evaluate.py

Reads what the run produced (data/stream.sqlite, data/injected_spikes.json,
data/stream_progress.jsonl, data/producer_run.json) and reports:

  * Detection: how many injected spikes were caught (recall), how many alerts were
    raised in total, and the share of alerts that were injected spikes. Alerts on
    windows we did not inject are not automatically false: real demand surges
    exist in the data, so that share is a LOWER bound on precision.
  * The same scores for other thresholds (k, min_trips), recomputed offline from
    the stored window counts with the same detector code.
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

from streaming.detector import detect_spikes  # noqa: E402
from streaming.store import TS_FORMAT, Store  # noqa: E402

DATA_DIR = ROOT / "data"


def percentile(values, q):
    values = sorted(values)
    if not values:
        return None
    index = (len(values) - 1) * q
    lo, hi = int(index), min(int(index) + 1, len(values) - 1)
    return values[lo] + (values[hi] - values[lo]) * (index - lo)


def offline_alerts(store: Store, k: float, min_trips: int):
    """Re-run the detector over every stored window, oldest first."""
    alerts = set()
    windows = [w for (w,) in store.conn.execute("SELECT DISTINCT window_start FROM zone_windows ORDER BY 1")]
    for window_start in windows:
        current = dict(store.conn.execute(
            "SELECT zone, trips FROM zone_windows WHERE window_start = ?", (window_start,)).fetchall())
        history = store.history(datetime.strptime(window_start, TS_FORMAT), current.keys())
        for spike in detect_spikes(current, history, k=k, min_trips=min_trips):
            alerts.add((spike.zone, window_start))
    return alerts


def score(alerts, truth):
    caught = alerts & truth
    return {
        "injected": len(truth),
        "caught": len(caught),
        "recall": round(len(caught) / len(truth), 3) if truth else None,
        "alerts": len(alerts),
        "precision_lower_bound": round(len(caught) / len(alerts), 3) if alerts else None,
    }


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

    sweep = {f"k={k}, min_trips={m}": score(offline_alerts(store, k, m), truth)
             for k in (2.0, 2.5, 3.0) for m in (1, 5)}

    report = {
        "stream": {"windows": windows, "zone_windows": zone_rows, "trips_counted": trips},
        "detection_live": score(live, truth),
        "detection_sweep_offline": sweep,
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
