"""
What happens to each Spark micro-batch of finished 5-minute windows.

Kept free of Spark so it can be unit-tested with plain lists of dicts:
spark_stream.py converts each batch to rows and calls process_batch().
"""

import json
import os
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable, Dict, List

from streaming.explain import _get_client, explain, fallback
from streaming.seasonal import RollingDetector
from streaming.store import TS_FORMAT, WINDOW, Store, now_ms

ANOMALY_FEED_CAP = 100


def window_label(window_start: str) -> str:
    start = datetime.strptime(window_start, TS_FORMAT)
    return f"{start:%Y-%m-%d %H:%M} - {start + WINDOW:%H:%M}"


def process_batch(store: Store, rows: List[dict], batch_id: int, k: float = 2.0, min_trips: int = 5,
                  max_llm_calls: int = 3, explain_fn: Callable = explain, detector=None) -> Dict:
    """Detect spikes and persist one micro-batch exactly once (see store.py).
    detector: RollingDetector (default, the v1 rule) or SeasonalDetector (streaming/seasonal.py)."""
    if store.batch_done(batch_id):
        return {"batch_id": batch_id, "skipped": True}
    detector = detector or RollingDetector(k=k, min_trips=min_trips)

    by_window = defaultdict(list)
    for row in rows:
        by_window[row["window_start"]].append(row)

    llm_enabled = _get_client() is not None
    llm_calls, spikes_total, processed_at = 0, 0, now_ms()
    with store.transaction():
        for window_start in sorted(by_window):  # oldest first: later windows see earlier ones as history
            window_rows = by_window[window_start]
            current = {r["zone"]: r["trips"] for r in window_rows}
            spikes = detector.detect(store, datetime.strptime(window_start, TS_FORMAT), current)

            store.upsert_windows(window_rows, batch_id, processed_at)
            label = window_label(window_start)
            explanations = {}
            for spike in spikes:
                if llm_enabled and llm_calls < max_llm_calls:
                    explanations[spike.zone] = explain_fn(spike, label)
                    llm_calls += 1
                else:
                    explanations[spike.zone] = fallback(spike, label)
            store.upsert_anomalies(window_start, spikes, explanations, detector=detector.name)
            spikes_total += len(spikes)
        store.mark_batch(batch_id, len(by_window), len(rows), spikes_total)

    return {"batch_id": batch_id, "skipped": False, "windows": len(by_window),
            "zone_rows": len(rows), "anomalies": spikes_total, "llm_calls": llm_calls}


def _atomic_json(path: Path, data) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, indent=2))
    os.replace(tmp, path)


def write_dashboard_snapshots(store: Store, data_dir: Path) -> None:
    """Write metrics.json / anomalies.json in the format dashboard/app.py already reads."""
    data_dir = Path(data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)
    conn = store.conn

    total_trips, = conn.execute("SELECT COALESCE(SUM(trips), 0) FROM zone_windows").fetchone()
    anomaly_count, = conn.execute("SELECT COUNT(*) FROM anomalies").fetchone()
    last_window, = conn.execute("SELECT MAX(window_start) FROM zone_windows").fetchone()
    top_zones = []
    if last_window:
        top_zones = [{"zone": str(zone), "trips": trips} for zone, trips in conn.execute(
            "SELECT zone, trips FROM zone_windows WHERE window_start = ? ORDER BY trips DESC LIMIT 10",
            (last_window,))]
    _atomic_json(data_dir / "metrics.json", {
        "total_records_processed": int(total_trips),
        "anomalies_detected": int(anomaly_count),
        "last_window_time": window_label(last_window) if last_window else "Waiting for first window...",
        "top_zones": top_zones,
    })

    feed = []
    for window_start, zone, trips, mean, std, text, detected_ms in conn.execute(
            "SELECT window_start, zone, trips, baseline_mean, baseline_std, explanation, detected_at_ms "
            "FROM anomalies ORDER BY window_start DESC, trips DESC LIMIT ?", (ANOMALY_FEED_CAP,)):
        feed.append({"zone_id": zone, "window": window_label(window_start), "observed_count": trips,
                     "baseline_mean": mean, "baseline_std": std, "explanation": text,
                     "detected_at": datetime.fromtimestamp(detected_ms / 1000).strftime("%H:%M:%S")})
    _atomic_json(data_dir / "anomalies.json", feed)
