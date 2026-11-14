"""
SQLite results store for the Spark stream.

Why SQLite: the dashboard reads while the stream writes, and SQLite in WAL mode
allows that. Why it gives exactly-once results: each Spark micro-batch is written
in ONE transaction together with its batch_id. If Spark crashes and replays a
batch after restart, the batch_id is already recorded and the batch is skipped;
if it crashed mid-write, the transaction rolled back and the replay writes it once.
"""

import sqlite3
import time
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, List

from streaming.detector import HISTORY_WINDOWS, Spike

WINDOW = timedelta(minutes=5)
TS_FORMAT = "%Y-%m-%d %H:%M:%S"

SCHEMA = """
CREATE TABLE IF NOT EXISTS zone_windows (
    window_start      TEXT    NOT NULL,
    zone              INTEGER NOT NULL,
    trips             INTEGER NOT NULL,
    avg_fare          REAL,
    last_produced_ms  INTEGER,
    processed_at_ms   INTEGER NOT NULL,
    batch_id          INTEGER NOT NULL,
    PRIMARY KEY (window_start, zone)
);
CREATE TABLE IF NOT EXISTS anomalies (
    window_start   TEXT    NOT NULL,
    zone           INTEGER NOT NULL,
    trips          INTEGER NOT NULL,
    baseline_mean  REAL    NOT NULL,
    baseline_std   REAL    NOT NULL,
    threshold      REAL    NOT NULL,
    explanation    TEXT,
    detected_at_ms INTEGER NOT NULL,
    PRIMARY KEY (window_start, zone)
);
CREATE TABLE IF NOT EXISTS batches (
    batch_id      INTEGER PRIMARY KEY,
    windows       INTEGER NOT NULL,
    zone_rows     INTEGER NOT NULL,
    anomalies     INTEGER NOT NULL,
    committed_ms  INTEGER NOT NULL
);
"""


def now_ms() -> int:
    return int(time.time() * 1000)


def fmt(ts: datetime) -> str:
    return ts.strftime(TS_FORMAT)


class Store:
    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(self.path), isolation_level=None, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.executescript(SCHEMA)

    def close(self):
        self.conn.close()

    @contextmanager
    def transaction(self):
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            yield self.conn
            self.conn.execute("COMMIT")
        except BaseException:
            self.conn.execute("ROLLBACK")
            raise

    # ---- batch bookkeeping -------------------------------------------------
    def batch_done(self, batch_id: int) -> bool:
        return self.conn.execute("SELECT 1 FROM batches WHERE batch_id = ?", (batch_id,)).fetchone() is not None

    # ---- reads used by detection (see this transaction's own writes) --------
    def first_window(self):
        row = self.conn.execute("SELECT MIN(window_start) FROM zone_windows").fetchone()
        return datetime.strptime(row[0], TS_FORMAT) if row and row[0] else None

    def history(self, window_start: datetime, zones: Iterable[int]) -> Dict[int, List[int]]:
        """Previous HISTORY_WINDOWS counts per zone, oldest first, missing windows filled with 0.
        Only windows since the stream started count, so the history grows from 0 to 12."""
        first = self.first_window()
        if first is None or first >= window_start:
            return {}
        start = max(window_start - HISTORY_WINDOWS * WINDOW, first)
        slots = []
        t = start
        while t < window_start:
            slots.append(fmt(t))
            t += WINDOW
        zones = list(zones)
        found = {}
        if zones:
            marks = ",".join("?" * len(zones))
            rows = self.conn.execute(
                f"SELECT zone, window_start, trips FROM zone_windows "
                f"WHERE window_start >= ? AND window_start < ? AND zone IN ({marks})",
                (fmt(start), fmt(window_start), *zones),
            ).fetchall()
            found = {(zone, ws): trips for zone, ws, trips in rows}
        return {zone: [found.get((zone, s), 0) for s in slots] for zone in zones}

    # ---- writes -------------------------------------------------------------
    def upsert_windows(self, rows: List[dict], batch_id: int, processed_at_ms: int):
        self.conn.executemany(
            "INSERT INTO zone_windows (window_start, zone, trips, avg_fare, last_produced_ms, processed_at_ms, batch_id) "
            "VALUES (?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(window_start, zone) DO UPDATE SET trips = excluded.trips, avg_fare = excluded.avg_fare, "
            "last_produced_ms = excluded.last_produced_ms, processed_at_ms = excluded.processed_at_ms, "
            "batch_id = excluded.batch_id",
            [(r["window_start"], r["zone"], r["trips"], r.get("avg_fare"), r.get("last_produced_ms"),
              processed_at_ms, batch_id) for r in rows],
        )

    def upsert_anomalies(self, window_start: str, spikes: List[Spike], explanations: Dict[int, str]):
        self.conn.executemany(
            "INSERT INTO anomalies (window_start, zone, trips, baseline_mean, baseline_std, threshold, explanation, detected_at_ms) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(window_start, zone) DO UPDATE SET trips = excluded.trips, baseline_mean = excluded.baseline_mean, "
            "baseline_std = excluded.baseline_std, threshold = excluded.threshold, explanation = excluded.explanation",
            [(window_start, s.zone, s.trips, s.baseline_mean, s.baseline_std, s.threshold,
              explanations.get(s.zone), now_ms()) for s in spikes],
        )

    def mark_batch(self, batch_id: int, windows: int, zone_rows: int, anomalies: int):
        self.conn.execute("INSERT INTO batches (batch_id, windows, zone_rows, anomalies, committed_ms) VALUES (?, ?, ?, ?, ?)",
                          (batch_id, windows, zone_rows, anomalies, now_ms()))
