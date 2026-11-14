"""
Two spike detectors with the same interface, so the stream and the evaluation can swap them.

RollingDetector (v1 rule, kept as the baseline)
    Normal = this zone's previous hour. Weak spot: the 6-8 am ramp happens every
    weekday, but an hour-long window sees it as a spike.

SeasonalDetector (default)
    Normal = this zone, this 5-minute slot, the same weekday, over the previous
    4 weeks of the BATCH Iceberg table (lake.nyc.trips_cleaned). The morning ramp
    is in the profile, so it is no longer a surprise.
    spike if trips > mean + k * max(std, sqrt(mean), 1)  and  trips >= min_trips
    (sqrt(mean) is the Poisson noise floor: counts this small are never smoother than that.)
"""

import math
from datetime import date, datetime, timedelta
from typing import Dict, List, Tuple

from streaming.detector import Spike, detect_spikes

PROFILE_WEEKS = 4


def slot_of(ts: datetime) -> int:
    return ts.hour * 12 + ts.minute // 5


class RollingDetector:
    name = "rolling_hour"

    def __init__(self, k: float = 2.0, min_trips: int = 5):
        self.k, self.min_trips = k, min_trips

    def detect(self, store, window_start: datetime, current: Dict[int, int]) -> List[Spike]:
        history = store.history(window_start, current.keys())
        return detect_spikes(current, history, k=self.k, min_trips=self.min_trips)


PROFILE_SQL = """
WITH trips AS (
    SELECT PULocationID AS zone, tpep_pickup_datetime AS ts
    FROM {source}
    WHERE tpep_pickup_datetime >= ? AND tpep_pickup_datetime < ?
      AND isodow(tpep_pickup_datetime) = ?
),
per_day AS (
    SELECT zone, CAST(ts AS DATE) AS d, hour(ts) * 12 + minute(ts) // 5 AS slot, COUNT(*) AS n
    FROM trips
    GROUP BY ALL
),
weeks AS (SELECT COUNT(DISTINCT CAST(ts AS DATE)) AS w FROM trips)
SELECT zone, slot,
       SUM(n) / w                                                              AS mean,
       sqrt(greatest((SUM(n * n) - w * pow(SUM(n) / w, 2)) / greatest(w - 1, 1), 0)) AS std,
       w                                                                       AS weeks
FROM per_day, weeks
GROUP BY zone, slot, w
"""


def build_profile(con, source: str, day: date) -> Tuple[int, Dict[Tuple[int, int], Tuple[float, float]]]:
    """(weeks found, {(zone, slot): (mean, std)}) for `day`'s weekday over the previous 4 weeks.
    Days with no trips in a slot count as 0, because the mean divides by the number of weeks."""
    start = datetime.combine(day, datetime.min.time()) - timedelta(weeks=PROFILE_WEEKS)
    end = datetime.combine(day, datetime.min.time())
    rows = con.execute(PROFILE_SQL.format(source=source), [start, end, day.isoweekday()]).fetchall()
    weeks = rows[0][4] if rows else 0
    return weeks, {(zone, slot): (mean, std) for zone, slot, mean, std, _ in rows}


class SeasonalDetector:
    name = "seasonal_4w"

    def __init__(self, source: str, k: float = 3.0, min_trips: int = 5, con=None, min_weeks: int = 2):
        import duckdb

        self.source, self.k, self.min_trips, self.min_weeks = source, k, min_trips, min_weeks
        self.con = con or duckdb.connect()
        if "iceberg_scan" in source:
            self.con.execute("INSTALL iceberg; LOAD iceberg;")
        self._profiles = {}

    def profile(self, day: date):
        if day not in self._profiles:
            self._profiles[day] = build_profile(self.con, self.source, day)
        return self._profiles[day]

    def detect(self, store, window_start: datetime, current: Dict[int, int]) -> List[Spike]:
        weeks, profile = self.profile(window_start.date())
        if weeks < self.min_weeks:
            return []
        slot = slot_of(window_start)
        spikes = []
        for zone, trips in sorted(current.items()):
            mean, std = profile.get((zone, slot), (0.0, 0.0))
            scale = max(std, math.sqrt(mean), 1.0)
            threshold = mean + self.k * scale
            if trips > threshold and trips >= self.min_trips:
                spikes.append(Spike(zone, int(trips), round(mean, 2), round(scale, 2), round(threshold, 2)))
        return spikes


def iceberg_source(warehouse) -> str:
    return f"iceberg_scan('{warehouse}/nyc/trips_cleaned')"
