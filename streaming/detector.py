"""
Spike detection for one 5-minute window: pure Python, no Spark or Kafka, so it is easy to test.

A zone's window is flagged when its trip count is far above that zone's recent normal:

    trips > mean + k * std      (mean/std over the zone's previous 12 windows = last hour)
    trips > mean + 1            (ignore tiny absolute changes)
    trips >= min_trips          (sparse zones jump from 0 to 2 all the time; that is noise)

Windows with no trips count as 0 in the history: an hour of silence is part of "normal".
"""

import statistics
from dataclasses import dataclass
from typing import Dict, List, Sequence

HISTORY_WINDOWS = 12          # 12 x 5 min = the previous hour
MIN_HISTORY_WINDOWS = 3       # the stream must have run this long before we judge anything


@dataclass(frozen=True)
class Spike:
    zone: int
    trips: int
    baseline_mean: float
    baseline_std: float
    threshold: float


def baseline(history: Sequence[int]):
    """Mean and sample standard deviation of a zone's recent window counts."""
    values = list(history)
    if len(values) < 2:
        return float(values[0]) if values else 0.0, 0.0
    return statistics.mean(values), statistics.stdev(values)


def detect_spikes(current: Dict[int, int], history: Dict[int, List[int]],
                  k: float = 2.0, min_trips: int = 5) -> List[Spike]:
    """
    current: {zone: trips in this window}
    history: {zone: trips in each of the previous windows, oldest first, zero-filled}
    """
    spikes = []
    for zone, trips in sorted(current.items()):
        past = history.get(zone, [])
        if len(past) < MIN_HISTORY_WINDOWS:
            continue
        mean, std = baseline(past)
        threshold = mean + k * std
        if trips > threshold and trips > mean + 1 and trips >= min_trips:
            spikes.append(Spike(zone, int(trips), round(mean, 2), round(std, 2), round(threshold, 2)))
    return spikes
