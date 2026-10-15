"""
Unit tests for the streaming consumer.

Tests cover:
- floor_to_5min: window boundary rounding
- Anomaly detection: threshold logic (mean + 2*std)

Run with:
    pip install pytest
    pytest tests/ -v
"""

import statistics
from datetime import datetime

import pytest

# Import the function we are testing directly from the consumer module
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from streaming.consumer import floor_to_5min


# ---------------------------------------------------------------------------
# floor_to_5min tests
# ---------------------------------------------------------------------------

class TestFloorTo5Min:
    """Verifies that pickup timestamps are bucketed into correct 5-minute windows."""

    def test_already_on_boundary(self):
        """A timestamp exactly on a 5-minute boundary should not move."""
        dt = datetime(2023, 1, 1, 9, 0, 0)
        assert floor_to_5min(dt) == datetime(2023, 1, 1, 9, 0, 0)

    def test_rounds_down_within_window(self):
        """09:03:45 should floor to 09:00."""
        dt = datetime(2023, 1, 1, 9, 3, 45)
        assert floor_to_5min(dt) == datetime(2023, 1, 1, 9, 0, 0)

    def test_rounds_down_to_previous_5min(self):
        """09:07:59 should floor to 09:05, not 09:10."""
        dt = datetime(2023, 1, 1, 9, 7, 59)
        assert floor_to_5min(dt) == datetime(2023, 1, 1, 9, 5, 0)

    def test_last_second_of_window(self):
        """09:09:59 should still floor to 09:05."""
        dt = datetime(2023, 1, 1, 9, 9, 59)
        assert floor_to_5min(dt) == datetime(2023, 1, 1, 9, 5, 0)

    def test_top_of_hour(self):
        """00:00:01 should floor to 00:00."""
        dt = datetime(2023, 1, 1, 0, 0, 1)
        assert floor_to_5min(dt) == datetime(2023, 1, 1, 0, 0, 0)

    def test_midnight_boundary(self):
        """23:58:30 should floor to 23:55."""
        dt = datetime(2023, 1, 1, 23, 58, 30)
        assert floor_to_5min(dt) == datetime(2023, 1, 1, 23, 55, 0)

    def test_seconds_and_microseconds_stripped(self):
        """Seconds and microseconds should always be zeroed out."""
        dt = datetime(2023, 1, 1, 14, 22, 58, 999999)
        result = floor_to_5min(dt)
        assert result.second == 0
        assert result.microsecond == 0
        assert result.minute == 20


# ---------------------------------------------------------------------------
# Anomaly detection threshold tests
#
# The rule in consumer.py:
#   anomaly if count > (mean + 2 * std) AND count > (mean + 1)
#
# We test the math directly — no need to run Kafka or call Gemini.
# ---------------------------------------------------------------------------

def is_anomaly(count: int, history: list) -> bool:
    """
    Mirrors the exact anomaly detection logic in flush_window().
    Returns True if this count would be flagged as an anomaly.
    """
    if len(history) < 3:
        return False
    mean = statistics.mean(history)
    std = statistics.stdev(history)
    threshold = mean + (2.0 * std)
    return count > threshold and count > (mean + 1)


class TestAnomalyDetection:
    """Verifies the rolling mean + 2*std anomaly detection rule."""

    def test_clear_spike_is_flagged(self):
        """A count 3x the stable baseline should always be an anomaly."""
        stable_history = [10, 11, 10, 9, 11, 10]
        assert is_anomaly(35, stable_history) is True

    def test_normal_count_not_flagged(self):
        """
        A count within 2-sigma of the baseline should not be flagged.
        History [10,11,10,9,11,10]: mean≈10.2, std≈0.75, threshold≈11.7.
        Count=11 is below the threshold.
        """
        stable_history = [10, 11, 10, 9, 11, 10]
        assert is_anomaly(11, stable_history) is False

    def test_requires_at_least_3_windows(self):
        """Detection must not fire until 3 baseline windows have been observed."""
        assert is_anomaly(999, []) is False
        assert is_anomaly(999, [5]) is False
        assert is_anomaly(999, [5, 5]) is False
        assert is_anomaly(999, [5, 5, 5]) is True  # now has 3 windows

    def test_zero_std_guard(self):
        """
        If history is perfectly flat, std=0 and threshold = mean.
        count > mean + 1 prevents false positives for tiny jumps.
        """
        flat_history = [5, 5, 5, 5, 5]
        assert is_anomaly(6, flat_history) is False   # mean=5, count=6: 6 > 5+0=5 but NOT > 5+1=6
        assert is_anomaly(7, flat_history) is True    # 7 > 5 and 7 > 6 → anomaly

    def test_high_variance_baseline_not_flagged(self):
        """
        A count that looks large in isolation should not be flagged if
        the baseline itself is already highly variable.
        """
        volatile_history = [2, 50, 3, 48, 5, 45]
        # mean ≈ 25.5, std ≈ 22.5, threshold ≈ 70.5
        assert is_anomaly(40, volatile_history) is False

    def test_airport_zone_spike_scenario(self):
        """
        Simulates a realistic airport zone with a clear New Year's Eve surge.
        Baseline: ~15 pickups per window. Spike: 60 pickups.
        """
        airport_history = [14, 16, 15, 13, 17, 15, 14, 16]
        assert is_anomaly(60, airport_history) is True

    def test_count_of_zero_never_anomaly(self):
        """A zone with zero pickups in a window should never be an anomaly."""
        stable_history = [10, 11, 10, 9, 11, 10]
        assert is_anomaly(0, stable_history) is False
