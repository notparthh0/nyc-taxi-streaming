from datetime import date, datetime, timedelta

import duckdb
import pandas as pd
import pytest

from streaming.seasonal import RollingDetector, SeasonalDetector, build_profile, slot_of
from streaming.store import Store

TUE = date(2023, 3, 14)  # a Tuesday


@pytest.fixture
def history_parquet(tmp_path):
    """Four previous Tuesdays: zone 132 has 4/5/6/5 trips at 08:00-08:05 and a 30-trip
    morning ramp at 07:00; zone 48 only appears in week 1."""
    rows = []
    for week, n in zip(range(1, 5), [4, 5, 6, 5]):
        day = datetime.combine(TUE - timedelta(weeks=week), datetime.min.time())
        rows += [(132, day + timedelta(hours=8, seconds=10 * i)) for i in range(n)]
        rows += [(132, day + timedelta(hours=7, seconds=5 * i)) for i in range(30)]
    rows.append((48, datetime.combine(TUE - timedelta(weeks=1), datetime.min.time()) + timedelta(hours=8)))
    rows.append((132, datetime.combine(TUE - timedelta(days=1), datetime.min.time()) + timedelta(hours=8)))  # a Monday
    path = tmp_path / "trips.parquet"
    pd.DataFrame(rows, columns=["PULocationID", "tpep_pickup_datetime"]).to_parquet(path, index=False)
    return f"read_parquet('{path}')"


def test_profile_uses_only_the_same_weekday_and_counts_missing_weeks_as_zero(history_parquet):
    weeks, profile = build_profile(duckdb.connect(), history_parquet, TUE)
    assert weeks == 4
    mean, std = profile[(132, slot_of(datetime(2023, 3, 14, 8, 0)))]
    assert mean == 5.0 and round(std, 3) == 0.816              # 4,5,6,5; the Monday trip is ignored
    assert profile[(48, slot_of(datetime(2023, 3, 14, 8, 0)))][0] == 0.25   # 1,0,0,0


def test_seasonal_ignores_the_regular_morning_ramp_that_the_rolling_rule_flags(history_parquet, tmp_path):
    store = Store(tmp_path / "s.sqlite")
    t = datetime(2023, 3, 14, 6, 0)
    with store.transaction():  # a quiet hour before the ramp
        store.upsert_windows([{"window_start": (t + timedelta(minutes=5 * i)).strftime("%Y-%m-%d %H:%M:%S"),
                               "zone": 132, "trips": 3} for i in range(12)], 0, 0)
    ramp = {132: 30}
    seasonal = SeasonalDetector(history_parquet, k=3.0, min_trips=5)
    rolling = RollingDetector(k=2.0, min_trips=5)
    assert rolling.detect(store, datetime(2023, 3, 14, 7, 0), ramp) != []       # false alarm
    assert seasonal.detect(store, datetime(2023, 3, 14, 7, 0), ramp) == []      # expected at 7 am


def test_seasonal_catches_a_real_spike(history_parquet, tmp_path):
    seasonal = SeasonalDetector(history_parquet, k=3.0, min_trips=5)
    spikes = seasonal.detect(Store(tmp_path / "s.sqlite"), datetime(2023, 3, 14, 8, 0), {132: 20})
    assert [(s.zone, s.trips) for s in spikes] == [(132, 20)]
    assert spikes[0].baseline_mean == 5.0
    # threshold = 5 + 3 * max(0.816, sqrt(5), 1) = 11.71
    assert spikes[0].threshold == 11.71


def test_without_enough_history_it_stays_quiet(history_parquet, tmp_path):
    seasonal = SeasonalDetector(history_parquet, k=3.0, min_trips=5)
    assert seasonal.detect(Store(tmp_path / "s.sqlite"), datetime(2023, 1, 3, 8, 0), {132: 500}) == []
