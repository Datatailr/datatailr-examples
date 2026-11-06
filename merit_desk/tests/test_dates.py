from datetime import date, datetime, timezone

import pytest

import merit_desk.common.dates as dates
from merit_desk.common.dates import london_today


@pytest.mark.parametrize("utc_time, expected", [
    (datetime(2026, 7, 14, 23, 30, tzinfo=timezone.utc), date(2026, 7, 15)),  # BST is UTC+1
    (datetime(2026, 1, 14, 23, 30, tzinfo=timezone.utc), date(2026, 1, 14)),  # GMT is UTC
])
def test_london_today_follows_the_london_clock(monkeypatch, utc_time, expected):
    class FixedClock(datetime):
        @classmethod
        def now(cls, tz=None):
            return utc_time.astimezone(tz)

    monkeypatch.setattr(dates, "datetime", FixedClock)
    assert london_today() == expected
