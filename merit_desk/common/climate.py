"""Normal temperatures: a seasonal cosine around each market's yearly mean."""

from __future__ import annotations

import math
from datetime import date

from merit_desk.common.markets import Market


def normal_temp(market: Market, day: date) -> float:
    """Normal temperature in degrees C, coldest on the market's coldest day."""
    phase = 2 * math.pi * (day.timetuple().tm_yday - market.coldest_day_of_year) / 365.25
    return market.normal_mean_c - market.normal_amplitude_c * math.cos(phase)
