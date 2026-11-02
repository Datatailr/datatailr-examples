"""The three power markets the desk trades: cities for weather, a stack for prices."""

from __future__ import annotations

from dataclasses import dataclass

from merit_desk.common.stack import Plant, StackConfig


@dataclass(frozen=True)
class City:
    name: str
    lat: float
    lon: float


@dataclass(frozen=True)
class Market:
    code: str
    name: str
    cities: tuple[City, ...]
    stack: StackConfig
    normal_mean_c: float  # yearly mean temperature
    normal_amplitude_c: float  # half the gap between the coldest and warmest day
    coldest_day_of_year: int = 20


class UnknownMarketError(ValueError):
    """Raised for a market code the desk does not trade."""


GB = Market(
    code="GB",
    name="Great Britain",
    cities=(City("London", 51.51, -0.13), City("Manchester", 53.48, -2.24)),
    stack=StackConfig(
        base_demand_gw=28.0, heating_gw_per_c=1.1, cooling_gw_per_c=0.4,
        plants=(Plant("Renewables", 10, 5), Plant("Nuclear", 6, 12), Plant("CCGT", 18, 85),
                Plant("Peaker", 6, 160), Plant("Imports", 4, 220)),
    ),
    normal_mean_c=10.5,
    normal_amplitude_c=6.5,
)

DE = Market(
    code="DE",
    name="Germany",
    cities=(City("Berlin", 52.52, 13.40), City("Frankfurt", 50.11, 8.68),
            City("Munich", 48.14, 11.58)),
    stack=StackConfig(
        base_demand_gw=50.0, heating_gw_per_c=1.4, cooling_gw_per_c=0.6,
        plants=(Plant("Renewables", 20, 5), Plant("Lignite", 15, 45), Plant("Coal", 10, 70),
                Plant("CCGT", 15, 110), Plant("Peaker", 8, 170)),
    ),
    normal_mean_c=9.5,
    normal_amplitude_c=9.0,
)

FR = Market(
    code="FR",
    name="France",
    cities=(City("Paris", 48.86, 2.35), City("Lyon", 45.76, 4.84)),
    stack=StackConfig(
        base_demand_gw=50.0, heating_gw_per_c=2.4, cooling_gw_per_c=0.5,
        plants=(Plant("Renewables", 5, 5), Plant("Nuclear", 45, 15), Plant("Hydro", 8, 30),
                Plant("CCGT", 8, 95), Plant("Peaker", 6, 180)),
    ),
    normal_mean_c=12.0,
    normal_amplitude_c=8.0,
)

MARKETS: tuple[Market, ...] = (GB, DE, FR)
MARKET_BY_CODE: dict[str, Market] = {m.code: m for m in MARKETS}


def get_market(code: str) -> Market:
    """Look a market up by code, ignoring case and surrounding spaces."""
    try:
        return MARKET_BY_CODE[code.strip().upper()]
    except KeyError:
        known = ", ".join(MARKET_BY_CODE)
        raise UnknownMarketError(f"Unknown market '{code.strip()}'. Use one of: {known}.") from None
