"""Shared trading universe used by every component of the demo.

A single source of truth keeps prices, positions, fundies, and the dashboard
all aligned on the same set of symbols.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class TickerInfo:
    symbol: str
    name: str
    sector: str
    initial_price: float
    annual_vol: float


UNIVERSE: tuple[TickerInfo, ...] = (
    TickerInfo("AAPL", "Apple Inc.",                 "Technology",         185.0, 0.22),
    TickerInfo("MSFT", "Microsoft Corp.",            "Technology",         420.0, 0.20),
    TickerInfo("GOOG", "Alphabet Inc.",              "Communication Svcs", 175.0, 0.24),
    TickerInfo("AMZN", "Amazon.com Inc.",            "Consumer Discr.",    185.0, 0.28),
    TickerInfo("TSLA", "Tesla Inc.",                 "Consumer Discr.",    250.0, 0.50),
    TickerInfo("NVDA", "NVIDIA Corp.",               "Technology",         950.0, 0.45),
    TickerInfo("META", "Meta Platforms Inc.",        "Communication Svcs", 510.0, 0.30),
    TickerInfo("JPM",  "JPMorgan Chase & Co.",       "Financials",         210.0, 0.18),
    TickerInfo("XOM",  "Exxon Mobil Corp.",          "Energy",             115.0, 0.25),
    TickerInfo("PFE",  "Pfizer Inc.",                "Health Care",         28.0, 0.21),
)


SYMBOLS: tuple[str, ...] = tuple(t.symbol for t in UNIVERSE)
BY_SYMBOL: dict[str, TickerInfo] = {t.symbol: t for t in UNIVERSE}


# Vendors whose research notes / broker mails get summarized in the inbox tab.
VENDORS: tuple[str, ...] = (
    "BloombergResearch",
    "GoldmanSachs Equity Research",
    "Morgan Stanley Desk Notes",
    "JPM Cazenove",
    "Refinitiv Datafeed",
    "Reuters Newswire",
    "Citi Velocity",
    "Barclays Capital Research",
)
