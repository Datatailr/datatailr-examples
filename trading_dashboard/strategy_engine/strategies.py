"""Initial book of strategies, positions and entry prices.

In a real deployment these would come from an OMS, a database, or a
positions feed. For the demo we hard-code a representative book so that
the dashboard always has plausible content out of the box.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Position:
    ticker: str
    quantity: int      # negative means short
    avg_price: float   # entry price


@dataclass(frozen=True)
class StrategyBook:
    name: str
    description: str
    style: str         # e.g. "Long/Short", "Mean Reversion", "Momentum"
    positions: tuple[Position, ...]


STRATEGIES: tuple[StrategyBook, ...] = (
    StrategyBook(
        name="Mega-Cap Momentum",
        description="Long the top US mega-cap names with positive 6-month price momentum.",
        style="Momentum",
        positions=(
            Position("AAPL",  5_000, 178.40),
            Position("MSFT",  2_500, 410.10),
            Position("NVDA",  1_200, 920.80),
            Position("META",  1_800, 495.20),
        ),
    ),
    StrategyBook(
        name="Mean Reversion Pairs",
        description="Long/short pairs trade fading short-term dislocations within sectors.",
        style="Mean Reversion",
        positions=(
            Position("GOOG",  3_000, 172.10),
            Position("META", -1_500, 505.40),
            Position("JPM",   2_500, 207.60),
            Position("XOM",  -2_000, 117.20),
        ),
    ),
    StrategyBook(
        name="Defensive Yield",
        description="Long lower-vol dividend payers, partially hedged via index proxies.",
        style="Carry",
        positions=(
            Position("PFE",  20_000,  27.40),
            Position("XOM",   4_000, 112.80),
            Position("JPM",   1_500, 209.20),
        ),
    ),
    StrategyBook(
        name="Tactical Tech Tilt",
        description="Short-horizon tactical book overweighting AI beneficiaries.",
        style="Long/Short",
        positions=(
            Position("NVDA",   800,  955.50),
            Position("AMZN",  2_500, 181.30),
            Position("TSLA", -1_000, 258.40),
            Position("AAPL", -1_500, 188.10),
        ),
    ),
)


STRATEGIES_BY_NAME: dict[str, StrategyBook] = {s.name: s for s in STRATEGIES}
