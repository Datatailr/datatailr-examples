import asyncio
import json
import logging
import sys
import datetime
from dataclasses import dataclass, field

import numpy as np
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse, JSONResponse
from sse_starlette.sse import EventSourceResponse
import uvicorn


HOST = "0.0.0.0"
PORT = 8080

log_format = logging.Formatter("[%(asctime)s] [%(levelname)s] - %(message)s")
log = logging.getLogger("Exchange feed")
log.setLevel(logging.INFO)
log_handler = logging.StreamHandler(sys.stdout)
log_handler.setLevel(logging.INFO)
log_handler.setFormatter(log_format)
log.addHandler(log_handler)


# ---------------------------------------------------------------------------
# Market micro-structure model
# ---------------------------------------------------------------------------

@dataclass
class TickerState:
    """Per-ticker state that drives realistic quote/trade generation.

    Price follows geometric Brownian motion.  The bid-ask spread is modelled
    as a mean-reverting (Ornstein-Uhlenbeck) process around a base width
    that scales with price level, so cheap stocks have tighter absolute
    spreads and expensive stocks have wider ones.
    """
    mid: float
    annual_vol: float = 0.25
    base_spread_bps: float = 5.0
    spread_vol_bps: float = 2.0
    spread_mean_reversion: float = 0.15
    _current_half_spread: float = field(init=False, default=0.0)

    def __post_init__(self):
        self._current_half_spread = self.mid * self.base_spread_bps / 10_000

    def step(self, dt: float):
        sigma = self.annual_vol
        drift = -0.5 * sigma * sigma * dt
        diffusion = sigma * np.sqrt(dt) * np.random.standard_normal()
        self.mid *= np.exp(drift + diffusion)

        target_half = self.mid * self.base_spread_bps / 10_000
        spread_noise = (self.mid * self.spread_vol_bps / 10_000) * np.sqrt(dt) * np.random.standard_normal()
        self._current_half_spread += self.spread_mean_reversion * (target_half - self._current_half_spread) * dt + spread_noise
        self._current_half_spread = max(self._current_half_spread, self.mid * 0.5 / 10_000)

    @property
    def bid(self):
        return round(self.mid - self._current_half_spread, 4)

    @property
    def ask(self):
        return round(self.mid + self._current_half_spread, 4)

    def random_trade(self):
        """Generate a trade at a realistic price between bid and ask."""
        aggressor = np.random.choice(["buy", "sell"])
        price = self.ask if aggressor == "buy" else self.bid
        size = int(np.random.lognormal(mean=4.5, sigma=1.0)) * 100
        return price, size, aggressor


INITIAL_TICKERS: dict[str, tuple[float, float]] = {
    "AAPL": (185.0, 0.22),
    "MSFT": (420.0, 0.20),
    "GOOG": (175.0, 0.24),
    "AMZN": (185.0, 0.28),
    "TSLA": (250.0, 0.50),
}

tickers: dict[str, TickerState] = {
    sym: TickerState(mid=mid, annual_vol=vol)
    for sym, (mid, vol) in INITIAL_TICKERS.items()
}

# Sequence number for exchange events
_seq = 0


def next_seq():
    global _seq
    _seq += 1
    return _seq


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------
# OpenAPI: GET /openapi.json  ·  Swagger UI: /docs  ·  ReDoc: /redoc

app = FastAPI(
    title="Price Server",
    description=(
        "Synthetic market feed: GBM mid prices, stochastic spread, random trades/quotes. "
        "REST for tickers and snapshots. **SSE:** `GET /stream` returns `text/event-stream` "
        "(one JSON object per `data:` line; `type` is `trade` or `quote`). "
        "That route is omitted from this spec because OpenAPI/Swagger do not model SSE cleanly."
    ),
    version="0.1.0",
    openapi_tags=[
        {"name": "health", "description": "Load balancer / platform probes"},
        {"name": "tickers", "description": "List and add/remove symbols"},
        {"name": "market-data", "description": "Point-in-time BBO-style quote"},
        {"name": "stream", "description": "Server-Sent Events — not a JSON body response"},
    ],
)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


@app.get("/__health_check__.html", tags=["health"])
async def health_check():
    """Plain-text OK for health checks."""
    return PlainTextResponse("OK\n")


@app.get("/tickers", response_model=list[str], tags=["tickers"])
async def get_tickers():
    tickers_keys = list(tickers.keys())
    log.info(f"GET /tickers -> {tickers_keys}")
    return tickers_keys


@app.put("/add/{ticker}", response_model=list[str], tags=["tickers"])
async def add_ticker(ticker: str, price: float = 100.0, vol: float = 0.25):
    ticker = ticker.upper()
    if ticker not in tickers:
        tickers[ticker] = TickerState(mid=price, annual_vol=vol)
        log.info(f"PUT /add/{ticker} -> added (price={price}, vol={vol})")
    else:
        log.info(f"PUT /add/{ticker} -> already exists, skipped")
    return list(tickers.keys())


@app.put("/remove/{ticker}", response_model=list[str], tags=["tickers"])
async def remove_ticker(ticker: str):
    ticker = ticker.upper()
    if ticker in tickers:
        tickers.pop(ticker)
        log.info(f"PUT /remove/{ticker} -> removed")
    else:
        log.info(f"PUT /remove/{ticker} -> not found, skipped")
    return list(tickers.keys())


@app.get("/quote/{ticker}", tags=["market-data"])
async def get_quote(ticker: str):
    ticker = ticker.upper()
    state = tickers.get(ticker)
    if state is None:
        log.info(f"GET /quote/{ticker} -> not found")
        return JSONResponse({"error": f"unknown ticker {ticker}"}, status_code=404)
    log.info(f"GET /quote/{ticker} -> bid={state.bid} ask={state.ask} mid={round(state.mid, 4)}")
    return JSONResponse({
        "ticker": ticker,
        "bid": state.bid,
        "ask": state.ask,
        "mid": round(state.mid, 4),
        "spread": round(state.ask - state.bid, 4),
        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    })


# ---------------------------------------------------------------------------
# SSE streaming
# ---------------------------------------------------------------------------

DT_PER_EVENT = 1.0 / (252 * 6.5 * 3600)


async def _event_generator():
    """Yield a mix of quote and trade events across all active tickers."""
    while True:
        if not tickers:
            await asyncio.sleep(0.1)
            continue

        symbols = list(tickers.keys())
        sym = symbols[np.random.randint(len(symbols))]
        state = tickers.get(sym)
        if state is None:
            continue

        state.step(DT_PER_EVENT)
        now = datetime.datetime.now(datetime.timezone.utc).isoformat()

        r = np.random.random()

        if r < 0.4:
            price, size, side = state.random_trade()
            event = {
                "type": "trade",
                "seq": next_seq(),
                "ticker": sym,
                "price": price,
                "size": size,
                "side": side,
                "timestamp": now,
            }
        else:
            quote_side = "bid" if r < 0.7 else "ask"
            size = int(np.random.lognormal(mean=5.0, sigma=0.8)) * 100
            event = {
                "type": "quote",
                "seq": next_seq(),
                "ticker": sym,
                "quote_side": quote_side,
                "quote_price": state.bid if quote_side == "bid" else state.ask,
                "quote_size": size,
                "bid": state.bid,
                "ask": state.ask,
                "mid": round(state.mid, 4),
                "spread": round(state.ask - state.bid, 4),
                "timestamp": now,
            }

        yield json.dumps(event)

        await asyncio.sleep(np.random.exponential(1.0 / 50.0))


@app.get(
    "/stream",
    tags=["stream"],
    summary="SSE trade and quote events",
    response_class=EventSourceResponse,
    include_in_schema=False,
)
async def stream_events():
    """`text/event-stream`. Each event is one JSON object per `data:` line (`type`: `trade` | `quote`)."""
    return EventSourceResponse(_event_generator())


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

def main(port: int = PORT):
    log.info(f"Exchange feed on {HOST}:{port}")
    uvicorn.run(app, host=HOST, port=int(port), log_level="info")


if __name__ == "__main__":
    main(PORT)
