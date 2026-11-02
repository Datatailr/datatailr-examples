"""The Market Feed: simulated live prices that wander around the latest curve."""

from __future__ import annotations

import logging
import random
import threading
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import uvicorn
from fastapi import FastAPI, HTTPException

from merit_desk.common.climate import normal_temp
from merit_desk.common.dates import london_today
from merit_desk.common.markets import MARKETS, UnknownMarketError, get_market
from merit_desk.common.stack import stack_price
from merit_desk.common.store import TextStore, default_store, latest_curve

logger = logging.getLogger(__name__)

ANCHOR_REFRESH_SECONDS = 60


def next_price(prev: float, anchor: float, rng: random.Random) -> float:
    """One tick: a fifth of the way back to the anchor, plus noise. Never below zero."""
    return max(0.0, prev + 0.2 * (anchor - prev) + rng.gauss(0, 0.004 * anchor))


def anchor_price(market: str, store: TextStore) -> float:
    """What the price settles around: the front month of the newest curve, else a normal day."""
    m = get_market(market)
    curve = latest_curve(m.code, store)
    if curve is not None:
        return float(curve["prices"][0])
    return float(stack_price(normal_temp(m, london_today()), m.stack))


def quote(price: float) -> dict:
    return {"price": round(price, 2), "time": datetime.now(timezone.utc).isoformat()}


class Feed:
    """The latest quote for each market. A thread calls run(); the endpoints only read."""

    def __init__(self, store: TextStore, seed: int) -> None:
        self.store = store
        self.rng = random.Random(seed)  # a seeded feed repeats itself
        self.anchors: dict[str, float] = {}
        self.refresh_anchors()
        self.latest = {code: quote(anchor) for code, anchor in self.anchors.items()}

    def refresh_anchors(self) -> None:
        self.anchors = {m.code: anchor_price(m.code, self.store) for m in MARKETS}

    def tick(self) -> None:
        for code, anchor in self.anchors.items():
            self.latest[code] = quote(next_price(self.latest[code]["price"], anchor, self.rng))

    def run(self, stop: threading.Event, tick_seconds: float) -> None:
        refresh_at = time.monotonic() + ANCHOR_REFRESH_SECONDS
        while not stop.wait(tick_seconds):
            if time.monotonic() >= refresh_at:
                refresh_at += ANCHOR_REFRESH_SECONDS
                try:
                    self.refresh_anchors()
                except Exception as err:  # one bad read must not stop the feed
                    logger.warning("Anchor refresh failed, keeping the old anchors: %s.", err)
            self.tick()


def create_app(store: TextStore | None = None, seed: int = 42,
               tick_seconds: float = 1.0) -> FastAPI:
    store = default_store() if store is None else store
    feed = Feed(store, seed)  # prices start on their anchors, so /latest is never empty

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        stop = threading.Event()
        thread = threading.Thread(target=feed.run, args=(stop, tick_seconds), daemon=True)
        thread.start()
        yield
        stop.set()
        thread.join(timeout=5)

    app = FastAPI(title="Merit Desk Market Feed", lifespan=lifespan)

    @app.get("/health")
    def health() -> dict:
        return {"status": "ok"}

    @app.get("/latest")
    def latest() -> dict:
        return dict(feed.latest)

    @app.get("/latest/{market}")
    def latest_for(market: str) -> dict:
        try:
            code = get_market(market).code
        except UnknownMarketError as err:
            raise HTTPException(status_code=404, detail=str(err)) from None
        return {"market": code, **feed.latest[code]}

    return app


def main(port):
    """The Datatailr service entry point: the platform gives the port."""
    uvicorn.run(create_app(), host="0.0.0.0", port=int(port))
