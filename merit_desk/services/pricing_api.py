"""The Pricing API: the newest forward curves and a stack price, over HTTP."""

from __future__ import annotations

import uvicorn
from fastapi import FastAPI, HTTPException, Query

from merit_desk.common.curve import Curve, MonthNotInCurveError
from merit_desk.common.markets import Market, UnknownMarketError, get_market
from merit_desk.common.stack import stack_price
from merit_desk.common.store import TextStore, default_store, latest_curve

NO_CURVE = "No curve yet. The daily curve workflow has not run."


def create_app(store: TextStore | None = None) -> FastAPI:
    store = default_store() if store is None else store
    app = FastAPI(title="Merit Desk Pricing API")

    def market_or_404(code: str) -> Market:
        try:
            return get_market(code)
        except UnknownMarketError as err:
            raise HTTPException(status_code=404, detail=str(err)) from None

    def curve_or_503(code: str) -> dict:
        stored = latest_curve(market_or_404(code).code, store)  # an unknown market is 404 first
        if stored is None:
            raise HTTPException(status_code=503, detail=NO_CURVE)
        return stored

    @app.get("/health")
    def health() -> dict:
        return {"status": "ok"}

    @app.get("/price")
    def price(market: str, temp_c: float = Query(ge=-60, le=60)) -> float:  # nan and inf fail too
        return stack_price(temp_c, market_or_404(market).stack)

    @app.get("/curve/{market}")
    def curve(market: str) -> dict:
        return curve_or_503(market)

    @app.get("/curve/{market}/{month}")
    def curve_month(market: str, month: str) -> dict:
        stored = curve_or_503(market)
        try:
            value = Curve.from_dict(stored).price_for(month)
        except MonthNotInCurveError as err:
            raise HTTPException(status_code=404, detail=str(err)) from None
        return {"market": stored["market"], "month": month, "price": value}

    return app


def main(port):
    """The Datatailr service entry point: the platform gives the port."""
    uvicorn.run(create_app(), host="0.0.0.0", port=int(port))
