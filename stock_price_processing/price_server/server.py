import asyncio
import numpy as np
import json
import datetime
import time
import logging
import sys

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import PlainTextResponse, JSONResponse
import uvicorn


HOST = "0.0.0.0"
PORT = 8080
last_price: dict[str, float] = {"ABC": 100, "DEF": 35, "XYZ": 49}
log_format = logging.Formatter("[%(asctime)s] [%(levelname)s] - %(message)s")
log = logging.getLogger("Quote server")
log.setLevel(logging.INFO)
log_handler = logging.StreamHandler(sys.stdout)
log_handler.setLevel(logging.INFO)
log_handler.setFormatter(log_format)
log.addHandler(log_handler)

app = FastAPI()


def generate_ohlc(last_price, μ=0.1, σ=0.2, N=1000):
    freq = 1 / 100
    time.sleep(np.random.exponential(freq))
    scale = N * 365 * 24 * 60 * 60 / freq
    returns = np.random.normal(loc=μ / scale, scale=σ / np.sqrt(scale), size=N)
    prices = last_price * np.exp(returns.cumsum())
    return {
        "Open": np.round(prices[0], 4),
        "Low": np.round(min(prices), 4),
        "High": np.round(max(prices), 4),
        "Close": np.round(prices[-1], 4),
        "timestamp": datetime.datetime.now(),
    }


def all_tickers():
    return list(last_price.keys())


# --- HTTP endpoints ---


@app.get("/__health_check__.html")
async def health_check():
    return PlainTextResponse("OK\n")


@app.get("/tickers")
async def get_tickers():
    return JSONResponse(all_tickers())


@app.put("/add/{ticker}")
async def add_ticker(ticker: str):
    if ticker not in last_price:
        last_price[ticker] = 100
        log.info(f"Added ticker {ticker}")
    return JSONResponse(all_tickers())


@app.put("/remove/{ticker}")
async def remove_ticker(ticker: str):
    if ticker in last_price:
        last_price.pop(ticker)
        log.info(f"Removed ticker {ticker}")
    return JSONResponse(all_tickers())


@app.get("/quote/{ticker}")
async def get_quote(ticker: str):
    if ticker in last_price:
        prices = generate_ohlc(last_price[ticker])
        prices["Ticker"] = ticker
        last_price[ticker] = prices["Close"]
    else:
        prices = {
            "Open": None,
            "High": None,
            "Low": None,
            "Close": None,
            "timestamp": None,
            "Ticker": ticker,
        }
    prices = {k: [v] for k, v in prices.items()}
    return JSONResponse(json.loads(json.dumps(prices, default=str)))


# --- WebSocket endpoint ---


async def _consumer(websocket: WebSocket):
    """Listen for incoming commands: '?' lists tickers, '-TICKER' removes, 'TICKER' adds."""
    try:
        message = await websocket.receive_text()
    except WebSocketDisconnect:
        return
    message = message.strip()
    if message == "?":
        await websocket.send_text(json.dumps(all_tickers()))
    elif message.startswith("-"):
        ticker = message[1:]
        if ticker in last_price:
            last_price.pop(ticker)
            log.info(f"Removed ticker {ticker}")
    else:
        ticker = message
        if ticker not in last_price:
            last_price[ticker] = 100
            log.info(f"Added ticker {ticker}")


async def _producer(websocket: WebSocket):
    """Push OHLC quotes for a random subset of tracked tickers."""
    num_tickers = len(last_price)
    if num_tickers < 1:
        return
    num_quotes = np.random.randint(1, num_tickers) if num_tickers > 1 else 1
    for ticker in np.random.choice(list(last_price.keys()), num_quotes):
        prices = generate_ohlc(last_price[ticker])
        prices["Ticker"] = ticker
        if ticker in last_price:
            last_price[ticker] = prices["Close"]
        try:
            await websocket.send_text(json.dumps(prices, default=str))
        except WebSocketDisconnect:
            return


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            consumer_task = asyncio.create_task(_consumer(websocket))
            producer_task = asyncio.create_task(_producer(websocket))
            done, pending = await asyncio.wait(
                [consumer_task, producer_task],
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in pending:
                task.cancel()
    except WebSocketDisconnect:
        log.info("Client disconnected")
    except Exception:
        log.info("WebSocket connection closed")


def main(port: int = PORT):
    log.info(f"Serving quotes on {HOST}:{port}")
    uvicorn.run(app, host=HOST, port=int(port), log_level="info")


if __name__ == "__main__":
    main(PORT)
