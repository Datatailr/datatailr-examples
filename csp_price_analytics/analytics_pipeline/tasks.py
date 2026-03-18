import json
import math
import logging
from datetime import datetime

from datatailr import task

logger = logging.getLogger(__name__)

BLOB_PREFIX = "csp_price_analytics"


@task(memory="1g", cpu=1)
def ingest_tick_data() -> dict:
    """Read raw tick data from blob storage. Falls back to synthetic generation if no data exists."""
    import random
    from csp_price_analytics.common.models import ALL_SYMBOLS, INITIAL_PRICES, VOLATILITIES

    try:
        from datatailr import Blob
        blob = Blob()
        if blob.exists(f"{BLOB_PREFIX}/raw_ticks/latest.json"):
            raw = blob.get(f"{BLOB_PREFIX}/raw_ticks/latest.json")
            data = json.loads(raw)
            logger.info(f"Ingested {len(data.get('ticks', []))} ticks from blob storage")
            return data
    except Exception as e:
        logger.info(f"Blob read skipped ({e}), generating synthetic data")

    ticks = []
    for symbol in ALL_SYMBOLS:
        price = INITIAL_PRICES[symbol]
        vol = VOLATILITIES[symbol]
        for i in range(500):
            z = random.gauss(0, 1)
            price *= math.exp(-0.5 * vol ** 2 * 0.01 + vol * math.sqrt(0.01) * z)
            volume = round(random.lognormvariate(math.log(1000), 1.0), 2)
            ticks.append({
                "symbol": symbol,
                "price": round(price, 6),
                "volume": volume,
                "bid": round(price * 0.9995, 6),
                "ask": round(price * 1.0005, 6),
                "sequence": i,
            })
    return {"ticks": ticks, "generated_at": datetime.utcnow().isoformat(), "count": len(ticks)}


@task(memory="1g", cpu=1)
def aggregate_ohlcv(raw_data: dict) -> dict:
    """Compute OHLCV bars and daily returns per symbol."""
    ticks = raw_data.get("ticks", [])

    by_symbol = {}
    for t in ticks:
        sym = t["symbol"]
        if sym not in by_symbol:
            by_symbol[sym] = []
        by_symbol[sym].append(t)

    ohlcv = {}
    returns = {}
    for sym, sym_ticks in by_symbol.items():
        prices = [t["price"] for t in sym_ticks]
        volumes = [t.get("volume", 0) for t in sym_ticks]
        ohlcv[sym] = {
            "open": prices[0],
            "high": max(prices),
            "low": min(prices),
            "close": prices[-1],
            "volume": sum(volumes),
            "tick_count": len(prices),
        }
        if len(prices) > 1:
            daily_return = (prices[-1] - prices[0]) / prices[0] * 100
        else:
            daily_return = 0.0
        returns[sym] = round(daily_return, 4)

    return {
        "ohlcv": ohlcv,
        "returns": returns,
        "num_symbols": len(ohlcv),
        "total_ticks": len(ticks),
        "computed_at": datetime.utcnow().isoformat(),
    }


@task(memory="1g", cpu=1)
def compute_statistics(aggregated: dict) -> dict:
    """Compute historical volatility, correlations, and performance metrics."""
    ohlcv = aggregated.get("ohlcv", {})
    returns = aggregated.get("returns", {})

    volatility = {}
    for sym, bars in ohlcv.items():
        if bars["tick_count"] > 1:
            price_range = (bars["high"] - bars["low"]) / bars["open"] if bars["open"] > 0 else 0
            volatility[sym] = round(price_range, 6)
        else:
            volatility[sym] = 0.0

    symbols = sorted(returns.keys())
    n = len(symbols)
    correlations = {}
    for i, s1 in enumerate(symbols):
        row = {}
        for j, s2 in enumerate(symbols):
            if i == j:
                row[s2] = 1.0
            else:
                r1 = returns.get(s1, 0)
                r2 = returns.get(s2, 0)
                row[s2] = round(1.0 / (1.0 + abs(r1 - r2)) if (r1 != 0 or r2 != 0) else 1.0, 4)
        correlations[s1] = row

    top_gainers = sorted(returns.items(), key=lambda x: x[1], reverse=True)[:5]
    top_losers = sorted(returns.items(), key=lambda x: x[1])[:5]
    avg_return = sum(returns.values()) / len(returns) if returns else 0

    return {
        "volatility": volatility,
        "correlations": correlations,
        "top_gainers": [{"symbol": s, "return": r} for s, r in top_gainers],
        "top_losers": [{"symbol": s, "return": r} for s, r in top_losers],
        "avg_return": round(avg_return, 4),
        "computed_at": datetime.utcnow().isoformat(),
    }


@task(memory="512m", cpu=0.5)
def generate_report(aggregated: dict, statistics: dict) -> dict:
    """Generate summary report and persist to blob storage."""
    report = {
        "title": "CSP Price Analytics - Daily Report",
        "generated_at": datetime.utcnow().isoformat(),
        "summary": {
            "num_symbols": aggregated.get("num_symbols", 0),
            "total_ticks_processed": aggregated.get("total_ticks", 0),
            "avg_return": statistics.get("avg_return", 0),
        },
        "ohlcv": aggregated.get("ohlcv", {}),
        "returns": aggregated.get("returns", {}),
        "volatility": statistics.get("volatility", {}),
        "top_gainers": statistics.get("top_gainers", []),
        "top_losers": statistics.get("top_losers", []),
        "correlations": statistics.get("correlations", {}),
    }

    try:
        from datatailr import Blob
        blob = Blob()
        report_json = json.dumps(report)
        blob.put(f"{BLOB_PREFIX}/reports/daily_report.json", report_json)
        date_key = datetime.utcnow().strftime("%Y-%m-%d")
        blob.put(f"{BLOB_PREFIX}/reports/daily_report_{date_key}.json", report_json)
        logger.info("Report written to blob storage")
    except Exception as e:
        logger.warning(f"Could not write report to blob storage: {e}")

    return report
