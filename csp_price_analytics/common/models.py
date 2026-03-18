import csp


class Tick(csp.Struct):
    symbol: str
    price: float
    volume: float
    bid: float
    ask: float
    timestamp: str


class Analytics(csp.Struct):
    symbol: str
    price: float
    vwap: float
    sma_short: float
    sma_long: float
    ema_short: float
    rsi: float
    bollinger_upper: float
    bollinger_lower: float
    spread: float
    timestamp: str


class Signal(csp.Struct):
    symbol: str
    signal_type: str  # "BUY", "SELL", "HOLD"
    price: float
    reason: str
    strength: float
    timestamp: str


class Alert(csp.Struct):
    alert_id: str
    symbol: str
    alert_type: str  # "PRICE_SPIKE", "HIGH_VOLATILITY", "DRAWDOWN", "CORRELATION_BREAK"
    severity: str  # "LOW", "MEDIUM", "HIGH", "CRITICAL"
    message: str
    value: float
    threshold: float
    timestamp: str


class PortfolioMetrics(csp.Struct):
    total_pnl: float
    max_drawdown: float
    current_drawdown: float
    volatility: float
    sharpe_ratio: float
    num_positions: int
    timestamp: str


SYMBOLS = {
    "equities": ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA", "JPM", "GS"],
    "fx": ["EUR/USD", "GBP/USD", "USD/JPY", "AUD/USD"],
    "crypto": ["BTC/USD", "ETH/USD", "SOL/USD"],
    "commodities": ["GOLD", "SILVER", "OIL"],
}

ALL_SYMBOLS = []
for _syms in SYMBOLS.values():
    ALL_SYMBOLS.extend(_syms)

INITIAL_PRICES = {
    "AAPL": 185.0, "GOOGL": 175.0, "MSFT": 420.0, "AMZN": 195.0,
    "TSLA": 245.0, "JPM": 210.0, "GS": 475.0,
    "EUR/USD": 1.085, "GBP/USD": 1.265, "USD/JPY": 154.5, "AUD/USD": 0.655,
    "BTC/USD": 68500.0, "ETH/USD": 3650.0, "SOL/USD": 155.0,
    "GOLD": 2350.0, "SILVER": 28.5, "OIL": 78.0,
}

VOLATILITIES = {
    "AAPL": 0.02, "GOOGL": 0.022, "MSFT": 0.018, "AMZN": 0.025,
    "TSLA": 0.04, "JPM": 0.015, "GS": 0.02,
    "EUR/USD": 0.005, "GBP/USD": 0.006, "USD/JPY": 0.007, "AUD/USD": 0.006,
    "BTC/USD": 0.035, "ETH/USD": 0.04, "SOL/USD": 0.055,
    "GOLD": 0.01, "SILVER": 0.018, "OIL": 0.025,
}

WS_PATH = "/ws"
