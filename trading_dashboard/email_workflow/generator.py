"""Synthetic vendor-email generator.

Stands in for a real inbox connector (Exchange, Gmail, Outlook Graph, ...).
Each invocation produces a small batch of plausible-looking research notes,
broker reports and newswire blurbs about tickers from the trading universe.

The generator is deterministic for a given (run_id, batch_size) so the same
workflow run never produces duplicate content within itself, but every new
run yields a fresh batch.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import random
import uuid

from trading_dashboard.universe import BY_SYMBOL, SYMBOLS, VENDORS


_TEMPLATES: list[tuple[str, str]] = [
    (
        "{vendor} | {ticker} - Reiterate {rating} after Q{quarter} print",
        "We reiterate our {rating} rating on {ticker} ({name}) after the latest "
        "quarterly results came in {tone}. Revenue grew {rev_growth:.1f}% YoY, beating "
        "the consensus by {beat:.1f}%. Margins expanded {margin:.0f}bps QoQ on "
        "operating leverage in the {sector} segment. Management raised full-year "
        "EPS guidance to ${eps:.2f} (prior ${eps_prev:.2f}). We move our 12-month "
        "price target from ${pt_old:.0f} to ${pt_new:.0f}, implying {upside:.0f}% "
        "upside. Risks: input-cost inflation, FX headwinds and slower discretionary "
        "spend in {sector}."
    ),
    (
        "{vendor} | Sector note: {sector} - {ticker} positioned to {action}",
        "Our latest cross-sector check finds {ticker} ({name}) particularly well "
        "positioned in {sector} given {driver}. Channel checks are {tone}, with "
        "order books extending into Q{quarter}+1. We forecast {rev_growth:.1f}% top "
        "line growth this year and free-cash-flow conversion of {fcf:.0f}%. The "
        "stock trades on {pe:.1f}x forward earnings vs {pe_peer:.1f}x for peers, "
        "which we view as {valuation}. Maintain {rating}; PT ${pt_new:.0f}."
    ),
    (
        "{vendor} | Trading desk colour - {ticker} flow update",
        "Active two-way flow in {ticker} this morning. Real-money buyers at the "
        "open ({size_m}M shares) absorbed early selling pressure from a single "
        "European LO seller. Spreads tightened to {spread_bps:.0f}bps mid-session. "
        "Options skew implies {iv:.0f}% 30d IV ({iv_chg:+.0f}vs last week). Block "
        "of {block_size}k printed at ${block_px:.2f} -- likely a portfolio rebalance "
        "tied to month-end. Desk is {tone} for the day."
    ),
    (
        "{vendor} | Macro brief - implications for {ticker}",
        "Following yesterday's {macro_event}, we see {ticker} ({name}) as a "
        "{tone} beneficiary. Our economists revised the path of policy rates by "
        "{rate_chg:+d}bps over the next 12 months, which historically correlates "
        "with multiple expansion in {sector} names. Reiterate {rating}. Key catalysts: "
        "next earnings on {next_earnings}, capital markets day in {cmd_month}, and "
        "potential M&A activity in the space."
    ),
    (
        "{vendor} | Newsflash: {ticker} - {headline}",
        "Breaking: {ticker} ({name}) {headline}. Initial market reaction was "
        "{tone} ({px_chg:+.1f}% in pre-market). Volume already trading "
        "{vol_mult:.1f}x the 20-day average. Our analyst's first take: this is "
        "broadly {assessment} for the {sector} thesis. We will publish a more "
        "detailed note this afternoon after the conference call at {call_time} ET."
    ),
]


_TONES = ["constructive", "cautious", "broadly in line", "ahead of expectations", "below expectations"]
_RATINGS = ["Buy", "Overweight", "Hold", "Neutral", "Sell"]
_ACTIONS = ["outperform", "rerate higher", "consolidate", "lag the sector", "lead the rebound"]
_DRIVERS = [
    "AI infrastructure capex",
    "share buyback acceleration",
    "improving free cash flow",
    "easing input cost inflation",
    "market-share gains in the high-end segment",
    "favorable regulatory developments",
]
_VALUATIONS = ["a discount", "fair value", "a modest premium", "a meaningful premium"]
_MACRO_EVENTS = [
    "the FOMC's slightly dovish minutes",
    "softer-than-expected US payrolls",
    "the ECB's hold decision",
    "a cooler CPI print",
    "weaker China manufacturing PMI",
]
_HEADLINES = [
    "raises full-year guidance",
    "announces $20bn buyback",
    "appoints new CFO",
    "discloses cybersecurity incident",
    "wins major government contract",
    "delays product launch by one quarter",
]
_ASSESSMENTS = ["positive", "neutral but watch the conference call", "incrementally negative"]


def _vendor_address(vendor: str) -> str:
    """Plausible vendor email like research@goldmansachs.com."""
    domain = (
        vendor.lower()
        .replace("research", "")
        .replace("desk notes", "")
        .replace("newswire", "")
        .replace("equity", "")
        .replace("datafeed", "")
        .replace("velocity", "")
        .replace("capital", "")
        .replace("cazenove", "")
        .strip()
        .replace(" ", "")
    ) or "vendor"
    return f"research@{domain}.com"


def _gen_one(rng: random.Random, ticker_symbol: str, vendor: str, idx: int) -> dict:
    info = BY_SYMBOL[ticker_symbol]
    template_subj, template_body = rng.choice(_TEMPLATES)

    pt_old = round(info.initial_price * rng.uniform(0.9, 1.1), 0)
    pt_new = round(pt_old * rng.uniform(0.92, 1.18), 0)

    fields = {
        "vendor": vendor,
        "ticker": info.symbol,
        "name": info.name,
        "sector": info.sector,
        "rating": rng.choice(_RATINGS),
        "tone": rng.choice(_TONES),
        "action": rng.choice(_ACTIONS),
        "driver": rng.choice(_DRIVERS),
        "valuation": rng.choice(_VALUATIONS),
        "macro_event": rng.choice(_MACRO_EVENTS),
        "headline": rng.choice(_HEADLINES),
        "assessment": rng.choice(_ASSESSMENTS),
        "quarter": rng.randint(1, 4),
        "rev_growth": rng.uniform(-5, 28),
        "beat": rng.uniform(0.5, 8),
        "margin": rng.uniform(20, 350),
        "eps": round(rng.uniform(2, 14), 2),
        "eps_prev": round(rng.uniform(2, 14), 2),
        "pt_old": pt_old,
        "pt_new": pt_new,
        "upside": (pt_new - info.initial_price) / info.initial_price * 100,
        "fcf": rng.uniform(45, 110),
        "pe": rng.uniform(10, 35),
        "pe_peer": rng.uniform(10, 35),
        "size_m": rng.uniform(0.5, 12.0),
        "spread_bps": rng.uniform(1, 8),
        "iv": rng.uniform(18, 65),
        "iv_chg": rng.uniform(-8, 8),
        "block_size": rng.randint(50, 950),
        "block_px": info.initial_price * rng.uniform(0.985, 1.015),
        "rate_chg": rng.choice([-50, -25, 0, 25, 50]),
        "next_earnings": (
            dt.date.today() + dt.timedelta(days=rng.randint(7, 75))
        ).strftime("%b %d"),
        "cmd_month": rng.choice(["June", "September", "October", "November"]),
        "px_chg": rng.uniform(-6, 6),
        "vol_mult": rng.uniform(1.2, 4.5),
        "call_time": rng.choice(["8:30am", "9:00am", "10:00am", "4:30pm", "5:00pm"]),
    }

    subject = template_subj.format(**fields)
    body = template_body.format(**fields)

    received_at = dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=rng.randint(0, 240))

    payload = f"{vendor}|{ticker_symbol}|{idx}|{received_at.isoformat()}|{subject}".encode()
    eid = hashlib.sha256(payload).hexdigest()[:16]

    return {
        "id": eid,
        "received_at": received_at.isoformat(),
        "from_name": vendor,
        "from_email": _vendor_address(vendor),
        "to_email": "trading-desk@firm.com",
        "ticker": info.symbol,
        "subject": subject,
        "body": body,
    }


def generate_batch(batch_size: int = 6, run_id: str | None = None) -> list[dict]:
    seed = run_id or uuid.uuid4().hex
    rng = random.Random(seed)

    chosen_symbols = rng.sample(list(SYMBOLS), k=min(batch_size, len(SYMBOLS)))
    chosen_vendors = [rng.choice(VENDORS) for _ in chosen_symbols]

    return [
        _gen_one(rng, sym, vnd, idx)
        for idx, (sym, vnd) in enumerate(zip(chosen_symbols, chosen_vendors))
    ]
