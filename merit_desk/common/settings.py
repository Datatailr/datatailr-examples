"""Desk settings that people change in Datatailr variables (KV).

A workflow reads them when its DAG is built, so a change takes effect at the next deploy
or instant run, not at the next scheduled run.
"""

from __future__ import annotations

import logging

from merit_desk.common.markets import Market, UnknownMarketError, get_market

logger = logging.getLogger(__name__)

DEFAULT_MARKETS = "GB,DE,FR"
SWEEP_SIZES = ("small", "full")
DEFAULT_SWEEP_SIZE = "small"


def _read(key: str, default: str) -> str:
    """The variable's text, or the default when it is not set, not text or cannot be read."""
    try:
        from datatailr import KV

        value = KV().get(key)
    except Exception as err:
        logger.info("Variable '%s' not read (%s: %s). Using '%s'.",
                    key, type(err).__name__, err, default)
        return default
    if value is None or value == "":
        return default
    if not isinstance(value, str):  # the SDK decodes JSON, so '["GB"]' or '3' arrive as non-text
        logger.warning("Variable '%s' is %r. It must be text. Using '%s'.", key, value, default)
        return default
    return value


def get_markets() -> tuple[Market, ...]:
    """The markets to price, from the variable 'markets' (a comma list such as 'GB,DE').

    The daily curve reads it when its DAG is built, at deploy or instant run.
    """
    value = _read("markets", DEFAULT_MARKETS)
    codes = dict.fromkeys(code.strip().upper() for code in value.split(",") if code.strip())
    try:
        if not codes:
            raise UnknownMarketError("The list has no market codes.")
        return tuple(get_market(code) for code in codes)
    except UnknownMarketError as err:
        logger.warning("Variable 'markets' is '%s'. %s Using '%s'.", value, err, DEFAULT_MARKETS)
        return tuple(get_market(code) for code in DEFAULT_MARKETS.split(","))


def get_sweep_size() -> str:
    """'small' or 'full', from the variable 'sweep_size'.

    The strategy sweep reads it when its DAG is built, at deploy or instant run.
    """
    value = _read("sweep_size", DEFAULT_SWEEP_SIZE).strip().lower()
    if value not in SWEEP_SIZES:
        logger.warning("Variable 'sweep_size' is '%s'. Use 'small' or 'full'. Using '%s'.",
                       value, DEFAULT_SWEEP_SIZE)
        return DEFAULT_SWEEP_SIZE
    return value
