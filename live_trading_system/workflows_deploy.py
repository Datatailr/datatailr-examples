"""Deploy the Live Trading System workflows on Datatailr.

Two scheduled workflows that participate in the live ZMQ fabric:

* **Pre-Market Warmup** -- runs before market open; seeds the running
  services with yesterday's positions (from blob storage) and today's
  strategy / risk configuration (from KV), then publishes a
  ``market_open`` event through the notification-bus.
* **EOD Reconciliation** -- runs after market close; snapshots every
  service via CTL, briefly subscribes to the analytics stream to
  measure quality, computes a PnL report, persists it to blob
  storage, and publishes ``eod_complete`` through the notification-bus.

Usage::

    python live_trading_system/workflows_deploy.py            # both workflows
    python live_trading_system/workflows_deploy.py warmup     # warm-up only
    python live_trading_system/workflows_deploy.py eod        # EOD only
    python live_trading_system/workflows_deploy.py warmup --local
    python live_trading_system/workflows_deploy.py eod --local

The ``--local`` form runs the workflow locally via
``local_run=True`` so you can dry-run end-to-end against running
services without scheduling a real run.
"""

from __future__ import annotations

import os
import pathlib
import sys

current_dir = pathlib.Path(__file__).parent
sys.path.append(str(current_dir.parent))

from datatailr import Schedule, workflow  # noqa: E402

from live_trading_system.workflows.eod import (  # noqa: E402
    broadcast_eod_complete,
    compute_pnl_report,
    flush_persistence,
    persist_to_blob,
    sample_market_quality,
    snapshot_orders,
    snapshot_positions,
    snapshot_risk,
    snapshot_signals,
)
from live_trading_system.workflows.warmup import (  # noqa: E402
    broadcast_market_open,
    enable_strategies,
    load_previous_positions,
    load_strategy_config,
    seed_execution_simulator,
    seed_risk_engine,
)

REQUIREMENTS = str(current_dir / "requirements.txt")


@workflow(
    name="Pre-Market Warmup",
    python_requirements=REQUIREMENTS,
    schedule=Schedule(
        at_hours=[8],
        weekdays=["Mon", "Tue", "Wed", "Thu", "Fri"],
    ),
)
def pre_market_warmup() -> None:
    prev = load_previous_positions().alias("load_previous_positions")
    config = load_strategy_config().alias("load_strategy_config")

    seed_exec = seed_execution_simulator(prev).alias("seed_execution_simulator")
    seed_risk = seed_risk_engine(config, prev).alias("seed_risk_engine")
    strategies = enable_strategies(config).alias("enable_strategies")

    broadcast_market_open(prev, seed_exec, seed_risk, strategies).alias(
        "broadcast_market_open"
    )


@workflow(
    name="EOD Reconciliation",
    python_requirements=REQUIREMENTS,
    schedule=Schedule(
        at_hours=[22],
        weekdays=["Mon", "Tue", "Wed", "Thu", "Fri"],
    ),
)
def eod_reconciliation() -> None:
    flushed = flush_persistence().alias("flush_persistence")
    positions = snapshot_positions(flushed).alias("snapshot_positions")
    orders = snapshot_orders(flushed).alias("snapshot_orders")
    risk = snapshot_risk().alias("snapshot_risk")
    signals = snapshot_signals().alias("snapshot_signals")
    quality = sample_market_quality().alias("sample_market_quality")

    report = compute_pnl_report(
        positions, orders, risk, signals, quality
    ).alias("compute_pnl_report")
    persisted = persist_to_blob(report).alias("persist_to_blob")
    broadcast_eod_complete(persisted).alias("broadcast_eod_complete")


def deploy_warmup(local: bool = False) -> None:
    pre_market_warmup(local_run=local)


def deploy_eod(local: bool = False) -> None:
    eod_reconciliation(local_run=local)


def deploy_all(local: bool = False) -> None:
    deploy_warmup(local=local)
    deploy_eod(local=local)


if __name__ == "__main__":
    args = sys.argv[1:]
    local = "--local" in args
    args = [a for a in args if a != "--local"]
    cmd = args[0] if args else "all"
    actions = {
        "all": deploy_all,
        "warmup": deploy_warmup,
        "pre-market-warmup": deploy_warmup,
        "eod": deploy_eod,
        "eod-reconciliation": deploy_eod,
    }
    fn = actions.get(cmd)
    if fn:
        if local:
            os.environ.setdefault("DATATAILR_BATCH_ARG_RUNDATE",
                                  os.environ.get("DATATAILR_BATCH_ARG_RUNDATE", ""))
        fn(local=local)
    else:
        print(__doc__)
        sys.exit(1)
