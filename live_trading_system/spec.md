# Live Trading System -- Specification

A simulated trading desk: long-running services and scheduled workflows
streaming over ZMQ with a live FastAPI dashboard, designed to exercise
the full Datatailr stack in a single coherent demo.

Under the hood the pipeline is structured as a *reactive graph* of
typed protobuf messages -- every node stamps the message it processes
so the dashboard can reconstruct the full lineage of every event from
the stream itself.

## Goals

1. **Show data streaming between services** in a way that is visible to
   the audience.  The dashboard renders a live topology with per-edge
   messages-per-second, fed by every event's `hops` field.
2. **Show data being consumed by the app**, not just relayed.  The
   dashboard maintains stock prices, analytics, positions, PnL, and an
   order blotter from the same stream.
3. **Show workflows participating in the messaging fabric** -- not just
   running batch tasks.  Two scheduled workflows publish into and read
   from the same ZMQ bus used by the services.

## Non-goals

* Real broker integration; everything is simulated.
* Durable / replayable queues; ZMQ is in-memory and best-effort.
* Authentication on ZMQ sockets -- relying on Datatailr's network
  isolation.

## Messaging

* Transport: **ZMQ ROUTER/DEALER** on a single port per service.
* Serialisation: **protobuf** with a single `GraphMessage` envelope
  carrying a typed `oneof payload` -- `Tick`, `Analytics`, `Signal`,
  `OrderIntent`, `Fill`, `PositionUpdate`, `SystemEvent`, `RejectedTick`,
  or fallback `text`.
* Three-frame protocol: `SUB`, `EVT topic protobuf`, `CTL json`.

## Components

### Services

| Role | Subscribes to | Emits | Notable CTL actions |
|------|---------------|-------|---------------------|
| `market-feed` | -- | `tick` | `pause`, `resume`, `set_interval`, `add_symbol`, `remove_symbol`, `set_symbols`, `snapshot` |
| `analytics` | market-feed | `validated_tick`, `analytics`, `rejected` | `set_analytics_window`, `snapshot` |
| `signal-engine` | analytics | `signal` | `enable_strategy`, `disable_strategy`, `enable_strategies`, `set_min_strength`, `set_cooldown`, `snapshot` |
| `risk-engine` | signal-engine, execution-simulator | `order_intent` (approved or rejected) | `set_limits`, `seed_positions`, `snapshot` |
| `execution-simulator` | risk-engine, market-feed (for last price) | `fill`, `position_update` | `seed_positions`, `set_slippage_bps`, `set_fill_delay`, `snapshot_orders`, `snapshot` |
| `notification-bus` | -- | re-broadcasts CTL `broadcast` payloads | `broadcast`, `snapshot` |

### App

`Live Trading System Dashboard` -- FastAPI single-page UI with:

* live SVG topology graph (nodes + edges + msg/s);
* ticker, analytics, positions/PnL, order blotter, system events
  ribbon, raw event feed;
* runtime controls that emit CTL frames to the relevant services.

### Workflows

* `Pre-Market Warmup` (scheduled at 08:00 weekdays):
  1. `load_previous_positions` (Blob)
  2. `load_strategy_config` (KV with sane defaults)
  3. `seed_execution_simulator` (CTL)
  4. `seed_risk_engine` (CTL)
  5. `enable_strategies` (CTL)
  6. `broadcast_market_open` (CTL to notification-bus)

* `EOD Reconciliation` (scheduled at 22:00 weekdays):
  1. `snapshot_positions`, `snapshot_orders`, `snapshot_risk`,
     `snapshot_signals` (CTL fan-out)
  2. `sample_market_quality` (SUB to analytics-engine for ~20 s)
  3. `compute_pnl_report` (fan-in)
  4. `persist_to_blob` (Blob)
  5. `broadcast_eod_complete` (CTL to notification-bus)

## Acceptance

* Deploying everything in `live_trading_system/deploy.py` plus the two
  workflows results in a live trading graph with the dashboard
  visible at `http://<dashboard-host>`.
* Every Service and the dashboard App are deployed with
  `app_section="Live Trading System"` so they appear together on the
  Datatailr launcher page.
* All edges in the dashboard topology view show a non-zero msg/s rate
  within ~30 s of the services being up.
* Triggering `python workflows_deploy.py eod --local` produces:
  * a new blob under `live_trading_system/eod/<rundate>.json`,
  * an `eod_complete` event visible on the dashboard's system-events
    ribbon within ~30 s,
  * a workflow run with all tasks succeeded in the Datatailr UI.
* Triggering `python workflows_deploy.py warmup --local` produces:
  * `position_update` events from `execution-simulator` reflecting the
    seeded state,
  * a `market_open` event on the dashboard's system-events ribbon.

The `tests/pong.py` minimal service is retained as a smoke test for the
ROUTER/DEALER pattern.
