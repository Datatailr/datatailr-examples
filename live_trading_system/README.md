# Live Trading System

An end-to-end Datatailr demo built around a simulated algorithmic-trading
desk. **Seven long-running ZMQ services** stream market data through a
five-stage pipeline, a FastAPI dashboard subscribes to every node
and renders the live topology + a DuckDB-backed history panel, and two
scheduled workflows participate in the same ZMQ fabric to do pre-market
warm-up and end-of-day reconciliation.

> **Technology**: under the hood this demo is a *reactive graph* of
> typed protobuf messages. Every node stamps the message it processes
> with its own name, so downstream consumers (and the dashboard) can
> reconstruct the full lineage of every event purely from the stream
> itself. The shared transport in
> [`live_trading_system/node/transport.py`](node/transport.py) takes
> care of ROUTER/DEALER plumbing, SUB/EVT/CTL framing, periodic
> subscription refresh, and automatic recovery from stale DEALER
> sockets after a peer restart.

Trading is fully automated: the `signal-engine` reacts to the rolling
analytics produced from the market-feed, the `risk-engine` gates each
intent, and the `execution-simulator` fills approved orders. A
`persistence-sink` service continuously streams every fill and a fresh
position snapshot into **Parquet files in blob storage** -- the
dashboard and the workflows both query that same store via **DuckDB**.

The example exercises **Services**, **Apps**, **Workflows**, **KV**,
**Blob storage** (Parquet + DuckDB), and **Schedules** on a single
message bus.  All services and the dashboard share the same
``app_section`` so they appear together on the Datatailr launcher page.

## Architecture

```
                       single ZMQ port (8080) per service
                       ROUTER on every service, DEALER on every peer

   market-feed  --tick-->  analytics-engine  --analytics-->  signal-engine
        |                                                          |
        |                                                          v
        v                                                     risk-engine
   execution-simulator  <--order_intent--  risk-engine            |
        |    |     |                        ^                     |
        |    |     +------fills------------>|                     |
        |    v                                                    |
        |  persistence-sink  --parquet-->  Blob (trades/, positions/)
        v                                                         |
   notification-bus  <----CTL broadcast----  workflows  <---------+
        ^                                       |
        |                                       v
        +--system events-->   Live Trading System  <---all nodes
                                  Dashboard
                              (FastAPI + topology + DuckDB history)
                                       |
                                       +---- DuckDB read ----> Blob
```

### Single-port protocol

Every node binds **one** ZMQ ROUTER on port 8080 (the platform-assigned
port). Peers connect with DEALER sockets and exchange three frame types:

| Direction | Frames | Meaning |
|-----------|--------|---------|
| DEALER -> ROUTER | `[b"SUB"]` | register as event subscriber |
| ROUTER -> DEALER | `[b"EVT", topic, protobuf]` | broadcast event |
| DEALER -> ROUTER | `[b"CTL", json_bytes]` | control command |
| ROUTER -> DEALER | `[b"CTL", json_bytes]` | control reply |

Peers that only send `CTL` (e.g. one-shot workflow tasks) never see
`EVT` frames, which keeps short-lived control connections clean.

### Components

| Component | Type | Port | Description |
|-----------|------|------|-------------|
| **market-feed** | Service | 8080 | Generates `Tick` events |
| **analytics-engine** | Service | 8080 | Validates ticks, computes SMA/VWAP/volatility |
| **signal-engine** | Service | 8080 | Momentum + mean-reversion signals |
| **risk-engine** | Service | 8080 | Pre-trade limits, position tracking |
| **execution-simulator** | Service | 8080 | Simulated broker fills with slippage |
| **notification-bus** | Service | 8080 | Relays `broadcast` CTL frames as `EVT` to all subscribers (used by workflows to inject events) |
| **persistence-sink** | Service | 8080 | Subscribes to execution-simulator, flushes fills + position snapshots to Parquet in blob storage every `FLUSH_INTERVAL_S` (default 10 s) |
| **Live Trading System Dashboard** | App (FastAPI) | 8080 | Topology, ticker, analytics, live positions/PnL, blotter, system events; "Persisted history" panel runs DuckDB queries against the Parquet store on `/api/history` |
| **Pre-Market Warmup** | Workflow | -- | Loads yesterday's positions from `positions/latest.parquet` (DuckDB) + today's config (KV), seeds services, broadcasts `market_open` |
| **EOD Reconciliation** | Workflow | -- | Asks persistence-sink to flush, reads today's `trades/` partition + `positions/latest.parquet` via DuckDB, writes a JSON PnL report to blob, broadcasts `eod_complete` |

### Why a notification-bus?

Long-running services can already publish their own events. Workflow
tasks, however, run in **ephemeral containers** for a few seconds. To
make them visible on the dashboard we route their messages through a
shared **notification-bus**: any process can send a single
`CTL{action:"broadcast", topic, payload}` and the bus re-emits it as an
`EVT` to every dashboard subscriber. This is what makes the
`market_open` / `eod_complete` workflow events appear instantly on the
live stream.

## What's in the repo

```
live_trading_system/
|-- proto/
|   `-- messages.proto         # GraphMessage + typed payloads (oneof)
|-- node/
|   |-- app.py                 # ZmqNode dispatcher (NODE_ROLE -> role module)
|   |-- transport.py           # Shared ROUTER/DEALER + SUB/EVT/CTL helpers
|   |-- messages_pb2.py        # Generated protobuf bindings
|   `-- roles/
|       |-- market_feed.py
|       |-- analytics.py
|       |-- signals.py
|       |-- risk.py
|       |-- execution.py
|       |-- bus.py             # notification-bus (CTL broadcast relay)
|       `-- persistence.py     # persistence-sink (Parquet flush service)
|-- persistence/
|   `-- parquet_io.py          # Blob + Parquet + DuckDB helpers (shared
|                              #   by persistence-sink, dashboard, workflows)
|-- dashboard/
|   `-- app.py                 # FastAPI + embedded single-page UI
|-- workflows/
|   |-- zmq_client.py          # ctl_request / broadcast / sample_events
|   |-- warmup.py              # @task functions for pre-market
|   `-- eod.py                 # @task functions for end-of-day
|-- deploy.py                  # services + dashboard
|-- workflows_deploy.py        # @workflow definitions + scheduling
|-- requirements.txt
|-- spec.md
`-- README.md
```

## Deployment

Services and dashboard (all land under the `Live Trading System`
section on the Datatailr launcher page):

```bash
python live_trading_system/deploy.py             # all 7 services + dashboard
python live_trading_system/deploy.py services    # all 7 services
python live_trading_system/deploy.py feed        # market-feed only
python live_trading_system/deploy.py analytics   # analytics-engine only
python live_trading_system/deploy.py signals     # signal-engine only
python live_trading_system/deploy.py risk        # risk-engine only
python live_trading_system/deploy.py execution   # execution-simulator only
python live_trading_system/deploy.py bus         # notification-bus only
python live_trading_system/deploy.py persistence # persistence-sink only
python live_trading_system/deploy.py dashboard   # dashboard only
```

Workflows:

```bash
python live_trading_system/workflows_deploy.py             # both workflows
python live_trading_system/workflows_deploy.py warmup      # Pre-Market Warmup
python live_trading_system/workflows_deploy.py eod         # EOD Reconciliation
python live_trading_system/workflows_deploy.py warmup --local  # run locally
python live_trading_system/workflows_deploy.py eod --local
```

The workflows expect (but do not require) two KV entries -- create them
in the Datatailr UI to override the defaults:

| Key | Type | Example value |
|-----|------|---------------|
| `live_trading_system/strategies` | JSON list | `["momentum", "mean_reversion"]` |
| `live_trading_system/risk_limits` | JSON object | `{"max_position": 1000, "max_notional": 250000, "max_daily_loss": 50000}` |

## Persistence layout (Parquet in blob storage)

`persistence-sink` subscribes to `execution-simulator` and flushes
fills + the latest position book to blob storage every
`FLUSH_INTERVAL_S` seconds. The layout is fixed, the schema lives in
[`live_trading_system/persistence/parquet_io.py`](persistence/parquet_io.py),
and **every reader uses DuckDB**.

```
live_trading_system/
|-- trades/dt=YYYY-MM-DD/HHMMSS-<seq>.parquet
|     (order_id, symbol, side, qty, price, slippage, strategy, at,
|      correlation_id)
|-- positions/latest.parquet         # current book, overwritten on flush
|-- positions/history/dt=YYYY-MM-DD/HHMMSS-<seq>.parquet
|-- eod/<rundate>.json               # legacy JSON EOD report (still emitted)
```

A flush also broadcasts a `system.persistence_flush` event on the live
ZMQ stream, so you can see the dashboard's system-events ribbon tick
every ~10 seconds while trading is happening.

Query the store yourself:

```bash
# how many trades did we make today?
duckdb -c "SELECT COUNT(*), SUM(qty*price) AS notional \
           FROM read_parquet('live_trading_system/trades/dt=2026-05-28/*.parquet')"

# current book
duckdb -c "SELECT * FROM read_parquet('live_trading_system/positions/latest.parquet')"
```

The dashboard's `/api/history` endpoint runs equivalent queries
server-side and renders the result in the **Persisted history** panel.

## Demo walkthrough (suggested live-demo script)

1. **Open the dashboard.** The topology view fills in within seconds:
   five horizontal nodes (`market-feed -> analytics -> signal -> risk
   -> execution`), the `notification-bus` + `persistence-sink` below
   them. Edge labels show msg/s; hot edges turn green.
2. **Don't touch anything for ~15 seconds.** Trading is fully
   automated: the strategies in `signal-engine` react to the incoming
   analytics, the risk-engine approves intents, the execution-simulator
   fills them, and positions populate the live PnL panel.
3. **Watch the persistence ribbon.** Every ~10 seconds you'll see a
   `persistence_flush` event in the system-events ribbon and the
   **Persisted history** panel updates with file count + DuckDB
   aggregates that match the live PnL panel.
4. **Tune the system live** from the controls panel. Every control
   action pops a toast in the top-right with the result:
   * pause / resume the feed,
   * change tick interval, signal cooldown, min-strength,
   * disable a strategy (the edge from `signal` to `risk` slows down,
     trades drop within one cooldown),
   * tighten risk limits (the blotter fills with `rejected` intents).
5. **Broadcast a ping.** Click **Broadcast ping** to send a custom
   system event through `notification-bus`. The system-events ribbon
   flashes the new event in real time -- the same pathway the EOD and
   warm-up workflows use to publish into the live stream.
6. **Trigger the EOD workflow** (`python workflows_deploy.py eod
   --local`). It asks persistence-sink to flush, then reads today's
   trades partition + `positions/latest.parquet` via DuckDB, writes
   `live_trading_system/eod/<rundate>.json` and broadcasts
   `eod_complete`.  The dashboard's ribbon shows the event within
   seconds.
7. **Trigger the warm-up workflow** the next morning -- it loads
   yesterday's positions from `positions/latest.parquet` (DuckDB),
   seeds them back into `execution-simulator` and `risk-engine`, and
   broadcasts `market_open`.

## Local development

Run the components in separate terminals (replace ports as needed):

```bash
# market-feed
PYTHONPATH=. NODE_NAME=market-feed NODE_ROLE=market-feed \
  TICK_INTERVAL_S=1.0 PORT=8080 \
  python -m live_trading_system.node.app

# analytics-engine
PYTHONPATH=. NODE_NAME=analytics-engine NODE_ROLE=analytics \
  UPSTREAM_NODES=localhost:8080 PORT=8081 \
  python -m live_trading_system.node.app

# signal-engine
PYTHONPATH=. NODE_NAME=signal-engine NODE_ROLE=signal-engine \
  UPSTREAM_NODES=localhost:8081 PORT=8082 \
  python -m live_trading_system.node.app

# risk-engine
PYTHONPATH=. NODE_NAME=risk-engine NODE_ROLE=risk-engine \
  UPSTREAM_NODES=localhost:8082,localhost:8084 PORT=8083 \
  python -m live_trading_system.node.app

# execution-simulator
PYTHONPATH=. NODE_NAME=execution-simulator NODE_ROLE=execution-simulator \
  UPSTREAM_NODES=localhost:8083,localhost:8080 PORT=8084 \
  python -m live_trading_system.node.app

# notification-bus
PYTHONPATH=. NODE_NAME=notification-bus NODE_ROLE=notification-bus \
  PORT=8085 python -m live_trading_system.node.app

# persistence-sink (writes Parquet to LIVE_TRADING_SYSTEM_LOCAL_BLOB_DIR
# when datatailr.Blob isn't available; defaults to /tmp/live_trading_system_blob)
PYTHONPATH=. NODE_NAME=persistence-sink NODE_ROLE=persistence-sink \
  UPSTREAM_NODES=localhost:8084 PORT=8086 \
  FLUSH_INTERVAL_S=5 \
  python -m live_trading_system.node.app

# dashboard -- use the "name@host:port" alias syntax so controls
# can address services by their logical name even when every node
# is on localhost.
PYTHONPATH=. LIVE_TRADING_SYSTEM_NODES=\
market-feed@localhost:8080,analytics-engine@localhost:8081,\
signal-engine@localhost:8082,risk-engine@localhost:8083,\
execution-simulator@localhost:8084,notification-bus@localhost:8085,\
persistence-sink@localhost:8086 \
  PORT=8000 python -m live_trading_system.dashboard.app
```

`LIVE_TRADING_SYSTEM_NODES` accepts both plain `host[:port]` (Datatailr,
where the host *is* the service name) and the `name@host[:port]`
alias form (handy for local dev with all nodes on `127.0.0.1`).

Open `http://localhost:8000`.

## Regenerating protobuf bindings

```bash
protoc --python_out=live_trading_system/node \
       --proto_path=live_trading_system/proto \
       live_trading_system/proto/messages.proto
```

## What this demo proves on the platform

| Capability | Where it shows up |
|-----------|-------------------|
| Service <-> Service streaming | 5-stage ZMQ pipeline (`market-feed` -> `analytics` -> `signal` -> `risk` -> `execution`) |
| Service <-> App | Dashboard topology updates per edge in real time |
| App <-> Service control | Pause/resume, interval, strategies, risk limits |
| Workflow consuming live stream | `sample_market_quality` task (DEALER + SUB inside a task) |
| Workflow producing into live stream | `broadcast_market_open` / `broadcast_eod_complete` via `notification-bus` |
| Blob storage | EOD report at `live_trading_system/eod/<rundate>.json` |
| KV | `live_trading_system/strategies`, `live_trading_system/risk_limits` |
| Scheduling | `Schedule(at_hours=[8])` and `Schedule(at_hours=[22])` on the two workflows |
| Shared `app_section` | Every Service and the dashboard App are deployed with `app_section="Live Trading System"` so they group together on the launcher page |
