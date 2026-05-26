# Reactive Graph — Stock Exchange Demo

A reactive graph running on Datatailr that streams simulated stock exchange
data through a pipeline of ZMQ services and displays the results on a live
FastAPI dashboard.

## Architecture

```
┌──────────────────┐  ZMQ DEALER→ROUTER  ┌──────────────────────┐
│   market-feed    │  (SUB + EVT frames) │   analytics-engine   │
│  (ZMQ ROUTER)    │ ──────────────────> │   (ZMQ ROUTER)       │
│  Simulates stock │                     │  Validates + computes │
│  exchange ticks  │                     │  SMA, VWAP, vol, …   │
└───────┬──────────┘                     └───────┬──────────────┘
        │ ROUTER :8080                           │ ROUTER :8080
        │ (EVT frames)                           │ (EVT frames)
        │                                        │
        └──────────┐             ┌───────────────┘
                   v             v
           ┌───────────────────────────┐
           │  Reactive Graph Dashboard │
           │  (FastAPI app)            │
           │  DEALER→ROUTER to nodes   │
           │  Live UI + controls       │
           └───────────────────────────┘
```

### Single-port protocol

Every node binds a single ZMQ **ROUTER** socket on port 8080 (the
platform-assigned port).  Peers connect with **DEALER** sockets and
exchange three frame types:

| Direction | Frames | Meaning |
|-----------|--------|---------|
| DEALER → ROUTER | `[b"SUB"]` | Register as event subscriber |
| ROUTER → DEALER | `[b"EVT", topic, protobuf]` | Broadcast event |
| DEALER → ROUTER | `[b"CTL", json]` | Control command |
| ROUTER → DEALER | `[b"CTL", json]` | Control reply |

Peers that only send `CTL` (never `SUB`) do **not** receive `EVT`
frames, keeping short-lived control connections clean.

### Components

| Component | Type | Port | Description |
|-----------|------|------|-------------|
| **market-feed** | Service (ZMQ ROUTER) | 8080 | Generates simulated stock ticks |
| **analytics-engine** | Service (ZMQ ROUTER) | 8080 | Validates ticks, computes analytics |
| **Dashboard** | App (FastAPI) | 8080 | Subscribes to both, renders live UI, sends controls |

## What's inside

```
reactive_graph/
├── proto/
│   └── messages.proto         # GraphMessage schema
├── node/
│   ├── __init__.py
│   ├── app.py                 # ZMQ node service (market-feed & analytics)
│   └── messages_pb2.py        # Pre-generated protobuf bindings
├── dashboard/
│   ├── __init__.py
│   └── app.py                 # FastAPI dashboard + embedded HTML
├── minimal/
│   ├── __init__.py
│   ├── publisher.py           # Minimal ZMQ PUB helper class
│   ├── subscriber.py          # Minimal ZMQ SUB helper class
│   └── demo.ipynb             # Jupyter notebook demo
├── tests/
│   ├── pong.py                # Proven ZMQ service (smoke test)
│   ├── test_pong.py           # Client for pong
│   └── deploy.py              # Deploy pong
├── deploy.py
├── requirements.txt
├── spec.md
└── README.md
```

## Configuration

| Env var | Default | Used by | Description |
|---------|---------|---------|-------------|
| `NODE_NAME` | `market-feed` | node | Logical name stamped into messages |
| `NODE_ROLE` | `market-feed` | node | `market-feed` or `analytics` |
| `UPSTREAM_NODES` | _(empty)_ | node | Comma-separated upstream hostnames |
| `UPSTREAM_ZMQ_PORT` | `8080` | node | Port of upstream ROUTER sockets |
| `TICK_SYMBOLS` | `AAPL,GOOGL,MSFT,AMZN,TSLA` | node (feed) | Symbols to generate |
| `TICK_INTERVAL_S` | `1.0` | node (feed) | Seconds per full tick round |
| `ANALYTICS_WINDOW` | `20` | node (analytics) | Short SMA window size |
| `REACTIVE_GRAPH_NODES` | `market-feed,analytics-engine` | dashboard | Nodes to subscribe to |
| `ZMQ_PORT` | `8080` | dashboard | ROUTER port for all nodes |

## Deployment

```bash
python reactive_graph/deploy.py             # everything
python reactive_graph/deploy.py services    # both ZMQ services
python reactive_graph/deploy.py feed        # market-feed only
python reactive_graph/deploy.py analytics   # analytics-engine only
python reactive_graph/deploy.py dashboard   # dashboard only
```

## Local development

Run the three components in separate terminals:

```bash
# Terminal 1: market-feed (ROUTER on 8080)
PYTHONPATH=. NODE_NAME=market-feed NODE_ROLE=market-feed \
  TICK_INTERVAL_S=1.0 PORT=8080 \
  python -m reactive_graph.node.app

# Terminal 2: analytics-engine (ROUTER on 8082, DEALER→localhost:8080)
PYTHONPATH=. NODE_NAME=analytics-engine NODE_ROLE=analytics \
  UPSTREAM_NODES=localhost UPSTREAM_ZMQ_PORT=8080 PORT=8082 \
  python -m reactive_graph.node.app

# Terminal 3: dashboard (FastAPI on 8000)
PYTHONPATH=. REACTIVE_GRAPH_NODES=localhost:8080,localhost:8082 \
  PORT=8000 \
  python -m reactive_graph.dashboard.app
```

Then open http://localhost:8000 in a browser.

## Regenerating protobuf bindings

```bash
protoc --python_out=reactive_graph/node \
       --proto_path=reactive_graph/proto \
       reactive_graph/proto/messages.proto
```
