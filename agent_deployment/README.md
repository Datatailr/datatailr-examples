# SWE Agent Deployment on Datatailr

A template for deploying a **complete software-development agent** on the
Datatailr platform. It ships a long-running **main agent** that users talk to,
gives that agent the ability to **launch its own sub-agents** to work in
parallel, and equips every agent with **full, first-class knowledge of the
Datatailr platform** (its SDK primitives, deployment model, and operational
tooling).

The agent runtime is the [`pi`](https://github.com/badlogic/pi-mono) coding
agent, running in-container. The main agent runs interactively; sub-agents run
headless (`pi --mode json`) as single-task batch workflows and report back.

## What this template gives you

- **A conversational main agent** — a FastAPI `App` ("SWE Main Agent") that runs
  `pi` in-container and serves an interactive xterm.js terminal, a JSON HTTP API
  (`/chat`, `/chat/stream`), an activity dashboard, and a sub-agent panel.
- **Sub-agent launching** — the main agent exposes a `spawn_subagent(...)` tool
  to `pi`. Each spawned sub-agent runs one scoped task inside its own Datatailr
  batch workflow, operates on a shared git repository, and hands its result back
  to the agent that created it.
- **A coordinator** that spawns, tracks, and harvests sub-agent runs via the
  `Workflow` API, folding their reports back into the originating `pi` session.
- **Full Datatailr platform knowledge** — a bundled `pi` extension
  (`pi_extension/datatailr-system-builder`) plus skills and prompts that teach
  the agent how to scaffold, build, and deploy Datatailr Apps, Services,
  Workflows, and Excel add-ins, and how to inspect job status and logs.
- **Bounded, safe operation** — per-turn timeouts, workflow `fail_after`, token/
  cost budgets, turn caps, recursion-depth and fan-out limits, and a watchdog.
  Sub-agents may open/push PRs but **never merge**.

## Architecture

```
   Browser / API  ───────►  MAIN AGENT  (Datatailr App, framework=fastapi)
   (user)                 │  - pi in-container (PTY terminal + /chat)     │
                          │  - coordinator: spawn / track / harvest       │
                          │  - run registry (Blob/KV)                     │
                          └───────┬───────────────────────▲──────────────┘
                                  │ launch workflow run    │ poll runs() / result()
                                  │ write assignment.json  │ read result.json
                                  ▼                        │
   Blob storage  ◄─────────  agent_runs/<subagent_id>/  ───┘
   (assignments + results)          ▲
                 ┌──────────────────┘
                 │  SUB-AGENT (batch @workflow run, unique name)
                 │   1. bootstrap git (SSH key from Secrets, clone)
                 │   2. read assignment.json
                 │   3. run pi --mode json on the brief
                 │   4. commit / push / open PR (optional)
                 │   5. write result.json + return final reply
                 └───────────────────────────────────────────
```

- **Main agent = `App`, not a `Service`** — the interactive terminal needs a
  WebSocket, and Datatailr forwards WebSocket upgrades through the public app
  ingress but not through internal service-to-service routing, so the agent
  runtime must live in the App.
- **Sub-agent = batch `@workflow`, not a `Service`** — a sub-agent does one
  scoped job and exits; batch runs complete, cache a result, and stop.

## Folder layout

```text
agent_deployment/
  deploy_app.py             # deploy the main agent (SWE Main Agent App)
  deploy_callback.py        # deploy the optional wake-up callback service
  specification.md          # the full build-and-operate contract
  screenshot.png            # UI preview
  metadata.json

  agent_app/                # main agent implementation (the App)
    app.py                  # FastAPI app: terminal, /chat, /subagents
    coordinator.py          # spawn / track / harvest sub-agents
    pi_runner.py            # runs pi in JSON mode
    pty_runner.py           # wires the xterm.js terminal to a pi PTY
    sessions.py             # per-user session + workspace isolation
    spawn_tool.py           # the spawn_subagent tool exposed to pi
    blob_sync.py            # Blob-backed session persistence
    monitor_tool.py         # run-status inspection tool
    build_script_pre.sh     # installs Node + pi CLI, git, gh into the image
    requirements.txt
    agent_common/           # shared code (git bootstrap, briefing, orchestration)
    pi_extension/           # pi package: Datatailr platform knowledge + skills
    subagent/               # sub-agent workflow (build.py, run_subagent.py)

  agent_callback/           # optional low-latency completion callback service
    app.py
    requirements.txt
```

## Configuration

Create these in the Datatailr Secrets Manager and KV store before deploying.
**Secrets can only be created in the Secrets Manager UI** — jobs only read them.

### Secrets (encrypted, read-only at runtime)

| Secret key | Purpose |
|---|---|
| `agent_git_ssh_key` | Private SSH deploy key for cloning/pushing the repo. |
| `agent_git_token` | Git-host API token (e.g. GitHub PAT) for opening PRs. |
| `openai_api_key` | Model provider key for `pi`. |

Scope the SSH deploy key to write access on the target repo only, and limit the
PR token to `contents:write` + `pull_requests:write`. **Never** log or echo
secret values.

### KV (plain-text config, read-only at runtime)

| KV key | Example | Purpose |
|---|---|---|
| `agent_git_repo_url` | `git@github.com:org/repo.git` | Repository every agent clones. |
| `agent_git_default_branch` | `main` | Branch sub-agents branch off from. |
| `agent_git_known_hosts` | (host key) | Pinned SSH host key for verification. |
| `agent_model` | `openai/gpt-5.1` | Default `pi` model. |
| `agent_limits` | JSON (see `specification.md` §11) | Timeouts, max turns/depth, fan-out, cost ceiling. |

## Deploy

Run from this folder with the project venv active so the `dt` CLI is on `PATH`.

```bash
cd agent_deployment

# (optional) low-latency wake-up service — deploy first so its hostname exists
python deploy_callback.py

# the main agent App
python deploy_app.py
```

The callback service is optional: the system works poll-only without it. Ship it
if sub-agent harvest latency matters.

## Learn more

- `specification.md` — the complete build-and-operate contract: architecture,
  configuration, coordinator lifecycle, workflow naming, assignment/result
  schemas, limits, and safety rules.
- `agent_app/pi_extension/README.md` — the bundled `pi` package that gives the
  agent guided Datatailr system-building capabilities.
