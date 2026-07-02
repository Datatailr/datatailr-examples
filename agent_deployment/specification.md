# Agent Deployment Specification

A software‑development **agentic flow** deployed on the Datatailr platform. A
long‑running **main agent** (the user's point of contact) runs the `pi` coding
agent in‑container and can **spawn sub‑agents** — additional `pi` instances that
each run inside a Datatailr batch job, execute one scoped task against a git
repository, and report their result and status back to the agent that created
them.

This document is the build‑and‑operate contract for every component: what to
deploy, how the pieces talk to each other, and the rules that keep the system
bounded and safe. It builds on the existing `agent_app/` implementation (the
"Pi Agent UI" FastAPI App) and the Datatailr SDK primitives (`App`, `@workflow`,
`Secrets`, `KV`, `Blob`, `Workflow`).

---

## 1. Overview & goals

| Goal | How it is met |
|---|---|
| A conversational main agent that users talk to | FastAPI `App` running `pi` in‑container (existing `Pi Agent UI`), exposing a terminal, `/chat`, and `/chat/stream`. |
| Delegate scoped work to parallel sub‑agents | Main agent launches uniquely‑named single‑task `@workflow` runs, each running `pi` headless (`pi --mode json`). |
| All agents operate on a shared git repository | Every agent container clones the repo on startup using an SSH deploy key retrieved from the Secrets Manager. |
| Results flow back to the creator | Sub‑agents write a structured `result.json` to Blob storage; the main agent's **coordinator** polls run state via the `Workflow` API and folds reports back into the originating `pi` session. |
| Work is always bounded | Per‑turn timeouts, workflow `fail_after`, token/cost budgets, turn caps, recursion‑depth and fan‑out limits, and a watchdog that stops runaway runs. |

### Non‑goals

- Auto‑merging pull requests. Sub‑agents may open/push PRs but **never merge**;
  merge is a human (or explicitly authorized) decision.
- Cross‑user work sharing. Sessions, workspaces, and git checkouts are isolated
  per authenticated user, consistent with the current app.

---

## 2. Terminology

| Term | Meaning |
|---|---|
| **Main agent** | The deployed `App` ("SWE Main Agent"). Long‑running; the user's point of contact. Owns orchestration. |
| **Sub‑agent** | One batch `@workflow` run executing a single `pi` task with a specific brief. Terminates when the task finishes. |
| **Assignment** | The JSON brief the main agent writes for a sub‑agent (task, repo, branch, constraints, budget, depth). |
| **Report / result** | The JSON document a sub‑agent writes back (status, summary, final reply, git/PR info, usage). |
| **Coordinator** | The orchestration module inside the main agent that spawns, tracks, and harvests sub‑agents. |
| **Run registry** | A small Blob/KV index recording every spawned sub‑agent (name, run id, parent, state). |

---

## 3. Architecture overview

```
                         ┌──────────────────────────────────────────┐
   Browser / API  ───────►  MAIN AGENT  (Datatailr App, framework=fastapi)
   (user)                 │  - pi in-container (PTY terminal + /chat) │
                          │  - coordinator: spawn / track / harvest   │
                          │  - run registry (Blob/KV)                 │
                          └───────┬───────────────────────▲──────────┘
                                  │ launch workflow run    │ poll runs() / result()
                                  │ write assignment.json  │ read result.json
                                  ▼                        │
   Blob storage  ◄─────────  agent_runs/<subagent_id>/  ───┘
   (assignments + results)          ▲   ▲
                                    │   │ write result.json
                 ┌──────────────────┘   │
                 │  SUB-AGENT (batch @workflow run, unique name)
                 │  single task container:
                 │   1. bootstrap git (SSH key from Secrets, clone)
                 │   2. read assignment.json
                 │   3. run pi --mode json on the brief
                 │   4. commit / push / open PR (optional)
                 │   5. write result.json + return final reply
                 └──────────────────────────────────────────────
   Secrets Manager:  agent_git_ssh_key, agent_git_token, openai_api_key
   KV:               agent_git_repo_url, agent_model, agent_limits
```

Why these component types:

- **Main agent = `App`, not a `Service`.** The interactive terminal needs a
  WebSocket, and Datatailr forwards WebSocket upgrades through the public **app**
  ingress but **not** through internal service‑to‑service routing (documented in
  `agent_app/app.py`). The agent runtime must therefore live in the App.
- **Sub‑agent = batch `@workflow`, not a `Service`.** A sub‑agent does one
  scoped job and exits. Services auto‑restart on exit, which is wrong for a
  one‑shot task; batch runs complete, cache a result, and stop.
- **A sub‑agent is a single‑task workflow.** Datatailr tasks each run in their
  own container with no shared filesystem, so "clone → run pi → push/PR → report"
  must happen inside **one** task (cloning in a separate task would not persist).

---

## 4. Shared configuration (Secrets, KV, Blob)

Create these before deploying. **Secrets can only be created in the Datatailr
Secrets Manager UI** — ask the operator to create them; jobs only read them.

### Secrets (encrypted, read‑only at runtime)

| Secret key | Purpose |
|---|---|
| `agent_git_ssh_key` | Private SSH **deploy key** for cloning/pushing the repo. |
| `agent_git_token` | Git‑host API token (e.g. GitHub PAT / fine‑grained token) for opening PRs via `gh`/REST. |
| `openai_api_key` | Model provider key for `pi` (already used by `agent_app`). |

> The SSH deploy key must have write access **only** to the target repository,
> scoped as narrowly as the host allows. The PR token should be limited to
> `contents:write` + `pull_requests:write` on that repo.

### KV (plain‑text config, read‑only at runtime)

| KV key | Example | Purpose |
|---|---|---|
| `agent_git_repo_url` | `git@github.com:org/repo.git` | Repository every agent clones. |
| `agent_git_default_branch` | `main` | Branch sub‑agents branch off from. |
| `agent_model` | `openai/gpt-5.1` | Default `pi` model (already used). |
| `agent_limits` | JSON (see §11) | Central budgets: timeouts, max turns, max depth, fan‑out, cost ceiling. |

`KV().get()` may return a string or a parsed object and **raises** if missing —
always wrap in try/except and `json.loads` when a string is returned (per the
kv‑and‑secrets skill).

### Blob layout (shared handoff medium)

```
agent_runs/<subagent_id>/assignment.json    # written by main agent (the brief)
agent_runs/<subagent_id>/result.json        # written by sub-agent (the report)
agent_runs/<subagent_id>/logs/              # optional artifacts (patches, diffs)
agent_registry/<parent_id>.json             # index of children per parent
```

`<subagent_id>` is the stable identifier described in §8. Blob is the primary
handoff channel because it needs no assumptions about network reachability
between an App and a batch container.

---

## 5. Git repository access (shared bootstrap)

Every agent — main and sub — runs the same bootstrap on startup. Package it as a
shared module (e.g. `agent_common/git_bootstrap.py`) imported by both the App
and the sub‑agent workflow.

**Image build (`build_script_pre`)** must install git + SSH client alongside the
existing Node/`pi` install:

```bash
apt-get update
apt-get install -y --no-install-recommends git openssh-client
# (GitHub CLI, if used for PRs)
# curl -fsSL https://cli.github.com/... && apt-get install -y gh
```

**Runtime bootstrap:**

```python
from datatailr import Secrets, KV

def bootstrap_git(workdir: str) -> str:
    key = Secrets().get("agent_git_ssh_key")          # never logged
    ssh_dir = os.path.expanduser("~/.ssh")
    os.makedirs(ssh_dir, mode=0o700, exist_ok=True)
    key_path = os.path.join(ssh_dir, "id_ed25519")
    with open(key_path, "w") as fh:
        fh.write(key if key.endswith("\n") else key + "\n")
    os.chmod(key_path, 0o600)
    # Pin host key verification (store the host key in KV, don't disable checking).
    known_hosts = KV().get("agent_git_known_hosts")
    with open(os.path.join(ssh_dir, "known_hosts"), "w") as fh:
        fh.write(known_hosts)
    os.environ["GIT_SSH_COMMAND"] = f"ssh -i {key_path} -o IdentitiesOnly=yes"

    repo_url = KV().get("agent_git_repo_url")
    subprocess.run(["git", "clone", "--depth", "50", repo_url, workdir], check=True)
    return workdir
```

Rules:

- **Never** log, print, or echo the SSH key or PR token (kv‑and‑secrets safety
  rule). Describe, don't display.
- Prefer pinned `known_hosts` over `StrictHostKeyChecking=no`.
- The **main agent** clones into each user's isolated workspace
  (`PI_WORKSPACE_DIR/<user>`), matching current per‑user isolation.
- Each **sub‑agent** clones a fresh checkout into the task container's working
  directory and immediately creates its own working branch (see §10).

---

## 6. Main agent (App)

Extends the existing `agent_app` ("Pi Agent UI") — reuse `pi_runner`,
`pty_runner`, `sessions`, `blob_sync`, and the single‑page UI as‑is. Add:

1. **Git‑aware workspace.** On first use per user, `bootstrap_git` clones the
   repo into that user's workspace so the main `pi` can read/edit code directly.
2. **A spawn tool exposed to `pi`.** Register a `pi` skill/tool
   `spawn_subagent(task, files=None, branch=None, budget=None)` that calls the
   coordinator. This is how the main agent delegates. (Implemented as a small
   `pi` agent‑skill or a bash‑callable helper on `PATH`.)
3. **The coordinator module** (§7).
4. **Orchestration endpoints** for the UI/coordinator:
   - `POST /subagents` — spawn one or more sub‑agents (body: list of briefs).
   - `GET  /subagents` — list this user's sub‑agents and their live state.
   - `GET  /subagents/{id}` — one sub‑agent's assignment + latest report.
   - `POST /subagents/{id}/stop` — cooperative stop (§11).

Rename the deployed app to **`SWE Main Agent`** (keep `deploy_app.py` structure;
only `name` and orchestration wiring change).

---

## 7. Coordinator (orchestration inside the main agent)

The coordinator owns the sub‑agent lifecycle and is the only place that talks to
the `Workflow` API.

**Spawn** (`coordinator.spawn(parent_id, brief) -> subagent_id`):

1. Allocate a `subagent_id` and a **workflow name** (§8).
2. Enforce limits **before** launching: depth, per‑parent fan‑out, global active
   count, remaining budget (§11). Refuse (and tell the parent `pi`) if exceeded.
3. Write `agent_runs/<subagent_id>/assignment.json` to Blob (§9).
4. Build the sub‑agent workflow with that exact name and **launch a run** by
   calling the workflow function (the same "build‑then‑call" pattern used by
   `gas_curve_backtest`'s `build_regime_workflow` / `trigger_parent_run`). Pass
   only the small `subagent_id` as the task argument.
5. Append the child to `agent_registry/<parent_id>.json` (name, id, launch time,
   `state=launched`).

**Track & harvest** (background thread/task per user, or a single poller):

- Open each child with `Workflow(name=..., environment=..., get_existing=True)`
  and poll `runs(refresh=True)` / `run_details(run_id)` for state (`pending` →
  `running` → `completed`/`failed`/`failed_after`/`out_of_memory`/`stopped`).
- On terminal state, read `agent_runs/<subagent_id>/result.json` from Blob (and,
  as a fallback, `wf.result(run_id, task_name="run_subagent")`).
- **Fold the report back into the originating `pi` session**: call the local
  `pi_runner.run_pi(..., session_id=<parent session>)` with a synthesized user
  message summarizing the child's status, summary, and any PR link — so the
  main agent naturally continues the conversation with the delegated results.
- Update the registry to `state=reported`.

Environment resolution mirrors `workflow_io._environment()`
(`DATATAILR_JOB_ENVIRONMENT` → `Environment.DEV/PRE/PROD`).

---

## 8. Workflow naming (Q1 — concurrent sub‑agents)

Multiple sub‑agents can be spawned at the same instant, so names must be
**globally unique, deterministic, and recorded by the parent** for later lookup.

**Identifier:**

```
subagent_id = f"{parent_id}.{seq:03d}.{shortuuid8}"
# parent_id  = main session id (or the parent sub-agent's id, when nested)
# seq        = per-parent monotonic counter (ordering / readability)
# shortuuid8 = 8 hex chars — guarantees uniqueness across restarts, retries,
#              and simultaneous spawns even if seq collides
```

**Workflow display name** (this is the key the `Workflow` API looks up by):

```
name = f"SWE Sub-Agent — {slug(task_title)} [{subagent_id}]"
# e.g. "SWE Sub-Agent — add-retry-to-client [sess-9f2.007.a3b1c8d0]"
```

Rationale:

- Workflow **runs** are individually addressable by `run_id`, but each distinct
  concurrent sub‑agent gets its **own uniquely‑named workflow definition**, so
  parallel builds/launches never clobber one another's version history. This is
  the exact approach `gas_curve_backtest` uses (`Regime Sweep — {run_id} (...)`).
- The `shortuuid8` suffix removes any chance of collision when many spawn at
  once; the `parent_id`/`seq` prefix keeps names greppable and groupable.
- Because workflows are resolved by **exact display name**, the parent must
  **persist the exact name** it generated (in the run registry / assignment) so
  it can reopen the handle later — names are never re‑derived by guessing.
- Do **not** rely on hostname normalization here — that only matters for
  `Service` URLs; sub‑agents are looked up via the `Workflow` API, not HTTP.

**Alternative (documented, not default):** a single reusable `SWE Sub-Agent`
workflow launched many times, distinguished only by `run_id` + `subagent_id`
argument. Rejected as the default because concurrent re‑deploys of one
definition serialize on version builds and make per‑run naming/observability in
the UI harder; the unique‑name approach parallelizes cleanly.

---

## 9. Passing initial `pi` instructions (Q2)

Briefs can be large, multi‑line, and contain arbitrary code/markdown, so they
are **not** baked into workflow names or passed as bulky task arguments.

The main agent writes `agent_runs/<subagent_id>/assignment.json` to Blob and
passes **only the `subagent_id`** (a short string) as the workflow/task
argument. The sub‑agent reads and validates the assignment at startup.

**`assignment.json` schema:**

```jsonc
{
  "subagent_id": "sess-9f2.007.a3b1c8d0",
  "parent_id":   "sess-9f2",
  "created_by":  "alice",                 // owning user (isolation + run_as)
  "depth":       1,                        // recursion depth (root user req = 0)
  "task": {
    "title":       "Add retry to the HTTP client",
    "instructions":"<full pi prompt — the brief the sub-agent's pi receives>",
    "definition_of_done": [                // explicit, checkable exit criteria
      "requests wrapped with retry/backoff",
      "unit test added and passing"
    ],
    "context_files": ["src/client.py"]     // optional focus hints
  },
  "git": {
    "repo_url":     "git@github.com:org/repo.git",
    "base_branch":  "main",
    "work_branch":  "agent/add-retry-a3b1c8d0",  // pre-allocated, unique
    "may_push":     true,
    "may_open_pr":  true
  },
  "budget": {                              // per-sub-agent overrides of §11
    "max_turns":        20,
    "turn_timeout_s":   600,
    "wall_clock":       "45m",
    "max_cost_usd":     2.00,
    "max_child_agents": 0                  // 0 = this sub-agent may not spawn
  }
}
```

The sub‑agent's first `pi` prompt is assembled from `task.instructions` +
`definition_of_done` + repository/branch context + the operating rules (commit
message conventions, "never merge", how to report). Storing the brief as a
document (not a prompt string) also lets the coordinator reconstruct, audit, and
re‑issue an assignment on retry.

---

## 10. Sub‑agent lifecycle & response finalization (Q3)

Each sub‑agent is a **single‑task `@workflow`** (`run_subagent`) built with the
name from §8 and launched by the coordinator. The task, in one container:

1. **Bootstrap git** (§5): clone the repo, `git checkout -b <work_branch>` off
   `base_branch`.
2. **Load** `agent_runs/<subagent_id>/assignment.json`.
3. **Run `pi` headless** via `pi_runner.run_pi(...)` (`pi --mode json`) with the
   assembled brief, capturing the final reply, token usage, and the event
   stream. `pi` performs the edits/tests in the checkout.
4. **Persist work** (only if `git.may_push`):
   - Stage and commit with a conventional message
     (`agent(<subagent_id>): <title>`), including a trailer referencing the
     assignment.
   - Push the `work_branch`.
   - **PR handling** (only if `git.may_open_pr`): before opening, **check whether
     an open PR already exists for `work_branch`** (idempotency — protects
     against workflow retries). If not, open a PR via `gh`/REST using
     `agent_git_token`; capture number, URL, title, and CI status. If a PR
     already exists (e.g. the sub‑agent pushed additional commits to an existing
     branch/PR), **update** it and record the same PR reference. **Never merge.**
5. **Write** `agent_runs/<subagent_id>/result.json` (schema below) and **return**
   the final reply as the task's value (so it is also retrievable via
   `wf.result(run_id, "run_subagent")`).

**`result.json` schema:**

```jsonc
{
  "subagent_id": "sess-9f2.007.a3b1c8d0",
  "status": "succeeded",        // succeeded | failed | timed_out | stopped | blocked
  "reason": null,               // populated for non-success (e.g. "budget_exceeded")
  "summary": "Added urllib3 Retry; 1 test added; all tests pass.",
  "final_reply": "<pi's final assistant message>",
  "done_checklist": [           // self-assessment against definition_of_done
    {"item": "retry/backoff added", "met": true},
    {"item": "unit test added and passing", "met": true}
  ],
  "git": {
    "pushed": true,
    "branch": "agent/add-retry-a3b1c8d0",
    "base_branch": "main",
    "commits": ["e4c1a9b", "77f0d2e"],
    "pr": {                     // null if no PR was opened
      "action": "opened",       // opened | updated | none
      "number": 128,
      "url": "https://github.com/org/repo/pull/128",
      "title": "agent: add retry to HTTP client",
      "ci_state": "pending"
    }
  },
  "usage": { "input": 0, "output": 0, "totalTokens": 0, "cost": 0.0 },
  "turns": 7,
  "children": [],               // subagent_ids this sub-agent spawned (if allowed)
  "artifacts": ["agent_runs/sess-9f2.007.a3b1c8d0/logs/change.diff"]
}
```

**Transmission & finalization back to the creator:**

- **Primary (pull):** the coordinator detects terminal run state via the
  `Workflow` API and reads `result.json` from Blob. This needs no network path
  from the batch container to the App.
- **Optional (push, low‑latency):** the sub‑agent may additionally `POST` a
  compact `{subagent_id, status, pr_url}` notification to a lightweight
  **callback `Service`** (`SWE Agent Callback`, reachable at
  `http://swe-agent-callback` by internal hostname) which nudges the coordinator
  to harvest immediately instead of waiting for the next poll. Blob remains the
  source of truth; the callback is only a wake‑up.
- **Folding in:** the coordinator injects a synthesized user turn into the
  parent `pi` session summarizing status + PR link, so the main agent continues
  the conversation with the delegated outcome and can present the PR to the user.

**What the user sees for PR outcomes:**

- **Committed & pushed to an existing PR branch:** report shows
  `git.pr.action = "updated"`, commit SHAs, branch, and PR URL; the main agent
  tells the user "pushed N commits to PR #x".
- **Opened a new PR:** `git.pr.action = "opened"` with number/URL/title/CI state;
  the main agent surfaces the link and can optionally chain a review sub‑agent.
- **Pushed a branch without a PR:** `pushed = true`, `pr = null`; the main agent
  offers to open one.
- Merge is always deferred to a human unless the operator has explicitly
  authorized auto‑merge for a given assignment.

---

## 11. Stop conditions & preventing indefinite work (Q4)

Bounds are enforced at three layers; a task cannot run forever even if `pi`
misbehaves.

### Central limits (`KV: agent_limits`)

```jsonc
{
  "per_turn_timeout_s": 600,     // PI_TIMEOUT_SECONDS — hard ceiling per pi turn
  "max_turns": 25,               // max pi turns per sub-agent
  "wall_clock": "45m",           // workflow fail_after
  "max_cost_usd": 3.0,           // abort when accumulated usage.cost exceeds this
  "max_depth": 2,                // sub-agents may nest at most this deep
  "max_children_per_agent": 8,   // fan-out cap per parent
  "max_active_global": 40,       // concurrent sub-agents across the system
  "max_total_per_request": 30    // sub-agents spawned per originating user request
}
```

### Enforcement points

1. **Per `pi` turn:** `PI_TIMEOUT_SECONDS` already hard‑caps a single turn in
   `pi_runner` (`subprocess.run(..., timeout=...)`).
2. **Turn / iteration cap:** the sub‑agent counts `pi` turns and stops with
   `status="stopped", reason="max_turns"` when `max_turns` is reached.
3. **Wall‑clock:** set `fail_after` on the sub‑agent `@workflow` (e.g. `"45m"`).
   Datatailr marks the run `failed_after`; the coordinator records a timeout.
4. **Cost/token budget:** accumulate `usage.cost` (already tracked in
   `pi_runner._accumulate_usage`); abort the run when `max_cost_usd` is exceeded.
5. **Recursion depth:** the assignment carries `depth`. A sub‑agent may spawn
   children only if `depth < max_depth` **and** its `budget.max_child_agents > 0`.
   Beyond that, the spawn tool refuses and instructs `pi` to finish directly.
6. **Fan‑out & global caps:** the coordinator refuses `spawn` when
   `max_children_per_agent`, `max_active_global`, or `max_total_per_request`
   would be exceeded, returning a clear message to the requesting `pi`.
7. **Definition of done as the primary stop signal:** the brief's
   `definition_of_done` gives `pi` an explicit, checkable target; the report's
   `done_checklist` records whether it was met. Convergence — not open‑ended
   exploration — is the goal.
8. **Idempotency / dedup:** the coordinator hashes `(parent_id, task.title,
   instructions)` and refuses to spawn a duplicate that is already active,
   preventing re‑spawn loops.
9. **Cooperative + hard stop:** `POST /subagents/{id}/stop` sets a stop flag the
   sub‑agent checks between turns (cooperative). A **watchdog** in the
   coordinator additionally issues `dt job stop <workflow-name>` for runs that
   exceed limits or are orphaned (hard stop). Mutating job commands are only
   issued by the system watchdog, never surfaced as a casual agent action.

### Main‑agent orchestration loop

The main agent must **converge**: once `max_total_per_request` is consumed for a
user request, further `spawn_subagent` calls are refused and `pi` is told to
summarize with the results already gathered. The coordinator never re‑spawns a
completed assignment except on explicit user instruction.

---

## 12. Deployment

Layout (extends the existing `agent_deployment/`):

```
agent_deployment/
├── agent_app/                 # main agent (existing; extended)
│   ├── app.py                 # + orchestration endpoints
│   ├── pi_runner.py           # reused as-is
│   ├── pty_runner.py          # reused as-is
│   ├── sessions.py            # reused as-is
│   ├── blob_sync.py           # reused as-is
│   ├── coordinator.py         # NEW — spawn / track / harvest
│   ├── build_script_pre.sh    # + git, openssh-client (, gh)
│   └── requirements.txt
├── agent_common/              # NEW — shared by app & sub-agent
│   └── git_bootstrap.py
├── subagent/                  # NEW — sub-agent workflow package
│   ├── __init__.py
│   ├── run_subagent.py        # single @task: bootstrap → pi → push/PR → report
│   └── build.py               # build_subagent_workflow(subagent_id, name) -> @workflow
├── deploy_app.py              # deploys "SWE Main Agent" App
├── deploy_callback.py         # NEW — deploys optional "SWE Agent Callback" Service
└── specification.md           # this document
```

Notes tied to platform rules:

- **Packaging boundary:** only the entrypoint package directory is shipped to the
  remote. `agent_common/` and everything the sub‑agent imports must be importable
  from within the sub‑agent's packaged directory (vendor/copy `pi_runner` +
  `git_bootstrap` into `subagent/`, or make them a package included in the
  bundle). Use **absolute imports** and **underscores** in directory names.
- **Build scripts:** every image (App and sub‑agent workflow) needs the
  Node/`pi`/`fd`/`rg` install (existing `build_script_pre.sh`) **plus** `git` and
  `openssh-client`. The build image is minimal Debian — install everything
  explicitly; do not rely on cached layers.
- **Sub‑agent workflow parameters:** set `fail_after` (wall‑clock), `resources`
  (e.g. `Resources(memory="2g", cpu=1)`), `python_requirements`, `env_vars`
  (model, blob prefixes), and `build_script_pre`. Pass `run_as`/`acl` only if the
  operator requests access restrictions.

**Deploy order:**

1. Operator creates Secrets (`agent_git_ssh_key`, `agent_git_token`,
   `openai_api_key`) and KV (`agent_git_repo_url`, `agent_git_known_hosts`,
   `agent_git_default_branch`, `agent_model`, `agent_limits`) in the UI.
2. Deploy the (optional) callback service: `python deploy_callback.py`.
3. Deploy the main agent: `python deploy_app.py` (run with the project venv
   active so the `dt` CLI is on `PATH`).
4. The sub‑agent workflow is **not** pre‑deployed as a fixed job — the
   coordinator builds and launches a uniquely‑named workflow per spawn at
   runtime (build‑then‑call, like `gas_curve_backtest`). Its image is built on
   first launch and cached for subsequent runs.

---

## 13. Observability

- **Sub‑agent runs:** `dt job runs -f "name starts_with 'SWE Sub-Agent'"`,
  `dt job get <name> --json` (state/exit code), `dt log read <name> -r`
  (stderr), and the SDK `wf.runs()/run_details()/logs()/result()`.
- **Main agent:** existing per‑user activity dashboard (`/api/stats`) sourced
  from the `~/.pi` session store; extend with a **sub‑agent panel** listing each
  child's state, PR link, cost, and turns from the run registry.
- **Registry:** `agent_registry/<parent_id>.json` is the authoritative map from a
  conversation to its spawned sub‑agents and their outcomes.
- Never emit secret values to logs; the `pi` runtime already runs with
  `PI_OFFLINE=1` and no telemetry.

---

## 14. Security & isolation

- Per‑user isolation of sessions, workspaces, and git checkouts (as today);
  sub‑agents run `run_as` the requesting user where the platform supports it.
- Deploy key and PR token are least‑privilege and scoped to the single repo.
- Sub‑agents work on dedicated `agent/*` branches and never push to protected
  branches; no auto‑merge.
- Host‑key verification pinned via KV; `StrictHostKeyChecking=no` is disallowed.
- All sensitive values come from the Secrets Manager at runtime — nothing is
  baked into images, arguments, workflow names, or logs.

---

## 15. Open questions

- **Callback vs. poll‑only:** ship poll‑only first (simplest, no networking
  assumptions); add the callback service only if harvest latency matters.
- **Auto‑merge policy:** default is human‑merge; if the operator wants gated
  auto‑merge (e.g. after green CI + review sub‑agent), define the policy per
  assignment.
- **Result size:** very large diffs/artifacts belong in
  `agent_runs/<id>/logs/` (Blob), with `result.json` carrying references, not
  inline blobs.

---

## 16. Implementation checklist

- [ ] Operator creates all Secrets and KV keys (§4).
- [ ] `build_script_pre.sh` adds `git` + `openssh-client` (+ `gh`) to both images.
- [ ] `agent_common/git_bootstrap.py` clones via SSH key from Secrets, pinned
      host keys (§5).
- [ ] Main agent clones per‑user workspace and exposes `spawn_subagent` to `pi`.
- [ ] `coordinator.py`: unique naming (§8), assignment write (§9), build‑and‑
      launch, registry, poll/harvest, fold report into parent session (§7).
- [ ] `subagent/run_subagent.py`: bootstrap → load assignment → run `pi` →
      commit/push/PR (idempotent, never merge) → write `result.json` (§10).
- [ ] Limits enforced at all layers; watchdog + cooperative stop (§11).
- [ ] `deploy_app.py` (renamed `SWE Main Agent`) and optional
      `deploy_callback.py`.
- [ ] Sub‑agent panel + `dt`/SDK observability wired (§13).
