# LLM service on Datatailr: design

## Goal

This example serves an open weights LLM (a language model whose weight files
are public) as a Datatailr service. The service speaks the OpenAI API, so a
client can switch between OpenAI and this service by changing the base URL.

## Requirements

- The service serves one model from Hugging Face at a time.
- An API call sets the model. Another API call lists the available models.
- The service does not download the weights at each start or model switch. It
  keeps them in Datatailr blob storage.
- The service writes an audit log: who made each request, and with what prompt.
  Only an admin can read it.
- An admin can read usage metrics for each user, for example tokens used.
- The service serves several users at the same time.
- The service runs on a GPU compute node on an Azure installation. The operator
  starts that node manually.
- Clients run inside Datatailr (jobs, workstations, notebooks) and outside
  (OpenAI SDK on a laptop, other OpenAI-compatible tools).

## Architecture

The example is one Datatailr `Service`. Two processes run in its container:

1. The gateway, a FastAPI application on the port that the platform assigns.
2. The engine, `vllm serve`, a child process of the gateway on `127.0.0.1`.

vLLM is an open source inference server. It implements the OpenAI API and
batches concurrent requests on the GPU. The gateway forwards `/v1/*` to vLLM
and adds the parts that vLLM does not have: user identity, the audit log, the
usage metrics, the weight cache and the model switch.

```
client -> platform edge -> gateway (FastAPI) -> vLLM (127.0.0.1) -> GPU
                              |
                              +-> blob storage: weights, audit log, state
```

## Folder layout

```
llm_service/
├── README.md
├── metadata.json
├── deploy.py              # builds and deploys the Service and the admin app
├── requirements.txt       # runtime: fastapi, uvicorn, httpx, huggingface_hub, vllm
├── requirements-dev.txt   # tests: pytest, fastapi, httpx (no vllm, no GPU)
├── docs/
│   ├── design.md          # this document
│   └── admin-app-design.md
├── admin_app/             # the admin app: FastAPI backend and one page
├── gateway/
│   ├── app.py             # FastAPI routes and main(port)
│   ├── config.py          # configuration from environment variables
│   ├── identity.py        # user from X-Datatailr-User, admin check
│   ├── audit.py           # audit records, JSONL files in blob storage
│   ├── usage.py           # usage totals by user, model and day
│   ├── weights.py         # local disk and blob storage weight cache
│   ├── engine.py          # start, stop and health of the vLLM process
│   ├── models.py          # active model state and the switch state machine
│   └── proxy.py           # forward requests to vLLM, capture token counts
└── tests/
```

Each module has one purpose. `app.py` connects them. `engine.py` and the blob
storage client sit behind small interfaces, so the tests replace them with
fakes.

## Endpoints

### OpenAI API, open to every user with access to the service

| Endpoint | Behavior |
|---|---|
| `POST /v1/chat/completions` | Forwarded to vLLM, streaming included. |
| `POST /v1/completions` | Forwarded to vLLM, streaming included. |
| `POST /v1/embeddings` | Forwarded to vLLM. Works only if the active model is an embedding model. |
| `GET /v1/models` | The active model, the loading model and each model in the blob storage cache, in the OpenAI list format. See below. |
| `GET /v1/models/{id}` | One entry of that list. The id contains `/`, for example `GET /v1/models/Qwen/Qwen2.5-1.5B-Instruct`. An unknown id gets 404 with code `model_not_found`. |
| `GET /v1/usage/me` | The usage totals of the caller. Not part of the OpenAI API. |

The model list has one entry for each model id. Each entry has the OpenAI
fields `id`, `object`, `created` and `owned_by`, and four extra fields:

- `status`: `loading` for the model that loads, `active` for the model that
  vLLM serves now, and `cached` for a model that is only in blob storage. If one id has two
  states, `loading` comes before `active`, and `active` comes before `cached`.
- `revision`: the loading or active revision. For a cached model, it is the
  first cached revision.
- `revisions`: the sorted list of the revisions in blob storage.
- `size_bytes`: the sum of the file sizes in the manifests of the cached revisions, or null if the model has no cached revision.

Errors use the OpenAI error body: `{"error": {"message", "type", "code"}}`.

- If a request names a model that vLLM does not serve now, the gateway returns
  404 with code `model_not_found`. A request without a model gets the model
  that vLLM serves.
- If vLLM serves no model and a model is loading, the gateway returns 503 with
  code `model_loading` and a `Retry-After` header.
- If no model is loaded, the gateway returns 503 with code `no_model` and no
  `Retry-After` header. An admin must switch the model first.
- If the request has no platform user, the gateway returns 401.

### Admin endpoints

| Endpoint | Behavior |
|---|---|
| `POST /admin/model` | Body `{"model": "org/name", "revision": "main"}`. Starts a switch and returns 202. `revision` is optional. |
| `GET /admin/model` | The active model, the served model, the switch state and the last switch error. |
| `GET /admin/audit` | Audit entries, newest first. Filters: `user`, `model`, `since`, `until`, `limit`, `offset`. |
| `GET /admin/audit/users` | The users in the log, with the first and last request time of each. |
| `GET /admin/usage` | Usage totals. Filters: `since`, `until`, `user`, `model`. `group_by` is `user` (default), `model` or `day`. |
| `GET /admin/usage/{user}` | The totals of one user, by model and by day. |
| `DELETE /admin/models/{model}` | Removes the cached revisions of a model from blob storage. The query parameter `revision` limits it to one revision. Returns 200 with `{"deleted": [...]}`. The errors are 404 `model_not_found`, 409 `model_in_use` and 400 `invalid_model`. One audit record has the event `model_delete`. |

A caller who is not an admin gets 403.

The admin app calls these endpoints with the header `X-LLM-Acting-User`. See [Acting user](#acting-user). The app is described in [admin-app-design.md](admin-app-design.md).

### Platform endpoints

`GET /health` returns `OK` when the gateway runs, also during a model switch,
so the platform does not restart the service while a model loads.
`GET /openapi.json` is the FastAPI specification.

## Identity and admin

The platform adds a signed `X-Datatailr-User` header to each request, at the
edge for outside clients and through the auth proxy for jobs inside the
platform. The gateway reads the user name from that header. The platform checks
the service ACL before the request reaches the gateway.

An admin is one of:

- the user who deployed the service (the `DATATAILR_USER` of the container),
- a member of the platform group named in `LLM_ADMIN_GROUP`, if it is set.

The gateway caches the group members for 60 seconds. The Datatailr SDK also
caches group lookups for 10 minutes. A change of the group members can
therefore take up to 10 minutes to apply.

### Acting user

The `/admin/*` endpoints read the header `X-LLM-Acting-User`. The signed
caller must be an admin. If the header names a user, that user must be an admin
too, or the answer is 403 `admin_required`. The audit records of a model switch
and a model delete name the acting user. A name that is too long or has a
control character gets 400 `invalid_acting_user`. Other endpoints ignore the
header.

### Outside clients

The platform edge accepts an API token as the `__datatailr_user_cookie__`
cookie, not as `Authorization: Bearer`. The README shows this form:

```python
client = OpenAI(
    base_url="https://<domain>/<service path>/v1",
    api_key="unused",
    default_headers={"Cookie": "__datatailr_user_cookie__=<token>"},
)
```

A tool that accepts only a base URL and an API key does not work until the
platform edge accepts a Bearer token. That change belongs to the platform and is not part of this example.

## Weights

Blob storage layout, under a root set by `LLM_BLOB_ROOT` (default `llm-service`):

```
<root>/models/<org>--<name>/<revision>/<files>
<root>/models/<org>--<name>/<revision>/.complete    # the file list, written last
<root>/state/active.json                             # {"model", "revision"}
<root>/audit/YYYY-MM-DD/<time>-<id>.jsonl
<root>/usage/YYYY-MM-DD.json                         # totals of one finished day
```

To get a model on local disk, `weights.py` does these steps:

1. If the local directory has a `.complete` marker, use it.
2. If blob storage has a valid `.complete` marker, download the files that the
   marker lists. Check the size of each file. Then write the local marker.
3. If not, download the repository from Hugging Face with `huggingface_hub`,
   upload each file to blob storage, then write `.complete`.

The blob storage marker is a JSON manifest of the files:

```json
{"files": [{"path": "config.json", "size": 663}, {"path": "model.safetensors", "size": 3087467144}]}
```

The marker is written last, so an interrupted upload is not used as a cache
entry. A download from blob storage gets exactly the files in the manifest.
It does not list the blob directory, so stale files of an older upload are not
used. If a file is missing or has the wrong size, the download stops with an
error that names the file. The gateway then removes the partial local copy and
writes no local marker. A marker that is empty or is not valid JSON counts as
"not in blob storage". The gateway then downloads the model from Hugging Face
again and writes a new marker.

A Hugging Face token for gated models comes from the platform secret store,
under the key named in `LLM_HF_TOKEN_KEY`. The token is optional. If the
gateway cannot read the secret, it logs an error with the key and the
environment, and it continues without a token. Public models still load.

`GET /v1/models` lists the directories that have a `.complete` marker.

After a successful load, the gateway deletes the local copies of the other
models in a worker thread, so the node disk does not fill up. It never deletes
the active model or a model that loads. The blob storage copies stay.

## Model switch

`models.py` holds one state: `empty`, `loading`, `ready` or `failed`. It also
holds three models:

- `active`: the model of the last successful load. `state/active.json` stores
  it.
- `target`: the model that loads now.
- `serving`: the model that vLLM serves now. It is empty when vLLM serves no
  healthy model.

The gateway forwards a `/v1/*` request only when `serving` is set, and only for
the model in `serving`. `GET /admin/model` returns all three models.

A switch runs these steps. Steps 2 and 3 are the two phases of a switch.

1. `POST /admin/model` validates the model name, sets the state to `loading`
   and starts a background task. A second switch during `loading` gets 409.
2. The fetch phase. The task gets the weights on local disk. The old vLLM
   process still runs, so the old model keeps serving requests. A request for
   the new model gets 404 during the fetch.
3. The restart phase. The task clears `serving` and stops the old vLLM process.
   Then it starts vLLM on the new directory with
   `--served-model-name <org/name>`. Requests get 503 with code
   `model_loading` and a `Retry-After` header. A request that is still running
   when vLLM stops does not complete.
4. The task waits for the vLLM health endpoint. Then it sets the state to
   `ready`, sets `serving` to the new model and writes `state/active.json`.
5. If the download fails, the old model keeps serving. If vLLM fails to start
   with the new model, the task starts the previous model again. In both cases
   the task stores the error. `GET /admin/model` shows it.

At startup, the gateway loads the model in `state/active.json`. If that file
does not exist, it loads `LLM_DEFAULT_MODEL`. A model switch is also an audit
event.

If the write of `state/active.json` fails, the gateway logs a warning. The
model stays `ready`, because the engine runs.

### Engine exit

The gateway checks the vLLM process at each request to
`/v1/chat/completions`, `/v1/completions` and `/v1/embeddings`. If the process has
exited while `serving` is set, the gateway clears `serving`. If the state is
`ready`, the gateway also starts one automatic reload of the active model. The
reload uses the startup load path and runs as a background task. The state is
`loading` during the reload. The audit log gets an `engine_reload` event with
the user `system`.

If the process exits during the fetch phase of a switch, the gateway starts no
reload. The switch goes on and starts vLLM with the new model.

If the reload fails, the state is `failed`. It stays `failed` until an admin
switches the model with `POST /admin/model`. The gateway makes no second
automatic attempt until a model is `ready` again.

### Transitions

The table shows the state and `serving` after each event. A and B are two
models. A request gets an answer from vLLM only when `serving` is set.

| Event | State | `serving` |
|---|---|---|
| Startup load begins | `loading` | empty |
| Startup load succeeds | `ready` | A |
| At startup, the download fails | `empty` | empty |
| At startup, vLLM fails to start | `failed` | empty |
| Admin switch to B begins (fetch phase) | `loading` | A |
| Download of B succeeds, the task stops vLLM (restart phase) | `loading` | empty |
| vLLM serves B | `ready` | B |
| Download of B fails | `ready` | A |
| Start of B fails, the restart of A succeeds | `ready` | A |
| Start of B fails, the restart of A fails | `failed` | empty |
| vLLM exits in `ready`, the automatic reload begins | `loading` | empty |
| The automatic reload succeeds | `ready` | A |
| The automatic reload fails | `failed` | empty |
| vLLM exits during the fetch phase of a switch to B | `loading` | empty |
| After that, vLLM serves B | `ready` | B |
| After that, the download of B fails | `failed` | empty |

## Audit log

Each request to `/v1/chat/completions`, `/v1/completions` and
`/v1/embeddings` produces one record. Each model load produces one record too.
Other requests produce no record. A request record looks like this:

```json
{"ts": "2026-11-10T12:00:00.123Z", "request_id": "…", "user": "alice",
 "endpoint": "/v1/chat/completions", "model": "org/name", "stream": true,
 "prompt": {"messages": [...]}, "status": 200,
 "prompt_tokens": 42, "completion_tokens": 128, "total_tokens": 170,
 "duration_ms": 950}
```

`prompt` holds the `messages`, `prompt` or `input` field of the request, in
full. The log does not store the model answer.

The gateway writes the record after the response ends, so it holds the status
and the token counts. Records go to an in-memory buffer. A flusher task wakes
every 0.25 seconds. It flushes the buffer to a new JSONL file in blob storage
when 5 seconds have passed since the last flush, or when the buffer holds 100
records. It also flushes at shutdown. After a failed flush, it waits the full 5
seconds before the next attempt. Each file is written once, so no file is ever
appended to or rewritten.

In normal operation, a kill of the container loses the records of at most
about the last 5 seconds. During a blob storage outage, the records wait in
memory. A kill then loses all records since the outage began. The README
states this.

`GET /admin/audit` reads the files of the days in the requested range, plus the
buffer, and applies the filters. It reads the days newest first, and it stops
when it has `offset + limit` matching records. During a flush, the records
that the flush writes stay visible to queries. A query shows each record once.

A stored line that is not a JSON object with a valid `ts` and a string `user`
is skipped with a warning.

## Usage metrics

The usage totals come from the audit records. There is no second store.

`usage.py` keeps totals in memory with the key `(day, user, model)`: request
count, failed request count, prompt tokens, completion tokens, total tokens and
last request time. Each new record updates them.

The container has no persistent disk, so the totals live in blob storage too.
When a day ends, the gateway writes the totals of that day to
`<root>/usage/YYYY-MM-DD.json`. At startup, the gateway reads those daily
files, and rebuilds only the days that have no file (normally the current day)
from the audit files. The startup time therefore does not grow with the size
of the audit log. The audit files stay the source of truth: if a daily file is
deleted, the gateway rebuilds it.

### Token counts for streaming requests

vLLM reports token counts for a streaming request only if the request asks for
them. For each streaming request, the gateway sets
`stream_options.include_usage` to true and reads the counts from the last
chunk. If the client did not ask for that chunk, the gateway removes it from
the stream, so the client gets the same stream as from OpenAI.

## Concurrency

The gateway is asynchronous and forwards requests with one shared `httpx`
client. vLLM batches the concurrent requests on the GPU. The gateway adds no
queue of its own. Audit writes and blob storage calls run in worker threads, so
they do not block request handling.

## Deployment

`deploy.py` builds the `Service` and the admin `App`. `python deploy.py service` or `python deploy.py app` deploys one of them. The script reads these environment variables:

| Variable | Default | Meaning |
|---|---|---|
| `LLM_SERVICE_NAME` | `LLM Service` | Service name. The internal host name comes from it. |
| `LLM_DEFAULT_MODEL` | `Qwen/Qwen2.5-1.5B-Instruct` | Model loaded at the first start. |
| `LLM_VRAM` | `16g` | GPU memory of the container. |
| `LLM_MEMORY` | `24g` | Memory of the container. |
| `LLM_CPU` | `4` | CPU count of the container. |
| `LLM_ADMIN_GROUP` | not set | Platform group whose members are admins. |
| `LLM_ACCESS_GROUP` | not set | Platform group that gets read and access permission. |
| `LLM_BLOB_ROOT` | `llm-service` | Root directory in blob storage. |
| `LLM_HF_TOKEN_KEY` | not set | Secret store key of the Hugging Face token. |
| `LLM_VLLM_ARGS` | not set | Extra arguments for `vllm serve`. |
| `LLM_ENVIRONMENT` | `dev` | Platform environment. |
| `LLM_ADMIN_APP_NAME` | `LLM Service Admin` | Name of the admin app. |

Because the service sets `vram`, the platform does not start a node for it. The
operator starts a GPU node manually. The README gives the steps.

## Testing

The unit tests run without a GPU and without vLLM. A fake engine (a small
FastAPI application that answers like vLLM) and an in-memory blob store replace
the real ones. The tests cover:

- identity and the admin check,
- audit records, the flush, and the audit filters,
- usage totals and the rebuild at startup,
- the weight cache: local hit, blob storage hit, Hugging Face download, an
  interrupted upload, the manifest check, and the deletion of unused local
  copies,
- the switch state machine, the model that serves in each phase, the
  rollback, the automatic reload, and the 409, 503 and 404 answers,
- the proxy, with and without streaming, and the token count chunk.

The run on a real GPU happens on the Azure installation, after the deploy.

## Known risk

It is not confirmed that a Datatailr job container on an Azure GPU node exposes
CUDA to the vLLM wheels. The first deploy answers this. If vLLM does not find
the GPU, the fix is in the image build script, and the design stays the same.

## Out of scope

- More than one model in GPU memory at the same time.
- A model switch triggered by the `model` field of a normal request.
- Storage of model answers in the audit log.
- Rate limits and quotas for each user.
- Bearer token support at the platform edge.
- A web interface for the admin endpoints.
