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
batches concurrent requests on the GPU. The gateway forwards `/v1/*` to vLLM.
It adds the parts that vLLM does not have: user identity, the audit log, the
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
│   ├── audit-storage-design.md
│   ├── admin-app-design.md
│   ├── limits-load-catalog-design.md
│   └── multi-service-app-design.md
├── admin_app/             # the admin app: FastAPI backend and one page
├── gateway/
│   ├── app.py             # FastAPI routes and main(port)
│   ├── config.py          # configuration from environment variables
│   ├── identity.py        # user from X-Datatailr-User, admin check
│   ├── audit.py           # audit buffer, flush, reads and the record lookup
│   ├── audit_layout.py    # file names, buckets and the split of a record
│   ├── audit_merge.py     # merges of segments and parts, prompt retention
│   ├── usage.py           # usage totals by user, model and day
│   ├── limits.py          # the limits file and the lookup of the limits of one user
│   ├── catalog.py         # the catalog file and the checks of its entries
│   ├── fit.py             # the GPU memory check: memory sizes and the rules of the answer
│   ├── limiter.py         # request window, token budget and parallel counter
│   ├── engine_metrics.py  # readers of the vLLM metrics endpoint and of nvidia-smi
│   ├── load.py            # live load: samples each 5 seconds, one point each minute
│   ├── metrics_store.py   # the history files in blob storage and the history read
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
| `GET /v1/usage/me` | The usage totals of the effective user. Not part of the OpenAI API. |
| `GET /v1/limits/me` | The limits, the current use and the source of the limits of the effective user, and `loaded`. Not part of the OpenAI API. |
| `GET /v1/service` | The descriptor of the service: `api`, `api_version`, name, environment, engine state, model, effective user, `admin` and `caller_admin`. The admin app uses it to find services. |

The model list has one entry for each model id. Each entry has the OpenAI
fields `id`, `object`, `created` and `owned_by`, and four extra fields:

- `status`: `loading` for the model that loads, `active` for the model that
  vLLM serves now, and `cached` for a model only in blob storage. If one id has two
  states, `loading` comes before `active`, and `active` comes before `cached`.
- `revision`: the loading or active revision. For a cached model, it is the
  first cached revision.
- `revisions`: the sorted list of the revisions in blob storage.
- `size_bytes`: the sum of the known sizes of the cached revisions, or null if no cached revision has a known size. The size of a revision comes from its manifest.

Errors use the OpenAI error body: `{"error": {"message", "type", "code"}}`.

- If a request names a model that vLLM does not serve now, the gateway returns
  404 with code `model_not_found`. A request without a model gets the model
  that vLLM serves.
- If vLLM serves no model and a model is loading, the gateway returns 503 with
  code `model_loading` and a `Retry-After` header.
- If no model is loaded, the gateway returns 503 with code `no_model` and no
  `Retry-After` header. An admin must switch the model first.
- If the request has no platform user, the gateway returns 401.
- If the user is over a limit, the gateway returns 429 with a `Retry-After` header. See [Limits](#limits).

### Admin endpoints

| Endpoint | Behavior |
|---|---|
| `POST /admin/model` | Body `{"model": "org/name", "revision": "main"}`. Starts a switch and returns 202. `revision` is optional. |
| `GET /admin/model` | The active model, the served model, the switch state and the last switch error. `rolling_back` is the model that a rollback loads now, or null. `download` is the last download without a switch, or null. |
| `GET /admin/audit` | Audit entries, newest first, without the prompt. Filters: `user`, `model`, `since`, `until`, `limit`, `offset`. |
| `GET /admin/audit/records/{request_id}?day=YYYY-MM-DD` | One full record with its prompt. `day` is the UTC day of the record time. The errors are 400 `invalid_time`, 404 `record_not_found` and 502 `prompt_unreadable`. |
| `GET /admin/audit/users` | The users in the log, with the first and last request time of each. |
| `GET /admin/usage` | Usage totals. Filters: `since`, `until` (days in the form `YYYY-MM-DD`), `user`, `model`. `group_by` is `user` (default), `model` or `day`. |
| `GET /admin/usage/{user}` | The totals of one user, by model and by day. |
| `GET /admin/limits` | The default limits, the user entries, and `loaded`. For each user who has an entry, use now or tokens today: the current use, and the effective limits with their source. |
| `PUT /admin/limits/default` | Replaces the default limits. The body has the three limit fields. |
| `PUT /admin/limits/users/{name}` | Sets the entry of one user. The body has one to three limit fields. |
| `DELETE /admin/limits/users/{name}` | Removes the entry of one user. The answer is 404 `limit_not_found` without an entry. |
| `GET /admin/catalog` | The catalog entries and `loaded`. Each entry has `cached` and the `fit` answer. |
| `PUT /admin/catalog` | Adds or replaces one entry. The body is the entry. The errors are 400 `invalid_catalog_entry`, 409 `catalog_full` and 502 `storage_unavailable`. One audit record has the event `catalog_change`. |
| `DELETE /admin/catalog` | Removes one entry. Query: `model` and `revision`. The errors are 400 `invalid_catalog_entry`, 404 `catalog_entry_not_found` and 502 `storage_unavailable`. One audit record has the event `catalog_change`. |
| `POST /admin/models/download` | Body `{"model": "org/name", "revision": "main"}`. Downloads the weights to the cache without a switch. Returns 202, or 200 with `"state": "cached"` for a pair in the cache. The errors are 400 `invalid_model`, 409 `download_in_progress`, 409 `model_in_use` and 500 `cache_check_failed`. One audit record has the event `model_download`. |
| `GET /admin/models/fit` | The GPU memory check of one model. Query: `model` and `revision`. The answer has `fits`, `needed_mib`, `available_mib`, `basis` and `available_source`. The error is 400 `invalid_model`. |
| `GET /admin/metrics/live` | The last sample of the load (`now`) and the last 60 points, from memory. |
| `GET /admin/metrics/history` | The points of a time range from the history files. Query: `since`, `until` (ISO times, at most 31 days apart). The answer has `step_seconds`, `points` and `missing_days`. The error is 400 `invalid_time`. |
| `DELETE /admin/models/{model}` | Removes the stored revisions of a model from blob storage, with or without a `.complete` marker. The query parameter `revision` limits it to one revision. Returns 200 with `{"deleted": [...]}`. The errors are 404 `model_not_found`, 409 `model_in_use` (also for the model that downloads), 409 `download_in_progress` (a download of another model runs now), 400 `invalid_model` and 500 `delete_failed`. One audit record has the event `model_delete`. |

A caller who is not an admin gets 403.

The admin app calls these endpoints with the header `X-LLM-Acting-User`. See [Acting user](#acting-user). The app is described in [admin-app-design.md](admin-app-design.md).

### Platform endpoints

`GET /health` returns `OK` when the gateway runs, also during a model switch,
so the platform does not restart the service while a model loads.
`GET /openapi.json` is the FastAPI specification.

## Identity and admin

The platform adds a signed `X-Datatailr-User` header to each request. It adds
the header at the edge for outside clients, and through the auth proxy for jobs
inside the platform. The gateway reads the user name from that header. The
platform checks the service ACL before the request reaches the gateway.

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
control character gets 400 `invalid_acting_user`.

The `/v1/*` endpoints of the table above read the header too, through the
dependency `effective_user`. The caller must be an admin, or the answer is 403
`acting_user_not_allowed`. The acting user must pass the access check, or the
answer is 403 `access_denied`. `GET /v1/service` never fails because of the
header. Usage numbers and audit records use the acting user. An audit record
with an acting user has the field `via` with the caller name. The rules are in
[multi-service-app-design.md](multi-service-app-design.md).

### Access check

The platform checks the access of the caller at its edge. It cannot check the
acting user, so the service does. An acting user has access if the user is an
admin or a member of the group in `LLM_ACCESS_GROUP`. Without that variable,
only admins have access. The gateway caches the group members for 60 seconds.
A failed lookup means no access, and the gateway tries again after 10 seconds. `deploy.py` passes `LLM_ACCESS_GROUP` to the
service container.

### Outside clients

On the URL of a job, the platform edge accepts two things from an outside client.
The first is the login cookie of the Datatailr CLI, `X-Datatailr-Oidc-Data`,
which `datatailr.get_remote_oidc_cookie_line()` returns after `datatailr login`.
The second is an API token of a user, only as the query argument
`__datatailr_user_cookie__`. The edge does not read a cookie with that name on
a job URL, and it does not read `Authorization: Bearer`. The README shows both
forms.

A tool that accepts only a base URL and an API key does not work until the
platform edge accepts a Bearer token. That change belongs to the platform and is not part of this example.

## Weights

Blob storage layout, under a root set by `LLM_BLOB_ROOT` (default `llm-service`):

```
<root>/models/<org>--<name>/<revision>/<files>
<root>/models/<org>--<name>/<revision>/.complete    # the file list, written last
<root>/state/active.json                             # {"model", "revision"}
<root>/audit/<day>/segments/<time>-<id>.jsonl        # metadata of one flush
<root>/audit/<day>/parts/<bucket>.jsonl              # metadata of one 10 minute bucket
<root>/audit/<day>/day.jsonl                         # metadata of one finished day
<root>/audit/<day>/prompts.marker                    # the day can have prompts, no rows
<root>/prompts/<day>/<time>-<id>.jsonl               # prompts of one flush
<root>/usage/<day>.json                              # totals of one finished day
<root>/usage/<day>.partial.json                      # checkpoint of the current day
<root>/limits.json                                   # the limits of the users, if an admin set any
<root>/catalog.json                                  # the model catalog, if an admin added an entry
<root>/metrics/<day>/<writer>.jsonl                  # load points of one process on one UTC day
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

After a successful load, a worker thread of the gateway deletes the local
copies of the other models. So the node disk does not fill up. It never deletes
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
   and starts a background task. A second switch during `loading` or during a
   rollback gets 409 `switch_in_progress`.
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
   with the new model, the task starts the previous model again. During this
   rollback, the state is `failed` and `rolling_back` names the previous model.
   In both cases the task stores the error. `GET /admin/model` shows it.

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
`loading` during the reload. The reload reads a complete local copy of the
active model without the lock of the weight cache, so a running download does
not hold it up. A rollback after a failed switch does the same. The audit log gets an `engine_reload` event with
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
| Start of B fails, the restart of A begins (`rolling_back` is A) | `failed` | empty |
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
 "duration_ms": 950, "ttft_ms": 310}
```

`ttft_ms` is the time from the start of the request to the first content of a stream. It is null for a request without a stream and for a request without content. A row of an older request has no such field.

`prompt` holds the `messages`, `prompt` or `input` field of the request, in
full. The log does not store the model answer.

The gateway writes the record after the response ends, so it holds the status
and the token counts. Records go to an in-memory buffer. A flusher task wakes
every 0.25 seconds. It flushes the buffer 5 seconds after the last flush, or when
the buffer holds 100 records. It also flushes at shutdown.
After a failed flush, it waits the full 5 seconds before the next attempt.

A flush splits each record in two parts. The prompt goes to a prompt file under
`prompts/<day>/`. The metadata goes to a segment file under
`audit/<day>/segments/`. Metadata is the record without the prompt, plus the
fields `event`, `detail`, `seg` and `prompt_ref`. Before the first prompt file of
a day, the flush writes the marker `audit/<day>/prompts.marker`. The flush writes
the prompt file before the segment file. The segment file is the commit: a record exists when its metadata
exists. A flush writes each prompt file and each segment file one time. A merge
writes the day file again when a late record arrives for a finished day.

A background task of its own merges the metadata, at most one time each minute.
A flush never waits for it, except for the write of one merged file. The merge
holds the flush lock for the write of a merged file, not for the deletes of its
inputs. It reads and deletes eight files at a time. The segments of a finished
10 minute bucket become one part file. The parts of a finished day become one
day file. A bucket merges 1 to 2 minutes after its end, and a day 15 to 16
minutes after its end. A reader skips a row whose `seg` it already read, so a
merge that stops half way loses no record and counts none twice. Files of the
old layout, with the full records, stay readable and are never merged.

The gateway keeps the metadata rows of the files in memory, up to 1,000,000
rows, and drops the least recently used file first. At that size, the cache
uses about 2 GB of memory. It keeps no prompts there.

`GET /admin/audit` reads the metadata of the days in the requested range, plus
the buffer, and applies the filters. It reads no prompt. It reads the days
newest first, and it stops when it has `offset + limit` matching records.
During a flush, the records that the flush writes stay visible to queries. A
query shows each record once. `GET /admin/audit/records/{request_id}` lists
the day, reads the metadata files of the day that are not in memory, and reads
one prompt file.

An admin can set `LLM_PROMPT_RETENTION_DAYS`. The same background task then
deletes the prompt files of the days that ended more than that many days ago.
The task runs one step at most every 5 seconds. One step deletes at most 64
prompt files, eight at a time, and lists at most 20 prompt folders. The metadata
stays. A record whose prompt is deleted answers with `prompt_deleted`. The task
finds the days in the listing of `audit/`. The prompt marker puts a day there,
also when the day has prompts and no metadata. A flush into an old day makes
the task handle that day again.

In normal operation, a kill of the container loses the records since the last
flush. A flush runs every 5 seconds. It can wait for the write of one merged
file, a few seconds on the platform. During a blob storage outage, the records
wait in memory. A kill then loses all records since the outage began. The README
states this.

A stored line that is not a JSON object with a valid `ts` and a string `user`
is skipped with a warning.

The document [audit-storage-design.md](audit-storage-design.md) gives the
layout, the merge rules, the read rules and the limits in full.

## Usage metrics

The usage totals come from the audit records. There is no second store.

`usage.py` keeps totals in memory with the key `(day, user, model)`. The totals
are: request count, failed request count, prompt tokens, completion tokens,
total tokens and last request time. Each new record updates them.

The container has no persistent disk, so the totals live in blob storage too.
When a day ends, the gateway writes the totals of that day to
`<root>/usage/<day>.json`. For the current day, the gateway writes a
checkpoint to `<root>/usage/<day>.partial.json`. The checkpoint holds the
totals of the rows that a flush wrote and the name of the last such segment
(`through`). The gateway writes it at most one time each 60 seconds and at a
clean stop.

At startup, for each day, the gateway reads the daily file if it exists.
Otherwise it reads the checkpoint and adds the rows whose segment name is
greater than `through`. Without a checkpoint, it reads all metadata of the day.
The startup lists `audit/` and `usage/` one time each. It reads one small usage
file for each past day, eight at a time. So the startup time grows slowly with
the number of days, not with the load of a day. The first start after the
upgrade reads every old layout file of the days without a daily usage file, as
the old version did. Later starts use the daily files and the checkpoint. The
metadata files stay the source of truth: if a daily file is deleted, the gateway
rebuilds it.

### Token counts for streaming requests

vLLM reports token counts for a streaming request only if the request asks for
them. For each streaming request, the gateway sets
`stream_options.include_usage` to true and reads the counts from the last
chunk. If the client did not ask for that chunk, the gateway removes it from
the stream. The client then gets the same stream as from OpenAI.

## Limits

An admin sets limits for each user: requests in one minute, tokens in one UTC
day and parallel requests. The gateway checks them in the handler of the three
OpenAI endpoints, after the body check and before the request goes to the
engine. A request over a limit gets 429 `rate_limit_error` with a `Retry-After`
header. The code of the error is the name of the limit. `limits.py` holds the
limits file and the lookup. `limiter.py` holds the window and the counters.

A storage error at the read of the limits file means no limits. The flusher then
reads the file again after each flush, until a read works. Meanwhile,
`GET /v1/limits/me` and `GET /admin/limits` answer `"loaded": false`, and the
page shows a note.

The design is in [limits-load-catalog-design.md](limits-load-catalog-design.md),
Part 1. The README has the usage.

## Load numbers

An admin sees how busy the service is: requests in flight, requests that wait,
tokens per second, latency, the KV cache use of the engine and GPU memory.
`engine_metrics.py` reads the vLLM metrics endpoint and `nvidia-smi`. The metrics read has its own HTTP client
with two connections, so the streams of the proxy never block it. `load.py` keeps a sample each 5 seconds and
builds one point for each minute from the samples and the audit records.
`metrics_store.py` writes the points of each process to its own file,
`<root>/metrics/<day>/<writer>.jsonl`, and reads the history from all files of a day. A sampler task runs next to the flusher. A request never waits for
the reads or for blob storage.

The design is in [limits-load-catalog-design.md](limits-load-catalog-design.md),
Part 2. The README has the usage.

## Model catalog

An admin keeps a list of recommended models, downloads a model without a
switch and checks the GPU memory before a switch. `catalog.py` holds the
catalog file, `<root>/catalog.json`, and the checks of its entries. `fit.py`
holds the parse of memory sizes and the rules of the answer. The download runs
in `models.py` as a background task that calls `WeightCache.ensure`. The
download holds the lock of the weight cache, so a switch to the same model
waits for it. No request of a user waits for the catalog, a download or blob
storage.

The design is in [limits-load-catalog-design.md](limits-load-catalog-design.md),
Part 3. The README has the usage.

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
| `LLM_VRAM` | `16g` | GPU memory of the container. The service also reads it for the GPU memory check. |
| `LLM_MEMORY` | `24g` | Memory of the container. |
| `LLM_CPU` | `4` | CPU count of the container. |
| `LLM_ADMIN_GROUP` | not set | Platform group whose members are admins. |
| `LLM_ACCESS_GROUP` | not set | Platform group that gets read and access permission. The service container gets it too. |
| `LLM_BLOB_ROOT` | `llm-service` | Root directory in blob storage. |
| `LLM_HF_TOKEN_KEY` | not set | Secret store key of the Hugging Face token. |
| `LLM_VLLM_ARGS` | not set | Extra arguments for `vllm serve`. |
| `LLM_PROMPT_RETENTION_DAYS` | not set | Days to keep prompts in the audit log. A whole number from 1 to 36500. |
| `LLM_METRICS_RETENTION_DAYS` | `30` | Days to keep the load history files. A whole number from 1 to 36500. |
| `LLM_ENVIRONMENT` | `dev` | Platform environment. |
| `LLM_ADMIN_APP_NAME` | `LLM Service Admin` | Name of the admin app. |

Because the service sets `vram`, the platform does not start a node for it. The
operator starts a GPU node manually. The README gives the steps.

## Testing

The unit tests run without a GPU and without vLLM. A fake engine (a small
FastAPI application that answers like vLLM) and an in-memory blob store replace
the real ones. The tests cover:

- identity and the admin check,
- audit records, the flush, the merges, the record lookup and the audit filters,
- prompt retention,
- usage totals, the checkpoint and the start,
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
- Bearer token support at the platform edge.
- A web interface for the admin endpoints.
