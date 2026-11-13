# Limits, live load and model catalog: design

## Goal

An admin protects a service from one heavy user, sees how busy the service is, and prepares models before a switch. This design adds three parts to the LLM service and to the admin app:

1. Limits for each user.
2. Live load and latency numbers, with a history.
3. A model catalog with a download without a switch and a GPU memory check.

Each part is one pull request, in this order. Each part works without the other two.

This design builds on [design.md](design.md) and [multi-service-app-design.md](multi-service-app-design.md).

## Terms

- Limit: a maximum for one user. The value null means no limit.
- Sample: the load numbers of one moment.
- Point: the numbers of one minute, made from the samples and the requests of that minute.
- Blob root: the directory of the service in blob storage, from `LLM_BLOB_ROOT`.

## Part 1: limits for each user

### Requirements

1. An admin sets a default limit for all users and other limits for single users.
2. The service refuses a request over a limit with status 429 and a `Retry-After` header.
3. Each user sees their own limits and their current use.
4. The audit log shows each refused request and each change of the limits.

### The three limits

| Name | Meaning | Check |
|---|---|---|
| `requests_per_minute` | Requests of the user in the last 60 seconds. | A sliding window in memory. |
| `tokens_per_day` | Total tokens of the user in the current UTC day. | The usage totals of the service. |
| `parallel_requests` | Requests of the user in flight at this moment. | A counter in memory. |

Each value is null or a whole number from 1 to 1,000,000,000.

The limits apply to the endpoints that reach the engine: `/v1/chat/completions`, `/v1/completions` and `/v1/embeddings`. They apply to the effective user, so a request from the testing area of the app counts for the viewer.

### Who gets which limit

The service finds the limits of a user in this order:

1. The entry of the user, if one exists. Each field of the entry is a number, null for no limit, or absent. An absent field takes the default.
2. For an admin without an entry: no limit.
3. For each other user: the default.

Without a limits file, no user has a limit. This is the behavior before this change.

### Storage

The limits are in one file, `<blob root>/limits.json`:

```json
{
  "default": {"requests_per_minute": 60, "tokens_per_day": 2000000, "parallel_requests": 4},
  "users": {
    "bob": {"tokens_per_day": null},
    "carol": {"requests_per_minute": 10, "parallel_requests": 1}
  }
}
```

The service reads the file at the start and keeps it in memory. A change through the API writes the file first and then replaces the copy in memory. A broken file at the start means no limits, and the service logs an error. The next PUT through the API replaces the broken file. A DELETE of an entry on a broken file gets 404 and writes nothing.

A storage error at the read is different. The service does not know the content of the file then, so it also applies no limits. Each change through the API gets 502 `storage_unavailable` until a read works. The service reads the file again at each change, but only while the last read failed.

The flusher of the audit log also reads the file again after each flush, but only while the last read failed. So the limits come back without a restart. While the read fails, `GET /v1/limits/me` and `GET /admin/limits` answer `"loaded": false`. The page then shows a note: "The service did not read the limits file. No limit applies now. The service tries again."

### Enforcement

The check runs in the handler of the OpenAI endpoints, after the body check and before the request goes to the engine. The order is:

1. `parallel_requests`. If the user has N requests in flight and the limit is N, the request is refused.
2. `requests_per_minute`. The window holds the start times of the accepted requests of the last 60 seconds.
3. `tokens_per_day`. The service reads the total tokens of the user for the current UTC day from the usage totals.

A refused request does not count in the window and not in the parallel counter.

The token check runs before the request, and the tokens of a request are known only at its end. So one request can pass the budget by its own size, and parallel requests can pass it together. The next request after that is refused.

The parallel counter goes down when the answer ends, when the stream ends, and when the client goes away.

A refused request gets this answer:

```json
{"error": {"message": "You reached the limit of 60 requests in one minute.",
           "type": "rate_limit_error", "code": "requests_per_minute"}}
```

The status is 429. The `code` is the name of the limit. The header `Retry-After` has whole seconds:

- `requests_per_minute`: the time until the oldest request leaves the window, at least 1.
- `tokens_per_day`: the time until the next UTC midnight.
- `parallel_requests`: 1.

A refused request gets an audit record with status 429, as each other refused request does. It counts as a failed request in the usage numbers and adds no tokens.

The window and the parallel counter are in the memory of the one container of the service. A restart clears them. The token budget comes from the usage totals, which a restart keeps.

### Endpoints

| Endpoint | For | Does |
|---|---|---|
| `GET /v1/limits/me` | Each user | The limits and the current use of the effective user. |
| `GET /admin/limits` | Admin | The default, the user entries, the current use and the effective limits of each listed user. |
| `PUT /admin/limits/default` | Admin | Replaces the default. Body: the three fields. |
| `PUT /admin/limits/users/{name}` | Admin | Sets the entry of one user. Body: one to three fields. |
| `DELETE /admin/limits/users/{name}` | Admin | Removes the entry of one user. 404 `limit_not_found` without an entry. |

The answer of `GET /v1/limits/me`:

```json
{
  "user": "bob",
  "limits": {"requests_per_minute": 60, "tokens_per_day": null, "parallel_requests": 4},
  "use": {"requests_last_minute": 3, "tokens_today": 15234, "parallel_requests": 1},
  "source": "user",
  "loaded": true
}
```

`source` is `user`, `default`, `admin` or `none`. `loaded` is false while the read of the limits file fails.

The answer of `GET /admin/limits` lists each user with an entry, with use now or with tokens today:

```json
{
  "default": {"requests_per_minute": 60, "tokens_per_day": 2000000, "parallel_requests": 4},
  "users": {"bob": {"tokens_per_day": null}},
  "use": {"bob": {"requests_last_minute": 3, "tokens_today": 15234, "parallel_requests": 1}},
  "effective": {"bob": {"limits": {"requests_per_minute": 60, "tokens_per_day": null, "parallel_requests": 4},
                        "source": "user"}},
  "loaded": true
}
```

`effective` holds the limits that apply to each listed user and their source, as `GET /v1/limits/me` gives them.

A body that is not JSON, has an unknown field, a wrong type or a number outside the range gets 400 `invalid_limit`. So does a default body with a missing field and an empty body for a user. A change that fails because the service cannot read or write the file gets 502 `storage_unavailable`. A user name follows the rules of the acting user name: at most 256 characters, no control character. A bad name gets 400 `invalid_user`.

Each change writes an audit event `limits_change` with the admin name, the target (`default` or the user name) and the old and new values.

### The app

- New routes that pass through: `GET /api/services/{name}/limits`, `PUT /api/services/{name}/limits/default`, `PUT` and `DELETE /api/services/{name}/limits/users/{user}`, and `GET /api/services/{name}/limits/me`.
- A Limits tab, for an admin of the picked service. It has a form for the default and a table of the user entries with the current use. The admin adds, changes and removes an entry there. An empty field means no limit.
- The table shows the effective limits from the service. A limit from the default is muted. An admin without an entry shows "No limit".
- The Connect tab shows the limits and the current use of the viewer.
- The testing area shows the message of a 429 answer and the wait time.

## Part 2: live load and latency

### Requirements

1. An admin sees how busy a service is now: requests in flight, requests that wait, tokens per second, latency, KV cache use and GPU memory.
2. An admin sees the same numbers for the last hour without a read from blob storage.
3. The service writes the history to blob storage, so an admin can load older days.
4. A failure of a number source does not stop the service. The number is null then.

### Sources

| Number | Source |
|---|---|
| Requests in flight | A counter in the gateway. |
| Requests that run and that wait in the engine | The metrics endpoint of vLLM, `GET /metrics` on the engine port. |
| Generated tokens and prompt tokens | The token counters of the same endpoint. The rate is the change between two samples. |
| KV cache use | The same endpoint: `vllm:kv_cache_usage_perc`, or `vllm:gpu_cache_usage_perc` of older versions. It is a fraction from 0 to 1. Over more than one label set it is the maximum, not the sum. A value above 1.5 is a percent and is divided by 100. Each value is limited to 0 to 1. |
| GPU memory used and total | `nvidia-smi --query-gpu=memory.used,memory.total --format=csv,noheader,nounits`. The task reads it at each third sample, so each 15 seconds. The samples between use the last value. |
| Time to the first token, total time | The gateway measures each request. |

The names of the vLLM metrics changed between versions. The gateway reads each number from a short list of known names and takes the first that exists. The first step of the work checks the names against the installed vLLM version.

With more than one GPU, the memory numbers are the sums.

vLLM takes most of the GPU memory at its start, so the GPU memory used says little about the load. The KV cache use shows how full the engine is. A container without `nvidia-smi` has no GPU memory numbers.

### Time to the first token

The proxy records the time from the start of the request to the first content chunk of a streamed answer. The audit row gets the new field `ttft_ms`. It is null for a request without a stream and for a request without content.

### Samples and points

A background task takes one sample each 5 seconds. A sample has: time, in flight, running, waiting, KV cache use, prompt tokens per second, completion tokens per second, GPU memory used, GPU memory total.

At the end of each minute the task makes one point:

```json
{"ts": "2026-11-12T10:15:00Z", "requests": 42, "failed_requests": 1,
 "in_flight_max": 6, "waiting_max": 2, "kv_cache_use_max": 0.52,
 "prompt_tokens": 9100, "completion_tokens": 5200,
 "ttft_ms_p50": 310, "ttft_ms_p95": 900,
 "duration_ms_p50": 2100, "duration_ms_p95": 7400,
 "gpu_memory_used_max": 14200, "gpu_memory_total": 15360}
```

- `requests`, `failed_requests` and the token sums come from the requests that ended in that minute.
- The percentiles come from the requests of that minute with a status below 400. A refused or failed request ends fast and stays out of them. They are null for a minute without such a request.
- The maximum values come from the samples of that minute. `kv_cache_use_max` is a fraction from 0 to 1.
- A point of a file from before `kv_cache_use_max` existed has no such field. Each reader takes it as null.
- GPU memory is in MiB.

The service keeps the last 60 points and the last 12 samples in memory.

### History in blob storage

The history has one directory for each UTC day and one file in it for each process: `<blob root>/metrics/<day>/<writer>.jsonl`. `<writer>` is fixed for the life of a process. It is the start time of the process as `YYYYMMDDTHHMMSSZ`, a dash and 8 random hex characters. A file holds the points of that day that this process produced, one JSON line for each point, in time order. A full day of one process has 1440 lines.

Each process writes only its own files. No process writes a file of another process. Blob storage has no conditional write, so this rule is the protection against a lost minute. During a redeploy, the old and the new container both write, each to its own file.

Each 5 minutes, and at shutdown, the task writes the own file of each day that has new points. It writes the whole file again, with all own points of the day so far. It reads nothing before the put. A restart starts a new file. The files of earlier processes stay as they are.

At the start, the service reads each file of the current day into a read-only part of the memory. The history of the current day then needs no blob read. The service never writes these points. After a failed read, that part stays empty, and the history reads the files itself. Points that another process writes after this read do not show in the history of the current day in this process. The points of the minutes when no process ran are missing.

A history read lists the directory of each day and reads each file in it, with up to 8 threads over all files of the range. The own points in memory count too. For a minute in the own file and in memory, the memory wins. Two points of one minute from different writers are two parts of that minute. They combine by the rules of the merge below: sums add, maximum values take the maximum, and `gpu_memory_total` takes the value that is not null. A failed listing or a failed file read puts the day into `missing_days`. A missing directory is an empty day.

Cost: a history read lists one directory for each day and reads one file for each process that ran on that day. A day with many restarts has many small files.

An earlier test version wrote one file for each day: `<blob root>/metrics/<day>.jsonl`. The history reads such a legacy file as one more source of its day. No process writes it.

The variable `LLM_METRICS_RETENTION_DAYS` sets how many days the service keeps. The default is 30, and the range is 1 to 36500. The maintenance task of the audit log removes the files of older days, in the day directories and the legacy files. It lists `metrics/` at most once in an hour. One call deletes at most 64 files, and the next call continues.

A failed write keeps the points in memory for the next try. If the file of a past day has no good write for one hour, the service drops its points and logs an error.

### Endpoints

Both are for admins.

`GET /admin/metrics/live` answers from memory:

```json
{"now": {"ts": "...", "in_flight": 3, "running": 3, "waiting": 0, "kv_cache_use": 0.37,
         "prompt_tokens_per_s": 210.0, "completion_tokens_per_s": 96.5,
         "gpu_memory_used": 14200, "gpu_memory_total": 15360},
 "points": [ ... the last 60 points ... ]}
```

`GET /admin/metrics/history?since=<time>&until=<time>` reads the files of the days in the range. `since` and `until` are ISO times. The range is at most 31 days as a duration. The range touches a count of minutes, counted from the minute of `since`. The step is that count divided by 500 and rounded up, at least 1 minute. The service puts each point into the time bucket of one step that holds it. The buckets start at the minute of `since`. Each bucket with points gives one point, with the start of the bucket as `ts`. A bucket without points gives no point, so a gap stays a gap. A range of up to 500 minutes keeps each point as it is. Sums stay sums and maximum values stay maximum values. A percentile becomes the maximum of the merged percentiles. The answer names the step in seconds.

The answer has `step_seconds`, `points` and `missing_days`. `missing_days` lists the UTC days with a failed listing or a failed file read. A day with no file is not a failure. The list is empty when each read worked.

A bad time, a `since` after `until` or a range that is too long gets 400 `invalid_time`.

### The app

- New routes that pass through: `GET /api/services/{name}/metrics/live` and `GET /api/services/{name}/metrics/history`.
- New route `GET /api/load`: the `now` part of each service where the viewer is an admin, in parallel, with the same failure rules as the combined usage.
- A Load tab, for an admin. With one service: tiles for the numbers of this moment and charts of the last hour. A tile and the chart "Engine memory" show the KV cache use in percent. That chart shows the GPU memory used on a second axis when a point has it. The tab reads the live endpoint each 5 seconds while it is open and the page is visible. Each read of the tab has a time limit of 30 seconds. A read over the limit counts as a failed read. A range picker loads the history and stops the refresh.
- With "All services": one table row for each service with the numbers of this moment, the KV cache use among them.

## Part 3: model catalog

### Requirements

1. Admins keep a list of recommended models with a note.
2. An admin downloads a model to the cache without a switch.
3. Before a switch, an admin sees whether the model fits in the GPU memory.

### The catalog

The catalog is one file, `<blob root>/catalog.json`:

```json
{"models": [
  {"model": "Qwen/Qwen2.5-1.5B-Instruct", "revision": "main",
   "note": "Small and fast. Good for tests.", "gpu_memory_mib": 6000}
]}
```

- `model` and `revision` follow the checks of the model switch. The pair is the key of an entry.
- `note` has at most 500 characters.
- `gpu_memory_mib` is optional. It is the GPU memory that the model needs, if the admin knows it.
- The catalog has at most 200 entries.

A model that is not in the catalog still works with the switch and the download, as today.

### Download without a switch

`POST /admin/models/download` with `{"model", "revision"}` starts a download and answers 202. The service runs `WeightCache.ensure` in the background. That step downloads the files and stores them in blob storage. After the step, the service removes the local copy if the engine does not use it.

- One download runs at a time. A second request gets 409 `download_in_progress`.
- A download of a model that is in the cache already answers 200 with `"state": "cached"` and starts nothing.
- A model switch during a download waits for the lock of the weight cache, as two switches do today.
- A delete of the model that downloads gets 409 `model_in_use`.

`GET /admin/model` gets the new field `download`: null, or `{"model", "revision", "state", "error", "started_at"}`. `state` is `running`, `done` or `failed`. The last result stays until the next download starts.

The service writes an audit event `model_download` at the end, with the admin name and the result.

### GPU memory check

`GET /admin/models/fit?model=<name>&revision=<revision>` answers:

```json
{"fits": "yes", "needed_mib": 6000, "available_mib": 15360, "basis": "catalog"}
```

- `available_mib` is the total GPU memory from `nvidia-smi`. It is null without that number.
- `needed_mib` and `basis`:
  - `catalog`: the `gpu_memory_mib` of the catalog entry.
  - `weights`: the size of the cached weights, times 1.2. The margin covers the working memory of the engine for a short context.
  - `unknown`: the model is not in the catalog and not in the cache. `needed_mib` is null.
- `fits` is `yes` when the needed memory is at most the available memory, `no` when it is more, and `unknown` when a number is missing.

The check is an estimate. A model that fits can still fail with a long context. The switch does not use the check: an admin can start a switch with each result.

### Endpoints

All are for admins.

| Endpoint | Does |
|---|---|
| `GET /admin/catalog` | The entries. Each one gets `cached` (true or false) and the `fit` answer. |
| `PUT /admin/catalog` | Adds or replaces one entry. Body: the entry. 400 `invalid_catalog_entry`, 409 `catalog_full`. |
| `DELETE /admin/catalog?model=<name>&revision=<revision>` | Removes one entry. 404 `catalog_entry_not_found`. |
| `POST /admin/models/download` | See above. |
| `GET /admin/models/fit` | See above. |

A change of the catalog writes an audit event `catalog_change`.

### The app

- New routes that pass through for each endpoint, under `/api/services/{name}/`.
- The Models tab gets a Catalog card: a table with model, revision, note, needed memory, a fit badge and a cached badge. Each row has the buttons Serve, Download and Remove from the list. A form adds an entry.
- The "Serve a model" form shows the fit result for the typed model before the switch, as a note. A `no` result shows as a warning and does not block the button.
- During a download, the Models tab shows its state and reads it again each 3 seconds.

## Errors

- A failure to read the metrics endpoint or `nvidia-smi` gives null numbers. The service logs the first failure and the recovery, not each try.
- A failure to write `limits.json` or `catalog.json` answers 502 `storage_unavailable`, and the copy in memory does not change.
- The app passes the status and the body of the service through, as its other routes do.

## Testing

- Limits: unit tests for the lookup order, each limit with a fake clock and the `Retry-After` values. Other tests cover the parallel counter on each end of a request, the audit records, and each endpoint with its errors.
- Load: unit tests for the metric parser with sample texts of two vLLM versions, and for the `nvidia-smi` parser. Other tests cover the point builder with percentiles and the merge into at most 500 time buckets. They also cover the write to blob storage with a failed write, and the retention.
- Catalog: unit tests for the file checks, the download states, the 409 cases and the fit rules.
- App: route tests with fake services, as for the existing routes. The page gets static checks and a check in a browser.
- On a cluster: a load test with two users, one over each limit. The Load tab during that test. A download of a second model and a switch to it.

## Known limits

- The service checks the token budget before a request. So a user can pass it by the size of the requests in flight.
- A restart clears the request window, the parallel counters and the points that are not in blob storage yet. A clean stop loses only the point of the open minute. A crash loses up to the last 5 minutes of points and the open minute.
- The limits are for each service. A user with access to two services has two budgets.
- The percentiles of a merged history point are the maximum of the merged minutes, not a true percentile of the longer time.
- The fit check does not know the context length or the engine arguments.

## Out of scope

- Limits for a platform group and a token budget for a month.
- A queue with priorities for users.
- Alerts and notifications.
- A search on the Hugging Face hub.
- A stop of the engine when no request comes.
