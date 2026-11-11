# LLM service admin app: design

## Goal

The admin app is a web page for the admins of the LLM service. An admin sees who uses the service and how much, reads the audit log, and manages the models. The app keeps no data. It reads everything from the endpoints of the service.

## Requirements

1. An admin sees usage numbers in a dashboard: requests, failed requests and tokens, by user, by model and by day.
2. An admin reads the audit log with filters for user, model and time.
3. An admin sees the state of the service: the engine state, the served model, the last load error and the cached models.
4. An admin changes the served model and sees the progress.
5. An admin removes the weights of a cached model from blob storage.
6. Only an admin of the service can use the app. The service makes this decision, not the app.
7. The audit log names the person who changed or removed a model, not the user who deployed the app.

## Architecture

```
browser ──> platform edge ──> admin app (FastAPI) ──> http://llm-service
                (adds X-Datatailr-User             (the platform signs the call as
                 of the viewer)                     the user who deployed the app)
```

The admin app is a second job, named `LLM Service Admin`, with `framework="fastapi"`. It has two parts:

- A backend. It serves the page and a small JSON API under `/api/`. Each API route calls one endpoint of the service.
- One HTML page with one JavaScript file and one CSS file. The page calls only the backend, with relative URLs, so it works under the path prefix that the platform gives the app.

The browser never calls the service directly.

## Identity

The platform edge adds the signed header `X-Datatailr-User` to each browser request. The app reads the viewer name from it with `user_from_headers` from `gateway/identity.py`.

When the app calls the service, the platform signs the call as the user who deployed the app. This user must be an admin of the service: the deployer of the service, or a member of `LLM_ADMIN_GROUP`. If one person deploys both jobs, this is true.

The app sends the viewer name to the service in the header `X-LLM-Acting-User` on each call. The service then decides for the viewer. See [Acting user](#acting-user).

The app strips the viewer name. It answers 401 `missing_user` and calls nothing in these cases:

- The request has no platform user, or the name is empty after the strip.
- The name is longer than 256 characters, has a control character or is not ASCII. The name goes into an HTTP header, so it must be ASCII. The message says that the name is not usable for the admin app.

## Changes in the service

### Acting user

The header `X-LLM-Acting-User` applies to the `/admin/*` endpoints only. The dependency `require_admin` handles it:

1. The signed caller must be an admin. If not, the answer is 403 `admin_required`, with or without the header.
2. If the header is missing or empty, the admin user is the caller. This is the behavior before this change.
3. If the header has a name, that user must be an admin too. If not, the answer is 403 `admin_required`. The message says that the acting user is not an admin.
4. If both checks pass, the admin user is the acting user. The audit records of a model switch and a model delete use this name.

The name in the header is stripped. A name that is longer than 256 characters or has a control character gets 400 `invalid_acting_user`.

The other endpoints ignore the header. A user who is not an admin cannot gain anything with it, because rule 1 comes first.

### Delete a cached model

`DELETE /admin/models/{model_id:path}` with an optional query parameter `revision`.

The service selects the revisions from `WeightCache.stored(model)`: each revision with at least one file in blob storage, with or without a `.complete` marker. Without `revision`, it removes all of them. With `revision`, it removes that one if it is stored.

- It removes the blob directory `models/<org>--<name>/<revision>/`. It deletes the `.complete` marker first, so a copy that is half removed never looks complete.
- It removes the local directory of the same revision, if one exists.
- The answer is 200 with `{"deleted": [{"model": ..., "revision": ...}]}`. The list has every revision whose files the service removed.
- 400 `invalid_model` for a model name or revision that fails the existing checks.
- 404 `model_not_found` if no stored revision matches.
- 409 `model_in_use` if a matching revision is in use. Nothing is removed in that case. See the list below.
- 500 `delete_failed` if a removal fails. The revisions removed before the error stay removed.

A revision is in use when it is one of these:

- the served model or the active model
- the target of a running switch, also in its fetch phase while the files upload
- the model of a running rollback
- a model that another delete removes now

While the service starts, every delete gets 409 `model_in_use`. The model manager sets the flag `starting` when `startup()` begins. It clears the flag when `startup()` has set the target, or when it fails. A manager that never calls `startup()` accepts deletes.

The delete runs in a worker thread. Before it removes anything, the model manager marks the revisions under removal. A switch to one of them gets 409 `model_in_use`. The mark ends when the delete ends.

A failed delete can leave files without a marker. `stored()` still lists that revision, so a second delete removes the rest.

`BlobStore` gets a method `delete(path)`. `MemoryBlobStore` removes the key. `DatatailrBlobStore` calls `Blob.delete`. A delete of a missing path is not an error.

`WeightCache` gets two methods. `remove(model, revision)` does the blob and local work for one revision. `stored(model)` returns the sorted stored revisions of a model and validates the model name.

### Switch during a rollback

If vLLM fails to start the new model, the service loads the previous model again. During this rollback, the state is `failed`. `GET /admin/model` gains the key `rolling_back`: `{"model", "revision"}` of the previous model during a rollback, else null. A switch during a rollback gets 409 `switch_in_progress`. So two loads never run at the same time.

### Audit record of a delete

A delete makes one audit record, in the form of the model switch record. Its fields: `endpoint` is `/admin/models`, `model` is the model name, `prompt` is `{"event": "model_delete", "revisions": [...]}`, `status` is 200, the token fields are null. The user is the admin user from `require_admin`. If a removal fails, the record has status 500, and `revisions` lists only the revisions removed before the error.

### Size of a cached model

Each entry of `GET /v1/models` and `GET /v1/models/{id}` gets the field `size_bytes`. It is the sum of the known sizes of its cached revisions. It is null if no cached revision has a known size. `WeightCache.cached()` returns the size of each revision from the `.complete` manifest. A manifest that is not usable gives the size null for that revision, and the sum skips it.

`WeightCache` keeps the size of each usable marker in memory, keyed by the marker path. A complete marker never changes, so `cached()` reads each manifest once. `remove()` drops the entries of the revision that it removes. A store error while the service reads one manifest gives the size null for that revision, with one warning in the log. The service does not keep that result, so the next call reads the manifest again. The list never fails because of a manifest read.

## The app backend

Package `llm_service/admin_app/`:

| File | Purpose |
|---|---|
| `app.py` | `create_app(service_url, http=None)` and the module object `app` that the platform runs. |
| `client.py` | `ServiceClient`: one method for each service call. It adds `X-LLM-Acting-User`. |
| `requirements.txt` | `fastapi`, `starlette`, `uvicorn`, `httpx`. |
| `static/index.html`, `static/app.js`, `static/app.css` | The page. |
| `static/vendor/chart.umd.js`, `static/vendor/LICENSE-chartjs.md` | Chart.js 4.4.7 (MIT), the same copy that `merit_desk` uses. |

Configuration, from the environment of the app:

| Variable | Default | Meaning |
|---|---|---|
| `LLM_SERVICE_URL` | `http://llm-service` | Base URL of the service. |

Routes:

| Route | Service call |
|---|---|
| `GET /` | The page. |
| `GET /static/...` | Static files. |
| `GET /health` | Answers `OK`. Calls nothing. |
| `GET /api/me` | `GET /admin/model`. Answers `{"user": ..., "admin": true}`, or `{"user": ..., "admin": false}` when the service answers 403. |
| `GET /api/status` | `GET /admin/model` and `GET /v1/models`, joined: `{"engine": ..., "models": [...]}`. |
| `POST /api/model` | `POST /admin/model` with the same body. The app answers 415 `unsupported_media_type` without the content type `application/json`, and 400 `invalid_json` for a body that is not JSON. |
| `DELETE /api/models/{model_id:path}` | `DELETE /admin/models/{model_id}`, with `revision` if given. The app answers 400 `invalid_model` for a bad id. |
| `GET /api/usage` | `GET /admin/usage` with `since`, `until`, `user`, `model`, `group_by`. |
| `GET /api/usage/{user}` | `GET /admin/usage/{user}` with `since`, `until`. The app answers 400 `invalid_user` for a bad name. |
| `GET /api/audit` | `GET /admin/audit` with `user`, `model`, `since`, `until`, `limit`, `offset`. |
| `GET /api/audit/users` | `GET /admin/audit/users`. |
| `GET /api/audit/records/{request_id}` | `GET /admin/audit/records/{request_id}` with `day`. The app answers 400 `invalid_record` for a bad id. |

Rules for the API routes:

- The backend answers 400 `invalid_model`, `invalid_user` or `invalid_record` when the id has an empty, `.` or `..` path segment. A record id that contains `/` gets 400 `invalid_record` too. It answers 400 `invalid_json` for a `POST /api/model` body that is not JSON.
- `POST /api/model` needs the content type `application/json`. Parameters such as `; charset=utf-8` are allowed. Otherwise the backend answers 415 `unsupported_media_type` and calls nothing. A form on another site cannot send this content type without a CORS preflight. No other route has this check.
- The backend passes the status code and the JSON body of the service through, for success and for errors. The page shows the `error.message` of the service.
- If the service does not answer or the connection fails, the backend answers 502 with `{"error": {"code": "service_unreachable", "message": ...}}`.
- The timeout is 300 seconds for `POST /api/model` and `DELETE /api/models/...`. A delete can wait for the weight cache while another model downloads. If the service does not answer in time, the message says that the action can still finish. The other routes have a timeout of 30 seconds.
- The backend forwards only the listed query parameters.
- The backend holds no state and no cache.

## The page

One page with three tabs. The header shows the viewer name and the served model. The page uses the same dark theme for all charts.

If Chart.js does not load, or a chart fails, the page hides the chart row and shows a message once. The tiles and the tables still load.

If `GET /api/me` says the viewer is not an admin, the page shows only this text: "You are not an admin of the LLM service." It loads no data.

### Usage tab

- A range control: 7 days, 30 days (the default), or two date fields.
- Four tiles: requests, failed requests, total tokens, active users (the number of users with a request in the range).
- A bar chart of total tokens for each day, from `group_by=day`.
- A bar chart of total tokens for each model, from `group_by=model`.
- A table with one row for each user: requests, failed requests, prompt tokens, completion tokens, total tokens, last request. A click on a row opens a panel with the numbers of that user by model and by day.

### Audit tab

- Filters: user (a text field with suggestions from `/api/audit/users`), model (free text), since and until.
- A table, newest first, 50 rows a page, with "newer" and "older" buttons: time, user, endpoint, model, status, total tokens, duration.
- A row of a model load or delete shows a tag with the name of its `event` next to the endpoint.
- The list has no prompt. A click on a row loads the full record from `/api/audit/records/{request_id}`, with the `day` set to the UTC day of the row time. The page shows the record as formatted JSON.
- While the record loads, the dialog says so. If the prompt is deleted by retention, the dialog says "The prompt of this record is deleted."
- If the load fails, the dialog shows the error message and the fields of the list row.

### Models tab

- A status panel: engine state, served model and revision, target of a running switch, last error.
- A table of the models: name, status (`active`, `loading`, `cached`), revisions, size. The size is "unknown" when `size_bytes` is null. Each cached row has two buttons: "Serve" and "Delete".
- A form to serve a model that is not in the table: model name and revision (default `main`).
- "Delete" asks for a confirmation that names the model. The served model and the loading model have no "Delete" button.
- The page is busy while the engine state is `loading` or `rolling_back` is set. While it is busy, it reads `/api/status` every 3 seconds and shows the state. The "Serve" and "Delete" buttons stay disabled.
- During a rollback, the status panel shows "The service loads <model> again after a failed switch."
- The poll stops when the state is `ready` or `failed` and `rolling_back` is null. It also stops after 5 failed reads.
- On page load and on Reload, the page starts the poll if it is busy.

All text that comes from the service goes into the page as text, never as HTML.

The page has a small inline script that adds a trailing slash to the address. The relative URLs need it. If a Content-Security-Policy forbids inline scripts, open the app with a trailing slash in the address.

## Deployment

`deploy.py` takes one optional argument:

```bash
python deploy.py            # the service and the app
python deploy.py service    # only the service
python deploy.py app        # only the app
```

Any other argument prints `usage: python deploy.py [service|app]`.

The app job:

- Name `LLM Service Admin`, or `LLM_ADMIN_APP_NAME`.
- The user who deploys the app must be an admin of the service. The platform signs the calls of the app as this user.
- `framework="fastapi"`, entrypoint `llm_service.admin_app.app`.
- 0.5 CPU and 512 MB of memory. No GPU and no pre-build script.
- `LLM_SERVICE_URL` is set from `LLM_SERVICE_NAME`: `LLM Service` gives `http://llm-service`. The helper follows the rule of the platform. It applies `unicodedata.normalize("NFKC", name)` first, then the lower case step. A deploy of the app alone needs the same `LLM_SERVICE_NAME` as the deploy of the service.
- The same `LLM_ENVIRONMENT`, and the same ACL from `LLM_ACCESS_GROUP`, as the service.

The pure helpers for this stay in `deploy_config.py`, with tests.

## Testing

- Service: unit tests for the acting user rules, and for the delete endpoint with its 404, 409, 500 and audit record. Other tests cover a second delete after a failed one and a switch during a rollback. Unit tests cover `BlobStore.delete`, `WeightCache.remove`, `WeightCache.stored`, the size memo and `size_bytes`.
- App backend: tests with the FastAPI test client and a fake service behind `httpx.MockTransport`. They cover each route, the acting user header, the pass-through of errors, the 502 answer, the 401 answer, the 415 answer and the timeouts.
- Deploy helpers: tests for the app name, the service URL and the argument of `deploy.py`.
- The page has no automated test. Check it in a browser on a real installation: each tab, a model switch, a delete, and a viewer who is not an admin.

## Known limits

- The service trusts an admin caller to name the viewer honestly. An admin who calls the service directly can put any admin name in the header.
- The usage charts show totals for each day and for each model, not the two together.
- The page has no automatic refresh outside a model switch. A reload button reads the data again.

## Out of scope

- Changes of users, groups or admin rights.
- Latency charts and data export.
- A stop or restart of the engine from the page.
