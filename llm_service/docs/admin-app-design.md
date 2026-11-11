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

If a browser request has no platform user, the app answers 401 and calls nothing.

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

- Without `revision`, the service removes all cached revisions of the model. With `revision`, it removes that one.
- It removes the blob directory `models/<org>--<name>/<revision>/`. It deletes the `.complete` marker first, so a copy that is half removed never looks complete.
- It removes the local directory of the same revision, if one exists.
- The answer is 200 with `{"deleted": [{"model": ..., "revision": ...}]}`.
- 404 `model_not_found` if no cached revision matches.
- 409 `model_in_use` if a matching revision is the served model, the active model, or the target of a running switch. Nothing is removed in that case.
- 400 `invalid_model` for a model name or revision that fails the existing checks.

The delete runs in a worker thread. It holds the same lock as a model switch for its check and its marker delete, so a switch cannot start on a model while the service removes it.

`BlobStore` gets a method `delete(path)`. `MemoryBlobStore` removes the key. `DatatailrBlobStore` calls `Blob.delete`. A delete of a missing path is not an error.

`WeightCache` gets a method `remove(model, revision)` that does the blob and local work for one revision.

### Audit record of a delete

A delete makes one audit record, in the form of the model switch record: `endpoint` is `/admin/models`, `model` is the model name, `prompt` is `{"event": "model_delete", "revisions": [...]}`, `status` is 200, the token fields are null. The user is the admin user from `require_admin`.

### Size of a cached model

Each entry of `GET /v1/models` and `GET /v1/models/{id}` gets the field `size_bytes`: the sum of the file sizes in the manifests of its cached revisions, or null if it has no cached revision. `WeightCache.cached()` returns the size of each revision from the `.complete` manifest. A manifest that cannot be read gives the size null for that revision, and the sum skips it.

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
| `POST /api/model` | `POST /admin/model` with the same body. |
| `DELETE /api/models/{model_id:path}` | `DELETE /admin/models/{model_id}`, with `revision` if given. |
| `GET /api/usage` | `GET /admin/usage` with `since`, `until`, `user`, `model`, `group_by`. |
| `GET /api/usage/{user}` | `GET /admin/usage/{user}` with `since`, `until`. |
| `GET /api/audit` | `GET /admin/audit` with `user`, `model`, `since`, `until`, `limit`, `offset`. |
| `GET /api/audit/users` | `GET /admin/audit/users`. |

Rules for the API routes:

- The backend passes the status code and the JSON body of the service through, for success and for errors. The page shows the `error.message` of the service.
- If the service does not answer or the connection fails, the backend answers 502 with `{"error": {"code": "service_unreachable", "message": ...}}`. The timeout is 30 seconds.
- The backend forwards only the listed query parameters.
- The backend holds no state and no cache.

## The page

One page with three tabs. The header shows the viewer name and the served model. The page uses the same dark theme for all charts.

If `GET /api/me` says the viewer is not an admin, the page shows only this text: "You are not an admin of the LLM service." It loads no data.

### Usage tab

- A range control: 7 days, 30 days (the default), or two date fields.
- Four tiles: requests, failed requests, total tokens, active users (the number of users with a request in the range).
- A bar chart of total tokens for each day, from `group_by=day`.
- A bar chart of total tokens for each model, from `group_by=model`.
- A table with one row for each user: requests, failed requests, prompt tokens, completion tokens, total tokens, last request. A click on a row opens a panel with the numbers of that user by model and by day.

### Audit tab

- Filters: user (a list from `/api/audit/users`), model (free text), since and until.
- A table, newest first, 50 rows a page, with "newer" and "older" buttons: time, user, endpoint, model, status, total tokens, duration.
- A click on a row shows the full record, with the prompt, as formatted JSON.

### Models tab

- A status panel: engine state, served model and revision, target of a running switch, last error.
- A table of the models: name, status (`active`, `loading`, `cached`), revisions, size. Each cached row has two buttons: "Serve" and "Delete".
- A form to serve a model that is not in the table: model name and revision (default `main`).
- "Delete" asks for a confirmation that names the model. The served model and the loading model have no "Delete" button.
- While the engine state is `loading`, the tab reads `/api/status` every 3 seconds and shows the state. It stops when the state is `ready` or `failed`.

All text that comes from the service goes into the page as text, never as HTML.

## Deployment

`deploy.py` takes one optional argument:

```bash
python deploy.py            # the service and the app
python deploy.py service    # only the service
python deploy.py app        # only the app
```

The app job:

- Name `LLM Service Admin`, or `LLM_ADMIN_APP_NAME`.
- `framework="fastapi"`, entrypoint `llm_service.admin_app.app`.
- 0.5 CPU and 512 MB of memory. No GPU and no pre-build script.
- `LLM_SERVICE_URL` is set from the service name: `LLM Service` gives `http://llm-service`.
- The same `LLM_ENVIRONMENT`, and the same ACL from `LLM_ACCESS_GROUP`, as the service.

The pure helpers for this stay in `deploy_config.py`, with tests.

## Testing

- Service: unit tests for the acting user rules, the delete endpoint with its 404, 409 and audit record, `BlobStore.delete`, `WeightCache.remove` and `size_bytes`.
- App backend: tests with the FastAPI test client and a fake service behind `httpx.MockTransport`. They cover each route, the acting user header, the pass-through of errors, the 502 answer and the 401 answer.
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
