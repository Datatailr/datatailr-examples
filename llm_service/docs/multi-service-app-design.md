# LLM service admin app for many services: design

## Goal

The admin app works with each LLM service that runs on the platform, not with one fixed service. It shows usage numbers from all of them together. It shows a user how to connect to a service, and it has a small area to test a service.

This design extends [admin-app-design.md](admin-app-design.md). Parts that this document does not name stay as they are.

## Requirements

1. The app finds the LLM services that run in its environment. The owner of the app can also add a service by name.
2. A viewer picks one service, or all services for the usage numbers.
3. The Usage tab shows totals over all services, by user and by service.
4. A Connect tab shows the URLs of a service and code examples that a user can copy.
5. The Connect tab has a testing area. A viewer sends a prompt to the service and sees the streamed answer.
6. Each user with access to the app can open it. The admin parts show only for the admins of the picked service.
7. A request from the testing area counts for the viewer, in the usage numbers and in the audit log.
8. A user cannot reach a service through the app without access to that service.
9. A slow or stopped service does not block the page.

## Terms

- Caller: the platform user who signs a call to the service. For a call from the app, this is the owner of the app.
- Viewer: the platform user who has the page open.
- Acting user: the name in the header `X-LLM-Acting-User`. The app sends the viewer name there.
- Service name: the job name of a service on the platform, for example `llm-service`. The internal URL is `http://<service name>`.

## Architecture

```
browser ──> platform edge ──> admin app ──┬──> http://llm-service
                                          ├──> http://llm-service-large
                                          └──> ...
```

The app keeps a registry of services in memory. A background task fills it. Each API route of the app that reads from a service takes the service name in its path. The browser still calls only the app.

The owner of the app must be an admin of a service. Without that, the service does not accept the acting user from the app. The app then shows the service with the state `no_access` and a short reason.

## Changes in the service

### Descriptor endpoint

`GET /v1/service` tells a client that this job is an LLM service. Each user with access can call it.

```json
{
  "api": "datatailr-llm-service",
  "api_version": 1,
  "name": "llm-service",
  "environment": "dev",
  "state": "ready",
  "model": "Qwen/Qwen2.5-1.5B-Instruct",
  "user": "alice",
  "admin": true,
  "caller_admin": true
}
```

- `api` and `api_version` are constants. The app accepts a service only when `api` is `datatailr-llm-service` and `api_version` is 1.
- `name` and `environment` come from `DATATAILR_JOB_NAME` and `DATATAILR_JOB_ENVIRONMENT`.
- `state` and `model` come from the model manager, as in `GET /admin/model`. `model` is null when no model is served.
- `user` is the effective user of the request. See [Effective user](#effective-user).
- `admin` says whether the effective user is an admin.
- `caller_admin` says whether the caller is an admin. The app reads it to learn whether the service accepts the acting user from it.

### Effective user

Today the header `X-LLM-Acting-User` applies to `/admin/*` only. After this change it also applies to these endpoints: `/v1/service`, `/v1/models`, `/v1/models/{id}`, `/v1/usage/me`, `/v1/chat/completions`, `/v1/completions` and `/v1/embeddings`.

A new dependency `effective_user` replaces `current_user` on these endpoints:

1. If the header is missing or empty, the effective user is the caller. This is the behavior before this change.
2. A name that is longer than 256 characters or has a control character gets 400 `invalid_acting_user`.
3. If the caller is not an admin, the answer is 403 `acting_user_not_allowed`. The service does not ignore the header, because a silent fallback hides a wrong setup.
4. The acting user must have access to the service. See [Access check](#access-check). If not, the answer is 403 `access_denied`.
5. If all checks pass, the effective user is the acting user.

There is one exception for rule 3 and rule 4. `GET /v1/service` never fails because of the header. If a rule fails there, the effective user is the caller and the body gets `"acting_user": "refused"`. The app needs the descriptor to explain the problem to the viewer.

Usage numbers and audit records use the effective user. An audit record of a request with an acting user gets a new field `via` with the caller name. Records without an acting user have no `via` field. When the acting user is the caller, the record has no `via` field either.

`require_admin` on `/admin/*` does not change.

### Access check

The platform checks the access of the caller at its edge. It cannot check the acting user, so the service does that.

The service container gets the new variable `LLM_ACCESS_GROUP`. `deploy.py` already reads this variable for the access list of the job, and now also passes it to the container.

An acting user has access in these cases:

- The acting user is an admin.
- `LLM_ACCESS_GROUP` is set and the acting user is a member of that group.

Without `LLM_ACCESS_GROUP`, only admins have access. This matches the access list that `deploy.py` builds: without the group, only the deploying user has access.

The group members are cached for 60 seconds, with the same class that caches the admin group. A failed lookup means no access.

## The app backend

### Registry

`admin_app/registry.py` holds the known services. Each entry has these fields:

| Field | Meaning |
|---|---|
| `name` | The service name. |
| `source` | `found`, `manual` or `configured`. |
| `state` | `ok`, `no_access`, `unreachable` or `not_llm`. |
| `detail` | A short reason for a state other than `ok`. |
| `model` | The served model from the descriptor, or null. |
| `engine` | The engine state from the descriptor. |
| `checked_at` | The time of the last probe, in seconds since the epoch. |

A probe is one `GET /v1/service` call without an acting user, with a timeout of 3 seconds:

- An answer with the right `api` and `api_version` and `caller_admin` true gives `ok`.
- The same answer with `caller_admin` false gives `no_access`. The detail says that the owner of the app is not an admin of the service.
- An answer with status 401 or 403 gives `no_access`.
- Any other answer gives `not_llm`. A transport error gives `unreachable`.

A background task runs each 60 seconds:

1. It builds the candidate names from three sources: the configured names, the manual names and the platform listing.
2. It probes the candidates, 10 at a time.
3. It replaces the registry in one step.

A found name with the state `not_llm` or `unreachable` is not shown, and the task probes it again only after 10 minutes. Configured and manual names always show, with their state.

The first run starts with the app. `GET /api/services` waits for the first run, for 10 seconds at most.

### Sources of names

- Configured: the variable `LLM_SERVICES`, a list of service names with commas. `deploy.py` sets it to the name of the service that it deploys. The old variable `LLM_SERVICE_URL` goes away.
- Platform listing: the command `dt job ls -j -f "type = service"`. The task keeps the jobs with the state `ready` and the environment of the app, from `DATATAILR_JOB_ENVIRONMENT`. If the command fails, the task logs the error once and uses the other two sources. `GET /api/services` then reports `"listing": "failed"`.
- Manual: a list in blob storage at `llm-service-admin/services.json`. Only the owner of the app changes it. The owner is the user in `DATATAILR_USER`.

A service name has 1 to 63 characters from `a-z`, `0-9` and `-`. The app refuses other names with 400 `invalid_service`. The app builds the URL from the name, so a viewer cannot point the app at another host.

### Routes

Routes of the registry:

| Route | Does |
|---|---|
| `GET /api/services` | The viewer, the entries and the listing state. For each `ok` entry, the app calls the descriptor with the acting user and adds `admin` and `access` for the viewer. |
| `POST /api/services` | Adds a manual name. Body: `{"name": "..."}`. Only for the owner of the app, else 403 `owner_required`. The app probes the name at once. |
| `DELETE /api/services/{name}` | Removes a manual name. Only for the owner of the app. |

The answer of `GET /api/services`:

```json
{
  "user": "alice",
  "owner": false,
  "listing": "ok",
  "services": [
    {"name": "llm-service", "source": "found", "state": "ok", "detail": null,
     "model": "Qwen/Qwen2.5-1.5B-Instruct", "engine": "ready", "access": true, "admin": true}
  ]
}
```

The per-viewer calls run in parallel with a timeout of 3 seconds. The app caches `access` and `admin` for each viewer and service for 30 seconds.

A refused acting user gives `access` false. A transport error, a timeout or an answer other than 200 gives `access` null and the field `role_error` with the text "The service did not answer." The app caches this outcome for 5 seconds only. The page shows that text for the service in the picker and in the message without access.

The routes of one service move under the service name. Each one calls the same service endpoint as before:

| Old route | New route |
|---|---|
| `GET /api/status` | `GET /api/services/{name}/status` |
| `POST /api/model` | `POST /api/services/{name}/model` |
| `DELETE /api/models/{id}` | `DELETE /api/services/{name}/models/{id}` |
| `GET /api/usage/{user}` | `GET /api/services/{name}/usage/{user}` |
| `GET /api/audit` | `GET /api/services/{name}/audit` |
| `GET /api/audit/users` | `GET /api/services/{name}/audit/users` |
| `GET /api/audit/records/{id}` | `GET /api/services/{name}/audit/records/{id}` |

`GET /api/me` goes away. `GET /api/services` gives the viewer name.

A route with a name that is not in the registry with the state `ok` answers 404 `unknown_service`.

New routes:

| Route | Does |
|---|---|
| `GET /api/usage` | Combined usage. See [Combined usage](#combined-usage). |
| `GET /api/services/{name}/models` | Calls `GET /v1/models` for the viewer. The Connect tab uses it. |
| `POST /api/services/{name}/chat` | Calls `POST /v1/chat/completions` for the viewer and streams the answer. See [Testing area](#testing-area). |

### Combined usage

`GET /api/usage` takes the parameters `since`, `until`, `user`, `model`, `group_by` and `services`. `services` is a list of names with commas. Without it, the app uses all `ok` services to which the viewer has access.

For each service, the app picks the call by the role of the viewer:

- The viewer is an admin of the service: `GET /admin/usage` with the parameters.
- The viewer is not an admin: `GET /v1/usage/me` with `since` and `until`. The app turns the answer into rows of the same form, with the viewer as the only user.

The calls run in parallel. The app adds the field `service` to each row and merges rows:

- `group_by=user`, `model` or `day`: rows with the same key from different services become one row. The app adds up the requests, the failed requests and the token counts.
- `group_by=service`: one row for each service. This value exists only in the app. The app calls each service with `group_by=day` and adds up the rows.

The answer:

```json
{
  "data": [{"user": "alice", "requests": 12, "failed_requests": 0, "prompt_tokens": 340,
            "completion_tokens": 900, "total_tokens": 1240}],
  "services": [
    {"name": "llm-service", "scope": "all", "ok": true},
    {"name": "llm-service-large", "scope": "own", "ok": false, "error": "The LLM service did not give a valid answer."}
  ]
}
```

`scope` is `all` for an admin and `own` for another user. A service whose role call failed appears with `scope` `own`, `ok` false and the error text. A service without access for the viewer stays out of the list. A failed service does not fail the request. The answer is 200 with `ok` false for that service. If each service fails, the answer is 502 `service_unreachable`.

### Testing area

`POST /api/services/{name}/chat` needs the content type `application/json`, as the other write routes do.

The body has `messages`, and optionally `model`, `temperature` and `max_tokens`. The app builds the request for the service from these fields only, and always sets `stream` to true and `stream_options.include_usage` to true. Limits of the app:

- At most 50 messages, and 100,000 characters in total. More gets 400 `request_too_large`.
- `max_tokens` from 1 to 4096. The default is 512.
- `temperature` from 0 to 2.

The app passes the server-sent events of the service to the browser without a change, with the content type `text/event-stream`. If the service answers with an error before the stream starts, the app passes the status and the JSON body. The app asks the service for a stream without compression. It sends the header `X-Accel-Buffering: no`, so a proxy at the platform edge passes each event at once. A timeout of 300 seconds covers the whole answer. If the browser closes the connection, the app closes the call to the service.

## The page

A bar at the top has the service picker and a reload button. The picker lists the services with their state. It has the entry "All services", which applies to the Usage tab only. The page keeps the picked service in the URL hash, and reacts to a change of the hash.

A panel "Services" opens from the bar. It lists each entry with source, state, detail, model and role of the viewer. The owner of the app sees a field to add a name and a button to remove a manual name.

The tabs:

- Usage. With "All services", the tab shows the combined totals, a table by user, a table by service and the chart by day. A line names each service whose numbers are missing, and each service where the viewer sees only their own numbers. With one service picked, the tab shows that service only.
- Connect. For each viewer with access. See below.
- Audit and Models. For one service. They show only when the viewer is an admin of the picked service. With "All services" they show a note that asks for one service.

If the viewer has access to no service, the page shows one message with the reason for each entry.

### Connect tab

The tab has three parts for the picked service.

Addresses:

- The internal base URL: `http://<service name>/v1`.
- The outside base URL: the origin of the page plus `/job/<environment>/<service name>/v1`. The page builds it only when its own path has the form `/job/<environment>/<app name>/`. Else the page shows a placeholder.
- The served model and the engine state.

Examples, each with a copy button:

- curl, from a job on the platform.
- Python with the `openai` package, from a job on the platform.
- Python with the `openai` package, from outside the platform, with the token in the cookie header.
- The command line for `chat.py`, with `LLM_BASE_URL` set.

The page fills in the URL and the model name. An example never has a token value. The outside example reads the token from the variable `LLM_TOKEN`, and a sentence says where a user gets a token.

Testing area:

- Fields: system prompt, prompt, model (the served model as default), temperature and maximum tokens.
- A Send button and a Stop button. The answer shows while it streams.
- After the answer: prompt tokens, completion tokens, the time to the first token and the total time.
- The area keeps the conversation of the session, so a second prompt continues it. A Clear button starts again.
- A note says that the requests count for the viewer.

## Deployment

- `deploy.py` sets `LLM_SERVICES` for the app and `LLM_ACCESS_GROUP` for the service. No new argument.
- The README gets a part for the app with many services. It explains how the app finds services, the manual list and the Connect tab. It states the rule that the owner of the app must be an admin of each service.
- An older service without `GET /v1/service` does not show in the app when the listing found it. When it is configured or manual, it shows with the state `not_llm`. Deploy the service again first.

## Testing

- Service: unit tests for the descriptor and for each rule of the effective user on a `/v1/*` endpoint. Other tests cover the access check with and without the group, and the `via` field of the audit record.
- Registry: unit tests for each probe result, the three sources, a failed listing, the 10 minute pause and the name check.
- App backend: tests with the FastAPI test client and fake services behind `httpx.MockTransport`. They cover the registry routes, the owner rule, each moved route and an unknown service. They cover the combined usage with an admin service, a non-admin service and a failed service. They cover the chat route with its limits and a stream.
- The page has no automated test. Check it in a browser on a real installation: the picker, the combined usage, each example, the testing area, and a viewer who is not an admin.
- First step of the work: make sure that `dt job ls` runs inside an app container. If it does not, the listing source stays off and the README says so.

## Known limits

- The app works with a service only when the owner of the app is an admin of it.
- The service trusts an admin caller to name the acting user honestly. An admin can send requests in the name of another user. The `via` field of the audit record shows the caller.
- The app finds services in its own environment only.
- A user name is one key over all services. The platform has one user directory, so this is correct.
- The combined chart shows totals for each day, not for each service and day together.
- A viewer who is not an admin of a service gets no numbers from it for a model filter by day. `GET /v1/usage/me` has no such split.
- Without `LLM_ACCESS_GROUP`, only admins can use a service through the app.

## Out of scope

- Limits for each user, live load and latency numbers, and a model catalog. A second design covers them.
- A comparison of two services side by side.
- Services in another environment or on another platform installation.
