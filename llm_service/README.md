# LLM service on Datatailr

This example serves a large language model (LLM) as a Datatailr service. The service speaks the OpenAI API. A client switches between OpenAI and this service when it changes the base URL.

The model is an open weights model from Hugging Face. Open weights means that the weight files are public. The service serves one model at a time.

The service adds these parts to the model:

- An API call switches the model. Another API call lists the models.
- The service keeps the weight files in Datatailr blob storage. It does not download them again at each start.
- The service writes an audit log. The log records who sent each request and with what prompt. Only an admin can read it.
- An admin can read the usage of each user, for example the tokens used.
- An admin can remove the cached weights of a model.
- An admin app is a web page for the usage, the audit log and the models. It works with many LLM services at once.
- Many users can call the service at the same time.

## How it works

The example is one Datatailr `Service`. Two processes run in its container. The first is the gateway, a FastAPI application. The second is vLLM, an open source inference server. The gateway starts vLLM as a child process.

vLLM implements the OpenAI API and runs many requests together on the GPU. The gateway forwards each `/v1` request to vLLM. It adds the parts that vLLM does not have: the user name, the audit log, the usage metrics, the weight cache and the model switch.

```
client -> platform edge -> gateway (FastAPI) -> vLLM (127.0.0.1) -> GPU
                              |
                              +-> blob storage: weights, audit log, state
```

The platform checks the access list of the service before a request reaches the gateway. It also adds the name of the caller in the `X-Datatailr-User` header. The gateway reads the user name from this header.

An admin is the user who deployed the service. Members of the group in `LLM_ADMIN_GROUP` are admins too. The Datatailr SDK caches group lookups. A change of the group members can take up to 10 minutes to apply.

The design document [docs/design.md](docs/design.md) describes the parts of the service in more detail.

## Requirements

The service needs a GPU node. A GPU node is a machine with a GPU that runs jobs. The service sets `vram` (GPU memory), so the platform runs it only on a GPU node.

The platform does not start GPU nodes by itself. You must start one before you deploy. Only an admin of the platform can start a node.

To see the GPU nodes that exist, run this command:

```bash
dt node ls -f "node_class = gpu"
```

If the list is empty, look up the instance types that have a GPU:

```bash
dt node instance-types-by-class
```

Choose a type from the `gpu` class. It must have at least as much GPU memory as `LLM_VRAM`. Then create the node in the environment that you deploy to. Use the `Node` class of the Python SDK:

```python
from datatailr import Node

Node.new({
    "type": "<a GPU instance type>",
    "node_class": "gpu",
    "environment": "dev",
    "num_instances": 1,
    "disk_size": 100,
})
```

The value of `disk_size` is the size of the root disk in GB. The default is 10 GB, and that is too small. The disk must hold the container image, which is about 10 GB with vLLM. It must also hold the weights of the models that you switch to. During a switch, the disk holds the old and the new model. After a successful switch, the service deletes the local weights of the old model. A 1.5B model needs about 3 GB. A 7B model needs about 15 GB.

A new node costs money until you stop it. For all node commands, see `dt node --help`.

This example is made for an Azure installation. It runs on any installation that has a GPU node.

## Deploy

Set the environment variables that you need. All of them are optional. Then run the script:

```bash
python deploy.py
```

The script deploys two jobs: the service first, then the admin app. To deploy one job, name it:

```bash
python deploy.py service
python deploy.py app
```

The script reads these variables at deploy time:

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
| `LLM_PROMPT_RETENTION_DAYS` | not set | Days to keep prompts in the audit log. See [Prompt retention](#prompt-retention). |
| `LLM_METRICS_RETENTION_DAYS` | `30` | Days to keep the load history files. See [Load and latency](#load-and-latency). |
| `LLM_ENVIRONMENT` | `dev` | Platform environment: `dev`, `pre` or `prod`. |
| `LLM_ADMIN_APP_NAME` | `LLM Service Admin` | Name of the admin app. |

If `LLM_ACCESS_GROUP` is not set, only the user who deploys the service has access. If it is set, the owner and the group can read and use the service. Only the owner can change, operate or promote it.

The service container gets `LLM_ACCESS_GROUP` as well. The platform checks the access of the caller at its edge. It cannot check a user that the admin app names in `X-LLM-Acting-User`. The service checks that user against the same group. Without the group, only admins can use the service through the app. See [Acting user on the OpenAI endpoints](#acting-user-on-the-openai-endpoints).

The service name decides the host name. The default name `LLM Service` gives the host name `llm-service`.

### First deploy

The first deploy on a GPU node confirms that vLLM finds the GPU. Read the service log to check this. If vLLM reports a CUDA error, the image has no working CUDA runtime. The fix is in the image build and does not change the example.

The deploy script installs `build-essential` in the image, because vLLM compiles a small C module when it starts. A first start takes a few minutes: the node pulls the image, the service gets the weights, and vLLM loads the model.

The image has one large layer of several GB with vLLM, PyTorch and the CUDA libraries. If the image build fails when it pushes that layer to the platform registry, ask an admin of the platform to check the registry.

To pass extra arguments to vLLM, set `LLM_VLLM_ARGS`. Two common arguments are `--max-model-len` and `--gpu-memory-utilization`:

```bash
LLM_VLLM_ARGS="--max-model-len 8192 --gpu-memory-utilization 0.85" python deploy.py
```

At the first start, the service downloads the default model from Hugging Face. It uploads the files to blob storage. Later starts load the model from blob storage.

## Use from a job inside Datatailr

A job, workstation or notebook on the platform calls the service by its host name. The platform adds the user name. The API key is not checked, but the OpenAI client needs a value.

```python
from openai import OpenAI

client = OpenAI(base_url="http://llm-service/v1", api_key="unused")
reply = client.chat.completions.create(
    model="Qwen/Qwen2.5-1.5B-Instruct",
    messages=[{"role": "user", "content": "Say hello in one sentence."}],
)
print(reply.choices[0].message.content)
```

If you omit `model`, the service uses the model that it serves now. If you name another model, the service returns the error `model_not_found` with status 404.

### A conversation in the terminal

The service keeps no memory between requests. The script `chat.py` keeps the conversation and sends it with each request, so the model sees all earlier turns.

```bash
pip install openai
python chat.py
```

Type a prompt and press Enter. The command `/reset` starts a new conversation. The top of the file lists the environment variables, for example `LLM_MODEL` and `LLM_SYSTEM_PROMPT`.

## Use from outside Datatailr

A client outside the platform must prove who the user is. The platform edge accepts two things on the URL of a job.

The first is the login of the Datatailr CLI. Install the `datatailr` package and run `datatailr login` once. The package then gives the login as a cookie line:

```python
import datatailr
from openai import OpenAI

client = OpenAI(
    base_url="https://<your-domain>/job/<environment>/<service name>/v1",
    api_key="unused",
    default_headers={"Cookie": datatailr.get_remote_oidc_cookie_line()},
)
```

The login ends after some time. Run `datatailr login` again then.

The second is an API token of a platform user. The edge reads it only from the query argument `__datatailr_user_cookie__` of the URL. It does not read a cookie with that name on a job URL, and it does not read the `Authorization: Bearer` header.

```python
import os
from openai import OpenAI

client = OpenAI(
    base_url="https://<your-domain>/job/<environment>/<service name>/v1",
    api_key="unused",
    default_query={"__datatailr_user_cookie__": os.environ["LLM_TOKEN"]},
)
```

A token in a URL can show in the logs of a proxy. Use the login of the CLI where you can.

If the answer is the login page of the platform, the edge did not accept the request. Log in again, or check the token.

The Connect tab of the admin app shows the URL of each service. For the service `LLM Service` in the environment `dev`, the path is `/job/dev/llm-service/v1`.

The script `chat.py` uses both ways. It sends the token of `LLM_TOKEN` if you set one, and the login of the CLI otherwise.

A tool that accepts only a base URL and an API key does not work yet. The platform edge must first accept a Bearer token. That change belongs to the platform and is not part of this example.

The curl examples in this document use two shell variables for a call from outside the platform:

```bash
BASE=https://<your-domain>/job/<environment>/<service name>
COOKIE=$(python -c 'import datatailr; print(datatailr.get_remote_oidc_cookie_line())')
```

From a job on the platform, use `BASE=http://llm-service` and leave the `Cookie` header out.

## Check that a job is an LLM service

`GET /v1/service` describes the service. Each user with access can call it. The admin app uses it to find LLM services.

```bash
curl "$BASE/v1/service" -H "Cookie: $COOKIE"
```

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

- `api` and `api_version` are constants. A client checks them to know that the job is an LLM service.
- `state` and `model` show the engine state and the served model. `model` is `null` when no model is served.
- `user` is the effective user of the request, and `admin` says whether this user is an admin.
- `caller_admin` says whether the signed caller is an admin. The admin app reads it to learn whether the service accepts the acting user.

## Acting user on the OpenAI endpoints

The header `X-LLM-Acting-User` applies to `/admin/*` and to these endpoints: `/v1/service`, `/v1/models`, `/v1/models/{id}`, `/v1/usage/me`, `/v1/limits/me`, `/v1/chat/completions`, `/v1/completions` and `/v1/embeddings`. The admin app sends the name of the viewer in this header. The rules for a `/v1/*` endpoint are:

1. If the header is missing or empty, the effective user is the caller.
2. A name that is longer than 256 characters or has a control character gets 400 `invalid_acting_user`.
3. If the caller is not an admin, the answer is 403 `acting_user_not_allowed`.
4. The acting user must be an admin or a member of `LLM_ACCESS_GROUP`. Otherwise the answer is 403 `access_denied`.
5. If all checks pass, the effective user is the acting user.

`GET /v1/service` never fails because of the header. If a rule fails there, the effective user is the caller and the body has `"acting_user": "refused"`.

Usage numbers and audit records count the effective user. The service caches the members of `LLM_ACCESS_GROUP` for 60 seconds. A failed lookup means no access, and the service tries again after 10 seconds.

## Switch the model

Only an admin can switch the model. Send the name of the Hugging Face repository:

```bash
curl -X POST "$BASE/admin/model" \
  -H "Cookie: $COOKIE" \
  -H "Content-Type: application/json" \
  -d '{"model": "Qwen/Qwen2.5-7B-Instruct", "revision": "main"}'
```

The field `revision` is optional. The call returns status 202 and the switch runs in the background. In these commands, `BASE` is the service URL without `/v1`.

The old model keeps serving requests while the service downloads the new weights. During the download, a request for the new model gets the error `model_not_found` with status 404. Then the service stops vLLM and starts it again with the new model. Only during this restart, requests get status 503 with a `Retry-After` header. A request that is still running when vLLM stops does not complete. If the download fails, the old model keeps serving.

To check the switch, read the state:

```bash
curl "$BASE/admin/model" -H "Cookie: $COOKIE"
```

The answer shows the active model, the switch state and the last switch error. The field `serving` shows the model that vLLM serves now. It is `null` while vLLM restarts. If the new model fails to load, the service starts the previous model again and keeps the error. During this rollback, the field `rolling_back` names the previous model. It is `null` at other times.

While a switch or a rollback runs, a new switch gets status 409 with the code `switch_in_progress`.

### Gated models

Some models need a Hugging Face token. Store the token in the platform secret store. The secret must exist in the environment that you deploy the service to. Use a key in lowercase letters, because the store keeps every key in lowercase. To create the secret in the `dev` environment, run this command:

```bash
dt kv put -s -e dev hf_token <your Hugging Face token>
```

You can also create the entry in the Secrets Manager of the Datatailr interface. Then set `LLM_HF_TOKEN_KEY` to the key name and deploy again:

```bash
LLM_HF_TOKEN_KEY=hf_token python deploy.py
```

To check that the key exists in the `dev` environment, run this command:

```bash
dt kv ls -S -e dev
```

If the service cannot read the secret, it logs an error with the key name and the environment. It then starts without a token. Public models still load.

## List the models

Any user with access can list the models:

```bash
curl "$BASE/v1/models" -H "Cookie: $COOKIE"
```

The list has one entry for each model: the active model, the model that loads, and each model in the blob storage cache. Each entry has the OpenAI fields `id`, `object`, `created` and `owned_by`. It also has four extra fields:

- `status` is `active` for the model that vLLM serves now, `loading` for the model that loads, or `cached`.
- `revision` is the active or loading revision. For a cached model, it is the first cached revision.
- `revisions` is the sorted list of the revisions in blob storage.
- `size_bytes` is the total of the known sizes of the cached revisions. It is `null` if no cached revision has a known size.

To read one entry, add the model name to the path:

```bash
curl "$BASE/v1/models/Qwen/Qwen2.5-1.5B-Instruct" -H "Cookie: $COOKIE"
```

The OpenAI SDK call `client.models.retrieve("Qwen/Qwen2.5-1.5B-Instruct")` uses this path. If the service does not know the model, it returns the error `model_not_found` with status 404.

## Delete a cached model

Only an admin can remove the cached weights of a model from blob storage. The query parameter `revision` limits the call to one revision:

```bash
curl -X DELETE "$BASE/admin/models/Qwen/Qwen2.5-7B-Instruct?revision=main" \
  -H "Cookie: $COOKIE"
```

The call returns status 200 and the list of the removed revisions: `{"deleted": [...]}`. The service removes each matching revision that has files in blob storage, also a revision without a complete copy.

The service does not remove a model that it needs. The call returns status 409 with the code `model_in_use` for these models:

- the model that vLLM serves
- the active model
- the target of a running switch
- the model of a running rollback
- a model that another delete removes now
- the model of a running download, also before its files reach blob storage

The call also returns 409 `model_in_use` while the service starts. A switch to a model that the service removes gets the same error.

The other errors:

- 400 `invalid_model`: the model name or the revision is not valid.
- 404 `model_not_found`: no stored revision matches.
- 409 `download_in_progress`: a download of another model runs now. Delete after it ends.
- 500 `delete_failed`: a removal failed. The revisions removed before the error stay removed.

After a failed delete, send the same delete again. The second delete removes the files that the failed delete left. Each delete makes one audit record.

## Read the audit log

Only an admin can read the audit log. Each request to `/v1/chat/completions`, `/v1/completions` and `/v1/embeddings` makes one record. Each model load also makes one record: a switch, the load at startup and an automatic reload. Other requests make no record.

The list returns the metadata of each record: the time, request id, user, endpoint, model, status, token counts, duration and time to the first token. The list does not return the prompt.

```bash
curl "$BASE/admin/audit?user=bob&limit=20" -H "Cookie: $COOKIE"
```

The filters are `user`, `model`, `since`, `until`, `limit` and `offset`. The values of `since` and `until` are a day like `2026-11-10` or an ISO 8601 time. The default `limit` is 100 and the largest is 1000. The newest records come first.

A request that has an acting user has the extra field `via` with the name of the caller. A request without an acting user has no `via` field. When the acting user is the caller, the record has no `via` field either.

The field `ttft_ms` is the time in milliseconds from the start of the request to the first content of a stream. It is `null` for a request without a stream and for a request without content. A record from before the load numbers has no `ttft_ms` field.

A record of a model event has the field `event`. Its value is `model_switch`, `engine_reload`, `model_delete`, `model_download` or `catalog_change`. The field `detail` holds the revision, the error or the catalog change. A request record has `event` and `detail` set to `null`.

To read the prompt, open one record. Give the request id from the list and the UTC day of the record time. The day must have the form `YYYY-MM-DD`:

```bash
curl "$BASE/admin/audit/records/<request_id>?day=2026-11-10" \
  -H "Cookie: $COOKIE"
```

The answer has the fields of the list row and the field `prompt`. If prompt retention removed the prompt, `prompt` is `null` and `prompt_deleted` is `true`. The errors are:

- 400 `invalid_time`: `day` is missing or has another form.
- 404 `record_not_found`: the day has no record with this id.
- 502 `prompt_unreadable`: the prompt file exists and the service cannot read it, or its line does not hold the record. A missing file of the old layout gives the same error. So does a row with no prompt reference.
- 403 `admin_required`: the caller is not an admin.

To see the users in the log with their first and last request, call `/admin/audit/users`.

The user of a model switch or model delete record is the admin who started it. At startup and after an automatic reload, the user is `system`.

### Prompt retention

By default the service keeps all prompts. To delete old prompts, set `LLM_PROMPT_RETENTION_DAYS` to a whole number from 1 to 36500 and deploy the service again:

```bash
LLM_PROMPT_RETENTION_DAYS=30 python deploy.py service
```

The service then deletes the prompts of each UTC day that ended more than 30 days ago. It checks one time each hour. A background task deletes at most 64 prompt files each 5 seconds, eight at a time. It lists at most 20 prompt folders in that time. A large backlog of old prompts thus takes many steps. The metadata of the records stays, so the list and the usage totals do not change. Opening a record whose prompt is deleted gives `prompt_deleted: true`. Any other value stops the service at the start, with an error that names the variable.

Files of the old layout hold the metadata and the prompt together. Retention does not delete them.

## Read the usage metrics

An admin can read the usage of all users:

```bash
curl "$BASE/admin/usage?group_by=model&since=2026-11-01" \
  -H "Cookie: $COOKIE"
```

The filters are `since`, `until`, `user` and `model`. The values of `since` and `until` are days in the form `YYYY-MM-DD`, and both days are included. The value of `group_by` is `user` (the default), `model` or `day`.

Each row has the request count, the failed request count, the prompt, completion and total tokens, and the time of the last request.

To read one user with the split by model and by day, call `/admin/usage/bob`. Each user can read their own usage at `/v1/usage/me`. This call is not part of the OpenAI API.

## Model catalog

An admin keeps a list of recommended models, downloads a model before a switch, and checks whether a model fits in the GPU memory. All three work without a switch. A model that is not in the catalog still works with the switch and the download.

In the commands of this part, `BASE` is `http://llm-service` and `COOKIE` is the login cookie, as in the parts above.

### The catalog

The service keeps the catalog in `catalog.json` under `LLM_BLOB_ROOT`. It reads the file at the start. A missing or broken file means an empty catalog, and the service logs an error for a broken file. An entry has these fields:

| Field | Meaning |
|---|---|
| `model` | The Hugging Face repository, with the same checks as the model switch. |
| `revision` | The revision, with the same checks as the model switch. |
| `note` | A text of at most 500 characters. The default is an empty text. |
| `gpu_memory_mib` | The GPU memory that the model needs, in MiB, if you know it. Null or a whole number from 1 to 10,000,000. The default is null. |

The pair `model` and `revision` is the key of an entry. An entry has no other field. The catalog has at most 200 entries.

### Download without a switch

A download puts the weights of a model in the cache and does not touch the served model:

```bash
curl -X POST "$BASE/admin/models/download" \
  -H "Cookie: $COOKIE" \
  -H "Content-Type: application/json" \
  -d '{"model": "Qwen/Qwen2.5-0.5B-Instruct", "revision": "main"}'
```

The field `revision` is optional. The call returns status 202 and the download runs in the background. The body of the 202 answer is the model status of `GET /admin/model`. Its field `state` is the state of the engine, not of the download. The field `download` holds the new download. A client tells a new download from a cached model by the HTTP status: 202 or 200. The service downloads the files from Hugging Face and stores them in blob storage. Then it removes the local copy, unless the service needs the model.

Read the state in the field `download` of `GET /admin/model`. The field is null before the first download. It has these keys:

- `model` and `revision`: the pair.
- `state`: `running`, `done` or `failed`.
- `error`: null, or the error text, cut to 300 characters. A download that the service stops at shutdown ends as `failed` with the error `cancelled`.
- `started_at`: the start time in UTC.

The last result stays until the next download starts. The service writes the audit event `model_download` when a download ends.

The rules of the download:

- One download runs at a time. A second call gets 409 `download_in_progress`.
- A model that is in the cache already gets status 200 and the body `{"state": "cached", "model": ..., "revision": ...}`. The service starts nothing.
- An admin switch during a download waits for the lock of the weight cache, also a switch to another model. A switch to the same model then finds the files.
- An automatic reload after an engine exit and a rollback after a failed switch do not wait for a download. They use the complete local copy of the model when it exists.
- A delete of the model that downloads gets 409 `model_in_use`. This holds also before the first file reaches blob storage.
- A delete of another model during a download gets 409 `download_in_progress` with the message "A model download runs now. Delete after it ends."
- A pair that a delete removes now gets 409 `model_in_use`. While a delete of another model runs, a download also gets 409 `model_in_use` with the message "A delete runs now. Download after it ends."
- A bad name or revision gets 400 `invalid_model`.
- A store error in the check of the cache gets 500 `cache_check_failed`.

### The GPU memory check

The check tells an admin whether a model fits before a switch. The revision is `main` when absent:

```bash
curl "$BASE/admin/models/fit?model=Qwen/Qwen2.5-1.5B-Instruct&revision=main" \
  -H "Cookie: $COOKIE"
```

```json
{"fits": "yes", "needed_mib": 6000, "available_mib": 15360, "basis": "catalog", "available_source": "container"}
```

The service takes the needed memory from the first source that has a number:

| `basis` | Source |
|---|---|
| `catalog` | The `gpu_memory_mib` of the catalog entry. |
| `weights` | The size of the cached weights times 1.2, rounded up to a whole MiB. The margin of 1.2 covers the working memory of the engine for a short context. |
| `unknown` | The entry has no `gpu_memory_mib` and the model is not in the cache. `needed_mib` is null. |

The available memory has two sources:

| `available_source` | Source |
|---|---|
| `nvidia-smi` | The total GPU memory from `nvidia-smi` in the last load sample. |
| `container` | The deploy value `LLM_VRAM`. The service uses it when `nvidia-smi` gives no number. Without `nvidia-smi` in the container, this is the usual case. |
| null | `nvidia-smi` gives no number and `LLM_VRAM` is bad. `available_mib` is null. |

The deploy script always passes `LLM_VRAM` to the service. The default is `16g`. The service reads the units `m`, `g` and `t` as binary units, so `16g` is 16,384 MiB.

The field `fits` is `yes` when the needed memory is at most the available memory. It is `no` when the needed memory is larger. It is `unknown` when a number is missing. If the service cannot read blob storage, the answer uses the catalog only, and the call does not fail. A bad name gets 400 `invalid_model`.

### The catalog endpoints

All five endpoints are for admins. A user without admin rights gets 403.

```bash
# Read the entries. Each entry has `cached`, `fit` and the flag `loaded` for the whole list
curl "$BASE/admin/catalog" -H "Cookie: $COOKIE"

# Add an entry, or replace the entry with the same pair
curl -X PUT "$BASE/admin/catalog" \
  -H "Cookie: $COOKIE" \
  -H "Content-Type: application/json" \
  -d '{"model": "Qwen/Qwen2.5-1.5B-Instruct", "revision": "main", "note": "Small and fast. Good for tests.", "gpu_memory_mib": 6000}'

# Remove an entry. The cached files stay in blob storage
curl -X DELETE "$BASE/admin/catalog?model=Qwen/Qwen2.5-1.5B-Instruct&revision=main" \
  -H "Cookie: $COOKIE"

# Download a model, and check the fit
curl -X POST "$BASE/admin/models/download" -H "Cookie: $COOKIE" \
  -H "Content-Type: application/json" -d '{"model": "Qwen/Qwen2.5-0.5B-Instruct"}'
curl "$BASE/admin/models/fit?model=Qwen/Qwen2.5-0.5B-Instruct" -H "Cookie: $COOKIE"
```

| Endpoint | Answer |
|---|---|
| `GET /admin/catalog` | Status 200 and `{"models": [...], "loaded": true}`. `cached` is true when the pair is in the cache. `fit` is the answer of the check. `loaded` is false when the service cannot read `catalog.json` now. The list is then empty. |
| `PUT /admin/catalog` | Status 200 and the stored entry. The errors are 400 `invalid_catalog_entry`, 409 `catalog_full` and 502 `storage_unavailable`. |
| `DELETE /admin/catalog` | Status 200 and `{"removed": {"model": ..., "revision": ...}}`. The revision is `main` when absent. The errors are 400 `invalid_catalog_entry`, 404 `catalog_entry_not_found` and 502 `storage_unavailable`. |
| `POST /admin/models/download` | See above. |
| `GET /admin/models/fit` | See above. |

When the service cannot write `catalog.json`, it answers 502 and keeps the old catalog. After a failed read, the next write or the next flush tries to read the file again. Each change of the catalog writes the audit event `catalog_change` with the admin name, the target (`model@revision`) and the old and new entries.

The admin app has a route for each endpoint under `/api/services/{name}/`: `catalog` (GET, PUT and DELETE), `models/download` and `models/fit`. Each route passes the status and the body of the service through.

### The Catalog card

The Models tab of the admin app has the card "Catalog". It has one row for each entry, with the model, the revision, the note, the needed memory, a fit badge and a cached badge. The badges read `fits`, `does not fit` or `unknown`, and `cached` or `not cached`. Each row has the buttons Serve and Remove from the list. A row that is not cached also has the button Download. Serve fills the serve form and starts the switch. Remove asks for a confirmation and keeps the cached weights. A form under the table adds an entry or replaces the entry with the same pair. A Reload button reads the catalog again. A service without the catalog endpoints shows a note instead of the table.

Under the form "Serve a model", a note shows the fit result for the typed model. A result `does not fit` is a warning. The note says that a switch can still start. It does not block the Serve button. The page runs the check again when a download ends or an entry of the catalog is saved. The end of a switch or a download reads the catalog again.

A running download shows in the engine card with its state, its model and its start time. The tab reads the state of the service each 3 seconds while a switch or a download runs.

### Limits of the model catalog

- The check is an estimate. It does not know the context length or the arguments of the engine. A model that fits can still fail with a long context.
- The switch does not use the check. An admin can start a switch with each result.
- A restart of the service forgets a running download. The partial files do not count as a cached copy, so the next download of the pair starts again.
- One download runs at a time, and the service has no queue.
- A stop of the service during a download lasts until the fetch thread ends or the platform ends the process. The audit event then says `cancelled`, although the files can be complete.
- The `nvidia-smi` total is the sum over all GPUs of the node. On a node with several GPUs, the check can say `fits` for a model that needs one GPU.

## Limits for each user

An admin protects the service from one heavy user with limits. A limit is a maximum for one user. The service has three limits.

| Name | Meaning |
|---|---|
| `requests_per_minute` | The requests of the user in the last 60 seconds. |
| `tokens_per_day` | The total tokens of the user in the current UTC day. |
| `parallel_requests` | The requests of the user in flight now. |

Each value is null or a whole number from 1 to 1,000,000,000. Null means no limit. The value 0 is not valid. To block a user, an admin removes the access of the user on the platform.

The limits apply to `/v1/chat/completions`, `/v1/completions` and `/v1/embeddings`. They apply to the effective user, so a request from the testing area of the app counts for the viewer.

### Which limits a user gets

The service looks up the limits of a user in this order:

1. The entry of the user, if one exists. Each field of the entry is a number, null or absent. An absent field takes the default.
2. For an admin without an entry: no limit.
3. For each other user: the default.

Without a limits file, no user has a limit, and the service works as before. The service keeps the limits in `limits.json` under `LLM_BLOB_ROOT`. It reads the file at the start. A broken file means no limits, and the service logs an error. The next PUT through the API replaces the broken file. A DELETE of an entry on a broken file gets 404 and writes nothing.

If the read at the start fails because of a storage error, the service also applies no limits. Each change then gets 502 `storage_unavailable` until a read works. The service reads the file again at each change, but only while the last read failed. The service also reads it again after each flush of the audit log, so the limits come back without a restart. Until then, the answers of `GET /v1/limits/me` and `GET /admin/limits` have `"loaded": false`.

### The 429 answer

A request over a limit gets status 429. The service checks the limits in this order: `parallel_requests`, `requests_per_minute`, `tokens_per_day`. A refused request does not count in the window or in the parallel counter.

```json
{"error": {"message": "You reached the limit of 60 requests in one minute.",
           "type": "rate_limit_error", "code": "requests_per_minute"}}
```

The `code` is the name of the limit. The header `Retry-After` has whole seconds:

- `requests_per_minute`: the time until the oldest request leaves the window, at least 1.
- `tokens_per_day`: the time until the next UTC midnight.
- `parallel_requests`: 1.

A refused request gets an audit record with status 429. It counts as a failed request in the usage numbers and adds no tokens.

### Read and change the limits

Each user reads their own limits and use:

```bash
curl http://llm-service/v1/limits/me
```

```json
{
  "user": "bob",
  "limits": {"requests_per_minute": 60, "tokens_per_day": null, "parallel_requests": 4},
  "use": {"requests_last_minute": 3, "tokens_today": 15234, "parallel_requests": 1},
  "source": "user",
  "loaded": true
}
```

The `source` is `user`, `default`, `admin` or `none`. `loaded` is false while the service cannot read the limits file. The next calls are for admins.

```bash
# Read the default, the user entries, the current use and the effective limits of each user
curl http://llm-service/admin/limits

# Replace the default. The body needs all three fields. Null means no limit.
curl -X PUT http://llm-service/admin/limits/default \
  -H "Content-Type: application/json" \
  -d '{"requests_per_minute": 60, "tokens_per_day": 2000000, "parallel_requests": 4}'

# Set the entry of carol. The body needs one to three fields.
curl -X PUT http://llm-service/admin/limits/users/carol \
  -H "Content-Type: application/json" \
  -d '{"requests_per_minute": 10, "parallel_requests": 1}'

# Let bob use any number of tokens in a day
curl -X PUT http://llm-service/admin/limits/users/bob \
  -H "Content-Type: application/json" \
  -d '{"tokens_per_day": null}'

# Remove the entry of alice
curl -X DELETE http://llm-service/admin/limits/users/alice
```

The errors are:

- 400 `invalid_limit`: the body is not JSON, or it has an unknown field, a wrong type or a number outside the range. The default body lacks a field, or the body of a user is empty.
- 400 `invalid_user`: the user name is empty, is longer than 256 characters or has a control character.
- 403: the caller is not an admin.
- 404 `limit_not_found`: the user has no entry. This applies to the delete.
- 502 `storage_unavailable`: the service cannot read or write the limits file.

Each change writes an audit event `limits_change` with the admin name, the target (`default` or the user name) and the old and new values.

### The Limits tab

The admin app has the Limits tab for an admin of the picked service. It has a card for the default limits and a table of the user entries with the current use. The table shows the limits that apply to each user, from `effective` in the answer of `GET /admin/limits`. A limit from the default is muted. An admin without an entry has "No limit". If the service did not read the limits file, the tab and the card "Your limits" show a note. An admin adds, changes and removes an entry there. An empty field in the default card means no limit. Each viewer sees their own limits in the card "Your limits" on the Connect tab.

## Load and latency

An admin sees how busy a service is now and how busy it was before. The numbers are requests in flight, requests that wait, tokens per second, latency, the KV cache use of the engine and GPU memory.

### Where the numbers come from

| Number | Source |
|---|---|
| Requests in flight | A counter in the gateway. |
| Requests that run and that wait in the engine | The vLLM metrics endpoint, `GET /metrics` on the engine port. |
| Prompt and generated tokens per second | The token counters of the same endpoint. The rate is the change between two samples. |
| KV cache use | The same endpoint, `vllm:kv_cache_usage_perc` or the older `vllm:gpu_cache_usage_perc`. A fraction from 0 to 1. With more than one engine, the largest value. |
| GPU memory used and total | `nvidia-smi`. With more than one GPU, the numbers are sums. |
| Time to the first token and total time | The gateway measures each request. |

The service reads GPU memory at each third sample, so each 15 seconds. The samples between use the last value.

vLLM takes most of the GPU memory at its start and keeps it. So the GPU memory used stays near the total and says little about the load. The KV cache use is the number to watch. The KV cache holds the tokens of the running requests. When it is full, new requests wait. On a node without `nvidia-smi` in the container, the GPU memory numbers are empty.

Some vLLM versions report the KV cache use from 0 to 100. The service reads a value above 1.5 as a percent and divides it by 100. It limits each value to the range from 0 to 1.

The names of the vLLM metrics differ between versions. The service reads each number from a short list of known names. If the installed vLLM version uses another name, that number shows as empty. The service log then has a warning that names the metric names the service saw. A failure to read the metrics endpoint or `nvidia-smi` also gives an empty number. The service logs the first failure and the recovery. It does not stop, and no request fails.

### The sample and the point

A sample is a set of numbers that the service takes each 5 seconds. Its fields are `ts`, `in_flight`, `running`, `waiting`, `kv_cache_use`, `prompt_tokens_per_s`, `completion_tokens_per_s`, `gpu_memory_used` and `gpu_memory_total`. At the end of each minute, the service makes one point from the samples of that minute and the requests that ended in it. It keeps the last 60 points and the last 12 samples in memory.

A point has these fields:

| Field | Meaning |
|---|---|
| `ts` | The start of the minute, an ISO time in UTC with `Z`. |
| `requests`, `failed_requests` | The requests that ended in the minute, and those with status 400 or higher. |
| `in_flight_max`, `waiting_max` | The largest values of the samples of the minute. |
| `kv_cache_use_max` | The largest KV cache use of the samples of the minute, a fraction from 0 to 1. |
| `prompt_tokens`, `completion_tokens` | The token sums of the requests that ended in the minute. |
| `ttft_ms_p50`, `ttft_ms_p95` | The percentiles of the time to the first token, in milliseconds. Only requests with a status below 400 count. |
| `duration_ms_p50`, `duration_ms_p95` | The percentiles of the total time, in milliseconds. Only requests with a status below 400 count. |
| `gpu_memory_used_max`, `gpu_memory_total` | The largest used value and the total of the minute, in MiB. |

A refused or failed request ends fast, so it stays out of the percentiles. A percentile is `null` for a minute without a matching request. A maximum or a GPU value is `null` when no sample of the minute had it. A point in a file of an older version has no `kv_cache_use_max`. The service reads it as `null`.

### Read the load

Both calls are for admins. This call answers from memory. It has the last sample in `now` and the last 60 points.

```bash
curl http://llm-service/admin/metrics/live
```

```json
{"now": {"ts": "2026-11-12T10:15:35Z", "in_flight": 3, "running": 3, "waiting": 0, "kv_cache_use": 0.37,
         "prompt_tokens_per_s": 210.0, "completion_tokens_per_s": 96.5,
         "gpu_memory_used": 14200, "gpu_memory_total": 15360},
 "points": [{"ts": "2026-11-12T10:14:00Z", "requests": 42, "failed_requests": 1,
             "in_flight_max": 6, "waiting_max": 2, "kv_cache_use_max": 0.52,
             "prompt_tokens": 9100, "completion_tokens": 5200,
             "ttft_ms_p50": 310, "ttft_ms_p95": 900,
             "duration_ms_p50": 2100, "duration_ms_p95": 7400,
             "gpu_memory_used_max": 14200, "gpu_memory_total": 15360}]}
```

Before the first sample, each field of `now` is `null`.

The history call reads the points of a time range from blob storage. It adds the points that are still in memory. The values of `since` and `until` are ISO 8601 times.

```bash
curl "http://llm-service/admin/metrics/history?since=2026-11-12T00:00:00Z&until=2026-11-12T12:00:00Z"
```

```json
{"step_seconds": 60, "points": [ ... ], "missing_days": []}
```

The range is at most 31 days as a duration. The range touches a count of minutes, counted from the minute of `since`. The step is that count divided by 500 and rounded up, at least 1 minute. The service puts each point into the time bucket of one step that holds it. The buckets start at the minute of `since`. Each bucket with points gives one point, with the start of the bucket as `ts`. A bucket without points gives no point, so a gap stays a gap. A range of up to 500 minutes keeps each point as it is. Counts and token sums add up. Maximum values stay maximum values. A percentile becomes the maximum of the merged percentiles. `step_seconds` is the time that one returned point covers. `missing_days` lists the UTC days with a failed file read, or each day of the range after a failed listing. A day with no file is not a failure.

A time that is not valid, a `since` after `until` or a range over 31 days gets 400 `invalid_time`. A caller who is not an admin gets 403.

### The history files

Each process of the service writes its own file for each UTC day: `<root>/metrics/<day>/<writer>.jsonl`. `<writer>` is the start time of the process and 8 random hex characters, like `20261112T093000Z-3f9a0c1d`. The file has one JSON line for each point of that process, in time order. Each 5 minutes, and at a clean stop, the process writes its whole file of each day that has new points. It never writes the file of another process. So during a redeploy, the old and the new container cannot overwrite the points of each other. A restart starts a new file. A failed write keeps the points in memory for the next try. If a past day has no good write for one hour, the service drops its points and logs an error.

A history read lists `<root>/metrics/` once and reads each file of the days in the range, the current day too. So the last points of an old container show at once. Two points of one minute from two processes combine: sums add and maximum values take the maximum. A file `<root>/metrics/<day>.jsonl` of an earlier test version counts too.

`LLM_METRICS_RETENTION_DAYS` sets how many days the service keeps. The default is 30. The value is a whole number from 1 to 36500. A background task deletes the files of older days, at most 64 in one run. Set the variable and deploy the service again:

```bash
LLM_METRICS_RETENTION_DAYS=90 python deploy.py service
```

### The Load tab

The admin app has the Load tab, after the Usage tab, for an admin. With one service, the tab shows two rows of tiles and line charts.

- Six tiles for this moment: requests in flight, requests that wait, completion tokens per second, prompt tokens per second, KV cache use and GPU memory used. The KV cache use shows in percent.
- Six tiles for the last complete minute. They show requests and failed requests. They show the time to the first token as p50 and p95. They show the total time as p50 and p95.
- Line charts of the points. The chart "Engine memory" shows the largest KV cache use of each point in percent, on an axis from 0 to 100. It shows the GPU memory used in GiB on a second axis, only when a point has that value. A minute without a value is a gap in the line. A value between two gaps shows as a dot, so thin traffic stays visible.

A range control has "Last hour", "6 hours", "24 hours", "7 days" and a custom range. With "Last hour", the tab reads the numbers again each 5 seconds. The other ranges read the history once. A history range shows the step of its points, for example 5 minutes. Its time axis starts at the minute of the start of the range, as the points do. It also names the UTC days whose history the service did not read, from `missing_days`. The tab runs one history read at a time. A new range during a read ends that read, and only the latest range reads next.

A read that gets no answer in 30 seconds counts as a failed read. If 5 reads in a row fail, the tab stops the 5 second read and shows a note with the last error. Use the Reload button of the page bar to read again.

With "All services" picked, the tab shows a table that reads the numbers each 5 seconds. It has one row for each service where the viewer is an admin, with the numbers of this moment and the KV cache use. A service that is not usable has a row with the reason. A service where the viewer is not an admin is absent. The app route `GET /api/load` gives these rows. The routes `GET /api/services/{name}/metrics/live` and `GET /api/services/{name}/metrics/history` pass the two service calls through.

## Admin app

The admin app is a web page for the users of your LLM services. It is the second job that `deploy.py` deploys. It works with each LLM service in its environment, not with one fixed service. It keeps no data, except a list of service names that the owner of the app edits.

The page has a bar at the top with a service picker and a reload button. The picker lists the services with their state. It also has the entry "All services" for the Usage and Load tabs. The page keeps the picked service in the address, after the `#`.

### How the app finds services

The app collects service names from three sources. Each minute it asks each name that is not paused for `GET /v1/service`, and a name that answers as an LLM service shows in the picker. A name from the listing that does not answer, or does not answer as an LLM service, pauses for 10 minutes.

- The platform listing. The app runs `dt job ls` and keeps the services that are ready in its own environment. If the command fails, the app uses the other two sources and the page shows that the listing failed.
- `LLM_SERVICES`. A list of service names with commas. `deploy.py` sets it to the service that it deploys.
- The manual list. The owner of the app adds a name in the panel "Services". The app keeps the list in blob storage at `llm-service-admin/services.json`. The owner is the user who deployed the app.

A service name has 1 to 63 characters from `a-z`, `0-9` and `-`. The app builds the URL `http://<name>` from it, so a name cannot point the app at another host. A service found in the listing that is not an LLM service does not show. A name from `LLM_SERVICES` or the manual list always shows, with its state.

A service from an older version of this example has no `GET /v1/service`. When the listing found it, it does not show in the app. When it is in `LLM_SERVICES` or in the manual list, it shows with the state `not_llm`. Deploy that service again.

### Service states

- `ok`: the service answers and the owner of the app is an admin of it.
- `no_access`: the service answers, but it refuses the app. The owner of the app is not an admin of the service.
- `unreachable`: the app gets no answer from the service.
- `not_llm`: the job answers, but not as an LLM service.

### The owner of the app must be an admin

The platform signs each call of the app as the user who deployed the app. The service accepts the acting user only from an admin caller. So the owner of the app must be an admin of each service that the app shows. Add the owner to `LLM_ADMIN_GROUP` of the service, or deploy the service and the app as the same user. A service where the owner is not an admin shows the state `no_access`.

### Who sees what

Each viewer needs access to a service to see anything from it. The service checks this for the viewer, with `LLM_ACCESS_GROUP`. If the service does not answer this check, the page shows "The service did not answer." for it. The Usage tab then names that service as a service without numbers.

- Each user with access sees the Usage tab with their own numbers, and the Connect tab. The Connect tab has the card "Your limits".
- An admin of a service also sees the numbers of all users of that service. An admin also sees the Audit, Models, Limits and Load tabs. An admin switches the model, deletes cached weights and keeps the model catalog in the Models tab. An admin sets limits in the Limits tab.
- With "All services" picked, the Usage tab shows totals over all services, by user, by service and by day. A line names each service whose numbers are missing. Another line names each service where the viewer sees only their own numbers.
- The Audit, Models and Limits tabs work with one service. With "All services" they ask the viewer to pick one. The Load tab shows a table of the services where the viewer is an admin instead.

The platform user names of the viewers must be ASCII. The app sends the name in an HTTP header. For a name that is not ASCII, the app answers 401 with the code `missing_user`.

### The Connect tab

The Connect tab shows how to call the picked service. It has these parts.

- Addresses and examples. The tab shows the internal base URL `http://<name>/v1` and the outside base URL, with the served model. It has examples for curl, for the `openai` package from a job, for the `openai` package from outside the platform, and for `chat.py`. Each example has a copy button. No example holds a token. The outside example sends the login of the Datatailr CLI.
- The script `chat.py`. A card shows the file, with a Copy button and a Download link.
- The testing area. A viewer types a system prompt and a prompt, sets the model, the temperature and the maximum tokens, and presses Send. The answer streams into the page. After it, the area shows the token counts and the time to the first token. The area keeps the conversation, and Clear starts again. If the service answers 429, the area shows the message of the answer and the wait time.
- The card "Your limits". The card shows the three limits of the viewer, the current use and the source of the limits. The source is the entry of the user, the default, admin (no limit) or none. A limit with no value shows as no limit.

A request from the testing area counts for the viewer. It shows in the usage numbers and in the audit log under the name of the viewer. The `via` field of the record names the owner of the app. When the viewer is the owner of the app, the record has no `via` field.

If you deploy the app alone with `python deploy.py app`, set the same `LLM_SERVICE_NAME` as for the service. The app then lists that service in `LLM_SERVICES`.

Open the app from the Datatailr interface. If the page does not load its data, open it with a trailing slash in the address. The design documents [docs/multi-service-app-design.md](docs/multi-service-app-design.md) and [docs/admin-app-design.md](docs/admin-app-design.md) describe the app.

## Where the data lives

All data is in blob storage, under the root directory in `LLM_BLOB_ROOT`:

```
<root>/models/<org>--<name>/<revision>/<files>
<root>/models/<org>--<name>/<revision>/.complete    # the file list, written last
<root>/state/active.json                             # the active model
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

The service writes `.complete` after the last weight file. The marker lists each file with its size. An upload that stops half way is not used. When the service downloads a model from blob storage, it downloads only the listed files and checks their sizes. If a file is missing or has the wrong size, the load fails with an error that names the file.

A flush writes the prompts first, then the metadata. Before the first prompts of a day, it writes the small marker `prompts.marker` in the audit folder of the day. Prompt retention uses the marker to find the day. The service merges the metadata of each 10 minutes into one part file, and the parts of a finished day into one day file. The merges run in a background task of their own, so a flush does not wait for them. A bucket merges 1 to 2 minutes after its end, and a day 15 to 16 minutes after its end. A `<day>` is a UTC day like `2026-11-11`. A `<bucket>` is the date, the hour and the tens digit of the minute, like `20261111T140`. The prompts live in their own folder, so a list of the audit log never reads them. The design document [docs/audit-storage-design.md](docs/audit-storage-design.md) describes the layout.

Files of the old layout, `audit/<day>/<time>-<id>.jsonl` with the full records, stay readable. The service never merges or changes them. Their records show in the list and count in the usage totals.

The metadata files are the source of truth. The usage totals come from them. If you delete a daily usage file, the service builds it again from the metadata files.

The service writes a checkpoint of the usage totals of the current day to `usage/<day>.partial.json`. It writes at most one time each 60 seconds, and at a clean stop. At a start, the service reads for each day the daily file if it exists. Otherwise it reads the checkpoint and the metadata that came after it. Without a checkpoint, it reads all metadata of the day. A start lists `audit/` and `usage/` one time each. It reads one small usage file for each past day, eight at a time. So the start time grows slowly with the number of days, not with the load of a day.

The first start after the upgrade reads every old layout file of the days without a daily usage file, as the old version did. Later starts use the daily files and the checkpoint.

## Known limits

- The service buffers audit records in memory. It writes them to blob storage every 5 seconds, at 100 records, and at shutdown. In normal operation, a kill of the container loses the records since the last flush. A flush runs every 5 seconds. It can wait for the write of one merged file, a few seconds on the platform. During a blob storage outage, the records wait in memory. A kill then loses all records since the outage began.
- If vLLM stops, the next request to a model endpoint starts one reload of the active model. During the reload, requests get status 503 with a `Retry-After` header. If that reload fails, an admin must call `POST /admin/model`. Until then, requests get status 503 with the code `no_model`.
- If the engine fails in the middle of a stream, the stream ends without an error event. The audit record has the status 502.
- At startup, the service reads the usage totals from blob storage. If that read fails, the service does not start, and the platform restarts it.
- The service serves one model at a time. A switch stops the old model.
- An admin who calls the service directly can name any user with access in `X-LLM-Acting-User`. The `via` field of the audit record shows the caller.
- The audit log stores each prompt in full. It does not store the answers. A prompt is not searchable. The filters work on metadata.
- One service instance writes the audit log. Two instances on one `LLM_BLOB_ROOT` are not supported.
- A day file is read in one piece. The design fits about 300,000 requests a day.
- A blob write that fails after the blob was written leaves the records in the buffer. The next flush writes them again under a new segment name, so they appear two times in the audit list. The usage totals count such a record one time. A restart before the next checkpoint counts it two times. A day that the service rebuilds from the metadata, with no checkpoint or with an invalid usage file, also counts it two times.
- The merges assume that a blob write is atomic. A reader never sees half a file.
- A clock that steps back across a restart can give a new segment a name that is not greater than the last checkpoint. A crash before the next checkpoint then loses those rows from the usage totals.
- A stop ends the maintenance task first. It does not wait for a merge step that runs at that moment. The last flush waits at most for the write of one merged file. The process exits when the running merge step ends.
- A late record for a day whose daily usage file exists counts in memory at once. The service writes it to the daily file at the next flusher cycle. A crash between the two loses it from the usage totals.
- Token counts for a stream come from the last chunk. The service asks vLLM for that chunk. It removes the chunk again if the client did not ask for it.
- The token budget passes by the size of the requests in flight. The service checks `tokens_per_day` before a request, and the service knows the tokens of a request only at its end. So a user can pass the budget with parallel requests. The next request is refused.
- A restart of the service clears the request window and the parallel counter. The token budget stays, because it comes from the usage totals. A crash can lose the tokens of the last seconds, as it does for the usage numbers.
- The limits are for each service. Each service has its own limits file, and the app shows the limits of the picked service.
- A clean stop of the service loses only the load point of the open minute. A crash loses the points of up to the last 5 minutes and the open minute, the points that are not in blob storage yet.
- The service has no load numbers for the minutes when it did not run.
- A history read makes one listing of `<root>/metrics/` and one read for each file of the range. Each process that ran on a day has its own file. A day with many restarts has many small files, and its read takes longer.
- A point of a merged history range holds the maximum of the merged percentiles. It is not the true percentile of the longer time.
- The model catalog and the GPU memory check have their own limits. See [Limits of the model catalog](#limits-of-the-model-catalog).
- The platform does not start GPU nodes. You must start one.

## Run the tests

The tests need no GPU, no vLLM and no network. A fake engine and an in-memory blob store replace the real parts.

```bash
pip install -r llm_service/requirements-dev.txt
python -m pytest llm_service/tests -q
```

Run the command from the root of this repository.
