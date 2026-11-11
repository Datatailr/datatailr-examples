# LLM service on Datatailr

This example serves a large language model (LLM) as a Datatailr service. The service speaks the OpenAI API. A client switches between OpenAI and this service when it changes the base URL.

The model is an open weights model from Hugging Face. Open weights means that the weight files are public. The service serves one model at a time.

The service adds these parts to the model:

- An API call switches the model. Another API call lists the models.
- The service keeps the weight files in Datatailr blob storage. It does not download them again at each start.
- The service writes an audit log. The log records who sent each request and with what prompt. Only an admin can read it.
- An admin can read the usage of each user, for example the tokens used.
- An admin can remove the cached weights of a model.
- An admin app is a web page for the usage, the audit log and the models.
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
| `LLM_VRAM` | `16g` | GPU memory of the container. |
| `LLM_MEMORY` | `24g` | Memory of the container. |
| `LLM_CPU` | `4` | CPU count of the container. |
| `LLM_ADMIN_GROUP` | not set | Platform group whose members are admins. |
| `LLM_ACCESS_GROUP` | not set | Platform group that gets read and access permission. |
| `LLM_BLOB_ROOT` | `llm-service` | Root directory in blob storage. |
| `LLM_HF_TOKEN_KEY` | not set | Secret store key of the Hugging Face token. |
| `LLM_VLLM_ARGS` | not set | Extra arguments for `vllm serve`. |
| `LLM_PROMPT_RETENTION_DAYS` | not set | Days to keep prompts in the audit log. See [Prompt retention](#prompt-retention). |
| `LLM_ENVIRONMENT` | `dev` | Platform environment: `dev`, `pre` or `prod`. |
| `LLM_ADMIN_APP_NAME` | `LLM Service Admin` | Name of the admin app. |

If `LLM_ACCESS_GROUP` is not set, only the user who deploys the service has access. If it is set, the owner and the group can read and use the service. Only the owner can change, operate or promote it.

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

## Use from outside Datatailr

A client outside the platform must send an API token of a platform user. The platform edge reads the token from the `__datatailr_user_cookie__` cookie. It does not read the `Authorization: Bearer` header.

```python
from openai import OpenAI

client = OpenAI(
    base_url="https://<your-domain>/<service path>/v1",
    api_key="unused",
    default_headers={"Cookie": "__datatailr_user_cookie__=<token>"},
)
```

The service page in the Datatailr interface shows the URL. Use that URL in place of `https://<your-domain>/<service path>`.

A tool that accepts only a base URL and an API key does not work yet. The platform edge must first accept a Bearer token. That change belongs to the platform and is not part of this example.

## Switch the model

Only an admin can switch the model. Send the name of the Hugging Face repository:

```bash
curl -X POST "$BASE/admin/model" \
  -H "Cookie: __datatailr_user_cookie__=$TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"model": "Qwen/Qwen2.5-7B-Instruct", "revision": "main"}'
```

The field `revision` is optional. The call returns status 202 and the switch runs in the background. In these commands, `BASE` is the service URL without `/v1`.

The old model keeps serving requests while the service downloads the new weights. During the download, a request for the new model gets the error `model_not_found` with status 404. Then the service stops vLLM and starts it again with the new model. Only during this restart, requests get status 503 with a `Retry-After` header. A request that is still running when vLLM stops does not complete. If the download fails, the old model keeps serving.

To check the switch, read the state:

```bash
curl "$BASE/admin/model" -H "Cookie: __datatailr_user_cookie__=$TOKEN"
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
curl "$BASE/v1/models" -H "Cookie: __datatailr_user_cookie__=$TOKEN"
```

The list has one entry for each model: the active model, the model that loads, and each model in the blob storage cache. Each entry has the OpenAI fields `id`, `object`, `created` and `owned_by`. It also has four extra fields:

- `status` is `active` for the model that vLLM serves now, `loading` for the model that loads, or `cached`.
- `revision` is the active or loading revision. For a cached model, it is the first cached revision.
- `revisions` is the sorted list of the revisions in blob storage.
- `size_bytes` is the total of the known sizes of the cached revisions. It is `null` if no cached revision has a known size.

To read one entry, add the model name to the path:

```bash
curl "$BASE/v1/models/Qwen/Qwen2.5-1.5B-Instruct" -H "Cookie: __datatailr_user_cookie__=$TOKEN"
```

The OpenAI SDK call `client.models.retrieve("Qwen/Qwen2.5-1.5B-Instruct")` uses this path. If the service does not know the model, it returns the error `model_not_found` with status 404.

## Delete a cached model

Only an admin can remove the cached weights of a model from blob storage. The query parameter `revision` limits the call to one revision:

```bash
curl -X DELETE "$BASE/admin/models/Qwen/Qwen2.5-7B-Instruct?revision=main" \
  -H "Cookie: __datatailr_user_cookie__=$TOKEN"
```

The call returns status 200 and the list of the removed revisions: `{"deleted": [...]}`. The service removes each matching revision that has files in blob storage, also a revision without a complete copy.

The service does not remove a model that it needs. The call returns status 409 with the code `model_in_use` for these models:

- the model that vLLM serves
- the active model
- the target of a running switch
- the model of a running rollback
- a model that another delete removes now

The call also returns 409 `model_in_use` while the service starts. A switch to a model that the service removes gets the same error.

The other errors:

- 400 `invalid_model`: the model name or the revision is not valid.
- 404 `model_not_found`: no stored revision matches.
- 500 `delete_failed`: a removal failed. The revisions removed before the error stay removed.

After a failed delete, send the same delete again. The second delete removes the files that the failed delete left. Each delete makes one audit record.

## Read the audit log

Only an admin can read the audit log. Each request to `/v1/chat/completions`, `/v1/completions` and `/v1/embeddings` makes one record. Each model load also makes one record: a switch, the load at startup and an automatic reload. Other requests make no record.

The list returns the metadata of each record: the time, request id, user, endpoint, model, status, token counts and duration. The list does not return the prompt.

```bash
curl "$BASE/admin/audit?user=bob&limit=20" -H "Cookie: __datatailr_user_cookie__=$TOKEN"
```

The filters are `user`, `model`, `since`, `until`, `limit` and `offset`. The values of `since` and `until` are a day like `2026-11-10` or an ISO 8601 time. The default `limit` is 100 and the largest is 1000. The newest records come first.

A model load or delete record has the field `event` (`model_switch`, `engine_reload` or `model_delete`) and the field `detail` with the revision or the error. A request record has `event` and `detail` set to `null`.

To read the prompt, open one record. Give the request id from the list and the UTC day of the record time. The day must have the form `YYYY-MM-DD`:

```bash
curl "$BASE/admin/audit/records/<request_id>?day=2026-11-10" \
  -H "Cookie: __datatailr_user_cookie__=$TOKEN"
```

The answer has the fields of the list row and the field `prompt`. If prompt retention removed the prompt, `prompt` is `null` and `prompt_deleted` is `true`. The errors are:

- 400 `invalid_time`: `day` is missing or has another form.
- 404 `record_not_found`: the day has no record with this id.
- 502 `prompt_unreadable`: the prompt file exists and the service cannot read it.
- 403 `admin_required`: the caller is not an admin.

To see the users in the log with their first and last request, call `/admin/audit/users`.

The user of a model switch or model delete record is the admin who started it. At startup and after an automatic reload, the user is `system`.

### Prompt retention

By default the service keeps all prompts. To delete old prompts, set `LLM_PROMPT_RETENTION_DAYS` to a whole number from 1 to 36500 and deploy the service again:

```bash
LLM_PROMPT_RETENTION_DAYS=30 python deploy.py service
```

The service then deletes the prompts of each UTC day that ended more than 30 days ago. It checks one time each hour. The metadata of the records stays, so the list and the usage totals do not change. Opening a record whose prompt is deleted gives `prompt_deleted: true`. Any other value stops the service at the start, with an error that names the variable.

Files of the old layout hold the metadata and the prompt together. Retention does not delete them.

## Read the usage metrics

An admin can read the usage of all users:

```bash
curl "$BASE/admin/usage?group_by=model&since=2026-11-01" \
  -H "Cookie: __datatailr_user_cookie__=$TOKEN"
```

The filters are `since`, `until`, `user` and `model`. The values of `since` and `until` are days in the form `YYYY-MM-DD`, and both days are included. The value of `group_by` is `user` (the default), `model` or `day`.

Each row has the request count, the failed request count, the prompt, completion and total tokens, and the time of the last request.

To read one user with the split by model and by day, call `/admin/usage/bob`. Each user can read their own usage at `/v1/usage/me`. This call is not part of the OpenAI API.

## Admin app

The admin app is a web page for the admins of the service. It is the second job that `deploy.py` deploys. It has three tabs:

- Usage: requests, failed requests and tokens, by user, by model and by day.
- Audit: the audit log, with filters for user, model and time. A click on a row loads the full record with its prompt.
- Models: the engine state and the cached models. An admin switches the model, follows the progress and deletes cached weights.

The app keeps no data. It calls the service by its internal URL. The app takes this URL from the service name, so `LLM Service` gives `http://llm-service`. If you deploy the app alone with `python deploy.py app`, set the same `LLM_SERVICE_NAME` as for the deploy of the service. Otherwise the app calls a service that does not exist.

Only an admin of the service can use the app. The service decides this, not the app. A user who is not an admin sees the text "You are not an admin of the LLM service." If every viewer sees this text, the user who deployed the app is not an admin of the service.

The platform user names of the viewers must be ASCII. The app sends the name in an HTTP header. For a name that is not ASCII, the app answers 401 with the code `missing_user`.

The platform signs the calls of the app as the user who deployed it. This user must be an admin of the service: the user who deployed the service, or a member of `LLM_ADMIN_GROUP`. The app also sends the name of the viewer in the header `X-LLM-Acting-User`. The service then requires that the viewer is an admin too, and the audit log names the viewer.

Open the app from the Datatailr interface. If the page does not load its data, open it with a trailing slash in the address. The design document [docs/admin-app-design.md](docs/admin-app-design.md) describes the app.

## Where the data lives

All data is in blob storage, under the root directory in `LLM_BLOB_ROOT`:

```
<root>/models/<org>--<name>/<revision>/<files>
<root>/models/<org>--<name>/<revision>/.complete    # the file list, written last
<root>/state/active.json                             # the active model
<root>/audit/<day>/segments/<time>-<id>.jsonl        # metadata of one flush
<root>/audit/<day>/parts/<bucket>.jsonl              # metadata of one 10 minute bucket
<root>/audit/<day>/day.jsonl                         # metadata of one finished day
<root>/prompts/<day>/<time>-<id>.jsonl               # prompts of one flush
<root>/usage/<day>.json                              # totals of one finished day
<root>/usage/<day>.partial.json                      # checkpoint of the current day
```

The service writes `.complete` after the last weight file. The marker lists each file with its size. An upload that stops half way is not used. When the service downloads a model from blob storage, it downloads only the listed files and checks their sizes. If a file is missing or has the wrong size, the load fails with an error that names the file.

A flush writes the prompts first, then the metadata. The service merges the metadata of each 10 minutes into one part file, and the parts of a finished day into one day file. The merges run in the background. A bucket merges 1 to 2 minutes after its end, and a day 15 to 16 minutes after its end. A `<day>` is a UTC day like `2026-11-11`. A `<bucket>` is the date, the hour and the tens digit of the minute, like `20261111T140`. The prompts live in their own folder, so a list of the audit log never reads them. The design document [docs/audit-storage-design.md](docs/audit-storage-design.md) describes the layout.

Files of the old layout, `audit/<day>/<time>-<id>.jsonl` with the full records, stay readable. The service never merges or changes them. Their records show in the list and count in the usage totals.

The metadata files are the source of truth. The usage totals come from them. If you delete a daily usage file, the service builds it again from the metadata files.

The service writes a checkpoint of the usage totals of the current day to `usage/<day>.partial.json`. It writes at most one time each 60 seconds, and at a clean stop. At a start, the service reads for each day the daily file if it exists. Otherwise it reads the checkpoint and the metadata that came after it. Without a checkpoint, it reads all metadata of the day. So a start reads a small number of files, also on a busy day.

## Known limits

- The service buffers audit records in memory. It writes them to blob storage every 5 seconds, at 100 records, and at shutdown. In normal operation, a kill of the container loses the records of at most about the last 5 seconds. During a blob storage outage, the records wait in memory. A kill then loses all records since the outage began.
- If vLLM stops, the next request to a model endpoint starts one reload of the active model. During the reload, requests get status 503 with a `Retry-After` header. If that reload fails, an admin must call `POST /admin/model`. Until then, requests get status 503 with the code `no_model`.
- If the engine fails in the middle of a stream, the stream ends without an error event. The audit record has the status 502.
- At startup, the service reads the usage totals from blob storage. If that read fails, the service does not start, and the platform restarts it.
- The service serves one model at a time. A switch stops the old model.
- An admin who calls the service directly can name any admin in `X-LLM-Acting-User`.
- The audit log stores each prompt in full. It does not store the answers. A prompt is not searchable. The filters work on metadata.
- One service instance writes the audit log. Two instances on one `LLM_BLOB_ROOT` are not supported.
- A day file is read in one piece. The design fits about 300,000 requests a day.
- A blob write that fails after the blob was written leaves the records in the buffer. The next flush writes them again under a new segment name, so they appear two times.
- The merges assume that a blob write is atomic. A reader never sees half a file.
- A clock that steps back across a restart can give a new segment a name that is not greater than the last checkpoint. A crash before the next checkpoint then loses those rows from the usage totals.
- A stop of the service waits for a merge that runs at that moment.
- A late record for a day whose daily usage file exists counts in memory at once. The service writes it to the daily file at the next flusher cycle. A crash between the two loses it from the usage totals.
- Token counts for a stream come from the last chunk. The service asks vLLM for that chunk. It removes the chunk again if the client did not ask for it.
- There are no rate limits and no quotas for each user.
- The platform does not start GPU nodes. You must start one.

## Run the tests

The tests need no GPU, no vLLM and no network. A fake engine and an in-memory blob store replace the real parts.

```bash
pip install -r llm_service/requirements-dev.txt
python -m pytest llm_service/tests -q
```

Run the command from the root of this repository.
