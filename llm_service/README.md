# LLM service on Datatailr

This example serves a large language model (LLM) as a Datatailr service. The service speaks the OpenAI API. A client switches between OpenAI and this service when it changes the base URL.

The model is an open weights model from Hugging Face. Open weights means that the weight files are public. The service serves one model at a time.

The service adds these parts to the model:

- An API call switches the model. Another API call lists the models.
- The service keeps the weight files in Datatailr blob storage. It does not download them again at each start.
- The service writes an audit log. The log records who sent each request and with what prompt. Only an admin can read it.
- An admin can read the usage of each user, for example the tokens used.
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

An admin is the user who deployed the service. Members of the group in `LLM_ADMIN_GROUP` are admins too.

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

Node.new(instance_type="<a GPU instance type>", node_class="gpu", environment="dev")
```

A new node costs money until you stop it. For all node commands, see `dt node --help`.

This example is made for an Azure installation. It runs on any installation that has a GPU node.

## Deploy

Set the environment variables that you need. All of them are optional. Then run the script:

```bash
python deploy.py
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
| `LLM_ENVIRONMENT` | `dev` | Platform environment: `dev`, `pre` or `prod`. |

If `LLM_ACCESS_GROUP` is not set, only the user who deploys the service has access. If it is set, the owner and the group can read and use the service. Only the owner can change, operate or promote it.

The service name decides the host name. The default name `LLM Service` gives the host name `llm-service`.

### First deploy

The first deploy on a GPU node confirms that vLLM finds the GPU. Read the service log to check this. If vLLM reports a CUDA error, the image has no working CUDA runtime. The fix is in the image build and does not change the example.

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

If you omit `model`, the service uses the active model. If you name another model, the service returns the error `model_not_found` with status 404.

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

The old model keeps serving requests while the service fetches the new weights. Then the service stops vLLM and starts it again with the new model. During this time, requests get status 503 with a `Retry-After` header.

To check the switch, read the state:

```bash
curl "$BASE/admin/model" -H "Cookie: __datatailr_user_cookie__=$TOKEN"
```

The answer shows the active model, the switch state and the last switch error. If the new model fails to load, the service starts the previous model again and keeps the error. If a switch is already running, a new switch gets status 409.

### Gated models

Some models need a Hugging Face token. Store the token in the platform secret store. Create the entry in the Secrets Manager of the Datatailr interface. Then set `LLM_HF_TOKEN_KEY` to the key name and deploy again:

```bash
LLM_HF_TOKEN_KEY=hf_token python deploy.py
```

To check that the key exists, run this command:

```bash
dt kv ls -S
```

## List the models

Any user with access can list the models:

```bash
curl "$BASE/v1/models" -H "Cookie: __datatailr_user_cookie__=$TOKEN"
```

The list has the active model and each model in the blob storage cache. Each entry has the extra field `status`. Its value is `active`, `cached` or `loading`.

## Read the audit log

Only an admin can read the audit log. Each request makes one record. A record has the time, request id, user, endpoint, model, prompt, status, token counts and duration.

```bash
curl "$BASE/admin/audit?user=bob&limit=20" -H "Cookie: __datatailr_user_cookie__=$TOKEN"
```

The filters are `user`, `model`, `since`, `until`, `limit` and `offset`. The values of `since` and `until` are a day like `2026-11-10` or an ISO 8601 time. The default `limit` is 100 and the largest is 1000. The newest records come first.

To see the users in the log with their first and last request, call `/admin/audit/users`.

A model switch is also a record. Its user is the admin who started the switch. At startup the user is `system`.

## Read the usage metrics

An admin can read the usage of all users:

```bash
curl "$BASE/admin/usage?group_by=model&since=2026-11-01" \
  -H "Cookie: __datatailr_user_cookie__=$TOKEN"
```

The filters are `since`, `until`, `user` and `model`. Both days are included. The value of `group_by` is `user` (the default), `model` or `day`.

Each row has the request count, the failed request count, the prompt, completion and total tokens, and the time of the last request.

To read one user with the split by model and by day, call `/admin/usage/bob`. Each user can read their own usage at `/v1/usage/me`. This call is not part of the OpenAI API.

## Where the data lives

All data is in blob storage, under the root directory in `LLM_BLOB_ROOT`:

```
<root>/models/<org>--<name>/<revision>/<files>
<root>/models/<org>--<name>/<revision>/.complete    # written last
<root>/state/active.json                             # the active model
<root>/audit/YYYY-MM-DD/<time>-<id>.jsonl
<root>/usage/YYYY-MM-DD.json                         # totals of one finished day
```

The service writes `.complete` after the last weight file. An upload that stops half way is not used.

The audit files are the source of truth. The usage totals come from them. If you delete a daily usage file, the service builds it again from the audit files.

## Known limits

- The service buffers audit records in memory. It writes them to blob storage every 5 seconds, at 100 records, and at shutdown. If the container is killed, the records of the last 5 seconds are lost.
- The service serves one model at a time. A switch stops the old model.
- The audit log stores each prompt in full. It does not store the answers.
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
