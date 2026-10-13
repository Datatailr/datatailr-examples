---
name: datatailr-llm
description: Call LLMs hosted on a Datatailr cluster through the LLM Gateway, an OpenAI-compatible endpoint. Use when the user asks to chat with, embed with, or build against a model deployed on Datatailr, to list available models, or to configure an SDK, script, or coding agent (Claude Code) to use the cluster's LLMs.
---

# Use LLMs hosted on Datatailr

The cluster runs one or more LLMs behind a single gateway. The gateway speaks the OpenAI API, so any OpenAI SDK works once `base_url` and one auth header are set.

## Endpoint

```
https://<datatailr-domain>/job/<env>/llm-gateway/v1
```

From a job running inside the cluster (app, service, workflow) use the internal URL instead; the platform adds the job's identity automatically and no token is needed:

```
http://llm-gateway/job/<env>/llm-gateway/v1
```

`<env>` is normally `dev`. The LLM Admin app's **Connect** page shows the exact URL.

## Authentication (outside the cluster)

The Datatailr ingress authenticates requests with a cookie, not with `Authorization: Bearer`. Every request must carry:

```
Cookie: DatatailrUser=<API_TOKEN>
```

Tools that cannot set headers can append the token to the URL instead: `?__datatailr_user_cookie__=<API_TOKEN>`.

Users get `<API_TOKEN>` from the LLM Admin app (**Connect → Generate token**). The token is personal; requests are logged under the user's name. Never hard-code tokens in committed files; read them from an environment variable such as `DATATAILR_LLM_TOKEN`.

Verify a token:

```bash
curl -sS "$BASE_URL/whoami" -H "Cookie: DatatailrUser=$DATATAILR_LLM_TOKEN"
```

## Models

`GET /v1/models` lists what is running. Each LLM is reachable under two ids: its deployment name (e.g. `qwen-small`) and the Hugging Face id it serves (e.g. `Qwen/Qwen2.5-1.5B-Instruct`). Prefer the deployment name; it stays stable when an administrator changes the underlying model.

## Python (OpenAI SDK)

```python
import os
from openai import OpenAI

client = OpenAI(
    base_url=os.environ["DATATAILR_LLM_BASE_URL"],       # .../job/dev/llm-gateway/v1
    api_key="unused",
    default_headers={"Cookie": f"DatatailrUser={os.environ['DATATAILR_LLM_TOKEN']}"},
)
reply = client.chat.completions.create(
    model="qwen-small",
    messages=[{"role": "user", "content": "Summarise this in one line: ..."}],
)
print(reply.choices[0].message.content)
```

Streaming (`stream=True`) and `/v1/embeddings` work the same way. Inside a Datatailr job drop `default_headers` and use the internal base URL.

## Claude Code

vLLM serves the Anthropic Messages API at `/v1/messages`, so Claude Code can use a hosted model:

```bash
export ANTHROPIC_BASE_URL="https://<domain>/job/dev/llm-gateway"
export ANTHROPIC_AUTH_TOKEN="unused"
export ANTHROPIC_CUSTOM_HEADERS="Cookie: DatatailrUser=$DATATAILR_LLM_TOKEN"
export ANTHROPIC_MODEL="qwen-coder"
export ANTHROPIC_SMALL_FAST_MODEL="qwen-coder"
claude
```

## Cursor

Cursor's custom OpenAI base URL is **not supported**: Cursor's servers send `Authorization: Bearer`, which the Datatailr ingress rejects. Do not try to work around this with a public proxy that embeds a token.

## Errors

- `401`/`403`: token missing, expired or regenerated. Ask the user to generate a new token.
- `404 Model ... is not running`: the deployment is stopped or starting; the message lists available models.
- `502`: the vLLM service behind the gateway is unreachable (starting or failed). Retry later or check the LLM Admin app.
