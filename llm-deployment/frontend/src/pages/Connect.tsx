import { useMutation, useQuery } from '@tanstack/react-query'
import { AlertTriangle, KeyRound, ShieldCheck } from 'lucide-react'
import { useMemo, useState } from 'react'
import { PageHeader } from '../App'
import { api, generateApiToken } from '../api'
import { Card, CodeBlock, CopyButton, ErrorBanner, Pill, Spinner, StateBadge, Tabs } from '../components/ui'
import {
  claudeCodeSnippet,
  curlSnippet,
  internalSnippet,
  nodeSnippet,
  pythonOpenAISnippet,
  pythonRequestsSnippet,
  whoamiSnippet,
} from '../lib/snippets'

type Tool = 'curl' | 'python' | 'requests' | 'node' | 'claude' | 'internal'

export default function Connect() {
  const q = useQuery({ queryKey: ['connect'], queryFn: api.connect, refetchInterval: 15_000 })
  const [token, setToken] = useState('')
  const [tool, setTool] = useState<Tool>('curl')
  const [model, setModel] = useState('')
  const gen = useMutation({ mutationFn: generateApiToken, onSuccess: setToken })

  const info = q.data
  const baseUrl = useMemo(() => {
    if (!info) return ''
    return info.gateway.public_base_url || `${window.location.origin}${info.gateway.base_path}`
  }, [info])
  const running = info?.models.filter((m) => m.state === 'running') ?? []
  const selectedModel = model || running[0]?.id || info?.models[0]?.id || '<model>'
  const ctx = { baseUrl, model: selectedModel, token }

  const snippet = {
    curl: curlSnippet(ctx),
    python: pythonOpenAISnippet(ctx),
    requests: pythonRequestsSnippet(ctx),
    node: nodeSnippet(ctx),
    claude: claudeCodeSnippet(ctx),
    internal: internalSnippet(info?.gateway.internal_base_url ?? 'http://llm-gateway/v1', selectedModel),
  }[tool]

  return (
    <>
      <PageHeader title="Connect" subtitle="Use the deployed LLMs from your laptop, scripts and coding agents through one OpenAI-compatible endpoint." />
      {q.error && <ErrorBanner error={q.error} />}
      {q.isLoading && <Spinner />}
      {info && (
        <div className="grid gap-6 lg:grid-cols-3">
          <div className="space-y-6 lg:col-span-2">
            <Card title="1 · Endpoint" subtitle="Any OpenAI SDK works: point base_url here.">
              <div className="flex items-center justify-between gap-3 rounded-lg bg-ink-900 px-3 py-2">
                <code className="mono truncate">{baseUrl}</code>
                <CopyButton text={baseUrl} />
              </div>
              <div className="mt-3 flex flex-wrap gap-2">
                {info.models.length === 0 && <span className="text-sm text-ink-400">No LLMs deployed yet.</span>}
                {info.models.map((m) => (
                  <button
                    key={m.id}
                    type="button"
                    onClick={() => setModel(m.id)}
                    className={`flex items-center gap-2 rounded-lg border px-3 py-1.5 text-left text-xs transition-colors cursor-pointer ${
                      selectedModel === m.id ? 'border-accent-400 bg-accent-500/10' : 'border-ink-700 hover:border-ink-600'
                    }`}
                    title={m.description || m.model}
                  >
                    <span className="font-medium">{m.id}</span>
                    <span className="mono text-ink-400">{m.model}</span>
                    <StateBadge state={m.state} />
                  </button>
                ))}
              </div>
            </Card>

            <Card title="2 · Personal API token" subtitle="The platform authenticates external requests with a cookie carrying your token.">
              <div className="flex flex-wrap items-center gap-3">
                <button className="btn-primary" onClick={() => gen.mutate()} disabled={gen.isPending}>
                  {gen.isPending ? <Spinner className="text-white" /> : <KeyRound className="h-4 w-4" />}
                  {token ? 'Regenerate token' : 'Generate token'}
                </button>
                {token && (
                  <div className="flex min-w-0 flex-1 items-center justify-between gap-2 rounded-lg bg-ink-900 px-3 py-2">
                    <code className="mono truncate">{token}</code>
                    <CopyButton text={token} />
                  </div>
                )}
              </div>
              {gen.error && <div className="mt-3"><ErrorBanner error={gen.error} /></div>}
              <ul className="mt-3 space-y-1 text-xs text-ink-400">
                <li className="flex items-start gap-2"><ShieldCheck className="mt-0.5 h-3.5 w-3.5 shrink-0 text-emerald-300" /> The token is personal: requests made with it are logged under your name.</li>
                <li className="flex items-start gap-2"><AlertTriangle className="mt-0.5 h-3.5 w-3.5 shrink-0 text-amber-300" /> Generating a new token invalidates the previous one, including the one used by the Datatailr Excel add-in.</li>
              </ul>
            </Card>

            <Card
              title="3 · Use it"
              actions={
                <Tabs
                  value={tool}
                  onChange={setTool}
                  options={[
                    { value: 'curl', label: 'curl' },
                    { value: 'python', label: 'OpenAI SDK' },
                    { value: 'requests', label: 'requests' },
                    { value: 'node', label: 'Node' },
                    { value: 'claude', label: 'Claude Code' },
                    { value: 'internal', label: 'Inside Datatailr' },
                  ]}
                />
              }
            >
              <CodeBlock code={snippet} language={tool === 'curl' || tool === 'claude' ? 'bash' : tool === 'node' ? 'typescript' : 'python'} />
              {tool === 'claude' && (
                <p className="mt-3 text-xs text-ink-400">
                  Claude Code speaks the Anthropic Messages API, which vLLM serves at <code className="mono">/v1/messages</code>. Tool-calling quality depends on the model; pick an instruct model with tool support.
                </p>
              )}
              {tool === 'internal' && (
                <p className="mt-3 text-xs text-ink-400">
                  Jobs in the same environment reach the gateway at <code className="mono">{info.gateway.internal_base_url}</code>. The platform adds the job's identity, so requests are logged under the job's run-as user.
                </p>
              )}
            </Card>
          </div>

          <div className="space-y-6">
            <Card title="Check your connection">
              <CodeBlock title="bash" code={whoamiSnippet(ctx)} />
              <p className="mt-2 text-xs text-ink-400">Returns your username and the models you can use.</p>
            </Card>
            <Card title="Cursor">
              <div className="flex items-start gap-2 text-sm text-ink-300">
                <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0 text-amber-300" />
                <div>
                  <p>Cursor's "custom OpenAI base URL" is <span className="font-medium text-ink-100">not supported</span>.</p>
                  <p className="mt-1 text-xs text-ink-400">
                    Cursor sends requests from its own servers with an <code className="mono">Authorization: Bearer</code> header, and the Datatailr ingress only accepts a per-user token cookie. Use Claude Code, the OpenAI SDK, or any tool that lets you set custom headers.
                  </p>
                </div>
              </div>
            </Card>
            <Card title="Notes">
              <ul className="space-y-2 text-xs text-ink-400">
                <li>· <Pill tone="info">model</Pill> accepts the LLM name (e.g. <code className="mono">{selectedModel}</code>) or its Hugging Face id.</li>
                <li>· Streaming (<code className="mono">stream: true</code>) is supported; token usage is recorded for streamed replies too.</li>
                <li>· Every request and reply is logged and visible to administrators.</li>
                <li>· Signed in as <span className="text-ink-100">{info.user}</span> in <span className="text-ink-100">{info.environment}</span>.</li>
              </ul>
            </Card>
          </div>
        </div>
      )}
    </>
  )
}
