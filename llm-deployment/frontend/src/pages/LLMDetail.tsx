import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import clsx from 'clsx'
import { ArrowLeft, Play, RefreshCw, RotateCcw, Square, Trash2 } from 'lucide-react'
import { useState } from 'react'
import { Link, useNavigate, useParams } from 'react-router-dom'
import { PageHeader } from '../App'
import { api } from '../api'
import { Card, CodeBlock, ErrorBanner, Field, Modal, Pill, Spinner, StateBadge, Tabs } from '../components/ui'
import { duration, formatDate, timeAgo } from '../lib/format'
import { curlSnippet, internalSnippet, pythonOpenAISnippet } from '../lib/snippets'

const STEPS: { key: string; label: string }[] = [
  { key: 'saving', label: 'Save & build image' },
  { key: 'provisioning_node', label: 'GPU node' },
  { key: 'starting', label: 'Start service' },
  { key: 'running', label: 'Serving' },
]
const ORDER = ['saving', 'provisioning_node', 'waiting_node', 'starting', 'running']

export default function LLMDetail() {
  const { slug = '' } = useParams()
  const nav = useNavigate()
  const qc = useQueryClient()
  const q = useQuery({ queryKey: ['deployment', slug], queryFn: () => api.deployment(slug), refetchInterval: 5_000 })
  const [logTab, setLogTab] = useState<'stdout' | 'stderr' | 'build'>('stdout')
  const logs = useQuery({
    queryKey: ['logs', slug, logTab],
    queryFn: () => api.deploymentLogs(slug, logTab === 'stderr', 400, logTab === 'build'),
    refetchInterval: 10_000,
    enabled: !!q.data && (q.data.state !== 'stopped' || logTab === 'build'),
  })
  const [changeOpen, setChangeOpen] = useState(false)
  const [deleteOpen, setDeleteOpen] = useState(false)
  const invalidate = () => {
    qc.invalidateQueries({ queryKey: ['deployment', slug] })
    qc.invalidateQueries({ queryKey: ['deployments'] })
  }
  const start = useMutation({ mutationFn: () => api.startDeployment(slug), onSuccess: invalidate })
  const stop = useMutation({ mutationFn: () => api.stopDeployment(slug), onSuccess: invalidate })
  const retry = useMutation({ mutationFn: () => api.retryDeployment(slug), onSuccess: invalidate })
  const del = useMutation({ mutationFn: () => api.deleteDeployment(slug), onSuccess: () => nav('/llms') })

  const d = q.data
  if (q.error) return <ErrorBanner error={q.error} />
  if (!d) return <Spinner />

  const baseUrl = d.gateway?.public_base_url || `${window.location.origin}${d.gateway?.base_path ?? ''}`
  const stepIndex = ORDER.indexOf(d.state)
  const stepPos = (key: string) => {
    const idx = ORDER.indexOf(key === 'provisioning_node' ? 'provisioning_node' : key)
    if (d.state === 'failed' || d.state === 'stopped' || d.state === 'stopping') return 'idle'
    if (key === 'provisioning_node' && (d.state === 'provisioning_node' || d.state === 'waiting_node')) return 'active'
    if (idx < stepIndex) return 'done'
    if (idx === stepIndex) return d.state === 'running' ? 'done' : 'active'
    return 'idle'
  }

  return (
    <>
      <Link to="/llms" className="mb-3 inline-flex items-center gap-1 text-xs text-ink-400 hover:text-ink-100">
        <ArrowLeft className="h-3.5 w-3.5" /> All LLMs
      </Link>
      <PageHeader
        title={d.spec.name}
        subtitle={`${d.spec.model} · created by ${d.created_by || 'unknown'} ${timeAgo(d.created_at)}`}
        actions={
          <>
            {d.state === 'failed' && (
              <button className="btn-ghost" onClick={() => retry.mutate()}>
                <RotateCcw className="h-4 w-4" /> Retry
              </button>
            )}
            {d.state === 'stopped' || d.state === 'failed' ? (
              <button className="btn-primary" onClick={() => start.mutate()}>
                <Play className="h-4 w-4" /> Start
              </button>
            ) : (
              <button className="btn-ghost" onClick={() => stop.mutate()}>
                <Square className="h-4 w-4" /> Stop
              </button>
            )}
            <button className="btn-ghost" onClick={() => setChangeOpen(true)}>
              <RefreshCw className="h-4 w-4" /> Change model
            </button>
            <button className="btn-danger" onClick={() => setDeleteOpen(true)}>
              <Trash2 className="h-4 w-4" />
            </button>
          </>
        }
      />

      <div className="space-y-6">
        <Card>
          <div className="flex flex-wrap items-center justify-between gap-4">
            <div className="flex items-center gap-3">
              <StateBadge state={d.state} size="md" />
              <span className="text-xs text-ink-400">for {duration(Date.now() / 1000 - d.state_since)}</span>
              {d.pending_model && <Pill tone="warn">switching to {d.pending_model}</Pill>}
            </div>
            <ol className="flex items-center gap-2">
              {STEPS.map((s, i) => {
                const pos = stepPos(s.key)
                return (
                  <li key={s.key} className="flex items-center gap-2">
                    <span
                      className={clsx(
                        'flex h-6 w-6 items-center justify-center rounded-full text-[11px] font-semibold',
                        pos === 'done' && 'bg-emerald-500/20 text-emerald-300',
                        pos === 'active' && 'bg-accent-500 text-white pulse',
                        pos === 'idle' && 'bg-ink-700 text-ink-400',
                      )}
                    >
                      {i + 1}
                    </span>
                    <span className={clsx('text-xs', pos === 'idle' ? 'text-ink-400' : 'text-ink-100')}>{s.label}</span>
                    {i < STEPS.length - 1 && <span className="mx-1 h-px w-6 bg-ink-700" />}
                  </li>
                )
              })}
            </ol>
          </div>
          {d.last_error && <div className="mt-4 rounded-lg border border-rose-500/30 bg-rose-500/10 px-3 py-2 text-sm text-rose-200">{d.last_error}</div>}
        </Card>

        <div className="grid gap-6 lg:grid-cols-3">
          <Card title="Configuration">
            <dl className="grid grid-cols-[auto_1fr] gap-x-4 gap-y-2 text-sm">
              <dt className="text-ink-400">Model</dt>
              <dd className="mono break-all">{d.spec.model}</dd>
              <dt className="text-ink-400">Service</dt>
              <dd className="mono">{d.internal_name}{d.service_version ? ` · v${d.service_version}` : ''}</dd>
              <dt className="text-ink-400">Environment</dt>
              <dd>{d.spec.environment}</dd>
              <dt className="text-ink-400">GPU node</dt>
              <dd>{d.node_name ?? <span className="text-ink-400">not assigned</span>}</dd>
              <dt className="text-ink-400">Instance type</dt>
              <dd>{d.spec.instance_type}</dd>
              <dt className="text-ink-400">Resources</dt>
              <dd>{d.spec.vram} VRAM · {d.spec.cpu} CPU · {d.spec.memory} RAM · {d.spec.disk_size} GB disk</dd>
              <dt className="text-ink-400">vLLM args</dt>
              <dd className="mono">{d.spec.extra_args || <span className="text-ink-400">none</span>}</dd>
              <dt className="text-ink-400">HF secret</dt>
              <dd>{d.spec.hf_token_secret || <span className="text-ink-400">none</span>}</dd>
              <dt className="text-ink-400">Run id</dt>
              <dd>{d.run_id ?? '–'}</dd>
            </dl>
          </Card>
          <Card title="Connect" subtitle="Use the gateway; set model to the LLM name." className="lg:col-span-2">
            <div className="space-y-3">
              <CodeBlock title="curl (outside the cluster)" code={curlSnippet({ baseUrl, model: d.slug, token: '' })} />
              <details>
                <summary className="cursor-pointer text-xs text-ink-400 hover:text-ink-100">Python (OpenAI SDK)</summary>
                <div className="mt-2">
                  <CodeBlock language="python" code={pythonOpenAISnippet({ baseUrl, model: d.slug, token: '' })} />
                </div>
              </details>
              <details>
                <summary className="cursor-pointer text-xs text-ink-400 hover:text-ink-100">From another Datatailr job</summary>
                <div className="mt-2">
                  <CodeBlock language="python" code={internalSnippet(d.gateway?.internal_base_url ?? 'http://llm-gateway/v1', d.slug)} />
                </div>
              </details>
              <p className="text-xs text-ink-400">
                Get a personal API token on the <Link to="/connect" className="text-accent-300 hover:underline">Connect</Link> page.
              </p>
            </div>
          </Card>
        </div>

        <div className="grid gap-6 lg:grid-cols-3">
          <Card
            title="Service logs"
            subtitle={
              logTab === 'build'
                ? 'Image build output'
                : logs.data?.run_id
                  ? `run ${logs.data.run_id} · ${logs.data.run_state ?? ''}`
                  : 'No run yet'
            }
            className="lg:col-span-2"
            actions={
              <>
                <Tabs
                  value={logTab}
                  onChange={setLogTab}
                  options={[
                    { value: 'stdout', label: 'stdout' },
                    { value: 'stderr', label: 'stderr' },
                    { value: 'build', label: 'build' },
                  ]}
                />
                <button className="btn-ghost !px-2" onClick={() => logs.refetch()} title="Refresh">
                  <RefreshCw className={clsx('h-3.5 w-3.5', logs.isFetching && 'animate-spin')} />
                </button>
              </>
            }
          >
            <pre className="mono max-h-[420px] overflow-auto rounded-lg bg-ink-900 p-3 text-ink-300 whitespace-pre-wrap">
              {logs.data?.text || (logs.isLoading ? 'Loading…' : 'No output yet.')}
            </pre>
          </Card>
          <Card title="Timeline">
            <ul className="space-y-3">
              {[...d.events].reverse().slice(0, 40).map((e, i) => (
                <li key={i} className="flex gap-3 text-xs">
                  <span className={clsx('mt-1 h-2 w-2 shrink-0 rounded-full', e.level === 'error' ? 'bg-rose-400' : e.level === 'warning' ? 'bg-amber-400' : 'bg-accent-400')} />
                  <div>
                    <div className="text-ink-100">{e.message}</div>
                    <div className="text-ink-400">{formatDate(e.ts)}</div>
                  </div>
                </li>
              ))}
            </ul>
          </Card>
        </div>
      </div>

      <ChangeModelModal open={changeOpen} onClose={() => setChangeOpen(false)} slug={slug} current={d.spec.model} extraArgs={d.spec.extra_args} vram={d.spec.vram} hfSecret={d.spec.hf_token_secret} onDone={invalidate} />
      <Modal open={deleteOpen} onClose={() => setDeleteOpen(false)} title={`Delete ${d.spec.name}?`}>
        <p className="text-sm text-ink-300">
          This stops the service, removes its definition from Datatailr and forgets the deployment. The GPU node is left running; stop or terminate it from the Nodes page if it is no longer needed.
        </p>
        {del.error && <div className="mt-3"><ErrorBanner error={del.error} /></div>}
        <div className="mt-5 flex justify-end gap-2">
          <button className="btn-ghost" onClick={() => setDeleteOpen(false)}>Cancel</button>
          <button className="btn-danger" disabled={del.isPending} onClick={() => del.mutate()}>
            {del.isPending && <Spinner />} Delete
          </button>
        </div>
      </Modal>
    </>
  )
}

function ChangeModelModal({ open, onClose, slug, current, extraArgs, vram, hfSecret, onDone }: {
  open: boolean
  onClose: () => void
  slug: string
  current: string
  extraArgs: string
  vram: string
  hfSecret: string
  onDone: () => void
}) {
  const [model, setModel] = useState(current)
  const [args, setArgs] = useState(extraArgs)
  const [vramValue, setVram] = useState(vram)
  const [secret, setSecret] = useState(hfSecret)
  const m = useMutation({
    mutationFn: () => api.changeModel(slug, { model, extra_args: args, vram: vramValue, hf_token_secret: secret }),
    onSuccess: () => {
      onDone()
      onClose()
    },
  })
  return (
    <Modal open={open} onClose={onClose} title="Change model">
      <form
        className="space-y-4"
        onSubmit={(e) => {
          e.preventDefault()
          m.mutate()
        }}
      >
        <Field label="Hugging Face model">
          <input className="input mono" required value={model} onChange={(e) => setModel(e.target.value)} />
        </Field>
        <div className="grid gap-4 sm:grid-cols-2">
          <Field label="GPU memory (vram)">
            <input className="input" value={vramValue} onChange={(e) => setVram(e.target.value)} />
          </Field>
          <Field label="HF token secret">
            <input className="input" value={secret} onChange={(e) => setSecret(e.target.value)} />
          </Field>
        </div>
        <Field label="Extra vLLM arguments">
          <input className="input mono" value={args} onChange={(e) => setArgs(e.target.value)} />
        </Field>
        <p className="text-xs text-ink-400">The service is stopped, re-saved with the new model and started again. Cached weights on the node are reused when available.</p>
        {m.error && <ErrorBanner error={m.error} />}
        <div className="flex justify-end gap-2 border-t border-ink-700/60 pt-4">
          <button type="button" className="btn-ghost" onClick={onClose}>Cancel</button>
          <button type="submit" className="btn-primary" disabled={m.isPending}>
            {m.isPending && <Spinner className="text-white" />} Apply and restart
          </button>
        </div>
      </form>
    </Modal>
  )
}
