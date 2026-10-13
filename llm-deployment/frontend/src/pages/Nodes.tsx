import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import clsx from 'clsx'
import { Play, Plus, Power, Trash2 } from 'lucide-react'
import { useEffect, useState } from 'react'
import { Link } from 'react-router-dom'
import { PageHeader } from '../App'
import { api, type Node } from '../api'
import { Card, EmptyState, ErrorBanner, Field, Modal, Pill, Spinner, StateBadge, Tabs } from '../components/ui'
import { formatBytes, money, timeAgo } from '../lib/format'

export default function Nodes() {
  const qc = useQueryClient()
  const [scope, setScope] = useState<'gpu' | 'all'>('gpu')
  const q = useQuery({ queryKey: ['nodes', scope], queryFn: () => api.nodes(scope === 'gpu'), refetchInterval: 10_000 })
  const [newOpen, setNewOpen] = useState(false)
  const [confirm, setConfirm] = useState<{ node: Node; action: 'stop' | 'terminate' | 'start' } | null>(null)
  const invalidate = () => qc.invalidateQueries({ queryKey: ['nodes'] })

  return (
    <>
      <PageHeader
        title="GPU nodes"
        subtitle="GPU nodes are not started by the autoscaler. The platform creates one when an LLM needs it; idle nodes are reclaimed by the platform or can be stopped here."
        actions={
          <>
            <Tabs value={scope} onChange={setScope} options={[{ value: 'gpu', label: 'GPU only' }, { value: 'all', label: 'All nodes' }]} />
            <button className="btn-primary" onClick={() => setNewOpen(true)}>
              <Plus className="h-4 w-4" /> New GPU node
            </button>
          </>
        }
      />
      {q.error && <ErrorBanner error={q.error} />}
      {q.isLoading && <Spinner />}
      {q.data && q.data.length === 0 && (
        <EmptyState title={scope === 'gpu' ? 'No GPU nodes' : 'No nodes'} body="Deploying an LLM creates a GPU node automatically, or create one here ahead of time." />
      )}
      {q.data && q.data.length > 0 && (
        <div className="grid gap-4 lg:grid-cols-2">
          {q.data.map((n) => (
            <NodeCard key={n.node_id} node={n} onAction={(action) => setConfirm({ node: n, action })} />
          ))}
        </div>
      )}
      <NewNodeModal open={newOpen} onClose={() => setNewOpen(false)} onDone={invalidate} />
      {confirm && <ConfirmNodeModal node={confirm.node} action={confirm.action} onClose={() => setConfirm(null)} onDone={invalidate} />}
    </>
  )
}

function NodeCard({ node: n, onAction }: { node: Node; onAction: (a: 'stop' | 'terminate' | 'start') => void }) {
  const memPct = n.memory?.max_memory ? Math.round(((n.memory.allocated_memory || 0) / n.memory.max_memory) * 100) : 0
  const cpuPct = n.cpu?.total_cpus ? Math.round(((n.cpu.allocated_cpus || 0) / n.cpu.total_cpus) * 100) : 0
  const status = (n.status || '').toLowerCase()
  const tone = n.ready ? 'good' : status === 'running' ? 'warn' : status === 'stopped' ? 'neutral' : 'warn'
  return (
    <Card>
      <div className="flex items-start justify-between gap-4">
        <div className="min-w-0">
          <div className="flex items-center gap-2">
            <h3 className="truncate font-semibold">{n.name}</h3>
            <Pill tone={tone}>{n.ready ? 'ready' : status || 'unknown'}</Pill>
            {n.is_gpu && <Pill tone="info">GPU</Pill>}
            {n.managed && <Pill>created for {n.managed.created_for}</Pill>}
          </div>
          <div className="mt-1 text-xs text-ink-400">
            {n.type} · {n.environment} · {n.private_ip} · {money(n.price_per_hour)} · created {timeAgo(n.created_at)}
          </div>
        </div>
        <div className="flex shrink-0 gap-1">
          {status === 'stopped' ? (
            <button className="btn-ghost !px-2" title="Start node" onClick={() => onAction('start')}>
              <Play className="h-3.5 w-3.5" />
            </button>
          ) : (
            <button className="btn-ghost !px-2" title="Stop node" onClick={() => onAction('stop')}>
              <Power className="h-3.5 w-3.5" />
            </button>
          )}
          <button className="btn-danger !px-2" title="Terminate node" onClick={() => onAction('terminate')}>
            <Trash2 className="h-3.5 w-3.5" />
          </button>
        </div>
      </div>
      <div className="mt-4 grid grid-cols-3 gap-4 text-xs">
        <Meter label="CPU" pct={cpuPct} detail={`${n.cpu?.allocated_cpus ?? 0}/${n.cpu?.total_cpus ?? 0}`} />
        <Meter label="Memory" pct={memPct} detail={`${formatBytes(n.memory?.allocated_memory)} / ${formatBytes(n.memory?.max_memory)}`} />
        <div>
          <div className="mb-1 flex justify-between text-ink-400">
            <span>GPU</span>
            <span>{n.gpu_memory_gib ? `${n.gpu_memory_gib} GB` : '–'}</span>
          </div>
          <div className="text-ink-300">
            {n.disk_size ? `${n.disk_size} GB disk` : n.managed?.disk_size ? `${n.managed.disk_size} GB disk (requested)` : 'default disk'}
          </div>
        </div>
      </div>
      <div className="mt-4">
        <div className="mb-1 text-xs text-ink-400">LLM services on this node</div>
        {n.llm_jobs.length === 0 ? (
          <div className="text-xs text-ink-400">none{n.jobs?.length ? ` · ${n.jobs.length} other job(s)` : ''}</div>
        ) : (
          <ul className="space-y-1">
            {n.llm_jobs.map((j) => (
              <li key={j.slug} className="flex items-center justify-between rounded-lg bg-ink-900/60 px-3 py-1.5 text-xs">
                <Link to={`/llms/${j.slug}`} className="font-medium hover:text-accent-300">
                  {j.slug} <span className="mono text-ink-400">{j.model}</span>
                </Link>
                <div className="flex items-center gap-2">
                  <span className="text-ink-400">{j.vram}</span>
                  <StateBadge state={j.state} />
                </div>
              </li>
            ))}
          </ul>
        )}
      </div>
    </Card>
  )
}

function Meter({ label, pct, detail }: { label: string; pct: number; detail: string }) {
  return (
    <div>
      <div className="mb-1 flex justify-between text-ink-400">
        <span>{label}</span>
        <span>{pct}%</span>
      </div>
      <div className="h-1.5 rounded-full bg-ink-700">
        <div className={clsx('h-1.5 rounded-full', pct > 85 ? 'bg-rose-400' : pct > 60 ? 'bg-amber-400' : 'bg-accent-400')} style={{ width: `${Math.min(100, pct)}%` }} />
      </div>
      <div className="mt-1 text-ink-300">{detail}</div>
    </div>
  )
}

function NewNodeModal({ open, onClose, onDone }: { open: boolean; onClose: () => void; onDone: () => void }) {
  const types = useQuery({ queryKey: ['instance-types'], queryFn: () => api.instanceTypes(true), enabled: open, staleTime: 3600_000 })
  const template = useQuery({ queryKey: ['node-template'], queryFn: api.nodeTemplate, enabled: open, staleTime: 3600_000 })
  const me = useQuery({ queryKey: ['me'], queryFn: api.me })
  const [form, setForm] = useState({ instance_type: '', environment: 'dev', disk_size: 100, num_instances: 1 })
  useEffect(() => {
    if (template.data && !form.instance_type) {
      setForm((f) => ({ ...f, instance_type: template.data.instance_type ?? '', disk_size: template.data.disk_size, environment: me.data?.environment ?? template.data.environment }))
    }
  }, [template.data, me.data, form.instance_type])
  const m = useMutation({
    mutationFn: () => api.newNode(form),
    onSuccess: () => {
      onDone()
      onClose()
    },
  })
  const selected = types.data?.find((t) => t.name === form.instance_type)
  return (
    <Modal open={open} onClose={onClose} title="Create GPU node">
      <form
        className="space-y-4"
        onSubmit={(e) => {
          e.preventDefault()
          m.mutate()
        }}
      >
        <Field label="Instance type" hint={selected ? `${selected.cpu ?? '?'} vCPU · ${formatBytes(selected.memory)} RAM · ${selected.gpu_memory_gib ?? '?'} GB GPU · ${money(selected.price)}` : undefined}>
          <select className="input" required value={form.instance_type} onChange={(e) => setForm({ ...form, instance_type: e.target.value })}>
            <option value="" disabled>{types.isLoading ? 'Loading…' : 'Select'}</option>
            {types.data?.map((t) => (
              <option key={t.name} value={t.name}>{t.name} {t.price ? `· ${money(t.price)}` : ''}</option>
            ))}
          </select>
        </Field>
        <div className="grid gap-4 sm:grid-cols-3">
          <Field label="Environment">
            <select className="input" value={form.environment} onChange={(e) => setForm({ ...form, environment: e.target.value })}>
              {['dev', 'pre', 'prod'].map((e) => <option key={e}>{e}</option>)}
            </select>
          </Field>
          <Field label={`Disk (GB, min ${template.data?.min_disk_size ?? 50})`}>
            <input className="input" type="number" min={template.data?.min_disk_size ?? 50} value={form.disk_size} onChange={(e) => setForm({ ...form, disk_size: Number(e.target.value) })} />
          </Field>
          <Field label="Count">
            <input className="input" type="number" min={1} max={4} value={form.num_instances} onChange={(e) => setForm({ ...form, num_instances: Number(e.target.value) })} />
          </Field>
        </div>
        <p className="text-xs text-amber-200/80">Provisioning cloud GPU capacity starts billing immediately{selected?.price ? ` (~${money(selected.price)} per node)` : ''}.</p>
        {m.error && <ErrorBanner error={m.error} />}
        <div className="flex justify-end gap-2 border-t border-ink-700/60 pt-4">
          <button type="button" className="btn-ghost" onClick={onClose}>Cancel</button>
          <button type="submit" className="btn-primary" disabled={m.isPending}>{m.isPending && <Spinner className="text-white" />} Create</button>
        </div>
      </form>
    </Modal>
  )
}

function ConfirmNodeModal({ node, action, onClose, onDone }: { node: Node; action: 'stop' | 'terminate' | 'start'; onClose: () => void; onDone: () => void }) {
  const [typed, setTyped] = useState('')
  const m = useMutation({
    mutationFn: () => api.nodeAction(node.node_id, action, typed),
    onSuccess: () => {
      onDone()
      onClose()
    },
  })
  const copy = {
    start: { title: `Start ${node.name}?`, body: 'The VM is started and rejoins the cluster in a few minutes. Billing resumes.', cta: 'Start node', danger: false },
    stop: { title: `Stop ${node.name}?`, body: 'Running jobs on this node are interrupted. The disk (and cached model weights) is kept; compute billing stops.', cta: 'Stop node', danger: true },
    terminate: { title: `Terminate ${node.name}?`, body: 'This permanently destroys the VM, its disk and every job running on it. This cannot be undone.', cta: 'Terminate node', danger: true },
  }[action]
  return (
    <Modal open onClose={onClose} title={copy.title}>
      <p className="text-sm text-ink-300">{copy.body}</p>
      {node.llm_jobs.length > 0 && (
        <div className="mt-3 rounded-lg border border-amber-500/30 bg-amber-500/10 px-3 py-2 text-xs text-amber-200">
          {node.llm_jobs.length} LLM service(s) run here: {node.llm_jobs.map((j) => j.slug).join(', ')}. Stop them first to avoid an automatic restart on another node.
        </div>
      )}
      {action === 'terminate' && (
        <div className="mt-4">
          <Field label={`Type ${node.name} to confirm`}>
            <input className="input mono" value={typed} onChange={(e) => setTyped(e.target.value)} autoFocus />
          </Field>
        </div>
      )}
      {m.error && <div className="mt-3"><ErrorBanner error={m.error} /></div>}
      <div className="mt-5 flex justify-end gap-2">
        <button className="btn-ghost" onClick={onClose}>Cancel</button>
        <button className={copy.danger ? 'btn-danger' : 'btn-primary'} disabled={m.isPending || (action === 'terminate' && typed !== node.name)} onClick={() => m.mutate()}>
          {m.isPending && <Spinner />} {copy.cta}
        </button>
      </div>
    </Modal>
  )
}
