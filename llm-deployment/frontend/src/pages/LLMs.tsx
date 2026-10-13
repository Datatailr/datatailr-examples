import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { Play, Plus, RotateCcw, Square } from 'lucide-react'
import { useEffect, useMemo, useState } from 'react'
import { Link } from 'react-router-dom'
import { PageHeader } from '../App'
import { api, type DeploymentSpec, type InstanceType } from '../api'
import { Card, EmptyState, ErrorBanner, Field, Modal, Spinner, StateBadge } from '../components/ui'
import { formatBytes, money, timeAgo } from '../lib/format'

const SUGGESTED_MODELS = [
  { id: 'Qwen/Qwen2.5-0.5B-Instruct', vram: '4g', note: 'Tiny; good for smoke tests' },
  { id: 'Qwen/Qwen2.5-1.5B-Instruct', vram: '6g', note: 'Small chat model' },
  { id: 'Qwen/Qwen2.5-7B-Instruct', vram: '16g', note: 'Needs ≥ 16 GB GPU (T4: use --max-model-len 4096 --dtype half)' },
  { id: 'meta-llama/Llama-3.1-8B-Instruct', vram: '16g', note: 'Gated: requires HF token secret' },
  { id: 'Qwen/Qwen2.5-Coder-7B-Instruct', vram: '16g', note: 'Code model' },
]

export default function LLMs() {
  const qc = useQueryClient()
  const q = useQuery({ queryKey: ['deployments'], queryFn: api.deployments, refetchInterval: 5_000 })
  const [open, setOpen] = useState(false)
  const invalidate = () => {
    qc.invalidateQueries({ queryKey: ['deployments'] })
    qc.invalidateQueries({ queryKey: ['overview'] })
  }
  const start = useMutation({ mutationFn: api.startDeployment, onSuccess: invalidate })
  const stop = useMutation({ mutationFn: api.stopDeployment, onSuccess: invalidate })
  const retry = useMutation({ mutationFn: api.retryDeployment, onSuccess: invalidate })

  return (
    <>
      <PageHeader
        title="LLMs"
        subtitle="Each LLM is a vLLM service on a GPU node, reachable through the gateway by its name."
        actions={
          <button className="btn-primary" onClick={() => setOpen(true)}>
            <Plus className="h-4 w-4" /> New LLM
          </button>
        }
      />
      {q.error && <ErrorBanner error={q.error} />}
      {q.isLoading && <Spinner />}
      {q.data && q.data.length === 0 && (
        <EmptyState
          title="No LLMs yet"
          body="Deploy a model: the platform builds the vLLM image, makes sure a GPU node is running, then starts the service."
          action={
            <button className="btn-primary" onClick={() => setOpen(true)}>
              <Plus className="h-4 w-4" /> New LLM
            </button>
          }
        />
      )}
      {q.data && q.data.length > 0 && (
        <Card>
          <table className="table">
            <thead>
              <tr>
                <th>Name</th>
                <th>Model</th>
                <th>State</th>
                <th>Resources</th>
                <th>Node</th>
                <th>Updated</th>
                <th className="text-right">Actions</th>
              </tr>
            </thead>
            <tbody>
              {q.data.map((d) => (
                <tr key={d.slug}>
                  <td>
                    <Link to={`/llms/${d.slug}`} className="font-medium hover:text-accent-300">
                      {d.spec.name}
                    </Link>
                    <div className="mono text-ink-400">{d.slug}</div>
                  </td>
                  <td className="mono">
                    {d.spec.model}
                    {d.pending_model && <div className="text-amber-300">→ {d.pending_model}</div>}
                  </td>
                  <td>
                    <StateBadge state={d.state} />
                    {d.last_error && <div className="mt-1 max-w-xs truncate text-xs text-rose-300" title={d.last_error}>{d.last_error}</div>}
                  </td>
                  <td className="text-ink-300">
                    {d.spec.vram} VRAM · {d.spec.cpu} CPU · {d.spec.memory}
                    <div className="text-xs text-ink-400">{d.spec.instance_type}</div>
                  </td>
                  <td className="text-ink-300">{d.node_name ?? '–'}</td>
                  <td className="text-ink-400">{timeAgo(d.updated_at)}</td>
                  <td>
                    <div className="flex justify-end gap-1">
                      {d.state === 'failed' && (
                        <button className="btn-ghost !px-2" title="Retry" onClick={() => retry.mutate(d.slug)}>
                          <RotateCcw className="h-3.5 w-3.5" />
                        </button>
                      )}
                      {d.state === 'stopped' || d.state === 'failed' ? (
                        <button className="btn-ghost !px-2" title="Start" onClick={() => start.mutate(d.slug)}>
                          <Play className="h-3.5 w-3.5" />
                        </button>
                      ) : (
                        <button className="btn-ghost !px-2" title="Stop" onClick={() => stop.mutate(d.slug)}>
                          <Square className="h-3.5 w-3.5" />
                        </button>
                      )}
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </Card>
      )}
      <NewLLMModal open={open} onClose={() => setOpen(false)} onCreated={invalidate} />
    </>
  )
}

export function NewLLMModal({ open, onClose, onCreated }: { open: boolean; onClose: () => void; onCreated: () => void }) {
  const types = useQuery({ queryKey: ['instance-types'], queryFn: () => api.instanceTypes(true), enabled: open, staleTime: 3600_000 })
  const template = useQuery({ queryKey: ['node-template'], queryFn: api.nodeTemplate, enabled: open, staleTime: 3600_000 })
  const me = useQuery({ queryKey: ['me'], queryFn: api.me })
  const [form, setForm] = useState<Partial<DeploymentSpec> & { start: boolean }>({
    name: '',
    model: '',
    environment: 'dev',
    instance_type: '',
    disk_size: 100,
    vram: '8g',
    cpu: 2,
    memory: '16g',
    extra_args: '',
    hf_token_secret: '',
    description: '',
    start: true,
  })
  const set = <K extends keyof typeof form>(k: K, v: (typeof form)[K]) => setForm((f) => ({ ...f, [k]: v }))

  useEffect(() => {
    if (template.data && !form.instance_type) {
      setForm((f) => ({
        ...f,
        instance_type: template.data.instance_type ?? f.instance_type,
        disk_size: template.data.disk_size,
        environment: me.data?.environment ?? template.data.environment,
      }))
    }
  }, [template.data, me.data, form.instance_type])

  const selectedType: InstanceType | undefined = useMemo(() => types.data?.find((t) => t.name === form.instance_type), [types.data, form.instance_type])
  const minDisk = template.data?.min_disk_size ?? 50

  const create = useMutation({
    mutationFn: () => api.createDeployment(form),
    onSuccess: () => {
      onCreated()
      onClose()
      setForm((f) => ({ ...f, name: '', model: '', description: '' }))
    },
  })

  return (
    <Modal open={open} onClose={onClose} title="Deploy a new LLM" width="max-w-2xl">
      <form
        className="space-y-5"
        onSubmit={(e) => {
          e.preventDefault()
          create.mutate()
        }}
      >
        <div className="grid gap-4 sm:grid-cols-2">
          <Field label="Name" hint="Becomes the model id clients use, e.g. “qwen-small”.">
            <input className="input" required value={form.name} onChange={(e) => set('name', e.target.value)} placeholder="qwen-small" />
          </Field>
          <Field label="Hugging Face model">
            <input className="input" required list="models" value={form.model} onChange={(e) => set('model', e.target.value)} placeholder="Qwen/Qwen2.5-0.5B-Instruct" />
            <datalist id="models">
              {SUGGESTED_MODELS.map((m) => (
                <option key={m.id} value={m.id}>
                  {m.note}
                </option>
              ))}
            </datalist>
          </Field>
        </div>
        <div className="flex flex-wrap gap-2">
          {SUGGESTED_MODELS.map((m) => (
            <button
              type="button"
              key={m.id}
              className="rounded-full border border-ink-600 px-2.5 py-1 text-[11px] text-ink-300 hover:border-accent-400 hover:text-ink-100"
              title={m.note}
              onClick={() => setForm((f) => ({ ...f, model: m.id, vram: m.vram, name: f.name || m.id.split('/')[1].toLowerCase().replace(/-instruct$/, '') }))}
            >
              {m.id}
            </button>
          ))}
        </div>

        <div className="grid gap-4 sm:grid-cols-2">
          <Field label="GPU instance type" hint={selectedType ? `${selectedType.cpu ?? '?'} vCPU · ${formatBytes(selectedType.memory)} RAM · ${selectedType.gpu_memory_gib ?? '?'} GB GPU · ${money(selectedType.price)}` : 'Used only when a new node must be created.'}>
            <select className="input" required value={form.instance_type} onChange={(e) => set('instance_type', e.target.value)}>
              <option value="" disabled>
                {types.isLoading ? 'Loading…' : 'Select a GPU type'}
              </option>
              {types.data?.map((t) => (
                <option key={t.name} value={t.name}>
                  {t.name} {t.price ? `· ${money(t.price)}` : ''} {t.gpu_memory_gib ? `· ${t.gpu_memory_gib} GB GPU` : ''}
                </option>
              ))}
            </select>
          </Field>
          <Field label={`Node disk (GB, min ${minDisk})`} hint="Model weights are cached on the node's disk.">
            <input className="input" type="number" min={minDisk} required value={form.disk_size} onChange={(e) => set('disk_size', Number(e.target.value))} />
          </Field>
        </div>
        <div className="grid gap-4 sm:grid-cols-3">
          <Field label="GPU memory (vram)">
            <input className="input" required value={form.vram} onChange={(e) => set('vram', e.target.value)} placeholder="8g" />
          </Field>
          <Field label="CPUs">
            <input className="input" type="number" step="0.5" min={0.5} required value={form.cpu} onChange={(e) => set('cpu', Number(e.target.value))} />
          </Field>
          <Field label="Memory">
            <input className="input" required value={form.memory} onChange={(e) => set('memory', e.target.value)} placeholder="16g" />
          </Field>
        </div>
        <div className="grid gap-4 sm:grid-cols-2">
          <Field label="Extra vLLM arguments" hint="Appended to `vllm serve`, e.g. --max-model-len 8192 --dtype half">
            <input className="input mono" value={form.extra_args} onChange={(e) => set('extra_args', e.target.value)} placeholder="--max-model-len 8192" />
          </Field>
          <Field label="HF token secret (optional)" hint="Name of a Datatailr secret with a Hugging Face token, for gated models.">
            <input className="input" value={form.hf_token_secret} onChange={(e) => set('hf_token_secret', e.target.value)} placeholder="hf_token" />
          </Field>
        </div>
        <Field label="Description (optional)">
          <input className="input" value={form.description} onChange={(e) => set('description', e.target.value)} placeholder="Shown to users on the Connect page" />
        </Field>
        <label className="flex items-center gap-2 text-sm text-ink-300">
          <input type="checkbox" checked={form.start} onChange={(e) => set('start', e.target.checked)} className="accent-accent-500" />
          Start immediately (build image, ensure a GPU node, start the service)
        </label>
        {create.error && <ErrorBanner error={create.error} />}
        <div className="flex justify-end gap-2 border-t border-ink-700/60 pt-4">
          <button type="button" className="btn-ghost" onClick={onClose}>
            Cancel
          </button>
          <button type="submit" className="btn-primary" disabled={create.isPending}>
            {create.isPending && <Spinner className="text-white" />} Deploy
          </button>
        </div>
      </form>
    </Modal>
  )
}
