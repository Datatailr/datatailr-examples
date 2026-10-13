import { useQuery } from '@tanstack/react-query'
import clsx from 'clsx'
import { ChevronLeft, ChevronRight, RefreshCw, Search } from 'lucide-react'
import { useState } from 'react'
import { PageHeader } from '../App'
import { api, type RequestRow } from '../api'
import { Card, CodeBlock, EmptyState, ErrorBanner, Modal, Pill, Spinner } from '../components/ui'
import { formatDate, formatNumber } from '../lib/format'

const PAGE = 50
const RANGES = [
  { label: '1h', hours: 1 },
  { label: '24h', hours: 24 },
  { label: '7d', hours: 24 * 7 },
  { label: '30d', hours: 24 * 30 },
  { label: 'All', hours: 0 },
]

export default function Requests() {
  const [filters, setFilters] = useState({ user: '', model: '', status: '', search: '', hours: 24 })
  const [page, setPage] = useState(0)
  const [selected, setSelected] = useState<string | null>(null)
  const params = {
    user: filters.user || undefined,
    model: filters.model || undefined,
    status: filters.status || undefined,
    search: filters.search || undefined,
    // Rounded to the minute so the query key is stable across renders.
    since: filters.hours ? Math.floor(Date.now() / 60_000) * 60 - filters.hours * 3600 : undefined,
    limit: PAGE,
    offset: page * PAGE,
  }
  const q = useQuery({ queryKey: ['requests', params], queryFn: () => api.requests(params), refetchInterval: 15_000 })
  const stats = useQuery({ queryKey: ['stats', filters.hours], queryFn: () => api.stats(filters.hours || 24 * 365), refetchInterval: 30_000 })
  const update = (patch: Partial<typeof filters>) => {
    setFilters((f) => ({ ...f, ...patch }))
    setPage(0)
  }
  const total = q.data?.total ?? 0
  const pages = Math.max(1, Math.ceil(total / PAGE))

  return (
    <>
      <PageHeader
        title="Requests"
        subtitle="Every call through the gateway, with prompt and reply, queryable per user and model."
        actions={
          <button
            className="btn-ghost"
            onClick={async () => {
              await api.refreshLogs()
              q.refetch()
              stats.refetch()
            }}
          >
            <RefreshCw className={clsx('h-4 w-4', q.isFetching && 'animate-spin')} /> Refresh
          </button>
        }
      />
      <Card className="mb-4">
        <div className="grid gap-3 md:grid-cols-[1fr_1fr_auto_auto_auto]">
          <div className="relative">
            <Search className="pointer-events-none absolute left-2.5 top-2.5 h-4 w-4 text-ink-400" />
            <input className="input !pl-8" placeholder="Search prompts and replies" value={filters.search} onChange={(e) => update({ search: e.target.value })} />
          </div>
          <input className="input" placeholder="User" value={filters.user} onChange={(e) => update({ user: e.target.value })} list="users" />
          <datalist id="users">{stats.data?.by_user.map((u) => <option key={u.user} value={u.user} />)}</datalist>
          <select className="input" value={filters.model} onChange={(e) => update({ model: e.target.value })}>
            <option value="">All models</option>
            {stats.data?.by_model.map((m) => <option key={m.model} value={m.model}>{m.model}</option>)}
          </select>
          <select className="input" value={filters.status} onChange={(e) => update({ status: e.target.value })}>
            <option value="">Any status</option>
            <option value="ok">Success</option>
            <option value="error">Errors</option>
          </select>
          <div className="inline-flex rounded-lg border border-ink-700 bg-ink-900 p-0.5">
            {RANGES.map((r) => (
              <button key={r.label} className={clsx('rounded-md px-2.5 py-1 text-xs', filters.hours === r.hours ? 'bg-ink-700 text-ink-100' : 'text-ink-400 hover:text-ink-100')} onClick={() => update({ hours: r.hours })}>
                {r.label}
              </button>
            ))}
          </div>
        </div>
      </Card>
      {q.error && <ErrorBanner error={q.error} />}
      {q.isLoading && <Spinner />}
      {q.data && q.data.items.length === 0 && <EmptyState title="No requests match" body="Logs are flushed every ~10 seconds; use Refresh to pull the latest files." />}
      {q.data && q.data.items.length > 0 && (
        <Card>
          <table className="table">
            <thead>
              <tr>
                <th>Time</th>
                <th>User</th>
                <th>Model</th>
                <th>Endpoint</th>
                <th>Status</th>
                <th className="text-right">Latency</th>
                <th className="text-right">Tokens</th>
                <th>Reply</th>
              </tr>
            </thead>
            <tbody>
              {q.data.items.map((r) => (
                <tr key={r.request_id} className="cursor-pointer" onClick={() => setSelected(r.request_id)}>
                  <td className="whitespace-nowrap text-ink-300">{formatDate(r.ts)}</td>
                  <td className="font-medium">{r.user || <span className="text-ink-400">anonymous</span>}</td>
                  <td className="mono">{r.deployment || r.model}</td>
                  <td className="mono text-ink-400">{r.endpoint.replace('/v1/', '')}{r.stream ? ' ⚡' : ''}</td>
                  <td><StatusPill status={r.status} /></td>
                  <td className="text-right tabular-nums text-ink-300">{r.latency_ms} ms</td>
                  <td className="text-right tabular-nums text-ink-300">{r.prompt_tokens != null ? `${formatNumber(r.prompt_tokens)} → ${formatNumber(r.completion_tokens)}` : '–'}</td>
                  <td className="max-w-xs truncate text-ink-400">{r.error ? <span className="text-rose-300">{r.error}</span> : r.response_preview}</td>
                </tr>
              ))}
            </tbody>
          </table>
          <div className="mt-3 flex items-center justify-between text-xs text-ink-400">
            <span>{formatNumber(total)} requests</span>
            <div className="flex items-center gap-2">
              <button className="btn-ghost !px-2" disabled={page === 0} onClick={() => setPage((p) => p - 1)}><ChevronLeft className="h-3.5 w-3.5" /></button>
              <span>Page {page + 1} / {pages}</span>
              <button className="btn-ghost !px-2" disabled={page + 1 >= pages} onClick={() => setPage((p) => p + 1)}><ChevronRight className="h-3.5 w-3.5" /></button>
            </div>
          </div>
        </Card>
      )}
      {selected && <RequestModal id={selected} onClose={() => setSelected(null)} />}
    </>
  )
}

export function StatusPill({ status }: { status: number }) {
  const ok = status >= 200 && status < 300
  return <Pill tone={ok ? 'good' : status === 0 || status >= 500 ? 'bad' : 'warn'}>{status || 'n/a'}</Pill>
}

function RequestModal({ id, onClose }: { id: string; onClose: () => void }) {
  const q = useQuery({ queryKey: ['request', id], queryFn: () => api.requestDetail(id) })
  const r: RequestRow | undefined = q.data
  let pretty = r?.request_json ?? ''
  try {
    pretty = r?.request_json ? JSON.stringify(JSON.parse(r.request_json), null, 2) : ''
  } catch {
    /* keep raw */
  }
  return (
    <Modal open onClose={onClose} title={<span className="mono">{id}</span>} width="max-w-4xl">
      {q.isLoading && <Spinner />}
      {q.error && <ErrorBanner error={q.error} />}
      {r && (
        <div className="space-y-4">
          <div className="flex flex-wrap gap-2 text-xs">
            <Pill>{r.user || 'anonymous'}</Pill>
            <Pill tone="info">{r.deployment || r.model}</Pill>
            <Pill>{r.endpoint}</Pill>
            <StatusPill status={r.status} />
            <Pill>{r.latency_ms} ms</Pill>
            {r.prompt_tokens != null && <Pill>{r.prompt_tokens} → {r.completion_tokens} tokens</Pill>}
            {r.stream && <Pill tone="warn">streamed</Pill>}
            <span className="text-ink-400">{formatDate(r.ts)} · {r.client}</span>
          </div>
          {r.error && <div className="rounded-lg border border-rose-500/30 bg-rose-500/10 px-3 py-2 text-sm text-rose-200 whitespace-pre-wrap">{r.error}</div>}
          <div className="grid gap-4 lg:grid-cols-2">
            <CodeBlock title="request" code={pretty || '(bodies not logged)'} />
            <CodeBlock title="reply" code={r.response_text || '(empty)'} />
          </div>
        </div>
      )}
    </Modal>
  )
}
