import { useQuery } from '@tanstack/react-query'
import clsx from 'clsx'
import { AlertTriangle, ArrowRight } from 'lucide-react'
import { Link } from 'react-router-dom'
import { Area, AreaChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts'
import { PageHeader } from '../App'
import { api } from '../api'
import { Card, EmptyState, ErrorBanner, Pill, Spinner, Stat, StateBadge } from '../components/ui'
import { formatNumber, timeAgo } from '../lib/format'

export default function Overview() {
  const q = useQuery({ queryKey: ['overview'], queryFn: api.overview, refetchInterval: 10_000 })
  const data = q.data

  const running = data?.deployments.filter((d) => d.state === 'running').length ?? 0
  const busy = data?.deployments.filter((d) => !['running', 'stopped', 'failed'].includes(d.state)).length ?? 0
  const failed = data?.deployments.filter((d) => d.state === 'failed').length ?? 0
  const readyNodes = data?.gpu_nodes.filter((n) => n.ready).length ?? 0
  const totals = data?.stats.totals ?? {}
  const series = (data?.stats.by_hour ?? []).map((b) => ({
    t: new Date(b.bucket).toLocaleTimeString(undefined, { hour: '2-digit' }),
    requests: b.requests,
    tokens: b.tokens,
  }))

  return (
    <>
      <PageHeader title="Overview" subtitle="Deployed LLMs, GPU capacity and traffic over the last 24 hours." />
      {q.error && <ErrorBanner error={q.error} />}
      {q.isLoading && <Spinner />}
      {data && (
        <div className="space-y-6">
          {data.orchestrator.last_error && (
            <div className="flex items-center gap-2 rounded-lg border border-amber-500/30 bg-amber-500/10 px-3 py-2 text-sm text-amber-200">
              <AlertTriangle className="h-4 w-4" /> Orchestrator error: {data.orchestrator.last_error}
            </div>
          )}
          <div className="grid grid-cols-2 gap-4 lg:grid-cols-4">
            <Stat label="LLMs running" value={running} hint={busy ? `${busy} in progress` : failed ? `${failed} failed` : `${data.deployments.length} total`} accent="text-emerald-300" />
            <Stat label="GPU nodes ready" value={readyNodes} hint={`${data.gpu_nodes.length} GPU nodes`} />
            <Stat label="Requests · 24h" value={formatNumber(totals.requests ?? 0)} hint={`${formatNumber(totals.errors ?? 0)} errors · ${Math.round(totals.avg_latency_ms ?? 0)} ms avg`} />
            <Stat label="Tokens · 24h" value={formatNumber((totals.prompt_tokens ?? 0) + (totals.completion_tokens ?? 0))} hint={`${formatNumber(totals.users ?? 0)} active users`} />
          </div>

          <div className="grid gap-6 lg:grid-cols-3">
            <Card title="Traffic" subtitle="Requests per hour" className="lg:col-span-2">
              {series.length === 0 ? (
                <EmptyState title="No requests yet" body="Requests through the gateway will show up here within a few seconds." />
              ) : (
                <div className="h-56">
                  <ResponsiveContainer>
                    <AreaChart data={series} margin={{ left: -20, right: 8, top: 8 }}>
                      <defs>
                        <linearGradient id="g1" x1="0" y1="0" x2="0" y2="1">
                          <stop offset="0%" stopColor="#818cf8" stopOpacity={0.5} />
                          <stop offset="100%" stopColor="#818cf8" stopOpacity={0} />
                        </linearGradient>
                      </defs>
                      <CartesianGrid stroke="#232838" vertical={false} />
                      <XAxis dataKey="t" stroke="#7a839c" fontSize={11} tickLine={false} axisLine={false} />
                      <YAxis stroke="#7a839c" fontSize={11} tickLine={false} axisLine={false} allowDecimals={false} />
                      <Tooltip contentStyle={{ background: '#12151d', border: '1px solid #343b50', borderRadius: 8, fontSize: 12 }} />
                      <Area type="monotone" dataKey="requests" stroke="#818cf8" fill="url(#g1)" strokeWidth={2} />
                    </AreaChart>
                  </ResponsiveContainer>
                </div>
              )}
            </Card>
            <Card title="Models by usage" subtitle="Last 24 hours">
              {data.stats.by_model.length === 0 ? (
                <div className="text-sm text-ink-400">No traffic yet.</div>
              ) : (
                <ul className="space-y-3">
                  {data.stats.by_model.slice(0, 6).map((m) => {
                    const max = data.stats.by_model[0].requests || 1
                    return (
                      <li key={m.model}>
                        <div className="mb-1 flex items-center justify-between text-xs">
                          <span className="mono truncate">{m.model}</span>
                          <span className="text-ink-400">{formatNumber(m.requests)} req · {formatNumber(m.tokens)} tok</span>
                        </div>
                        <div className="h-1.5 rounded-full bg-ink-700">
                          <div className="h-1.5 rounded-full bg-accent-400" style={{ width: `${Math.max(4, (m.requests / max) * 100)}%` }} />
                        </div>
                      </li>
                    )
                  })}
                </ul>
              )}
            </Card>
          </div>

          <div className="grid gap-6 lg:grid-cols-3">
            <Card
              title="LLMs"
              className="lg:col-span-2"
              actions={
                <Link to="/llms" className="btn-ghost text-xs">
                  Manage <ArrowRight className="h-3.5 w-3.5" />
                </Link>
              }
            >
              {data.deployments.length === 0 ? (
                <EmptyState title="No LLMs deployed" action={<Link to="/llms" className="btn-primary">Deploy your first LLM</Link>} />
              ) : (
                <table className="table">
                  <thead>
                    <tr>
                      <th>Name</th>
                      <th>Model</th>
                      <th>State</th>
                      <th>Node</th>
                      <th>Since</th>
                    </tr>
                  </thead>
                  <tbody>
                    {data.deployments.map((d) => (
                      <tr key={d.slug}>
                        <td>
                          <Link to={`/llms/${d.slug}`} className="font-medium hover:text-accent-300">
                            {d.spec.name}
                          </Link>
                          <div className="mono text-ink-400">{d.slug}</div>
                        </td>
                        <td className="mono">{d.spec.model}</td>
                        <td>
                          <StateBadge state={d.state} />
                        </td>
                        <td className="text-ink-300">{d.node_name ?? '–'}</td>
                        <td className="text-ink-400">{timeAgo(d.state_since)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </Card>
            <Card title="Recent activity">
              {data.events.length === 0 ? (
                <div className="text-sm text-ink-400">Nothing yet.</div>
              ) : (
                <ul className="space-y-3">
                  {data.events.slice(0, 12).map((e, i) => (
                    <li key={i} className="flex gap-3 text-xs">
                      <span
                        className={clsx(
                          'mt-1 h-2 w-2 shrink-0 rounded-full',
                          e.level === 'error' ? 'bg-rose-400' : e.level === 'warning' ? 'bg-amber-400' : 'bg-accent-400',
                        )}
                      />
                      <div className="min-w-0">
                        <div className="text-ink-100">{e.message}</div>
                        <div className="mt-0.5 flex items-center gap-2 text-ink-400">
                          {e.deployment && <Pill>{e.deployment}</Pill>}
                          {timeAgo(e.ts)}
                        </div>
                      </div>
                    </li>
                  ))}
                </ul>
              )}
            </Card>
          </div>
        </div>
      )}
    </>
  )
}
