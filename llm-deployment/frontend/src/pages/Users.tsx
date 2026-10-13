import { useQuery } from '@tanstack/react-query'
import { Link } from 'react-router-dom'
import { PageHeader } from '../App'
import { api } from '../api'
import { Card, EmptyState, ErrorBanner, Spinner } from '../components/ui'
import { formatDate, formatNumber, timeAgo } from '../lib/format'

export default function UsersPage() {
  const q = useQuery({ queryKey: ['users'], queryFn: api.users, refetchInterval: 30_000 })
  return (
    <>
      <PageHeader title="Users" subtitle="Who is using the LLMs, and how much." />
      {q.error && <ErrorBanner error={q.error} />}
      {q.isLoading && <Spinner />}
      {q.data && q.data.length === 0 && <EmptyState title="No usage yet" body="Users appear here after their first request through the gateway." />}
      {q.data && q.data.length > 0 && (
        <Card>
          <table className="table">
            <thead>
              <tr>
                <th>User</th>
                <th className="text-right">Requests</th>
                <th className="text-right">Errors</th>
                <th className="text-right">Prompt tokens</th>
                <th className="text-right">Completion tokens</th>
                <th className="text-right">Models</th>
                <th>First seen</th>
                <th>Last seen</th>
                <th />
              </tr>
            </thead>
            <tbody>
              {q.data.map((u) => (
                <tr key={u.user}>
                  <td className="font-medium">{u.user || <span className="text-ink-400">anonymous</span>}</td>
                  <td className="text-right tabular-nums">{formatNumber(u.requests)}</td>
                  <td className="text-right tabular-nums text-rose-300">{u.errors ? formatNumber(u.errors) : <span className="text-ink-400">0</span>}</td>
                  <td className="text-right tabular-nums">{formatNumber(u.prompt_tokens)}</td>
                  <td className="text-right tabular-nums">{formatNumber(u.completion_tokens)}</td>
                  <td className="text-right tabular-nums">{u.models}</td>
                  <td className="text-ink-300">{formatDate(u.first_seen)}</td>
                  <td className="text-ink-300">{timeAgo(u.last_seen)}</td>
                  <td className="text-right">
                    <Link to={`/requests`} className="text-xs text-accent-300 hover:underline">requests</Link>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </Card>
      )}
    </>
  )
}
