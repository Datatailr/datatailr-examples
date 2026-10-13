import { useQuery } from '@tanstack/react-query'
import clsx from 'clsx'
import { Activity, Boxes, Cpu, LayoutDashboard, Plug, ScrollText, Users } from 'lucide-react'
import { NavLink, Navigate, Route, Routes } from 'react-router-dom'
import { api } from './api'
import Connect from './pages/Connect'
import LLMDetail from './pages/LLMDetail'
import LLMs from './pages/LLMs'
import Nodes from './pages/Nodes'
import Overview from './pages/Overview'
import Requests from './pages/Requests'
import UsersPage from './pages/Users'

const NAV = [
  { to: '/', label: 'Overview', icon: LayoutDashboard, admin: true, end: true },
  { to: '/llms', label: 'LLMs', icon: Boxes, admin: true },
  { to: '/nodes', label: 'GPU nodes', icon: Cpu, admin: true },
  { to: '/requests', label: 'Requests', icon: ScrollText, admin: true },
  { to: '/users', label: 'Users', icon: Users, admin: true },
  { to: '/connect', label: 'Connect', icon: Plug, admin: false },
]

export default function App() {
  const me = useQuery({ queryKey: ['me'], queryFn: api.me, staleTime: 60_000 })
  const isAdmin = me.data?.is_admin ?? false

  return (
    <div className="flex min-h-full">
      <aside className="sticky top-0 flex h-screen w-60 shrink-0 flex-col border-r border-ink-800 bg-ink-900/60 px-3 py-5 backdrop-blur">
        <div className="mb-6 flex items-center gap-3 px-2">
          <div className="flex h-9 w-9 items-center justify-center rounded-xl bg-accent-500 text-white shadow-lg shadow-accent-500/30">
            <Activity className="h-5 w-5" />
          </div>
          <div>
            <div className="text-sm font-semibold leading-tight">LLM Admin</div>
            <div className="text-[11px] text-ink-400">Datatailr · {me.data?.environment ?? '…'}</div>
          </div>
        </div>
        <nav className="flex flex-col gap-1">
          {NAV.filter((n) => !n.admin || isAdmin).map((n) => (
            <NavLink
              key={n.to}
              to={n.to}
              end={n.end}
              className={({ isActive }) =>
                clsx(
                  'flex items-center gap-3 rounded-lg px-3 py-2 text-sm font-medium transition-colors',
                  isActive ? 'bg-ink-700/80 text-ink-100' : 'text-ink-300 hover:bg-ink-800 hover:text-ink-100',
                )
              }
            >
              <n.icon className="h-4 w-4" />
              {n.label}
            </NavLink>
          ))}
        </nav>
        <div className="mt-auto px-2 text-xs text-ink-400">
          {me.data?.user ? (
            <>
              Signed in as <span className="text-ink-100">{me.data.user}</span>
              {isAdmin && <span className="ml-1 rounded bg-accent-500/20 px-1.5 py-0.5 text-[10px] font-semibold text-accent-300">ADMIN</span>}
            </>
          ) : (
            'Not signed in'
          )}
        </div>
      </aside>
      <main className="min-w-0 flex-1 px-8 py-7">
        {me.isLoading ? null : (
          <Routes>
            {isAdmin ? (
              <>
                <Route path="/" element={<Overview />} />
                <Route path="/llms" element={<LLMs />} />
                <Route path="/llms/:slug" element={<LLMDetail />} />
                <Route path="/nodes" element={<Nodes />} />
                <Route path="/requests" element={<Requests />} />
                <Route path="/users" element={<UsersPage />} />
              </>
            ) : (
              <Route path="/" element={<Navigate to="/connect" replace />} />
            )}
            <Route path="/connect" element={<Connect />} />
            <Route path="*" element={<Navigate to={isAdmin ? '/' : '/connect'} replace />} />
          </Routes>
        )}
      </main>
    </div>
  )
}

export function PageHeader({ title, subtitle, actions }: { title: string; subtitle?: string; actions?: React.ReactNode }) {
  return (
    <div className="mb-6 flex items-end justify-between gap-4">
      <div>
        <h1 className="text-xl font-semibold tracking-tight">{title}</h1>
        {subtitle && <p className="mt-1 text-sm text-ink-400">{subtitle}</p>}
      </div>
      {actions && <div className="flex items-center gap-2">{actions}</div>}
    </div>
  )
}
