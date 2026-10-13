import clsx from 'clsx'
import { Check, Copy, Loader2, X } from 'lucide-react'
import { useEffect, useState, type ReactNode } from 'react'
import type { DeploymentState } from '../api'

export function Card({ title, subtitle, actions, children, className }: {
  title?: ReactNode
  subtitle?: ReactNode
  actions?: ReactNode
  children: ReactNode
  className?: string
}) {
  return (
    <section className={clsx('card', className)}>
      {(title || actions) && (
        <header className="flex items-start justify-between gap-4 border-b border-ink-700/60 px-5 py-4">
          <div>
            {title && <h2 className="text-sm font-semibold text-ink-100">{title}</h2>}
            {subtitle && <p className="mt-0.5 text-xs text-ink-400">{subtitle}</p>}
          </div>
          {actions && <div className="flex shrink-0 items-center gap-2">{actions}</div>}
        </header>
      )}
      <div className="p-5">{children}</div>
    </section>
  )
}

export function Stat({ label, value, hint, accent }: { label: string; value: ReactNode; hint?: ReactNode; accent?: string }) {
  return (
    <div className="card px-5 py-4">
      <div className="text-xs font-medium uppercase tracking-wide text-ink-400">{label}</div>
      <div className={clsx('mt-2 text-2xl font-semibold tabular-nums', accent)}>{value}</div>
      {hint && <div className="mt-1 text-xs text-ink-400">{hint}</div>}
    </div>
  )
}

const STATE_STYLE: Record<DeploymentState, { label: string; cls: string; busy?: boolean }> = {
  saving: { label: 'Building image', cls: 'bg-sky-500/15 text-sky-300 border-sky-500/30', busy: true },
  provisioning_node: { label: 'Provisioning node', cls: 'bg-amber-500/15 text-amber-300 border-amber-500/30', busy: true },
  waiting_node: { label: 'Waiting for node', cls: 'bg-amber-500/15 text-amber-300 border-amber-500/30', busy: true },
  starting: { label: 'Starting', cls: 'bg-violet-500/15 text-violet-300 border-violet-500/30', busy: true },
  running: { label: 'Running', cls: 'bg-emerald-500/15 text-emerald-300 border-emerald-500/30' },
  stopping: { label: 'Stopping', cls: 'bg-orange-500/15 text-orange-300 border-orange-500/30', busy: true },
  stopped: { label: 'Stopped', cls: 'bg-ink-700 text-ink-300 border-ink-600' },
  failed: { label: 'Failed', cls: 'bg-rose-500/15 text-rose-300 border-rose-500/30' },
}

export function StateBadge({ state, size = 'sm' }: { state: DeploymentState; size?: 'sm' | 'md' }) {
  const s = STATE_STYLE[state] ?? STATE_STYLE.stopped
  return (
    <span
      className={clsx(
        'inline-flex items-center gap-1.5 rounded-full border font-medium',
        size === 'sm' ? 'px-2 py-0.5 text-[11px]' : 'px-2.5 py-1 text-xs',
        s.cls,
      )}
    >
      {s.busy ? <Loader2 className="h-3 w-3 animate-spin" /> : <span className={clsx('h-1.5 w-1.5 rounded-full bg-current', state === 'running' && 'pulse')} />}
      {s.label}
    </span>
  )
}

export function Pill({ children, tone = 'neutral' }: { children: ReactNode; tone?: 'neutral' | 'good' | 'warn' | 'bad' | 'info' }) {
  const cls = {
    neutral: 'bg-ink-700 text-ink-300 border-ink-600',
    good: 'bg-emerald-500/15 text-emerald-300 border-emerald-500/30',
    warn: 'bg-amber-500/15 text-amber-300 border-amber-500/30',
    bad: 'bg-rose-500/15 text-rose-300 border-rose-500/30',
    info: 'bg-sky-500/15 text-sky-300 border-sky-500/30',
  }[tone]
  return <span className={clsx('inline-flex items-center rounded-full border px-2 py-0.5 text-[11px] font-medium', cls)}>{children}</span>
}

export function CopyButton({ text, label }: { text: string; label?: string }) {
  const [copied, setCopied] = useState(false)
  useEffect(() => {
    if (!copied) return
    const t = setTimeout(() => setCopied(false), 1500)
    return () => clearTimeout(t)
  }, [copied])
  return (
    <button
      type="button"
      className="btn-ghost !px-2 !py-1 text-xs"
      onClick={async () => {
        await navigator.clipboard.writeText(text)
        setCopied(true)
      }}
    >
      {copied ? <Check className="h-3.5 w-3.5 text-emerald-300" /> : <Copy className="h-3.5 w-3.5" />}
      {label ?? (copied ? 'Copied' : 'Copy')}
    </button>
  )
}

export function CodeBlock({ code, language, title }: { code: string; language?: string; title?: string }) {
  return (
    <div className="overflow-hidden rounded-xl border border-ink-700 bg-ink-900">
      <div className="flex items-center justify-between border-b border-ink-700/60 px-3 py-1.5">
        <span className="text-[11px] uppercase tracking-wide text-ink-400">{title ?? language ?? 'code'}</span>
        <CopyButton text={code} />
      </div>
      <pre className="mono overflow-x-auto p-3 leading-relaxed text-ink-100 whitespace-pre">{code}</pre>
    </div>
  )
}

export function Modal({ open, onClose, title, children, width = 'max-w-xl' }: {
  open: boolean
  onClose: () => void
  title: ReactNode
  children: ReactNode
  width?: string
}) {
  useEffect(() => {
    if (!open) return
    const onKey = (e: KeyboardEvent) => e.key === 'Escape' && onClose()
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [open, onClose])
  if (!open) return null
  return (
    <div className="fixed inset-0 z-50 flex items-start justify-center overflow-y-auto bg-black/60 p-4 pt-16 backdrop-blur-sm" onMouseDown={onClose}>
      <div className={clsx('card w-full', width)} onMouseDown={(e) => e.stopPropagation()}>
        <header className="flex items-center justify-between border-b border-ink-700/60 px-5 py-4">
          <h2 className="text-base font-semibold">{title}</h2>
          <button className="rounded-md p-1 text-ink-400 hover:bg-ink-700 hover:text-ink-100" onClick={onClose} aria-label="Close">
            <X className="h-4 w-4" />
          </button>
        </header>
        <div className="p-5">{children}</div>
      </div>
    </div>
  )
}

export function Field({ label, hint, children }: { label: string; hint?: ReactNode; children: ReactNode }) {
  return (
    <label className="block">
      <span className="label">{label}</span>
      {children}
      {hint && <span className="mt-1 block text-xs text-ink-400">{hint}</span>}
    </label>
  )
}

export function EmptyState({ title, body, action }: { title: string; body?: ReactNode; action?: ReactNode }) {
  return (
    <div className="flex flex-col items-center justify-center rounded-xl border border-dashed border-ink-700 px-6 py-12 text-center">
      <div className="text-sm font-medium text-ink-100">{title}</div>
      {body && <div className="mt-1 max-w-md text-xs text-ink-400">{body}</div>}
      {action && <div className="mt-4">{action}</div>}
    </div>
  )
}

export function ErrorBanner({ error }: { error: unknown }) {
  if (!error) return null
  const message = error instanceof Error ? error.message : String(error)
  return <div className="rounded-lg border border-rose-500/30 bg-rose-500/10 px-3 py-2 text-sm text-rose-200">{message}</div>
}

export function Spinner({ className }: { className?: string }) {
  return <Loader2 className={clsx('h-4 w-4 animate-spin text-ink-400', className)} />
}

export function Tabs<T extends string>({ value, onChange, options }: { value: T; onChange: (v: T) => void; options: { value: T; label: ReactNode }[] }) {
  return (
    <div className="inline-flex rounded-lg border border-ink-700 bg-ink-900 p-0.5">
      {options.map((o) => (
        <button
          key={o.value}
          type="button"
          onClick={() => onChange(o.value)}
          className={clsx(
            'rounded-md px-3 py-1 text-xs font-medium transition-colors cursor-pointer',
            o.value === value ? 'bg-ink-700 text-ink-100' : 'text-ink-400 hover:text-ink-100',
          )}
        >
          {o.label}
        </button>
      ))}
    </div>
  )
}
