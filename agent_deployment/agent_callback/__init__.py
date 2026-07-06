"""Optional low-latency callback Service for the SWE agent system (§10, §15).

Sub-agents may POST a compact ``{subagent_id, status, pr_url}`` notification to
this service the moment they finish. The service records the notification to
Blob and pings the main agent's coordinator to harvest immediately instead of
waiting for the next poll cycle. Blob remains the source of truth; the callback
is only a wake-up, so the whole system also works poll-only without it.
"""
