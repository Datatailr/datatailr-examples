# Integration Studio ingestion architecture

## Decision

Use an application-owned SQLite document store for shared Slack/HubSpot data. Fetch shared GitHub App data and personal Gmail/Outlook/Zoom data live without persistence.

The trial installation's PostgreSQL container is Sonic control-plane infrastructure. Modifying or sharing it would couple application availability and migrations to the platform control plane. This template therefore keeps its shared connector records and checkpoints in its own SQLite database. The bounded gateway capabilities filter and sort those structured records directly; the configuration app does not need a model or embedding provider. Personal connectors bypass the database entirely.

## Data flow

1. A scheduled or manually triggered Slack/HubSpot connector reads an initial snapshot or provider delta.
2. The connector normalizes each shared provider object into a stable document ID, content, citation reference, source timestamp, and metadata.
3. SQLite upserts changed documents transactionally and updates FTS5. Checkpoints are not advanced yet.
4. Provider checkpoints advance only after the authoritative write succeeds. A failed run is retryable and idempotent.
5. At query time, the gateway applies the requested capability's filters, ordering, and row limits to the shared records. GitHub requests use a short-lived installation token; Gmail uses the signed-in user's stored app password; Outlook and Zoom use the signed-in user's OAuth token. Live results remain only in request memory.
6. Personal connector results are returned to the requesting job and discarded; they are never passed to the shared index or audit payloads.

## Connector checkpoints

| Connector | Initial sync | Incremental checkpoint | Deletions / drift |
|---|---|---|---|
| Slack | All joined public channels, paginated history and replies | Latest timestamp per channel | Daily full reconciliation; retry/backoff honors `Retry-After` |
| GitHub | None—live request only | None | Nothing persisted to reconcile; selected repositories are enforced by the GitHub App installation |
| Gmail | None—live request only | None | Nothing persisted to reconcile |
| Outlook Mail and Calendar | None—live request only | None | Nothing persisted to reconcile |
| HubSpot | Paginated companies, contacts, deals, and tickets | Per-object last-modified checkpoints through CRM Search | Daily reconciliation; missing records are removed only when all configured object reads succeed |

## Security invariants

- The GitHub App private key, Gmail addresses/app passwords, administrator-managed OAuth application secrets, and per-user OAuth tokens remain in an owner-only `0600` state file; they are never written to the index or returned by the app API. This simplified template does not encrypt that state file at rest.
- GitHub result rows and personal connector content exist only in request memory and are never indexed, embedded, cached, or written to audit storage.
- Slack and HubSpot documents are workspace-shared and use the outer app/service ACL, not source ACL principals.
- A failed shared-source read does not erase previously indexed content.
- Disconnecting Gmail removes its address and app password. Disconnecting Outlook removes its OAuth token. There is no indexed mailbox scope to delete.

## Production migration

For higher shared-data volume, deploy a dedicated knowledge service and PostgreSQL database rather than using the Sonic control-plane database. Move shared `documents`, cursors, and ingestion runs into PostgreSQL and add GIN FTS indexes. Add pgvector only if a future capability actually requires semantic retrieval. Keep Gmail, Outlook, and Zoom out of that service and continue fetching them live.

Provider-native wake-up paths for persisted shared sources are Slack Events API and HubSpot webhooks. The webhook is a hint—not the source of truth; provider deltas and periodic reconciliation remain authoritative.
