# Connector gateway API

The trusted runtime service is `connector-gateway`. Use the bundled client; it supplies the correct route and request marker. The Python client uses the deployed job's signed `run_as` identity. The browser client uses the signed identity of the person viewing a shared app.

Slack and HubSpot are administrator-managed shared indexed sources. GitHub Organization is administrator-managed and shared but fetched live through its GitHub App; repository content is not indexed or persisted. The deployed App ACL decides who may see shared Slack/HubSpot/GitHub Organization data, so those Apps should use `connector_client.py` server-side. Gmail, Outlook Mail and Calendar, and Zoom are personal and live-only. The gateway never writes GitHub results, personal messages, calendar events, availability rows, AI Companion summaries, transcripts, or query values to persistent connector storage or its audit log. A platform-admin-only audit trail retains operational metadata. Use `connector_client.js` in a shared App so personal queries carry the viewer's identity. Use the Python client for workflows, services, owner-only Apps, and shared Slack/HubSpot/GitHub Organization Apps.

## Queries

### `slack.threads.recent`

Parameters: `channel` (name or id), `limit` (1–100, default 20).

Returns rows with `id`, `channel`, `text`, `updated_at`, `reply_count`, and `ref`.

Synthetic row:

```json
{"id":"synthetic-1","channel":"bug-reports","text":"Synthetic: export button remains disabled after refresh","updated_at":"2026-01-01T12:00:00Z","reply_count":4,"ref":"slack://synthetic/1"}
```

### `slack.threads.search`

Parameters: `query` (required substring), optional `channel`, and `limit` (1–100). Returns the same row shape as `slack.threads.recent`.

### `hubspot.objects.recent`

Parameters: `object_type` (`companies`, `contacts`, `deals`, `tickets`, `calls`, `meetings`, `notes`, or `tasks`), optional timezone-aware ISO-8601 `modified_after`, and `limit` (1–200, default 50).

Returns `id`, `object_type`, `title`, `properties`, `updated_at`, and `ref`. Property keys reflect the normalized HubSpot object, such as `dealname`, `dealstage`, `amount`, and `closedate` for deals. Contacts, companies, deals, and tickets also expose `notes_next_activity_date`, `hs_notes_next_activity`, `hs_notes_next_activity_type`, and `notes_last_updated` when HubSpot populates them.

### `hubspot.activities.recent`

Parameters: optional `activity_types` selected from `calls`, `meetings`, `notes`, and `tasks` (default all four); optional `query`; optional timezone-aware `modified_after`; and `limit` (1–200, default 50). Returns normalized activity rows with `id`, `activity_type`, `title`, `text`, `timestamp`, `start_time`, `end_time`, `status`, `outcome`, `owner_id`, `associations`, `properties`, `updated_at`, and `ref`. Associations contain bounded ID arrays for contacts, companies, deals, and tickets.

### `hubspot.activities.upcoming`

Parameters: `days` (1–90, default 14), optional `activity_types` (default calls, meetings, and tasks), optional `query`, optional `owner_id`, optional paired `association_type` and `association_id`, and `limit` (1–200, default 50). Returns the same normalized activity rows, sorted from soonest to latest. Completed tasks and completed/canceled/no-show meetings are excluded. This capability is intended for upcoming-demo, discovery-call, and sales-activity dashboards.

HubSpot activities are shared indexed workspace data. Their bodies and CRM associations are available to every user allowed to access the deployed App. Sales-email bodies are not ingested by this connector.

### `hubspot.deals.summary`

No parameters. Returns `total_deals`, `total_amount`, `open_deals`, `closed_won`, `by_stage`, and `latest_update`.

### GitHub queries

GitHub is a shared, administrator-managed GitHub App installation. The installation determines which organization repositories the gateway can read. Every query below is fetched live with a short-lived installation token and discarded after the response.

- `github.repositories.list`: optional `limit` (1–100, default 50). Returns `id`, `name`, `full_name`, `private`, `description`, `default_branch`, `updated_at`, and `ref`.
- `github.issues.recent`: required `repository` in `owner/name` form; optional `state` (`open`, `closed`, or `all`), `limit` (1–100), and `max_characters` (500–20,000). Returns issue identity, title, bounded body, state, author, labels, comment count, timestamps, and `ref`.
- `github.pull_requests.recent`: required `repository`; optional `state`, `limit`, and `max_characters`. Returns pull-request identity, bounded body, draft flag, author, head/base refs, timestamps, and `ref`.
- `github.commits.recent`: required `repository`; optional Git `ref`, `limit`, and `max_characters`. Returns `sha`, repository, bounded commit message, author, authored timestamp, and `ref`.
- `github.repository.file`: required `repository` and `path`; optional Git `ref` and `max_characters` (500–50,000). Returns one bounded UTF-8 text file with repository, path, SHA, text, truncation flag, and `ref`.

Do not store GitHub result rows in the generated app. Use Datatailr app/job ACLs to limit the audience, and select only the required repositories when the GitHub App is installed.

### `gmail.messages.recent` and `outlook.messages.recent`

Parameters: optional `query` substring and `limit` (1–100, default 20). Returns live message rows with `id`, `title`, `text`, `updated_at`, and `ref`. Gmail reads a bounded recent `INBOX` window using read-only IMAP and `BODY.PEEK[]`; Outlook reads through Graph. Keep these rows in request/browser memory only; do not cache, embed, log, or persist them in the generated app.

### `outlook.calendar.events.upcoming`

Parameters: `days` (1–62, default 14), `time_zone` (Microsoft Graph-supported Windows timezone name, default `UTC`), and `limit` (1–100, default 20). Returns the signed-in user's live calendar instances with `id`, `title`, `start`, `end`, `is_all_day`, `location`, `organizer`, `attendees`, `response_status`, `online_meeting_url`, and `ref`. It expands recurring events within the bounded calendar view.

### `outlook.calendar.events.range`

Parameters: required timezone-aware ISO-8601 `start` and `end`, optional `time_zone`, and `limit` (1–100). The range must be no longer than 62 days. Returns the same event shape as `outlook.calendar.events.upcoming`.

### `outlook.calendar.availability`

Parameters: `schedules` (1–20 email-style identifiers), required timezone-aware ISO-8601 `start` and `end`, optional `time_zone`, and `interval_minutes` (5–1,440; default 30). Returns `schedule_id`, `availability_view`, and bounded `schedule_items` containing only status, start/end, and the private flag.

All Outlook calendar capabilities require delegated `Calendars.ReadBasic`. They never request `Calendars.ReadWrite`, never modify calendars, and never return event bodies or attachments. Keep event and availability rows only in request/browser memory; do not cache, embed, log, or persist them.

### `zoom.recordings.recent`

Parameters: `days` (1–30, default 30) and `limit` (1–100, default 20). Returns live rows with `id`, `meeting_id`, `topic`, `start_time`, `duration`, `transcript_available`, and `ref`. Results are limited to cloud recordings Zoom makes available to the signed-in user, normally meetings that user hosted.

### `zoom.ai_companion.recent`

Parameters: optional `query`, `days` (1–180, default 30), `limit` (1–20, default 10), and `max_characters` per retained transcript (1,000–20,000, default 12,000). Returns the signed-in user's prior hosted meetings with `meeting_id`, `topic`, `start_time`, `summary`, `next_steps`, `transcript_text`, `transcript_available`, and `ref`. Meeting discovery and summaries use Zoom AI Companion; the gateway attempts the retained transcript endpoint independently. A cloud recording is not required, and a useful summary row is returned even when no transcript was retained. This path requires delegated granular scopes `meeting:read:list_meetings`, `meeting:read:summary`, and `cloud_recording:read:meeting_transcript`.

### `zoom.transcripts.get`

Parameters: required `meeting_id` and optional `max_characters` (1,000–50,000, default 20,000). Returns `meeting_id`, `topic`, `start_time`, `text`, and `ref` for one Zoom-authorized transcript.

### `zoom.transcripts.search`

Parameters: optional `query`, `days` (1–30), `limit` (1–20), and `max_characters` per transcript (1,000–20,000). Returns transcript rows with `meeting_id`, `topic`, `start_time`, `text`, and `ref`. The gateway first attempts retained AI Companion transcripts discovered through prior meetings, then falls back to cloud-recording transcripts for older Zoom authorizations. This performs a bounded live scan; it is not a persistent full-text index. Keep every result in request/browser memory only and never cache, embed, log, or persist it.

## Actions

### `slack.messages.post`

Parameters:

- `channel`: joined Slack channel name or id.
- `text`: 1–4,000 characters.
- `idempotency_key`: stable 1–160 character key using letters, digits, `. _ : / -`.
- `dry_run`: optional boolean.

The bot must be invited to the channel. Reusing a key for the same user returns the prior result without sending a second message.
The Slack bot configuration requires the `chat:write` OAuth scope for non-dry-run delivery.

Example deterministic key for a HubSpot notification:

```python
key = f"hubspot:{record['object_type']}:{record['id']}:{record['updated_at']}"
client.post_slack_message("sales-updates", text, idempotency_key=key)
```

Persist the last successful `updated_at` checkpoint in Datatailr KV. Query with a small overlap window if desired; idempotency prevents duplicate notifications.

### `slack.files.upload`

Parameters:

- `channel`: joined Slack channel name or id.
- `filename`: plain filename, including its extension; paths are rejected.
- `content_base64`: base64-encoded document bytes, limited to 5 MiB before encoding.
- `title`: optional display title, up to 255 characters.
- `initial_comment`: optional message accompanying the document, up to 2,000 characters.
- `idempotency_key`: stable 1–160 character key using letters, digits, `. _ : / -`.
- `dry_run`: optional boolean.

The gateway decodes the document in request memory, streams it to Slack through
`files.getUploadURLExternal`, and finalizes it with
`files.completeUploadExternal`. It never writes document bytes to connector
storage, blob storage, logs, or the audit database. The audit event contains only
bounded operational metadata such as the channel, extension, byte size, and
comment length. The Slack bot requires `files:write` and must be invited to the
destination channel. Do not use the retired `files.upload` method.

```python
content = report_path.read_bytes()
client.upload_slack_file(
    "sales-updates",
    content,
    "weekly-report.pdf",
    title="Weekly report",
    initial_comment="Generated from the latest CRM activity.",
    idempotency_key=f"weekly-report:{week_start.isoformat()}",
)
```

## Errors

- `400`: invalid or unsupported capability parameters.
- `401`: no signed Datatailr job identity.
- `403`: gateway service or app access denied; personal connector data may also be unavailable to the viewer.
- `404`: Slack channel unavailable to the bot.
- `409`: connector not configured, authorization expired, or Outlook must be reauthorized after an administrator adds `Calendars.ReadBasic`.
- `429`: per-user service rate limit exceeded.
- `502`: upstream provider failure.

Never retry `400`, `401`, or `403` under another identity. Retry `429` and `502` with bounded exponential backoff. Do not advance workflow checkpoints after a failed action.
