---
name: datatailr-connectors
description: Build Datatailr apps, dashboards, workflows, alerts, and automations that use managed Slack, HubSpot, GitHub, Gmail, Outlook Mail and Calendar, or Zoom connectors through the app-protected connector gateway. Use whenever a user asks an AI coding agent to read connected business data, build a data-aware app, react to HubSpot or GitHub changes, use a personal calendar or Zoom AI Companion meeting, or send Slack notifications.
---

# Use managed connectors

Build the requested Datatailr app or workflow directly. Do not ask the user to download a kit, paste JSON, configure credentials, or copy connector data.

## Required boundary

- Keep connector credentials and provider API calls inside `connector-gateway`.
- Never call query endpoints while authoring, prompting a model, or generating code. Use the bundled schema and synthetic examples instead.
- Make the deployed app/workflow call the gateway only at runtime. Slack and HubSpot are shared indexed workspace sources. GitHub Organization is a shared source fetched live through its administrator-installed GitHub App and limited to repositories selected for that app. Gmail, Outlook Mail and Calendar, and Zoom are personal, fetched for the signed-in request, and never persisted by Integration Studio or the gateway. Gmail authenticates through the user's stored app password over read-only IMAP; Outlook and Zoom use delegated OAuth. Zoom AI Companion summaries and retained transcripts do not require cloud recording.
- Never forward gateway results to an LLM unless the user explicitly asks to send that data to that model.
- Do not cache, persist, log, embed, or copy Gmail/Outlook/Zoom result rows into generated app storage. Keep them only in request/browser memory and discard them after rendering.
- Connector activity is recorded in a platform-admin-only audit log. Gmail, Outlook, and Zoom events retain operational metadata only—never query values, addresses, subjects, message bodies, transcript content, result rows, prompts, credentials, or model responses.
- Do not accept connector tokens or GitHub private keys, construct provider authorization headers, or bypass the gateway.
- Treat a `403` as an authorization decision. Do not retry under another identity.

## Build workflow

1. Read [references/api.md](references/api.md) for the applicable capability and result schema.
2. Select the identity pattern:
   - For Slack/HubSpot/GitHub Organization Workflows, Services, and Apps (including shared Apps), copy [scripts/connector_client.py](scripts/connector_client.py) into the deployed package and call it server-side. Include `requests` in `python_requirements`. Grant app access only to the intended audience; its ACL is the data boundary.
   - For a shared App that reads Gmail, Outlook, or Zoom, copy [scripts/connector_client.js](scripts/connector_client.js) into its static assets and call it from the browser. This lets Datatailr authenticate the actual viewer. Never proxy personal connector requests through an app backend running as the owner.
3. Design and test with the synthetic examples in the API reference. Mock `ConnectorClient.query` or `ConnectorClient.action`; do not fetch production data during development.
4. At runtime, call the selected client directly. Do not create a generic connector proxy in the generated app.
5. Render only the fields needed by the request. Escape text and cap displayed rows.
6. For actions, use a deterministic idempotency key derived from the source record/event and action—not a random value—and surface failures in job logs. Slack documents must be supplied as in-memory bytes to `upload_slack_file`; do not persist them merely to send them.
7. Deploy using the relevant Datatailr app, workflow, or service skill.

## Choosing a component

- Use an App for an interactive or periodically refreshed dashboard.
- Use a scheduled Workflow for polling HubSpot and sending notifications.
- Use a Service only when the result needs a reusable API.
- Split a dashboard and notification automation into separate App and Workflow jobs.

For “create an app which reads `#sonic-bug-report` and creates a dashboard of the 20 recent bugs,” build an App whose server calls `slack.threads.recent` with `channel="sonic-bug-report"` and `limit=20` on page load or refresh.

For “notify users on Slack when there’s an update on HubSpot,” build a scheduled Workflow that stores a successful checkpoint, calls `hubspot.objects.recent` with `modified_after=<checkpoint>`, and posts through `slack.messages.post` using one stable key per HubSpot object update. Advance the checkpoint only after all intended posts succeed.

For “generate a report and post the document to Slack,” build the report in memory, then call `upload_slack_file` with a stable key through `slack.files.upload`. The gateway streams documents of up to 5 MiB to a bot-joined channel and does not persist their bytes.

For “show our upcoming demos and discovery calls for the next two weeks,” build an App whose server calls `hubspot.activities.upcoming` with `days=14` and `activity_types=["meetings", "calls", "tasks"]`. Use the returned owner and CRM associations to group activities by owner, company, or deal. HubSpot activities are shared workspace data, so the App ACL defines the audience.

For “show the recent issues and pull requests in `acme/analytics`,” build an App whose server calls `github.issues.recent` and `github.pull_requests.recent` with `repository="acme/analytics"`. GitHub data is fetched live and discarded after the request; do not persist source files, issue bodies, pull-request bodies, or commit messages in the generated app.

For “build a dashboard of my next 20 Outlook meetings,” build a shared App whose browser calls `outlook.calendar.events.upcoming` with `days=14` and `limit=20`. Keep the returned event rows only in browser memory so each viewer sees only their own delegated calendar.

For “summarize my recent Zoom meetings and show action items,” build a shared App whose browser calls `zoom.ai_companion.recent` with `days=30` and a bounded `limit`. Render the returned summary, next steps, and optional retained transcript in browser memory only. Do not require or initiate cloud recording.
