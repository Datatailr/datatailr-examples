# Managed Connectors

This template deploys two Datatailr components together:

- **Integration Studio**, a Flask App where administrators configure shared connectors, every user connects personal sources, and teams get the instructions for adding the connector skill to an external coding agent.
- **Connector Gateway**, a Service that exposes bounded connector capabilities to authorized Datatailr Apps, Services, and Workflows.
- **`datatailr-connectors` Agent Skill**, a portable skill with the gateway contract, synthetic examples, and Python/JavaScript runtime clients.

The installer becomes the deployment owner and an Integration Studio administrator. The `dtusers` group can access both jobs, while platform `admin` members can configure shared connectors and view connector audit events.

## Prerequisite

Create an encrypted secret named `integration-studio/master-key` in Datatailr Secrets Manager. Its value must be a Fernet key. Generate one locally without saving it in this project:

```bash
python -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())'
```

## Deploy

Run from this directory as the Datatailr administrator who should own the installation:

```bash
python deploy.py
```

The command deploys `connector-gateway` first and then `integration-studio`. For maintenance, deploy an individual component with `python deploy.py service` or `python deploy.py app`.

After deployment, open Integration Studio from **AI & Integrations** and configure the required providers. The Outlook and Zoom configuration panels display the callback URLs to register with Microsoft Entra and Zoom for the current Datatailr domain and environment.

Integration Studio intentionally has no chat or app generator. Open **Agent Skills**, copy the bootstrap prompt, and paste it into a new coding-agent project. The agent installs the public skill from this repository and builds against synthetic schemas; the deployed Datatailr job reads real connector data only at runtime.

The skill source is [`skills/datatailr-connectors`](skills/datatailr-connectors). Claude Code project skills live under `.claude/skills/`; other Agent Skills-compatible tools may use a different project skills directory. Review the installed `SKILL.md`, API reference, and client scripts before allowing an agent to use them.

## Access and storage

- Slack and HubSpot are administrator-managed shared sources indexed under `/mnt/integration-studio/knowledge`.
- GitHub Organization is administrator-managed, fetched live, and limited to repositories selected for the installed GitHub App.
- Gmail, Outlook Mail and Calendar, and Zoom are personal and fetched live for the signed-in user. Their source records are not stored.
- Connector settings and delegated credentials are encrypted with `integration-studio/master-key` before being written under `/mnt/integration-studio`.
- Connector audit events are visible only to platform administrators; personal connector events contain operational metadata only.

The OpenAPI contract is served by Connector Gateway at `/job/<environment>/connector-gateway/openapi.json`.

See [INGESTION_ARCHITECTURE.md](INGESTION_ARCHITECTURE.md) for the shared-source indexing design.
