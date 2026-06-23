# GitFlow Deployment & Promotion

This repository ships GitHub Actions that drive a GitFlow-style release pipeline
on Datatailr. Code is **deployed once to `dev`**, then the exact same, immutable
version is **promoted** up the environment chain — and promotions are reviewed
and approved through the **pull-request process**:

```
deploy ──▶ dev ──PR + merge──▶ pre ──PR + merge──▶ prod
```

Promotion never rebuilds a job. It takes an existing version that was already
built in a lower environment and moves it to the next one, so what you test in
`dev`/`pre` is bit-for-bit what runs in `prod`.

## How it works in one picture

1. **Deploy to `dev`** with the *Deploy to dev* workflow (manual run).
2. **Declare a promotion** by editing `deployments/environments.yaml` in a branch
   and opening a **pull request**.
3. The *Promote (GitOps)* check posts a **promotion plan** on the PR.
4. A reviewer approves and the PR is **merged to `main`**.
5. On merge, the changed versions are **promoted automatically** (`dev → pre`,
   then `pre → prod`), each gated by a GitHub Environment approval.

The promotion manifest is the single source of truth for "what version is in
which environment", and Git history is the audit log of every promotion.

## What lives where

| File | Purpose |
|---|---|
| `deployments/environments.yaml` | **Promotion manifest** — declares the desired job version per environment (`pre`, `prod`). Edited via PR. |
| `.github/scripts/reconcile_promotions.py` | Diffs the manifest against the previous revision and turns changed entries into `dt job promote` calls (plan/apply). |
| `.github/actions/datatailr-cli/` | Composite action: installs `datatailr`, configures the remote `dt` CLI, logs in non-interactively. |
| `.github/workflows/deploy-dev.yml` | **Deploy** an example project's jobs into the `dev` environment (manual). |
| `.github/workflows/promote.yml` | **Promote** via PR: plan on PR, apply on merge to `main`. |

## One-time setup

### 1. Add credentials as GitHub Secrets

The workflows log in to Datatailr using the remote CLI's non-interactive login,
which reads three environment variables. Store them as **GitHub Secrets**
(`Settings → Secrets and variables → Actions`):

| Secret | Description |
|---|---|
| `DATATAILR_BASE_URL` | Base URL of your Datatailr installation, e.g. `https://acme.datatailr.com`. |
| `DATATAILR_USER_NAME` | Username used to authenticate. |
| `DATATAILR_USER_PASSWORD` | Password for that user. |

These map directly to `DATATAILR_BASE_URL`, `DATATAILR_USER_NAME`, and
`DATATAILR_USER_PASSWORD`, which the CLI uses to log in without any prompts.

> Use a dedicated service/automation account rather than a personal login, and
> make sure it has permission to deploy and promote jobs.

### 2. Protect `main` (the PR review gate)

Promotions only happen when a change to the manifest is **merged into `main`**, so
branch protection on `main` is the primary control:

- `Settings → Branches → Branch protection rules → main`
- Enable **Require a pull request before merging** and **Require approvals**
  (e.g. 1–2 reviewers).
- Optionally **Require status checks to pass** and select the
  *Promotion plan* check so a PR can't be merged until the plan has run.

### 3. Create the GitHub Environments (approval gates)

Create the [GitHub Environments](https://docs.github.com/en/actions/deployment/targeting-different-environments/using-environments-for-deployment)
`dev`, `pre`, and `prod`. The apply jobs reference `pre` and `prod` so you get a
clean deployment history and a **second approval** right before anything changes:

- On the **`prod`** environment (and optionally `pre`), add **Required reviewers**.
  The merge-triggered apply job pauses until an approver signs off.
- If different environments need different credentials, define the three secrets
  **at the environment level**; environment-scoped secrets override repo-scoped
  ones for that job.

## Day-to-day usage

### Step 1 — Deploy to `dev`

1. Go to **Actions → Deploy to dev → Run workflow**.
2. Pick the **project** folder to deploy (e.g. `datatailr_demo`) and, if needed,
   the deploy script name (defaults to `deploy.py`).
3. Run it. The workflow installs the project's `requirements.txt`, logs in, and
   runs the deploy script, which builds/registers the jobs in **`dev`**.
4. At the end it prints the jobs currently in `dev` so you can grab their names.

> The deploy script (`Service.run()`, `App.run()`, `@workflow`, …) always targets
> the `dev` environment. That is the only place new code enters the pipeline.

### Step 2 — Open a promotion PR

Find the version you want to ship (from the deploy log, or `dt job versions
"<job>" -e dev`). Then, on a branch, edit `deployments/environments.yaml`:

```yaml
pre:
  "Price Server": 3        # promote v3 dev -> pre
  "Price Processor": 3
prod: {}
```

Open a pull request. The **Promotion plan** check runs and posts a comment like:

> ## Promotion plan
>
> | Target | Job | Version | Promote from | Command |
> |---|---|---|---|---|
> | pre | Price Server | 3 | dev | `dt job promote "Price Server" 3 dev` |

The plan is computed purely from the manifest diff — no jobs are touched yet.
Reviewers see exactly which versions will move and to where.

### Step 3 — Merge to promote `dev → pre`

Once approved and merged, the *Promote (GitOps)* workflow runs on `main`:

- The **Promote to pre** job (gated by the `pre` Environment) runs
  `dt job promote "Price Server" 3 dev`, moving the version into `pre`, and
  prints `dt job versions` to confirm.

### Step 4 — Promote `pre → prod`

Open a second PR that sets the version under `prod`:

```yaml
pre:
  "Price Server": 3
prod:
  "Price Server": 3        # promote v3 pre -> prod
```

After review and merge, the **Promote to prod** job (gated by the `prod`
Environment, with required reviewers) runs `dt job promote "Price Server" 3 pre`.

> You can combine both in one PR (set the version under both `pre` and `prod`).
> On merge, `pre` is applied first, then `prod`, in order.

## How the manifest maps to promotions

The reconcile script compares the merged manifest to the previous revision and
acts only on **added or changed** entries:

| Manifest section changed | Target | Source | CLI command run |
|---|---|---|---|
| `pre: "<job>": <v>` | `pre` | `dev` | `dt job promote "<job>" <v> dev` |
| `prod: "<job>": <v>` | `prod` | `pre` | `dt job promote "<job>" <v> pre` |

The third CLI argument is the environment you promote **from**; Datatailr moves
the job to the next environment automatically.

Notes:

- **Idempotent per merge** — only the entries that changed in that merge are
  promoted. Unrelated merges that don't touch the manifest do nothing (the
  workflow is path-filtered to `deployments/environments.yaml`).
- **No automatic rollback** — removing or lowering an entry does **not** demote a
  job. To roll back, deploy/promote the desired version forward, or use the CLI
  directly.
- A version must exist in the source environment before it can be promoted
  (deploy to `dev` before adding to `pre`; land in `pre` before adding to `prod`).

## Verifying & troubleshooting

The apply jobs print `dt job versions` for both the source and target
environments so you can confirm the version landed. To inspect further:

```bash
dt job ls -f "environment = prod"     # what is running in prod
dt job get "Price Server" -e prod     # details for one job
dt log read "Price Server" -e prod    # runtime logs
```

Common issues:

- **Login fails** — double-check the three secrets and that the account is valid
  and authorized on the target installation.
- **`dt: command not found`** — the composite action installs the `dt` shim onto
  `PATH`; make sure the *Set up Datatailr CLI* step ran before any `dt` step.
- **Promotion rejected** — confirm the job/version exists in the source
  environment (`dt job versions "<job>" -e <source>`). For `prod`, the version
  must already be in `pre`.
- **Plan looks empty on a PR** — make sure the branch is up to date with `main`
  and that you actually changed a value under `pre`/`prod`.

## Extending the flow

- **Auto-deploy to `dev` on merge to `main`:** add a `push` trigger to
  `deploy-dev.yml` (path-filtered to a project folder) so integration code lands
  in `dev` automatically, while promotions stay PR-gated.
- **Tag-based releases:** trigger `promote.yml` from Git tags instead of (or in
  addition to) `main` merges; keep the `prod` environment approval in place.
