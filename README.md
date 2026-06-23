# Datatailr Examples

This is a fully public repository of runnable examples for the Datatailr platform.

It is intended to include two types of content:
- Datatailr-maintained reference examples provided by the company
- community-contributed examples added through pull requests

## What this repo is for

- show practical patterns for building on Datatailr
- provide starter projects you can copy and adapt
- create a shared public library of examples maintained by both Datatailr and the community

## Current example package

The main package in this repository is:
- `datatailr_demo/` - deployment-ready examples covering services, pipelines, dashboards, and Excel add-ins

See `datatailr_demo/README.md` for detailed commands and component-level documentation.

## How to use this repo

1. Clone the repository.
2. Open the example folder you want to run (start with `datatailr_demo/`).
3. Follow that folder's README for setup, authentication, and deployment steps.

Quick start from the demo folder:

```bash
cd datatailr_demo
python deploy.py
```

## Deployment & promotion (CI/CD)

This repo includes GitHub Actions for a GitFlow-style release pipeline. Jobs are
deployed to `dev`, then promoted up to `pre` and `prod` through the **pull-request
review and merge process**: you declare the target version in
`deployments/environments.yaml`, open a PR (which posts a promotion plan), and on
merge the change is promoted via the remote Datatailr CLI, gated by GitHub
Environment approvals. See [`docs/gitflow-deployment.md`](docs/gitflow-deployment.md)
for setup (secrets, branch protection, environments) and step-by-step usage.

## Contributing

Contributions are welcome.
If you want to add a new example, open a PR with:

- clear documentation (README with setup and usage)
- runnable, self-contained code
- no secrets, private credentials, or proprietary datasets
- a short explanation of the use case the example demonstrates

By contributing, you help expand the public catalog of Datatailr examples for everyone.
