# Datatailr Examples

This is a fully public repository of runnable examples for the Datatailr platform.

It is intended to include two types of content:
- Datatailr-maintained reference examples provided by the company
- community-contributed examples added through pull requests

## What this repo is for

- show practical patterns for building on Datatailr
- provide starter projects you can copy and adapt
- create a shared public library of examples maintained by both Datatailr and the community

## Current example packages

The main packages in this repository are:
- `datatailr_demo/` - broad deployment-ready examples covering services, pipelines, dashboards, and Excel add-ins
- `stock_price_processing/` - end-to-end market data pipeline with realtime processing, dashboards, and scheduled compaction
- `smart_building_energy/` - end-to-end building telemetry pipeline with ingestion, processing workflow, analytics API, dashboard, and lake compaction

See each folder's `README.md` for detailed commands and component-level documentation.

## How to use this repo

1. Clone the repository.
2. Open the example folder you want to run.
3. Follow that folder's README for setup, authentication, and deployment steps.

Quick start examples:

```bash
cd datatailr_demo
python deploy.py all
```

```bash
cd smart_building_energy
python deploy.py all
```

## Contributing

Contributions are welcome.
If you want to add a new example, open a PR with:

- clear documentation (README with setup and usage)
- runnable, self-contained code
- no secrets, private credentials, or proprietary datasets
- a short explanation of the use case the example demonstrates

By contributing, you help expand the public catalog of Datatailr examples for everyone.
