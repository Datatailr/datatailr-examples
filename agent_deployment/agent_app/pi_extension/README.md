# pi-datatailr-system-builder

A Pi package for building Datatailr systems with a guided workflow.

## Install locally

```bash
pi install .
```

## Golden path

```text
/dt-system new
```

This flow scaffolds Datatailr components, delegates implementation through `spawn_subagent`, deploys generated components, and helps inspect status/logs.

## Supported targets

- App
- Service
- Workflow
- Excel add-in
