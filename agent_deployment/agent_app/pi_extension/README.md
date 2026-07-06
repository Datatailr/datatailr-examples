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

## Startup header

On `session_start` (TUI mode) the package installs a custom header that shows
side-by-side ASCII logos for Datatailr and pi, plus a live inventory of
everything pi loaded for the session: skills, extensions, prompt templates, and
context files.

- `/dt-header` — re-show the Datatailr header.
- `/builtin-header` — restore pi's built-in header.

Data sources:

- **Skills / Prompts** — read live on every redraw from `pi.getCommands()`.
- **Extensions** — `pi.getCommands()` (source `extension`) plus non-built-in
  tools from `pi.getAllTools()`; this package is always listed since its
  extension is, by definition, loaded.
- **Context** — discovered from disk the same way pi does (the global
  `~/.pi/agent/AGENTS.md`, then the nearest `AGENTS.md`/`CLAUDE.md` walking from
  the cwd up to the filesystem root), because the loaded set is not exposed to
  extensions.

## Supported targets

- App
- Service
- Workflow
- Excel add-in
