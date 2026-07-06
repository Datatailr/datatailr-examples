"""Shared primitives used by both the main-agent coordinator and the sub-agent
workflow task.

This package is deliberately kept as a *subpackage* of ``agent_app`` rather than
a separate top-level directory: Datatailr ships only the entrypoint's top-level
package to the remote, and the coordinator (running inside the App) builds and
launches the sub-agent workflow at runtime (build-then-call). For that to work,
the sub-agent task code and every module it imports must be importable from
within the single shipped ``agent_app`` package. Making ``agent_common`` and
``subagent`` subpackages of ``agent_app`` satisfies the §12 "make them a package
included in the bundle" requirement with no vendoring/duplication.
"""
