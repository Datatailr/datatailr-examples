"""Sub-agent workflow package.

A sub-agent is a single-task Datatailr ``@workflow`` run that executes one
scoped ``pi`` task against the shared git repository and reports its result
back via Blob storage. The workflow is built and launched at runtime by the
main agent's coordinator (build-then-call), never pre-deployed as a fixed job
(specification §12).
"""
