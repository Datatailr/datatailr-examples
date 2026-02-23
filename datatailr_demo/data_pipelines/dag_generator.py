# *************************************************************************
#
#  Copyright (c) 2026 - Datatailr Inc.
#  All Rights Reserved.
#
#  This file is part of Datatailr and subject to the terms and conditions
#  defined in 'LICENSE.txt'. Unauthorized copying and/or distribution
#  of this file, in parts or full, via any medium is strictly prohibited.
# *************************************************************************

"""DAG Generator — create a workflow with an arbitrary number of tasks.

Usage from the CLI
------------------
.. code-block:: bash

   python -m examples dag <num_tasks>

Programmatic usage
------------------
.. code-block:: python

   from data_pipelines.dag_generator import generate_dag
   wf = generate_dag(20)   # returns a workflow function
   wf()                     # deploy
   wf(local_run=True)       # or run locally
   wf(to_json=True)         # or just inspect the JSON
"""

from __future__ import annotations

import math
from typing import Any, List

from datatailr import task, workflow
from datatailr.logging import DatatailrLogger

logger = DatatailrLogger(__name__).get_logger()


# ---------------------------------------------------------------------------
# Generic task functions used as building blocks in the generated DAG.
# Each task simulates a lightweight computation so the DAG can actually run.
# ---------------------------------------------------------------------------


@task(memory="150m", cpu=0.1)
def generate_data(seed: int) -> dict:
    """Produce a small synthetic data payload keyed by *seed*."""
    return {"seed": seed, "value": seed * 7 + 3}


@task(memory="150m", cpu=0.1)
def transform(data: Any, factor: int) -> Any:
    """Apply a simple transformation to *data*."""
    if isinstance(data, dict):
        return {
            k: v * factor if isinstance(v, (int, float)) else v for k, v in data.items()
        }
    return data


@task(memory="150m", cpu=0.1)
def aggregate(*values) -> dict:
    """Aggregate multiple upstream results into a single summary."""
    total = 0
    count = 0
    for v in values:
        if isinstance(v, dict):
            total += sum(x for x in v.values() if isinstance(x, (int, float)))
            count += 1
        elif isinstance(v, (int, float)):
            total += v
            count += 1
    return {"total": total, "count": count}


@task(memory="150m", cpu=0.1)
def passthrough(data: Any) -> Any:
    """Identity task — forwards *data* unchanged (useful for padding the DAG)."""
    return data


# ---------------------------------------------------------------------------
# DAG layout helpers
# ---------------------------------------------------------------------------


def _build_layer_sizes(num_tasks: int) -> List[int]:
    """Partition *num_tasks* into layer sizes that form a diamond shape.

    The first layer fans out, middle layers stay wide, and the last layer
    fans in to a single aggregation task.

    Returns a list where each element is the number of tasks in that layer.
    """
    if num_tasks <= 0:
        raise ValueError("num_tasks must be a positive integer")
    if num_tasks == 1:
        return [1]
    if num_tasks == 2:
        return [1, 1]

    # Reserve 1 task for the final aggregation
    remaining = num_tasks - 1
    # First layer: ~sqrt of remaining, at least 1
    first_layer = max(1, int(math.sqrt(remaining)))
    remaining -= first_layer

    layers: List[int] = [first_layer]

    if remaining <= 0:
        layers.append(1)
        return layers

    # Distribute the rest into middle layers of roughly the same width
    width = first_layer
    while remaining > 0:
        layer = min(width, remaining)
        layers.append(layer)
        remaining -= layer

    # Final aggregation task (already counted in num_tasks - 1 above)
    layers.append(1)
    return layers


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def generate_dag(num_tasks: int):
    """Return a ``@workflow``-decorated function containing *num_tasks* tasks.

    The generated DAG has a diamond-like shape:

    1. **Source layer** — ``generate_data`` tasks that fan out.
    2. **Middle layers** — ``transform`` / ``passthrough`` tasks that process
       outputs from the previous layer.
    3. **Sink layer** — a single ``aggregate`` task that fans in all results
       from the last middle layer.

    Args:
        num_tasks: Total number of tasks to include in the workflow.
                   Must be >= 1.

    Returns:
        A callable workflow function (the result of the ``@workflow`` decorator).
    """
    layer_sizes = _build_layer_sizes(num_tasks)

    @workflow(name=f"Generated DAG ({num_tasks} tasks) - <>USERNAME<>")
    def generated_dag():
        prev_layer_outputs: List[Any] = []

        for layer_idx, size in enumerate(layer_sizes):
            current_outputs: List[Any] = []

            if layer_idx == 0:
                # Source layer — generate initial data
                for i in range(size):
                    out = generate_data(i).alias(f"generate_{i}")
                    current_outputs.append(out)

            elif layer_idx == len(layer_sizes) - 1:
                # Sink layer — aggregate everything from the previous layer
                out = aggregate(*prev_layer_outputs).alias("aggregate_final")
                current_outputs.append(out)

            else:
                # Middle layer — transform / passthrough
                for i in range(size):
                    # Pick an upstream output (round-robin over previous layer)
                    upstream = prev_layer_outputs[i % len(prev_layer_outputs)]
                    if i % 2 == 0:
                        out = transform(upstream, layer_idx + 1).alias(
                            f"transform_L{layer_idx}_{i}"
                        )
                    else:
                        out = passthrough(upstream).alias(
                            f"passthrough_L{layer_idx}_{i}"
                        )
                    current_outputs.append(out)

            prev_layer_outputs = current_outputs

    return generated_dag
