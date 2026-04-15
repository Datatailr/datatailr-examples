from __future__ import annotations

import os
from pathlib import Path

from datatailr import Resources, Schedule, workflow

from stock_price_processing.compaction_workflow.tasks import (
    DEFAULT_BASE_PREFIX,
    DEFAULT_DATASETS,
    DEFAULT_LAST_N_HOURS,
    DEFAULT_MIN_FILES,
    compact_partitions,
    list_candidate_partitions,
    summarize,
)

LAST_N_HOURS = int(os.environ.get("COMPACTION_LAST_N_HOURS", str(DEFAULT_LAST_N_HOURS)))
MIN_FILES = int(os.environ.get("COMPACTION_MIN_FILES", str(DEFAULT_MIN_FILES)))
DATASETS = os.environ.get("COMPACTION_DATASETS", DEFAULT_DATASETS)
DRY_RUN = os.environ.get("COMPACTION_DRY_RUN", "0").lower() in ("1", "true", "yes")
BASE_PREFIX = os.environ.get("COLLECTOR_BLOB_PREFIX", DEFAULT_BASE_PREFIX)

schedule = Schedule(at_minutes=[0])

@workflow(
    name="Stock Lake Hourly Compaction",
    schedule=schedule,
    python_requirements=str(str(Path(__file__).parent.parent / "requirements.txt")),
    resources=Resources(memory="2g", cpu=1),
    env_vars={
        "COLLECTOR_BLOB_PREFIX": BASE_PREFIX,
        "COMPACTION_LAST_N_HOURS": str(LAST_N_HOURS),
        "COMPACTION_MIN_FILES": str(MIN_FILES),
        "COMPACTION_DRY_RUN": "1" if DRY_RUN else "0",
    },
)
def hourly_compaction_workflow():
    parts = list_candidate_partitions(BASE_PREFIX, DATASETS, LAST_N_HOURS)
    results = compact_partitions(parts, MIN_FILES, DRY_RUN).set_resources(memory="2g", cpu=1)
    summarize(results).alias("summary")


if __name__ == "__main__":
    hourly_compaction_workflow()
