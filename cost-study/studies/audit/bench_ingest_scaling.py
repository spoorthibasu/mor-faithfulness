#!/usr/bin/env python3
"""Where does bulk ingest actually pay off? Throughput vs ROWS PER COMMIT.

The 8-cell workload is 51 commits x ~140 rows, so its cost is dominated by per-commit metadata overhead
and bulk ingest only gains ~1.4x. The GB-scale run is the opposite shape: few commits, ~500K+ rows each,
where per-record py4j writes dominate. This measures both paths across rows-per-commit to find the
crossover and to size the GB run honestly.
"""
import os
import sys
import tempfile
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "src"))
from mor_harness.adapters.base import iceberg_columns, run_driver, serialize_plan  # noqa: E402
from mor_harness.model import Checkpoint, WritePlan                                # noqa: E402

WH = os.path.join(tempfile.gettempdir(), "mor_ingest_scaling")
SIZES = [1_000, 10_000, 100_000, 500_000]


def run(n_rows, bulk):
    """One commit of n_rows (plus a second commit with an equality delete), timed end to end."""
    rows = [{"id": i, "val": f"payload-value-{i}", "lsn": i} for i in range(1, n_rows + 1)]
    cks = [Checkpoint(index=1, data=rows, deletes=[], schema_flush=False),
           Checkpoint(index=2, data=rows[: max(1, n_rows // 10)],
                      deletes=[(i,) for i in range(1, max(2, n_rows // 10) + 1)], schema_flush=False)]
    plan = WritePlan(checkpoints=cks, key_columns=["id"], payload_columns=["val"],
                     version_column="lsn", enforcement_mode="unsafe")
    name = f"ing_{n_rows}_{int(bulk)}"
    plan_json = serialize_plan(plan, name, os.path.join(WH, "db", name), WH, "lsn",
                               iceberg_columns(plan))
    os.environ["MOR_BULK_INGEST"] = "1" if bulk else "0"
    os.environ["MOR_AUDIT"] = "0"
    t0 = time.time()
    res = run_driver("iceberg_driver.py", plan_json, os.path.join(WH, "_io", name))
    wall = time.time() - t0
    total_rows = n_rows + max(1, n_rows // 10)
    return res["stats"]["apply_time_s"], wall, total_rows


print(f"{'rows/commit':>12} {'per-record':>22} {'bulk':>22} {'speedup':>9}")
print(f"{'':>12} {'apply_s':>10} {'rows/s':>11} {'apply_s':>10} {'rows/s':>11}")
for n in SIZES:
    a_apply, _, total = run(n, False)
    b_apply, _, _ = run(n, True)
    print(f"{n:>12,} {a_apply:>10.2f} {total/a_apply:>11,.0f} "
          f"{b_apply:>10.2f} {total/b_apply:>11,.0f} {a_apply/b_apply:>8.1f}x", flush=True)
