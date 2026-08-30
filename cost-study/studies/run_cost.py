"""Enforcement-cost study.

Knobs FIXED at the realistic operating point (skew=400, ooo=0.05, dup=0.05, schema=0.2);
sweep enforcement_mode over {unsafe, safe, safe_compact} for all three formats. The SAFE
enforcement priced per format is the discipline that fixes THAT format's failure mode:

  iceberg : per-snapshot ascending-sequence discipline (one version per key per commit).
            UNSAFE = coarse commits (commit_coarsening>1) that co-locate versions at one
            sequence number (cheap, high-throughput, but duplicates).
  hudi    : monotonic LSN precombine instead of ts_ms (same physical layout).
  delta   : LSN-ordered apply instead of out-of-order commit order (same physical layout).

safe_compact adds a compaction pass (Iceberg rewrite_data_files / Delta OPTIMIZE / Hudi
inline compaction). Emits results/cost_sf<N>.jsonl + .csv.

Usage: python studies/run_cost.py <base_keys> <sf_label>   (e.g. 1200 1  |  4000 10)
"""

import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from mor_harness import sweep
from mor_harness.config import RunConfig, REALISTIC_OPERATING_POINT

HARNESS = os.path.join(os.path.dirname(__file__), "..")
WAREHOUSE = os.environ.get(
    "MOR_COST_WH",
    os.path.join(tempfile.gettempdir(), "mor_harness", "cost_wh"),
)

# The realistic operating point from the sensitivity study.
OP = dict(clock_skew_ms=400, ooo_rate=0.05, dup_rate=0.05, schema_change_freq=0.2)

# Enforcement mechanism priced per format (for the report).
MECHANISM = {
    "iceberg": "per-snapshot ascending-seq (fine commits) vs coarse-commit default",
    "hudi": "LSN precombine vs ts_ms precombine",
    "delta": "LSN-ordered apply vs out-of-order commit order",
}


def build(base_keys):
    base = dict(base_keys=base_keys, keys_sampled=1.0, versions_per_key_mean=4,
                op_mix=(0.8, 0.15, 0.05), key_columns=("id",), payload_columns=("val",),
                ts_step_ms=1, seed=101, commit_coarsening=6, **OP)
    cfgs = []
    for fmt in ("iceberg", "hudi", "delta"):
        for mode in ("unsafe", "safe", "safe_compact"):
            cfgs.append(RunConfig(**{**base, "format": fmt, "enforcement_mode": mode}))
    return cfgs


if __name__ == "__main__":
    base_keys = int(sys.argv[1]) if len(sys.argv) > 1 else 1200
    sf = sys.argv[2] if len(sys.argv) > 2 else "1"
    out = os.path.join(HARNESS, "results", f"cost_sf{sf}")
    cfgs = build(base_keys)
    print(f"cost study SF{sf}: {len(cfgs)} configs (base_keys={base_keys})")
    sweep.run_sweep(cfgs, out, warehouse=WAREHOUSE)
    print("DONE:", out + ".jsonl")
