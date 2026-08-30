"""Sensitivity study sweep (coarse pass).

OFAT from an all-zero baseline, enforcement_mode=unsafe throughout, SF1 (base cardinality
via base_keys, since volume is decoupled from SF). Each knob is swept for the format it is
theoretically sensitive to (primary), with control endpoints for the other formats to
demonstrate flatness. A few combined points on all three formats. One seed for the coarse
pass (sweep is resumable; multi-seed CIs are a refinement).

Run (from the repo root or this folder):
  PYTHONPATH=../cost-study/src JAVA_HOME=<jdk17> python run_sensitivity.py
Emits results/sensitivity.jsonl + .csv and prints trend tables.
"""

import os
import sys
import tempfile

# The harness lives in the sibling cost-study/ package; override with MOR_HARNESS_SRC.
sys.path.insert(0, os.environ.get(
    "MOR_HARNESS_SRC",
    os.path.join(os.path.dirname(__file__), "..", "cost-study", "src"),
))

from mor_harness import sweep
from mor_harness.config import RunConfig

RESULTS = os.path.join(os.path.dirname(__file__), "results", "sensitivity")
WAREHOUSE = os.environ.get(
    "MOR_SWEEP_WH",
    os.path.join(tempfile.gettempdir(), "mor_harness", "sweep_wh"),
)

BASE = dict(scale_factor=1, base_keys=1200, keys_sampled=1.0, versions_per_key_mean=4,
            op_mix=(0.8, 0.15, 0.05), key_columns=("id",), payload_columns=("val",),
            enforcement_mode="unsafe", ts_step_ms=1, seed=101)

SKEW = [0, 400, 1500, 6000]              # none / small / medium / large
OOO = [0.0, 0.05, 0.10, 0.25, 0.50]
DUP = [0.0, 0.05, 0.15, 0.30]
SCHEMA = [0.0, 0.2, 0.6]                  # none / occasional / frequent


def cfg(fmt, **knobs):
    return RunConfig(**{**BASE, "format": fmt, **knobs})


def build_configs():
    cfgs = []
    for fmt in ("iceberg", "hudi", "delta"):
        cfgs.append(cfg(fmt))  # shared all-zero baseline (deduped by hash)

    # clock skew: primary Hudi (precombine=ts_ms); Iceberg/Delta controls at endpoints.
    for s in SKEW:
        cfgs.append(cfg("hudi", clock_skew_ms=s))
    for s in (0, 6000):
        cfgs.append(cfg("iceberg", clock_skew_ms=s))
        cfgs.append(cfg("delta", clock_skew_ms=s))

    # out-of-order: primary Iceberg (seq inversion); Hudi/Delta controls.
    for o in OOO:
        cfgs.append(cfg("iceberg", ooo_rate=o))
    for o in (0.0, 0.5):
        cfgs.append(cfg("hudi", ooo_rate=o))
    for o in (0.0, 0.25, 0.5):
        cfgs.append(cfg("delta", ooo_rate=o))   # Delta may show stale on commit-order inversion

    # duplicate: primary Iceberg (equal-seq dup); Hudi/Delta controls.
    for d in DUP:
        cfgs.append(cfg("iceberg", dup_rate=d))
    for d in (0.0, 0.30):
        cfgs.append(cfg("hudi", dup_rate=d))
        cfgs.append(cfg("delta", dup_rate=d))

    # schema-change: primary Iceberg (FLINK-38450 co-location); Hudi/Delta controls.
    for s in SCHEMA:
        cfgs.append(cfg("iceberg", schema_change_freq=s))
    for s in (0.0, 0.6):
        cfgs.append(cfg("hudi", schema_change_freq=s))
        cfgs.append(cfg("delta", schema_change_freq=s))

    # combined points on all three formats
    realistic = dict(clock_skew_ms=400, ooo_rate=0.05, dup_rate=0.05, schema_change_freq=0.2)
    stress = dict(ooo_rate=0.25, dup_rate=0.15)
    skew_ooo = dict(clock_skew_ms=1500, ooo_rate=0.10)
    for fmt in ("iceberg", "hudi", "delta"):
        cfgs.append(cfg(fmt, **realistic))
        cfgs.append(cfg(fmt, **stress))
        cfgs.append(cfg(fmt, **skew_ooo))
    return cfgs


if __name__ == "__main__":
    cfgs = build_configs()
    # dedupe by config_hash (baseline appears many times)
    seen, uniq = set(), []
    for c in cfgs:
        h = c.config_hash()
        if h not in seen:
            seen.add(h)
            uniq.append(c)
    print(f"sensitivity sweep: {len(uniq)} unique configs")
    sweep.run_sweep(uniq, RESULTS, warehouse=WAREHOUSE)
    print("DONE:", RESULTS + ".jsonl")
