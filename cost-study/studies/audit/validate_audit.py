#!/usr/bin/env python3
"""M1 validation: does the audited rewrite's captured verdict == the oracle's STALE_WINS keys?

Builds ooo50_sf1_s101 (single group), runs unsafe_compact with MOR_AUDIT=1 on the forked jar,
reads the per-group verdict the runner wrote, and set-compares it against the ENGINE oracle's
STALE_WINS keys (authoritative: materialized readback vs ground truth). The mechanism's stale-wins
predicate (discarded max ordering > survivor ordering) is exactly the oracle's STALE_WINS on a
single-group table, so they must match exactly.
"""
import os
import sys
import tempfile

HARNESS = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "src")
sys.path.insert(0, HARNESS)

from mor_harness import check, imperfections, tpcds          # noqa: E402
from mor_harness.adapters import make_adapter                # noqa: E402
from mor_harness.batching import build_write_plan            # noqa: E402
from mor_harness.config import RunConfig                     # noqa: E402
from mor_harness.rng import SeededRng                        # noqa: E402
from mor_harness.stream import synthesize                    # noqa: E402

WH = os.path.join(tempfile.gettempdir(), "mor_audit_validate")
os.makedirs(WH, exist_ok=True)

BASE = dict(keys_sampled=1.0, versions_per_key_mean=4, op_mix=(0.8, 0.15, 0.05),
            key_columns=("id",), payload_columns=("val",), ts_step_ms=1,
            commit_coarsening=1, clock_skew_ms=0, format="iceberg")
KNOBS = dict(ooo_rate=0.50, dup_rate=0.0, schema_change_freq=0.0)

cfg = RunConfig(**{**BASE, **KNOBS, "base_keys": 1200, "seed": 101,
                   "enforcement_mode": "unsafe_compact", "keep_tables": False})

seeded = SeededRng(cfg.seed)
base = tpcds.base_customer(cfg, os.path.join(WH, "_io"))
stream = synthesize(base, cfg, seeded)
imperfections.apply(stream, cfg, seeded)
plan = build_write_plan(stream, cfg, seeded)

name = "audit_validate"
tdir = os.path.join(WH, "db", name)
adapter = make_adapter(cfg.format)
res = adapter.apply(plan, name, tdir, WH, cfg.precombine_field(), os.path.join(WH, "_io", name))

kcols, vcol = stream.key_columns, stream.version_column
oracle = check.oracle_verdicts(res.materialized, stream.truth, kcols, vcol)
oracle_stale = {k for k, v in oracle.items() if v == check.OracleVerdict.STALE_WINS}

import json as _json

# captured verdict keys from the SNAPSHOT SUMMARY (the real M2 mechanism)
summ = res.audit_summary or {}
captured = set()
if summ.get("mor.audit.stale-wins-keys"):
    for key in _json.loads(summ["mor.audit.stale-wins-keys"]):
        captured.add(tuple(key))
print("snapshot summary props:",
      {k: (v if not k.endswith("keys") else f"<{len(_json.loads(v))} keys>")
       for k, v in summ.items()})

# cross-check: the debug side-file should carry the same keys
sidefile = set()
for ln in (res.audit_verdict_lines or []):
    for key in ln.get("keys", []):
        sidefile.add(tuple(key))
print(f"summary vs side-file agree: {captured == sidefile} "
      f"(summary {len(captured)}, side-file {len(sidefile)})")

print(f"captured stale-wins keys : {len(captured)}")
print(f"oracle  STALE_WINS keys  : {len(oracle_stale)}")
inter = captured & oracle_stale
print(f"intersection             : {len(inter)}")
print(f"captured - oracle (false positives): {len(captured - oracle_stale)}")
print(f"oracle - captured (missed)         : {len(oracle_stale - captured)}")
exact = captured == oracle_stale
print(f"\nEXACT MATCH: {exact}")
if not exact:
    print("  sample captured-only:", sorted(captured - oracle_stale)[:10])
    print("  sample oracle-only  :", sorted(oracle_stale - captured)[:10])
sys.exit(0 if exact else 1)
