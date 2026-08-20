#!/usr/bin/env python3
"""Phase 5: does the OPT-IN cross-group merge recover the straddling misses?

Per group size, two audited compactions of the same workload:
  base        (audit-cross-group=false)  -> per-group verdict; incomplete under straddling
  cross-group (audit-cross-group=true)   -> table-level merge; should be COMPLETE
Both compared against the ENGINE oracle's STALE_WINS. The cross-group arm must reach 0 misses and,
critically, still 0 false positives.
"""
import json
import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "src"))
from mor_harness import check, imperfections, tpcds          # noqa: E402
from mor_harness.adapters import make_adapter                # noqa: E402
from mor_harness.batching import build_write_plan            # noqa: E402
from mor_harness.config import RunConfig                     # noqa: E402
from mor_harness.rng import SeededRng                        # noqa: E402
from mor_harness.stream import synthesize                    # noqa: E402

WH = os.path.join(tempfile.gettempdir(), "mor_crossgroup")
BASE = dict(keys_sampled=1.0, versions_per_key_mean=4, op_mix=(0.8, 0.15, 0.05),
            key_columns=("id",), payload_columns=("val",), ts_step_ms=1,
            commit_coarsening=1, clock_skew_ms=0, format="iceberg")
KNOBS = dict(ooo_rate=0.50, dup_rate=0.0, schema_change_freq=0.0)
SIZES = [20_000, 50_000]  # multi-group regimes (100KB+ is a single group -> base is already exact)


def run(label, size, cross):
    cfg = RunConfig(**{**BASE, **KNOBS, "base_keys": 1200, "seed": 101,
                       "enforcement_mode": "unsafe_compact", "keep_tables": False})
    seeded = SeededRng(cfg.seed)
    base = tpcds.base_customer(cfg, os.path.join(WH, "_io"))
    stream = synthesize(base, cfg, seeded)
    imperfections.apply(stream, cfg, seeded)
    plan = build_write_plan(stream, cfg, seeded)
    os.environ["MOR_AUDIT"] = "1"
    os.environ["MOR_AUDIT_CROSS_GROUP"] = "1" if cross else "0"
    os.environ["MOR_REWRITE_OPTS"] = f"max-file-group-size-bytes={size},min-input-files=2"
    res = make_adapter(cfg.format).apply(
        plan, label, os.path.join(WH, "db", label), WH, cfg.precombine_field(),
        os.path.join(WH, "_io", label))
    kcols, vcol = stream.key_columns, stream.version_column
    oracle = check.oracle_verdicts(res.materialized, stream.truth, kcols, vcol)
    oracle_stale = {k for k, v in oracle.items() if v == check.OracleVerdict.STALE_WINS}
    s = res.audit_summary or {}
    prop = "mor.audit.cross-group-keys" if cross else "mor.audit.stale-wins-keys"
    captured = {tuple(k) for k in json.loads(s.get(prop, "[]"))}
    return s, oracle_stale, captured


print(f"{'size':>8} {'mode':>12} {'groups':>7} {'oracle':>7} {'captured':>9} {'miss':>6} {'FP':>4} {'cands':>7}")
ok = True
for size in SIZES:
    for cross in (False, True):
        s, oracle, cap = run(f"cg{size}_{int(cross)}", size, cross)
        miss, fp = len(oracle - cap), len(cap - oracle)
        print(f"{size:>8} {'cross-group' if cross else 'base':>12} "
              f"{s.get('mor.audit.groups-total','?'):>7} {len(oracle):>7} {len(cap):>9} "
              f"{miss:>6} {fp:>4} {s.get('mor.audit.straddle-candidates','-'):>7}", flush=True)
        if fp:
            print(f"   !! FALSE POSITIVES: {sorted(cap - oracle)[:10]}")
        if cross and (miss or fp):
            ok = False
print(f"\ncross-group merge COMPLETE and one-sided at every size: {ok}")
sys.exit(0 if ok else 1)
