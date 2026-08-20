#!/usr/bin/env python3
"""Phase 5: force multi-group compaction and measure what per-group detection misses.

Same workload, two compactions on the forked jar:
  (A) single group  (default max-file-group-size) -> the M4 baseline, full verdict
  (B) multi group   (small max-file-group-size)   -> keys straddle groups
Compare each captured verdict against the ENGINE oracle's STALE_WINS keys. The decisive question:
  - misses  (oracle - captured) => per-group detection is INCOMPLETE (false negatives; fits one-sided posture)
  - wrong   (captured - oracle) => per-group detection is WRONG (false positives; a different, worse problem)
"""
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

WH = os.path.join(tempfile.gettempdir(), "mor_phase5")
BASE = dict(keys_sampled=1.0, versions_per_key_mean=4, op_mix=(0.8, 0.15, 0.05),
            key_columns=("id",), payload_columns=("val",), ts_step_ms=1,
            commit_coarsening=1, clock_skew_ms=0, format="iceberg")


def run(label, knobs, rewrite_opts):
    cfg = RunConfig(**{**BASE, **knobs, "base_keys": 1200, "seed": 101,
                       "enforcement_mode": "unsafe_compact", "keep_tables": False})
    seeded = SeededRng(cfg.seed)
    base = tpcds.base_customer(cfg, os.path.join(WH, "_io"))
    stream = synthesize(base, cfg, seeded)
    imperfections.apply(stream, cfg, seeded)
    plan = build_write_plan(stream, cfg, seeded)
    os.environ["MOR_AUDIT"] = "1"
    os.environ["MOR_REWRITE_OPTS"] = rewrite_opts
    res = make_adapter(cfg.format).apply(
        plan, label, os.path.join(WH, "db", label), WH, cfg.precombine_field(),
        os.path.join(WH, "_io", label))
    kcols, vcol = stream.key_columns, stream.version_column
    oracle = check.oracle_verdicts(res.materialized, stream.truth, kcols, vcol)
    oracle_stale = {k for k, v in oracle.items() if v == check.OracleVerdict.STALE_WINS}
    summ = res.audit_summary or {}
    import json
    captured = {tuple(k) for k in json.loads(summ.get("mor.audit.stale-wins-keys", "[]"))}
    return summ, oracle_stale, captured


MULTI = "max-file-group-size-bytes=20000,min-input-files=2"
NODUP = dict(ooo_rate=0.50, dup_rate=0.0, schema_change_freq=0.0)
DUP = dict(ooo_rate=0.50, dup_rate=0.15, schema_change_freq=0.0)


def report(tag, summ, oracle_stale, captured):
    fn, fp = oracle_stale - captured, captured - oracle_stale
    print(f"=== {tag} ===")
    print(f"  groups={summ.get('mor.audit.groups-total')} captured={len(captured)} "
          f"oracle_STALE={len(oracle_stale)}  FN(miss)={len(fn)}  FP(wrong)={len(fp)}")
    if fp:
        print(f"  !! FALSE POSITIVES: {sorted(fp)[:10]}")
    return fp


report("(A) no-dup, single group", *run("single", NODUP, ""))
report("(B) no-dup, multi group", *run("multi", NODUP, MULTI))
# FP edge case: duplicates (dup_rate=0.15) whose two survivor rows may straddle groups.
fp_dup = report("(C) with-dup, multi group  [FP edge case]", *run("multidup", DUP, MULTI))

print("\nVERDICT: per-group detection under straddling is "
      + ("WRONG -- false positives present (duplicate straddle)." if fp_dup
         else "INCOMPLETE only -- false negatives, no false positives, even with duplicates."))
