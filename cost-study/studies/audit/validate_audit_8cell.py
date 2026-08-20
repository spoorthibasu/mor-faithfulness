#!/usr/bin/env python3
"""M4: validate the audited-rewrite verdict against the oracle across ALL EIGHT cells (the full 5,440).

Per cell, run unsafe_compact with MOR_AUDIT=1 on the forked jar, read the stale-wins keys from the
snapshot summary, and set-compare against the ENGINE oracle's STALE_WINS keys. Reports per-cell, with
false positives (captured - oracle) and misses (oracle - captured) SEPARATELY: the paper's claim is a
one-sided error profile, so any false positive is a hard failure to surface immediately.
"""
import json
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

WH = os.path.join(tempfile.gettempdir(), "mor_audit_8cell")

BASE = dict(keys_sampled=1.0, versions_per_key_mean=4, op_mix=(0.8, 0.15, 0.05),
            key_columns=("id",), payload_columns=("val",), ts_step_ms=1,
            commit_coarsening=1, clock_skew_ms=0, format="iceberg")
CELLS = {
    "ooo50_sf1_s101":  (1200, 101, dict(ooo_rate=0.50, dup_rate=0.0,  schema_change_freq=0.0)),
    "ooo50_sf1_s202":  (1200, 202, dict(ooo_rate=0.50, dup_rate=0.0,  schema_change_freq=0.0)),
    "ooo50_sf1_s303":  (1200, 303, dict(ooo_rate=0.50, dup_rate=0.0,  schema_change_freq=0.0)),
    "ooo25_sf1_s101":  (1200, 101, dict(ooo_rate=0.25, dup_rate=0.0,  schema_change_freq=0.0)),
    "ooo50_sf10_s101": (4000, 101, dict(ooo_rate=0.50, dup_rate=0.0,  schema_change_freq=0.0)),
    "ooo50_sf10_s202": (4000, 202, dict(ooo_rate=0.50, dup_rate=0.0,  schema_change_freq=0.0)),
    "mixed_sf1_s101":  (1200, 101, dict(ooo_rate=0.50, dup_rate=0.15, schema_change_freq=0.0)),
    "mixed_sf10_s101": (4000, 101, dict(ooo_rate=0.50, dup_rate=0.15, schema_change_freq=0.0)),
    # clean control: 0 violations. On the random-merge harness the gate still AUDITS it (over-audit
    # headline) because per-file ordering bounds overlap even when the table is faithful.
    "clean_sf1_s101":  (1200, 101, dict(ooo_rate=0.00, dup_rate=0.00, schema_change_freq=0.0)),
}


def run_cell(label):
    base_keys, seed, knobs = CELLS[label]
    cfg = RunConfig(**{**BASE, **knobs, "base_keys": base_keys, "seed": seed,
                       "enforcement_mode": "unsafe_compact", "keep_tables": False})
    seeded = SeededRng(cfg.seed)
    base = tpcds.base_customer(cfg, os.path.join(WH, "_io"))
    stream = synthesize(base, cfg, seeded)
    imperfections.apply(stream, cfg, seeded)
    plan = build_write_plan(stream, cfg, seeded)
    tdir = os.path.join(WH, "db", label)
    res = make_adapter(cfg.format).apply(
        plan, label, tdir, WH, cfg.precombine_field(), os.path.join(WH, "_io", label))

    kcols, vcol = stream.key_columns, stream.version_column
    oracle = check.oracle_verdicts(res.materialized, stream.truth, kcols, vcol)
    oracle_stale = {k for k, v in oracle.items() if v == check.OracleVerdict.STALE_WINS}
    oracle_dup = sum(1 for v in oracle.values() if v == check.OracleVerdict.DUPLICATE)

    summ = res.audit_summary or {}
    captured = set()
    if summ.get("mor.audit.stale-wins-keys"):
        captured = {tuple(k) for k in json.loads(summ["mor.audit.stale-wins-keys"])}

    fp = captured - oracle_stale
    miss = oracle_stale - captured
    return {
        "captured": len(captured), "oracle_stale": len(oracle_stale),
        "oracle_dup": oracle_dup,
        "false_positives": len(fp), "misses": len(miss),
        "fp_sample": sorted(fp)[:5], "miss_sample": sorted(miss)[:5],
        "summary_count_prop": summ.get("mor.audit.stale-wins-count"),
        "groups_total": summ.get("mor.audit.groups-total"),
        "groups_gated": summ.get("mor.audit.groups-gated"),
        "groups_audited": summ.get("mor.audit.groups-audited"),
    }


def main():
    results = {}
    print(f"{'cell':18} {'captured':>9} {'oracle_SW':>10} {'FP':>4} {'miss':>5} {'oracle_DUP':>11} "
          f"{'grp_tot':>8} {'gated':>6} {'audited':>8}")
    for label in CELLS:
        r = run_cell(label)
        results[label] = r
        print(f"{label:18} {r['captured']:9d} {r['oracle_stale']:10d} "
              f"{r['false_positives']:4d} {r['misses']:5d} {r['oracle_dup']:11d} "
              f"{str(r['groups_total']):>8} {str(r['groups_gated']):>6} {str(r['groups_audited']):>8}",
              flush=True)
        if r["false_positives"] or r["misses"]:
            print(f"   !! FP sample {r['fp_sample']}  MISS sample {r['miss_sample']}", flush=True)
    # soundness invariant: no group containing a real violation may be gated out. On these single-group
    # tables a gated violating cell would drop its keys (miss>0), so miss==0 with gate on IS the check.
    tot_cap = sum(r["captured"] for r in results.values())
    tot_sw = sum(r["oracle_stale"] for r in results.values())
    tot_fp = sum(r["false_positives"] for r in results.values())
    tot_miss = sum(r["misses"] for r in results.values())
    print(f"\n{'TOTAL':18} {tot_cap:9d} {tot_sw:10d} {tot_fp:4d} {tot_miss:5d}")
    print(f"\nEXACT one-sided: {tot_fp == 0 and tot_miss == 0 and tot_cap == tot_sw}  "
          f"(captured {tot_cap} == oracle STALE_WINS {tot_sw}; FP {tot_fp}; miss {tot_miss})")
    out = os.path.join(os.path.dirname(__file__), "audit_8cell_result.json")
    with open(out, "w") as f:
        json.dump({"cells": results, "total_captured": tot_cap, "total_oracle_stale": tot_sw,
                   "total_false_positives": tot_fp, "total_misses": tot_miss}, f, indent=1)
    print(f"evidence -> {out}")
    sys.exit(0 if (tot_fp == 0 and tot_miss == 0 and tot_cap == tot_sw) else 1)


if __name__ == "__main__":
    main()
