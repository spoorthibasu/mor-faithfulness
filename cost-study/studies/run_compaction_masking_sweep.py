"""Compaction masking at scale: does `rewrite_data_files` erase EVERY checker STALE_WINS
verdict, or only the handful the original mechanism run happened to produce?

`run_compaction_mechanism.py` established the *mechanism* (compaction applies the equality
deletes and discards the versions that lost, so `current_version_record` degenerates to the
stale survivor) but at the cost study's realistic operating point it produced only 3
STALE_WINS verdicts to test it on. That operating point is duplicate-dominated: it runs
`commit_coarsening=6` plus `dup_rate`/`schema_change_freq` > 0, and `classify()` returns
DUPLICATE for any key with `mult_phys >= 2` before the stale-wins test is ever reached, so
935 of 1,260 keys short-circuit.

STALE_WINS needs the opposite shape: exactly one visible row per key (`mult_phys == 1`) whose
version is not the current one. Per `batching.py`, `ooo_rate` is the knob that produces it
(it swaps the commit order of adjacent versions, so the stale version's later delete
suppresses the current one), while `dup_rate` / `schema_change_freq` / `commit_coarsening`
are the knobs that produce DUPLICATE. So this sweep drives `ooo_rate` with the duplicate
knobs at zero, and includes mixed cells that re-enable `dup_rate` so the "duplicates are not
masked" control is still exercised in the same table.

Per cell, two arms on the identical workload:

    arm A = unsafe          (perturbed layout, no compaction)
    arm B = unsafe_compact  (identical layout + rewrite_data_files)

and per arm two independent views of the same table:

  * the ENGINE readback (`ApplyResult.materialized`) -> oracle verdicts and per-key content.
    This is the content authority, and it is what the content-preservation and
    oracle-violation-count checks are computed from. The checker is never used to prove
    content preservation, because the checker's model is the thing under test.
  * the CHECKER layouts (`mor_checker`'s `IcebergAdapter` over the real metadata) -> the
    per-key verdicts whose masking is being measured.

Reported per cell: STALE_WINS before -> how many certified FAITHFUL after (the N-of-N),
DUPLICATE before -> how many survive, keys whose materialized content changed (expected 0),
and the oracle violation count before/after (expected unchanged).

Usage: JAVA_HOME=<jdk17> PYTHONPATH=src python studies/run_compaction_masking_sweep.py [cell ...]
       With no arguments runs every cell (~1h). Naming cells runs only those, e.g.
       `... run_compaction_masking_sweep.py ooo50_sf1_s101` for a single ~4 min probe.
Emits results/compaction_masking_sweep.json.
"""

import dataclasses
import json
import os
import shutil
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
sys.path.insert(0, os.path.dirname(__file__))

from mor_harness import check, imperfections, tpcds                 # noqa: E402
from mor_harness.adapters import make_adapter                       # noqa: E402
from mor_harness.batching import build_write_plan                   # noqa: E402
from mor_harness.config import RunConfig                            # noqa: E402
from mor_harness.rng import SeededRng                               # noqa: E402
from mor_harness.stream import synthesize                           # noqa: E402
from run_cost import HARNESS                                        # noqa: E402
from mor_checker.adapters.iceberg import IcebergAdapter             # noqa: E402
from mor_checker.core.classify import classify                      # noqa: E402
from mor_checker.core.model import (                                # noqa: E402
    mult_phys, s_d, visible_set, current_version_record,
)

WH = os.environ.get("MOR_MASKING_WH",
                    os.path.join(tempfile.gettempdir(), "mor_harness", "masking_wh"))

# Held fixed across cells so the only things varying are scale, seed, and the imperfection
# knobs. Mirrors run_cost.build()'s workload shape except commit_coarsening, which is 1 here
# (coarsening co-locates versions at one sequence number, which manufactures DUPLICATE).
BASE = dict(keys_sampled=1.0, versions_per_key_mean=4, op_mix=(0.8, 0.15, 0.05),
            key_columns=("id",), payload_columns=("val",), ts_step_ms=1,
            commit_coarsening=1, clock_skew_ms=0, format="iceberg")

# label -> (base_keys, seed, knobs). "mixed" cells keep dup_rate > 0 so the
# duplicates-are-not-masked control is exercised on the same table as the stale-wins.
CELLS = {
    "ooo50_sf1_s101":  (1200, 101, dict(ooo_rate=0.50, dup_rate=0.0,  schema_change_freq=0.0)),
    "ooo50_sf1_s202":  (1200, 202, dict(ooo_rate=0.50, dup_rate=0.0,  schema_change_freq=0.0)),
    "ooo50_sf1_s303":  (1200, 303, dict(ooo_rate=0.50, dup_rate=0.0,  schema_change_freq=0.0)),
    "ooo25_sf1_s101":  (1200, 101, dict(ooo_rate=0.25, dup_rate=0.0,  schema_change_freq=0.0)),
    "ooo50_sf10_s101": (4000, 101, dict(ooo_rate=0.50, dup_rate=0.0,  schema_change_freq=0.0)),
    "ooo50_sf10_s202": (4000, 202, dict(ooo_rate=0.50, dup_rate=0.0,  schema_change_freq=0.0)),
    "mixed_sf1_s101":  (1200, 101, dict(ooo_rate=0.50, dup_rate=0.15, schema_change_freq=0.0)),
    "mixed_sf10_s101": (4000, 101, dict(ooo_rate=0.50, dup_rate=0.15, schema_change_freq=0.0)),
}


def dump(layout):
    """Everything the checker's verdict is a function of, for one key."""
    cur = current_version_record(layout)
    return {
        "n_data_records": len(layout.data),
        "data": sorted([{"version": r.version, "seq": r.seq} for r in layout.data],
                       key=lambda x: (x["version"] is None, x["version"])),
        "delete_seqs": sorted(d.seq for d in layout.dels),
        "S_D": s_d(layout),
        "mult_phys": mult_phys(layout),
        "survivor_version": (visible_set(layout)[0].version
                             if mult_phys(layout) >= 1 else None),
        "current_version_record_version": cur.version if cur else None,
        "verdict": classify(layout).value,
    }


def run_arm(cfg, tag):
    """One engine run. Returns the checker's per-key verdicts, the ENGINE's per-key content,
    and the ENGINE-derived oracle verdicts. Content and oracle never go through the checker."""
    seeded = SeededRng(cfg.seed)
    base = tpcds.base_customer(cfg, os.path.join(WH, "_io"))
    stream = synthesize(base, cfg, seeded)
    imperfections.apply(stream, cfg, seeded)
    plan = build_write_plan(stream, cfg, seeded)
    adapter = make_adapter(cfg.format)
    name = f"mask_{tag}"
    tdir = os.path.join(WH, "db", name)
    res = adapter.apply(plan, name, tdir, WH, cfg.precombine_field(),
                        os.path.join(WH, "_io", name))

    kcols, vcol = stream.key_columns, stream.version_column
    oracle = check.oracle_verdicts(res.materialized, stream.truth, kcols, vcol)
    content = {}
    for row in res.materialized:
        content.setdefault(tuple(row[c] for c in kcols), []).append(row.get(vcol))
    content = {k: sorted(v) for k, v in content.items()}

    ck = IcebergAdapter(res.table_dir or tdir, key_columns=list(kcols),
                        version_column=vcol, upsert_only=False)
    layouts = {str(k): dump(lay) for k, lay in ck.layouts().items()}
    return {
        "layouts": layouts,
        "content": content,
        "oracle_violations": sum(1 for v in oracle.values() if v in check.VIOLATIONS),
        "n_oracle_keys": len(oracle),
        "table_dir": res.table_dir or tdir,
        "stats": {k: res.stats[k] for k in
                  ("commit_count", "data_files", "delete_files", "bytes_total")
                  if k in res.stats},
    }


def tally(layouts):
    out = {}
    for v in layouts.values():
        out[v["verdict"]] = out.get(v["verdict"], 0) + 1
    return out


def run_cell(label):
    base_keys, seed, knobs = CELLS[label]
    cfg_u = RunConfig(**{**BASE, **knobs, "base_keys": base_keys, "seed": seed,
                         "enforcement_mode": "unsafe", "keep_tables": True})
    cfg_c = dataclasses.replace(cfg_u, enforcement_mode="unsafe_compact")

    print(f"\n===== cell {label}  (base_keys={base_keys}, seed={seed}, {knobs}) =====",
          flush=True)
    u = run_arm(cfg_u, f"{label}_unsafe")
    print(f"  unsafe         {json.dumps(tally(u['layouts']))}", flush=True)
    c = run_arm(cfg_c, f"{label}_compact")
    print(f"  unsafe_compact {json.dumps(tally(c['layouts']))}", flush=True)

    def keys_with(layouts, verdict):
        return {k for k, v in layouts.items() if v["verdict"] == verdict}

    stale_before = keys_with(u["layouts"], "STALE_WINS")
    stale_masked = {k for k in stale_before
                    if c["layouts"].get(k, {}).get("verdict") == "FAITHFUL"}
    stale_other = {k: c["layouts"].get(k, {}).get("verdict")
                   for k in sorted(stale_before - stale_masked)}
    dup_before = keys_with(u["layouts"], "DUPLICATE")
    dup_survived = {k for k in dup_before
                    if c["layouts"].get(k, {}).get("verdict") == "DUPLICATE"}

    allk = set(u["content"]) | set(c["content"])
    changed = [list(k) for k in allk if u["content"].get(k, []) != c["content"].get(k, [])]

    res = {
        "config": {"base_keys": base_keys, "seed": seed, "commit_coarsening": 1, **knobs},
        "n_keys_checker": len(u["layouts"]),
        "n_keys_engine": len(allk),
        "verdicts_before": tally(u["layouts"]),
        "verdicts_after": tally(c["layouts"]),
        "stale_wins_before": len(stale_before),
        "stale_wins_masked_to_faithful": len(stale_masked),
        "stale_wins_not_masked": stale_other,
        "duplicate_before": len(dup_before),
        "duplicate_survived": len(dup_survived),
        "content_keys_changed": len(changed),
        "content_sample_changed": changed[:10],
        "oracle_violations_before": u["oracle_violations"],
        "oracle_violations_after": c["oracle_violations"],
        "oracle_violations_unchanged": u["oracle_violations"] == c["oracle_violations"],
        "files_before": u["stats"],
        "files_after": c["stats"],
        "flipped_sample": {
            k: {"before": u["layouts"][k], "after": c["layouts"][k]}
            for k in sorted(stale_masked)[:3]
        },
    }
    print(f"  -> STALE_WINS {res['stale_wins_masked_to_faithful']} of "
          f"{res['stale_wins_before']} masked to FAITHFUL; "
          f"DUPLICATE {res['duplicate_survived']} of {res['duplicate_before']} survived; "
          f"content changed {res['content_keys_changed']}; oracle "
          f"{res['oracle_violations_before']} -> {res['oracle_violations_after']}",
          flush=True)

    for arm in (u, c):
        shutil.rmtree(arm["table_dir"], ignore_errors=True)
    return res


def main(labels):
    out = {"what": "Iceberg rewrite_data_files: checker STALE_WINS masking, swept at scale",
           "method": ("Per cell, two arms on the identical workload (unsafe vs "
                      "unsafe_compact). Verdicts come from mor_checker over the real "
                      "Iceberg metadata; content preservation and oracle violation counts "
                      "come from the ENGINE readback, never from the checker."),
           "cells": {}}
    for label in labels:
        out["cells"][label] = run_cell(label)

    tot_s = sum(c["stale_wins_before"] for c in out["cells"].values())
    tot_m = sum(c["stale_wins_masked_to_faithful"] for c in out["cells"].values())
    tot_d = sum(c["duplicate_before"] for c in out["cells"].values())
    tot_ds = sum(c["duplicate_survived"] for c in out["cells"].values())
    out["totals"] = {
        "cells": len(out["cells"]),
        "stale_wins_before": tot_s,
        "stale_wins_masked_to_faithful": tot_m,
        "stale_wins_masked_fraction": (round(tot_m / tot_s, 6) if tot_s else None),
        "duplicate_before": tot_d,
        "duplicate_survived": tot_ds,
        "content_keys_changed": sum(c["content_keys_changed"] for c in out["cells"].values()),
        "all_oracle_counts_unchanged": all(c["oracle_violations_unchanged"]
                                           for c in out["cells"].values()),
    }
    print("\n================ TOTALS ================")
    print(json.dumps(out["totals"], indent=1))

    dst = os.environ.get(
        "MOR_MASKING_OUT", os.path.join(HARNESS, "results", "compaction_masking_sweep.json"))
    with open(dst, "w") as f:
        json.dump(out, f, indent=1)
        f.write("\n")
    print(f"\nevidence -> {dst}")


if __name__ == "__main__":
    sel = sys.argv[1:] or list(CELLS)
    bad = [s for s in sel if s not in CELLS]
    if bad:
        sys.exit(f"unknown cell(s) {bad}; known: {list(CELLS)}")
    main(sel)
