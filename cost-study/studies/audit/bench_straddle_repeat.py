#!/usr/bin/env python3
"""L2, re-run correctly, plus a repeat study of the false positive.

Two corrections to the first L2 run, both mine:

  * The cross-group arm was scored against the WRONG snapshot property. Cross-group mode writes its
    merged verdict to `mor.audit.cross-group-keys` and leaves the per-group list in place; the scorer
    only read the per-group list, so it reported 0% recall for the mode whose entire purpose is to
    restore recall. That zero was produced by the scorer, not the mechanism.
  * One false positive appeared in the per-group arm and did not reproduce in three later runs at
    different group sizes. Group formation is not stable between runs (the same 1 GB setting audited
    2 groups in one run and 1 in another), so a single observation cannot be called either a fluke or a
    finding. Here the identical configuration is repeated N times and the rate is reported as a rate.

Why a false positive is structurally possible, which is why it is being hunted rather than dismissed:
the single-survivor guard is evaluated within a group. A key with several surviving versions spread
across groups can present as single-survivor locally, and if that group also holds a discarded version
with a higher ordering value, the key is reported. Under this generator such a pair exists -- e.g. a key
last deleted at commit 14 has survivors at commits 14, 15, 16 with ordering bases 120M, 150M, 140M and a
discarded commit 13 at 130M: globally clean (150M > 130M), locally a violation if a group holds only the
commit-13 and commit-14 versions. Whether bin-packing ever produces that co-location is the empirical
question.
"""
import json
import os
import shutil
import sys
import tempfile
import time

_REPO = os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__)))))
sys.path.insert(0, os.path.join(_REPO, "cost-study/src"))
from mor_harness.adapters.base import run_driver, serialize_plan  # noqa: E402
from mor_harness.model import WritePlan                            # noqa: E402

WH = os.path.join(tempfile.gettempdir(), "mor_straddle_rep")
JAR = os.environ.get("MOR_ICEBERG_JAR", os.path.expanduser(
    "~/IdeaProjects/iceberg-mor-fork/spark/v3.5/spark-runtime/build/libs/"
    "iceberg-spark-runtime-3.5_2.12-1.11.0-SNAPSHOT.jar"))
COLS = [{"name": "id", "type": "int"}, {"name": "val", "type": "string"},
        {"name": "lsn", "type": "int"}]
SYNTH = {"commits": 16, "rows_per_commit": 900_000, "payload_bytes": 400, "delete_frac": 0.2,
         "ordering": "inverted", "dup_frac": 0.05, "files_per_commit": 4}
GROUP = "max-file-group-size-bytes=1073741824"
N_BASE, N_CROSS = 6, 3


def run(name, cross):
    tdir = os.path.join(WH, "db", name)
    shutil.rmtree(tdir, ignore_errors=True)
    plan = WritePlan(checkpoints=[], key_columns=["id"], payload_columns=["val"],
                     version_column="lsn", enforcement_mode="unsafe_compact")
    pj = serialize_plan(plan, name, tdir, WH, "lsn", COLS)
    pj["synth"] = SYNTH
    os.environ["MOR_ICEBERG_JAR"] = JAR
    os.environ["MOR_BULK_INGEST"] = "1"
    os.environ["MOR_AUDIT"] = "1"
    os.environ["MOR_AUDIT_CROSS_GROUP"] = "1" if cross else "0"
    os.environ["MOR_REWRITE_OPTS"] = GROUP
    os.environ.pop("MOR_DROP_CACHE", None)
    t0 = time.time()
    res = run_driver("iceberg_driver.py", pj, os.path.join(WH, "_io", name))
    wall = time.time() - t0
    shutil.rmtree(tdir, ignore_errors=True)
    return res.get("oracle") or {}, res.get("audit_summary") or {}, res.get("stats") or {}, wall


out = {"base": [], "cross": []}
print("=" * 88 + f"\nPER-GROUP mode, {N_BASE} repeats of an identical configuration\n" + "=" * 88,
      flush=True)
for i in range(N_BASE):
    o, s, st, wall = run(f"rep_base_{i}", False)
    rec = {"groups_total": s.get("mor.audit.groups-total"),
           "groups_audited": s.get("mor.audit.groups-audited"),
           "groups_gated": s.get("mor.audit.groups-gated"),
           "captured": o.get("captured"), "tp": o.get("true_positives"),
           "misses": o.get("misses"),
           "fp": (o.get("false_positives_from_duplicates", 0) + o.get("false_positives_other", 0)),
           "fp_keys": o.get("false_positive_keys", []),
           "live_ok": o.get("live_rows_match"), "compact_s": st.get("compact_time_s")}
    out["base"].append(rec)
    print(f"  r{i}: groups {rec['groups_audited']}/{rec['groups_total']} audited "
          f"(gated {rec['groups_gated']})  captured={rec['captured']} TP={rec['tp']} "
          f"miss={rec['misses']} FP={rec['fp']} {rec['fp_keys'][:4]}  compact={rec['compact_s']}s",
          flush=True)

print("\n" + "=" * 88 + f"\nCROSS-GROUP mode, {N_CROSS} repeats, scored on the MERGED property\n"
      + "=" * 88, flush=True)
for i in range(N_CROSS):
    o, s, st, wall = run(f"rep_cross_{i}", True)
    rec = {"scored": o.get("scored_property"),
           "groups_audited": s.get("mor.audit.groups-audited"),
           "straddle_candidates": s.get("mor.audit.straddle-candidates"),
           "captured": o.get("captured"), "tp": o.get("true_positives"),
           "misses": o.get("misses"),
           "fp": (o.get("false_positives_from_duplicates", 0) + o.get("false_positives_other", 0)),
           "fp_keys": o.get("false_positive_keys", []),
           "per_group_captured": o.get("per_group_captured"),
           "per_group_tp": o.get("per_group_true_positives"),
           "per_group_fp": o.get("per_group_false_positives"),
           "expected": o.get("expected_stale_wins"),
           "live_ok": o.get("live_rows_match"), "compact_s": st.get("compact_time_s")}
    out["cross"].append(rec)
    rr = rec["tp"] / rec["expected"] if rec.get("expected") else float("nan")
    print(f"  r{i}: scored={rec['scored']} groups={rec['groups_audited']} "
          f"candidates={rec['straddle_candidates']}", flush=True)
    print(f"      merged: captured={rec['captured']} TP={rec['tp']} miss={rec['misses']} "
          f"FP={rec['fp']} recall={rr:.1%}   per-group alone: TP={rec['per_group_tp']} "
          f"FP={rec['per_group_fp']}   compact={rec['compact_s']}s", flush=True)

print("\n" + "=" * 88)
fp_runs = [r for r in out["base"] if r["fp"]]
print(f"PER-GROUP false positives: {len(fp_runs)} of {N_BASE} runs "
      f"({sum(r['fp'] for r in out['base'])} keys total)")
if fp_runs:
    print("  => straddling costs SOUNDNESS in per-group mode, not only recall.")
    print(f"     offending keys seen: {sorted({k for r in fp_runs for k in r['fp_keys']})[:12]}")
else:
    print("  => not reproduced here. Report as observed-once-not-reproduced; do not claim either way.")
tps = [r["tp"] for r in out["base"]]
print(f"PER-GROUP recall across runs: TP {min(tps)}-{max(tps)} of "
      f"{out['cross'][0]['expected'] if out['cross'] else '?'} expected")
cfp = sum(r["fp"] for r in out["cross"])
rec_str = ", ".join(f"{r['tp']}/{r['expected']}" for r in out['cross'])
print(f"CROSS-GROUP: false positives {cfp}; recall {rec_str}")

dst = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bench_straddle_repeat.json")
json.dump(out, open(dst, "w"), indent=1)
print(f"evidence -> {dst}")
