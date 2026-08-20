#!/usr/bin/env python3
"""Experiment 2 (priority 2) -- correctness at multi-group scale, both modes, duplicates injected.

This is the experiment that matters most for the paper's central claim, because it is the one that
can falsify it. Locally, per-group detection under straddling turned out to be UNSOUND: in 1 of 6
identical runs it reported 180,000 keys that are not violations, because the single-survivor guard is
evaluated within a group and a key whose survivors span groups can present as single-survivor locally.
Cross-group mode was sound and complete in 3 of 3. Both results were rates over repeats, because file
group formation is not stable between runs -- a single run can neither establish nor refute either.

So: the same configuration is repeated, and the rate is the result. A run of six that produces no
false positive does not overturn the local finding, and a run that produces them at a different rate
is the more interesting outcome. Either way it is reported as observed.

Every table carries injected FLINK-38450 duplicates -- two rows of a key in ONE commit as two data
files in a single row-delta, sharing a sequence number the co-committed delete cannot suppress, plus a
discarded high-ordering version so a discarded value out-orders both survivors. That is the only shape
that can make the mechanism emit a false positive, and a run without it cannot test the guard at all.

Controls, all fatal:
  * the closed-form oracle must predict the engine's surviving row count exactly. It shares no code
    with the mechanism, so a mismatch means the oracle's model of suppression is wrong, not the
    mechanism.
  * the duplicate trap must be non-empty, or the guard is untested and its zeros are vacuous.
  * the table must actually form multiple groups, or this is experiment 1 with extra steps.
  * cross-group mode must produce zero false positives; that is the claim under test.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import (ControlFailure, check_entropy, check_rewrote, emit, hostinfo,  # noqa: E402
                    preflight, run_one)

HEAP = os.environ.get("MOR_EXP2_HEAP", "32g")
N_BASE = int(os.environ.get("MOR_EXP2_BASE_REPEATS", "6"))
N_CROSS = int(os.environ.get("MOR_EXP2_CROSS_REPEATS", "3"))
COMMITS = int(os.environ.get("MOR_EXP2_COMMITS", "28"))
RPC = int(os.environ.get("MOR_EXP2_RPC", "2000000"))
FPC = int(os.environ.get("MOR_EXP2_FPC", "4"))          # 500K rows/file ~ 195 MB, under the floor
PAYLOAD = 400
GROUP_BYTES = int(os.environ.get("MOR_EXP2_GROUP_BYTES", str(2 * 1024 ** 3)))

SYNTH = {"commits": COMMITS, "rows_per_commit": RPC, "payload_bytes": PAYLOAD, "delete_frac": 0.2,
         "ordering": "inverted", "dup_frac": 0.05, "files_per_commit": FPC}
OPTS = f"max-file-group-size-bytes={GROUP_BYTES}"

p = preflight("exp2", COMMITS, RPC, FPC, PAYLOAD)
exp_groups = max(1, p["bytes_total"] // GROUP_BYTES)
print(f"exp2: {p['rows_total']:,} rows, ~{p['bytes_total']/2**30:.1f} GB, "
      f"{p['files_total']} files of ~{p['bytes_per_file']/2**20:.0f} MB, group cap "
      f"{GROUP_BYTES/2**30:.0f} GB => ~{exp_groups} groups, heap {HEAP}", flush=True)

out = {"base": [], "cross": []}
failures = []


def one(tag, i, cross):
    res, wall = run_one(f"e2_{tag}_{i}", SYNTH, heap=HEAP, cross=cross, opts=OPTS)
    if res.get("error"):
        failures.append(f"exp2/{tag}/r{i}: {res['error'][:300]}")
        print(f"  r{i} {tag}: FAILED {res['error'][:200]}", flush=True)
        return None
    o, s, st = res.get("oracle") or {}, res.get("audit_summary") or {}, res["stats"]
    try:
        check_entropy(f"exp2/{tag}/r{i}", res, SYNTH)
        check_rewrote(f"exp2/{tag}/r{i}", res)
    except ControlFailure as e:
        failures.append(str(e))
        print(f"  r{i} {tag}: CONTROL FAILURE {e}", flush=True)
    if not o:
        failures.append(f"exp2/{tag}/r{i}: no oracle block -- nothing can be scored")
        return None
    fp = o["false_positives_from_duplicates"] + o["false_positives_other"]
    rec = {"scored": o.get("scored_property"), "expected": o["expected_stale_wins"],
           "captured": o["captured"], "tp": o["true_positives"], "misses": o["misses"],
           "fp": fp, "fp_from_dups": o["false_positives_from_duplicates"],
           "fp_keys": o.get("false_positive_keys", [])[:20],
           "traps": o["expected_dup_risky"], "live_ok": o["live_rows_match"],
           "expected_live": o["expected_live_rows"], "measured_live": o["measured_live_rows"],
           "per_group_tp": o.get("per_group_true_positives"),
           "per_group_fp": o.get("per_group_false_positives"),
           "groups_total": s.get("mor.audit.groups-total"),
           "groups_audited": s.get("mor.audit.groups-audited"),
           "straddle_candidates": s.get("mor.audit.straddle-candidates"),
           "compact_s": st["compact_time_s"], "wall_s": round(wall, 1)}
    if not rec["live_ok"]:
        failures.append(f"exp2/{tag}/r{i}: closed form {rec['expected_live']:,} != engine "
                        f"{rec['measured_live']:,} -- the oracle's model of suppression is wrong")
    if rec["traps"] == 0:
        failures.append(f"exp2/{tag}/r{i}: no duplicate traps set; the guard is untested here")
    if int(rec["groups_total"] or 1) < 2:
        failures.append(f"exp2/{tag}/r{i}: only {rec['groups_total']} group -- nothing straddles, so "
                        f"this cell says nothing about the multi-group case")
    print(f"  r{i} {tag}: groups {rec['groups_audited']}/{rec['groups_total']} scored={rec['scored']} "
          f"exp={rec['expected']:,} captured={rec['captured']:,} TP={rec['tp']:,} "
          f"miss={rec['misses']:,} FP={rec['fp']:,} traps={rec['traps']:,} "
          f"compact={rec['compact_s']}s", flush=True)
    if rec["fp"]:
        print(f"       FALSE POSITIVES: {rec['fp']:,}  first keys {rec['fp_keys'][:6]}", flush=True)
    return rec


print("\n" + "=" * 92 + f"\nPER-GROUP mode, {N_BASE} repeats of one configuration\n" + "=" * 92)
for i in range(N_BASE):
    r = one("base", i, cross=False)
    if r:
        out["base"].append(r)

print("\n" + "=" * 92 + f"\nCROSS-GROUP mode, {N_CROSS} repeats, scored on the merged property\n"
      + "=" * 92)
for i in range(N_CROSS):
    r = one("cross", i, cross=True)
    if r:
        out["cross"].append(r)
        if r["fp"]:
            failures.append(f"exp2/cross/r{i}: {r['fp']} false positives in CROSS-GROUP mode -- this "
                            f"contradicts the paper's soundness claim for the mode and is the single "
                            f"most important result in this run")

print("\n" + "=" * 92)
bfp = [r for r in out["base"] if r["fp"]]
print(f"PER-GROUP false positives: {len(bfp)} of {len(out['base'])} runs "
      f"({sum(r['fp'] for r in out['base']):,} keys)   "
      f"recall {min((r['tp'] for r in out['base']), default=0):,}-"
      f"{max((r['tp'] for r in out['base']), default=0):,} of "
      f"{out['base'][0]['expected']:,}" if out["base"] else "PER-GROUP: no usable runs")
if out["cross"]:
    print(f"CROSS-GROUP: false positives {sum(r['fp'] for r in out['cross'])}; recall "
          + ", ".join(f"{r['tp']:,}/{r['expected']:,}" for r in out["cross"]))
if out["base"] and not bfp:
    print("NOTE: no per-group false positive reproduced here. That does NOT overturn the local "
          "finding -- it was 1 in 6 there. Report as a rate over this many runs, nothing stronger.")

print("\nPASS" if not failures else "\nFAIL:\n  " + "\n  ".join(failures))
emit("exp2_correctness.json", {"config": {**SYNTH, "heap": HEAP, "group_bytes": GROUP_BYTES},
                               "plan": p, "host": hostinfo(), "runs": out, "failures": failures})
sys.exit(1 if failures else 0)
