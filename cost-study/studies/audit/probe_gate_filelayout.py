#!/usr/bin/env python3
"""Does the gate survive a hash-partitioned CDC sink? A threat to Section 5.3, tested rather than argued.

The selectivity sweep holds files_per_commit=1 on purpose, so that every file's ordering interval is a
disjoint, advancing slice and the only thing varying is the interleave rate. Real CDC sinks do not write
one file per commit. They hash-partition by key, so a commit's F files each hold a scattered subset of
that commit's keys -- and every one of those files then has an ordering interval spanning almost the
WHOLE commit window rather than a slice of it.

That matters because the gate sorts a group's files by data sequence number and files of the same commit
SHARE a sequence number. Their relative order under the sort is not meaningful, and if their intervals
overlap, the running-maximum test sees file 1's maximum followed by file 2's much lower minimum and
declares an inversion -- on a workload whose ordering values are perfectly, monotonically advancing.

If that happens, "each commit writes a contiguous, advancing window of ordering values" is not a
sufficient condition for clearance, and Section 5.3's appeal to real CDC does not carry.

THE CONTROL THAT MAKES THIS ATTRIBUTABLE. Both arms write byte-identical ordering VALUES: every row
still has lsn_c(k) = LSN_BASE(c) + (k-1), zero interleaving, the same keys, the same commits, the same
deletes. The construction oracle stays valid for both. The only difference is which file each key lands
in. So any difference in clearance is caused by file layout and by nothing else.
"""
import json
import os
import shutil
import sys
import tempfile

_REPO = os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__)))))
sys.path.insert(0, os.path.join(_REPO, "cost-study/src"))
from mor_harness.adapters.base import run_driver, serialize_plan  # noqa: E402
from mor_harness.model import WritePlan                            # noqa: E402

WH = os.path.join(tempfile.gettempdir(), "mor_gate_layout")
JAR = os.environ.get("MOR_ICEBERG_JAR", os.path.expanduser(
    "~/IdeaProjects/iceberg-mor-fork/spark/v3.5/spark-runtime/build/libs/"
    "iceberg-spark-runtime-3.5_2.12-1.11.0-SNAPSHOT.jar"))
COLS = [{"name": "id", "type": "int"}, {"name": "val", "type": "string"},
        {"name": "lsn", "type": "int"}]
COMMITS, RPC, PAYLOAD = 40, 1_500, 200
OPTS = "max-file-group-size-bytes=1500000,min-input-files=2,audit-cache-scan=false"

# (label, files_per_commit, key_scatter, commits)
ARMS = [
    ("1 file/commit",           1, "block",      COMMITS),
    ("4 files, contiguous",     4, "block",      COMMITS),
    ("4 files, hash-scattered", 4, "roundrobin", COMMITS),
    ("8 files, hash-scattered", 8, "roundrobin", COMMITS),
    # VACUOUS CASE, asserted rather than assumed. One commit means every data file carries data
    # sequence number 1 BY CONSTRUCTION, so any group formed from them contains exactly one distinct
    # sequence number and there is no distinct-sequence pair to compare. GateSoundness's
    # discarded_seq_lt_visible_seq says a within-group stale-win needs two distinct sequences, so
    # clearing such a group is correct and not a gap. Hash-scattering makes the file intervals
    # maximally overlapping, which is precisely what the OLD per-file test called an inversion.
    ("1 commit, 8 scattered (vacuous)", 8, "roundrobin", 1),
]
VACUOUS_ARM = "1 commit, 8 scattered (vacuous)"


def one(fpc, scatter, commits=None):
    commits = COMMITS if commits is None else commits
    name = f"fl_{fpc}_{scatter}_{commits}c"
    tdir = os.path.join(WH, "db", name)
    shutil.rmtree(tdir, ignore_errors=True)
    plan = WritePlan(checkpoints=[], key_columns=["id"], payload_columns=["val"],
                     version_column="lsn", enforcement_mode="unsafe_compact")
    pj = serialize_plan(plan, name, tdir, WH, "lsn", COLS)
    pj["synth"] = {"commits": commits, "rows_per_commit": RPC, "payload_bytes": PAYLOAD,
                   "delete_frac": 0.2, "ordering": "contiguous",
                   "files_per_commit": fpc, "key_scatter": scatter,
                   "interleave_frac": 0.0}          # ZERO interleaving in every arm
    os.environ.update({"MOR_ICEBERG_JAR": JAR, "MOR_BULK_INGEST": "1", "MOR_AUDIT": "1",
                       "MOR_AUDIT_CROSS_GROUP": "0", "MOR_REWRITE_OPTS": OPTS})
    res = run_driver("iceberg_driver.py", pj, os.path.join(WH, "_io", name))
    shutil.rmtree(tdir, ignore_errors=True)
    if res.get("error"):
        raise RuntimeError(f"{name}: ...{res['error'][-1200:]}")  # tail: the Java exception lives there
    s = res.get("audit_summary") or {}
    total = int(s.get("mor.audit.groups-total", 0))
    gated = int(s.get("mor.audit.groups-gated", 0))
    audited = int(s.get("mor.audit.groups-audited", 0))
    if total == 0:
        raise RuntimeError(f"{name}: no group formed -- zero audited here is not clearance")
    if gated + audited != total:
        raise RuntimeError(f"{name}: counters do not account for every group")
    orc = res.get("oracle") or {}
    return {"files_per_commit": fpc, "scatter": scatter, "commits": commits,
            "groups_total": total,
            "groups_gated": gated, "groups_audited": audited, "clearance": gated / total,
            "live_rows": res["stats"]["live_rows"],
            "oracle_valid": orc.get("oracle_valid"),
            "oracle_expected_live_rows": orc.get("expected_live_rows"),
            "oracle_stale_wins": len(orc.get("expected_stale_wins") or [])}


print(f"file-layout probe: {COMMITS} commits x {RPC:,} rows, contiguous ordering, ZERO interleaving")
print(f"{'arm':<26} {'groups':>7} {'gated':>6} {'audited':>8} {'clearance':>10} {'live_rows':>10}"
      f" {'oracle':>8}")
out, fail = [], []
for label, fpc, sc, ncommits in ARMS:
    try:
        r = one(fpc, sc, ncommits)
    except RuntimeError as e:
        fail.append(str(e)); print(f"{label:<26} FAILED: {e}", flush=True); continue
    r["arm"] = label
    out.append(r)
    ok = "ok" if r["live_rows"] == r["oracle_expected_live_rows"] else "MISMATCH"
    print(f"{label:<26} {r['groups_total']:>7} {r['groups_gated']:>6} {r['groups_audited']:>8} "
          f"{r['clearance']:>9.1%} {r['live_rows']:>10,} {ok:>8}", flush=True)

print("\n" + "=" * 92)
# The oracle is the control that the two layouts really are the same data.
lr = {r["live_rows"] for r in out if r["arm"] != VACUOUS_ARM}
if len(lr) == 1:
    print(f"  CONTROL HOLDS: every arm materialises the same {lr.pop():,} rows, so the arms differ in "
          f"file layout and in nothing else")
else:
    fail.append(f"live_rows differs across arms ({lr}); the arms are not the same data and no "
                f"clearance difference is attributable to layout")
    print(f"  CONTROL FAILED: live_rows differs across arms: {lr}")

base = next((r for r in out if r["files_per_commit"] == 1), None)
scat = [r for r in out if r["scatter"] == "roundrobin" and r["arm"] != VACUOUS_ARM]
if base and scat:
    print(f"\n  1 file/commit:        {base['clearance']:.0%} cleared")
    for r in scat:
        print(f"  {r['files_per_commit']} files hash-scattered: {r['clearance']:.0%} cleared")
    if all(r["clearance"] < 0.5 for r in scat) and base["clearance"] > 0.9:
        print("\n  => A HASH-PARTITIONED SINK DEFEATS THE GATE ON PERFECTLY ORDERED DATA. Contiguous,")
        print("     advancing ordering values per commit are NOT sufficient for clearance; the files")
        print("     of one commit must also carry disjoint ordering ranges. Section 5.3's condition")
        print("     is incomplete as stated.")
    elif all(r["clearance"] > 0.9 for r in scat):
        print("\n  => the gate is insensitive to intra-commit file layout; the concern does not hold")

# ---- the vacuous case, asserted ----
v = next((r for r in out if r["arm"] == VACUOUS_ARM), None)
if v is None:
    fail.append("VACUOUS-CASE ASSERTION DID NOT RUN: the single-commit arm produced no result, so "
                "the consequence was neither confirmed nor refuted")
    print("\n  VACUOUS-CASE ASSERTION DID NOT RUN")
elif v["clearance"] == 1.0:
    print(f"\n  VACUOUS-CASE ASSERTION FIRED AND HELD: {v['groups_total']} group(s) built from a "
          f"single commit -- every file at data sequence number 1, maximally overlapping ordering "
          f"intervals -- cleared {v['clearance']:.0%}. No distinct-sequence pair exists, so by "
          f"discarded_seq_lt_visible_seq no within-group stale-win can exist either.")
else:
    fail.append(f"VACUOUS-CASE ASSERTION FAILED: a single-sequence group cleared only "
                f"{v['clearance']:.0%}. Either the fix does not group by sequence number as intended, "
                f"or the arm did not build single-sequence groups. Not a soundness problem, but the "
                f"gate is rejecting a case the theorem says is always safe.")
    print(f"\n  VACUOUS-CASE ASSERTION FAILED: cleared {v['clearance']:.0%}, expected 100%")

dst = os.path.join(os.path.dirname(__file__), "probe_gate_filelayout.json")
json.dump({"config": {"commits": COMMITS, "rows_per_commit": RPC, "payload_bytes": PAYLOAD,
                      "rewrite_opts": OPTS, "interleave_frac": 0.0},
           "arms": out, "failures": fail}, open(dst, "w"), indent=1)
print(f"\nevidence -> {dst}")
print("\nPASS" if not fail else "\nFAIL")
sys.exit(1 if fail else 0)
