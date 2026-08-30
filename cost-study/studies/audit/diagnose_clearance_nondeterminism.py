#!/usr/bin/env python3
"""What is still nondeterministic after the payload was seeded?

Seeding the payload made data files byte-for-byte identical across runs (verified: all 395 files, same
sizes, same total). Clearance still moved, 58% then 62%. So the remaining variation is not in the DATA
-- it is in how the data is partitioned into rewrite file groups, because clearance is a rate over
groups and identical files can still be bin-packed differently.

The hypothesis under test: Iceberg plans a rewrite by reading manifests through a shared worker pool
whose size defaults to the machine's processor count, so the ORDER in which planned files reach the
bin-packer can vary between runs. Group boundaries move, group composition changes, and the fraction
of groups containing at least one out-of-window row changes with it.

ARMS
  default        whatever the worker pool does by default
  single-thread  iceberg.worker.num-threads=1

Each arm runs the SAME configuration and the SAME interleave seed several times. The measurement is
whether the gated count is constant WITHIN an arm. If the single-thread arm is constant and the
default arm is not, planning order is the cause and pinning the pool is the fix. If neither is
constant, something else is responsible and this says so rather than guessing.

Nothing here is a timing measurement, so thread count is free to change.
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

WH = os.path.join(tempfile.gettempdir(), "mor_nondet")
JAR = os.environ.get("MOR_ICEBERG_JAR", os.path.expanduser(
    "~/IdeaProjects/iceberg-mor-fork/spark/v3.5/spark-runtime/build/libs/"
    "iceberg-spark-runtime-3.5_2.12-1.11.0-SNAPSHOT.jar"))
COLS = [{"name": "id", "type": "int"}, {"name": "val", "type": "string"},
        {"name": "lsn", "type": "int"}]
COMMITS, RPC, PAYLOAD, FRAC, SEED = 40, 1_500, 200, 1e-4, 1
REPEATS = int(os.environ.get("MOR_NONDET_REPEATS", "4"))
OPTS = "max-file-group-size-bytes=1500000,min-input-files=2,audit-cache-scan=false"

# NB: the tag becomes part of an SQL identifier, so no hyphens -- a hyphenated name makes every run
# of that arm fail on DROP TABLE, which then reads as "the arm was unstable" rather than "the arm did
# not run".
ARMS = [("default", None), ("singlethread", "1"), ("serialspark", "1")]


def one(tag, i, workers):
    name = f"nd_{tag}_{i}"
    tdir = os.path.join(WH, "db", name)
    shutil.rmtree(tdir, ignore_errors=True)
    plan = WritePlan(checkpoints=[], key_columns=["id"], payload_columns=["val"],
                     version_column="lsn", enforcement_mode="unsafe_compact")
    pj = serialize_plan(plan, name, tdir, WH, "lsn", COLS)
    pj["synth"] = {"commits": COMMITS, "rows_per_commit": RPC, "payload_bytes": PAYLOAD,
                   "delete_frac": 0.2, "ordering": "contiguous",
                   "interleave_frac": FRAC, "interleave_seed": SEED}
    os.environ.update({"MOR_ICEBERG_JAR": JAR, "MOR_BULK_INGEST": "1", "MOR_AUDIT": "1",
                       "MOR_AUDIT_CROSS_GROUP": "0", "MOR_REWRITE_OPTS": OPTS})
    # the third arm additionally serialises Spark itself: if planning is distributed, task completion
    # order varies with the number of executor threads and no JVM-side pool setting can fix it
    if tag == "serialspark":
        os.environ["MOR_SPARK_MASTER"] = "local[1]"
    else:
        os.environ.pop("MOR_SPARK_MASTER", None)
    if workers:
        os.environ["PYSPARK_SUBMIT_ARGS"] = (
            f"--driver-java-options -Diceberg.worker.num-threads={workers} pyspark-shell")
    else:
        os.environ.pop("PYSPARK_SUBMIT_ARGS", None)
    res = run_driver("iceberg_driver.py", pj, os.path.join(WH, "_io", name))
    shutil.rmtree(tdir, ignore_errors=True)
    if res.get("error"):
        raise RuntimeError(f"{name}: ...{res['error'][-1200:]}")
    s = res.get("audit_summary") or {}
    total = int(s.get("mor.audit.groups-total", 0))
    gated = int(s.get("mor.audit.groups-gated", 0))
    audited = int(s.get("mor.audit.groups-audited", 0))
    if total == 0:
        raise RuntimeError(f"{name}: no group formed")
    if gated + audited != total:
        raise RuntimeError(f"{name}: counters do not account for every group")
    return {"groups": total, "gated": gated, "audited": audited}


print(f"clearance nondeterminism: one seed ({SEED}), frac={FRAC:g}, {REPEATS} repeats per arm")
print(f"{'arm':<15} {'run':>4} {'groups':>7} {'gated':>6} {'audited':>8}")
out, fail = {}, []
for tag, workers in ARMS:
    out[tag] = []
    for i in range(REPEATS):
        try:
            r = one(tag, i, workers)
        except RuntimeError as e:
            fail.append(f"{tag}/{i}: {e}"); print(f"{tag:<15} {i:>4} FAILED: {e}", flush=True); continue
        out[tag].append(r)
        print(f"{tag:<15} {i:>4} {r['groups']:>7} {r['gated']:>6} {r['audited']:>8}", flush=True)

print("\n" + "=" * 80)
verdict = {}
for tag, _ in ARMS:
    runs = out.get(tag, [])
    if not runs:
        continue
    g = {r["gated"] for r in runs}
    t = {r["groups"] for r in runs}
    verdict[tag] = {"gated_values": sorted(g), "groups_values": sorted(t),
                    "stable": len(g) == 1 and len(t) == 1}
    print(f"  {tag:<15} groups {sorted(t)}  gated {sorted(g)}  -> "
          f"{'STABLE' if verdict[tag]['stable'] else 'VARIES'}")

d, s1 = verdict.get("default", {}), verdict.get("singlethread", {})
print()
missing = [t for t, _ in ARMS if not out.get(t)]
if missing:
    print(f"  => ARMS THAT DID NOT RUN: {missing}. No conclusion is drawn; an empty arm is not a")
    print("     stable one, and reading it as one is how a broken arm becomes a finding.")
elif d.get("stable") and s1.get("stable"):
    print("  => neither arm varies here. The variation seen earlier was across DIFFERENT seeds or")
    print("     is intermittent; this configuration did not reproduce it, so the cause is not")
    print("     established and the sweep's residual noise must still be reported as unexplained.")
elif not d.get("stable") and s1.get("stable"):
    print("  => PLANNING ORDER IS THE CAUSE. The default worker pool makes the order in which planned")
    print("     files reach the bin-packer vary, so group composition varies. Pinning")
    print("     iceberg.worker.num-threads=1 makes clearance reproducible.")
elif verdict.get("serialspark", {}).get("stable"):
    print("  => SPARK-SIDE PLANNING ORDER IS THE CAUSE. Pinning Iceberg's JVM worker pool is not")
    print("     enough; serialising Spark (local[1]) makes clearance reproducible, which means the")
    print("     order files reach the bin-packer is set by Spark task completion.")
elif not s1.get("stable"):
    print("  => PINNING THE POOL IS NOT SUFFICIENT: clearance still varies single-threaded, so the")
    print("     remaining nondeterminism is elsewhere and is NOT the manifest worker pool.")

dst = os.path.join(os.path.dirname(__file__), "diagnose_clearance_nondeterminism.json")
json.dump({"config": {"commits": COMMITS, "rows_per_commit": RPC, "interleave_frac": FRAC,
                      "seed": SEED, "repeats": REPEATS},
           "arms": out, "verdict": verdict, "failures": fail}, open(dst, "w"), indent=1)
print(f"\nevidence -> {dst}")
