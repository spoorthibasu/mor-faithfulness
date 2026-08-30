#!/usr/bin/env python3
"""L1c -- one-sidedness at GB scale.

Entry 40 scoped L1--L3 as cost-only and said so plainly: "What these three cannot give: correctness at
scale", because synth mode skips materialisation and so had no oracle. The construction oracle (Entry 43)
removes that limitation -- it needs only the generator's parameters and the surviving row count, neither
of which requires materialising the table -- so the one-sided result can now be extended from sf10
(thousands of rows) to tens of millions.

Two deviations from the Entry-40 scope, both deliberate:

  * 22 GB is dropped. The cold-cache run (Entry 42) showed this machine thrashes there: 9.14x baseline
    spread with ingest itself degrading 61%, which is a RAM ceiling, not a measurement. Running it would
    produce a number, not evidence. 11 GB is the largest point that behaves.
  * `files_per_commit` is used to hold commit depth down as bytes grow, so the run measures compaction
    rather than equality-delete-set construction.

Every configuration carries injected same-sequence duplicates, so the guard is under load at every
scale rather than only in the small validation.
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

WH = os.path.join(tempfile.gettempdir(), "mor_scale")
JAR = os.environ.get("MOR_ICEBERG_JAR", os.path.expanduser(
    "~/IdeaProjects/iceberg-mor-fork/spark/v3.5/spark-runtime/build/libs/"
    "iceberg-spark-runtime-3.5_2.12-1.11.0-SNAPSHOT.jar"))
COLS = [{"name": "id", "type": "int"}, {"name": "val", "type": "string"},
        {"name": "lsn", "type": "int"}]

# label, commits, rows/commit, files/commit, payload -- files stay under the 384 MB selection floor
LADDER = [
    ("S1_1GB",  4,  900_000, 1, 400),
    ("S2_3GB",  8,  900_000, 2, 400),
    ("S3_6GB",  16, 900_000, 4, 400),
    ("S4_11GB", 32, 900_000, 4, 400),
]
DUP_FRAC = 0.05


def one(label, commits, rpc, fpc, payload):
    name = f"sc_{label}"
    tdir = os.path.join(WH, "db", name)
    shutil.rmtree(tdir, ignore_errors=True)
    plan = WritePlan(checkpoints=[], key_columns=["id"], payload_columns=["val"],
                     version_column="lsn", enforcement_mode="unsafe_compact")
    pj = serialize_plan(plan, name, tdir, WH, "lsn", COLS)
    pj["synth"] = {"commits": commits, "rows_per_commit": rpc, "payload_bytes": payload,
                   "delete_frac": 0.2, "ordering": "inverted", "dup_frac": DUP_FRAC,
                   "files_per_commit": fpc}
    os.environ["MOR_ICEBERG_JAR"] = JAR
    os.environ["MOR_BULK_INGEST"] = "1"
    os.environ["MOR_AUDIT"] = "1"
    os.environ["MOR_AUDIT_CROSS_GROUP"] = "0"
    os.environ["MOR_REWRITE_OPTS"] = ""
    os.environ.pop("MOR_DROP_CACHE", None)
    t0 = time.time()
    res = run_driver("iceberg_driver.py", pj, os.path.join(WH, "_io", name))
    wall = time.time() - t0
    ddir = os.path.join(tdir, "data")
    gb = (sum(os.path.getsize(os.path.join(ddir, f)) for f in os.listdir(ddir))
          / 1024 ** 3) if os.path.isdir(ddir) else 0.0
    shutil.rmtree(tdir, ignore_errors=True)
    return res.get("oracle") or {}, res.get("stats") or {}, res.get("audit_summary") or {}, gb, wall


out, failures = {}, []
for label, commits, rpc, fpc, payload in LADDER:
    o, st, summ, gb, wall = one(label, commits, rpc, fpc, payload)
    out[label] = {"oracle": o, "stats": st, "pre_gb": round(gb, 2), "wall_s": round(wall, 1),
                  "spill": summ.get("mor.audit.spill-source"),
                  "spilled": summ.get("mor.audit.stale-wins-keys-spilled")}
    print(f"\n=== {label}  {gb:.2f} GB on disk, {commits} commits x {fpc} files, wall {wall:.0f}s ===",
          flush=True)
    if not o:
        failures.append(f"{label}: no oracle block")
        print("  NO ORACLE BLOCK", flush=True)
        continue
    print(f"  rows written {commits*rpc:>12,}   live rows expected={o['expected_live_rows']:>10,} "
          f"measured={o['measured_live_rows']:>10,}  match={o['live_rows_match']}")
    print(f"  expected stale={o['expected_stale_wins']:>8,}  injected duplicates="
          f"{o['expected_dup_risky']:>7,}")
    print(f"  captured      ={o['captured']:>8,}  TP={o['true_positives']:>8,}  "
          f"misses={o['misses']:>7,}")
    print(f"  FALSE POSITIVES: from duplicates={o['false_positives_from_duplicates']}  "
          f"other={o['false_positives_other']}   (both must be 0)")
    print(f"  verdict spilled={out[label]['spilled']} via {out[label]['spill']}   "
          f"compact={st.get('compact_time_s')}s")

    if not o["live_rows_match"]:
        failures.append(f"{label}: closed form {o['expected_live_rows']} != engine "
                        f"{o['measured_live_rows']}")
    if o["false_positives_from_duplicates"] or o["false_positives_other"]:
        failures.append(f"{label}: ONE-SIDEDNESS BROKEN -- "
                        f"{o['false_positives_from_duplicates']} dup FP, "
                        f"{o['false_positives_other']} other FP")
    if o["expected_dup_risky"] == 0:
        failures.append(f"{label}: no duplicates injected -- guard untested at this scale")

print("\n" + "=" * 84)
tot_tp = sum(v["oracle"].get("true_positives", 0) for v in out.values() if v["oracle"])
tot_fp = sum(v["oracle"].get("false_positives_from_duplicates", 0)
             + v["oracle"].get("false_positives_other", 0) for v in out.values() if v["oracle"])
tot_miss = sum(v["oracle"].get("misses", 0) for v in out.values() if v["oracle"])
tot_dup = sum(v["oracle"].get("expected_dup_risky", 0) for v in out.values() if v["oracle"])
tot_rows = sum(c * r for _, c, r, _, _ in LADDER)
print(f"across {len(LADDER)} configurations, {tot_rows:,} rows written:")
print(f"  true positives {tot_tp:,}   false positives {tot_fp}   misses {tot_miss}   "
      f"duplicate traps set {tot_dup:,}")
print("PASS" if not failures else "FAIL:\n  " + "\n  ".join(failures))

dst = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bench_scale_correctness.json")
json.dump({"ladder": out, "failures": failures,
           "totals": {"tp": tot_tp, "fp": tot_fp, "misses": tot_miss, "dup_traps": tot_dup,
                      "rows_written": tot_rows}}, open(dst, "w"), indent=1)
print(f"evidence -> {dst}")
sys.exit(1 if failures else 0)
