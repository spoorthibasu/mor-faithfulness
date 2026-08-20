#!/usr/bin/env python3
"""Construction oracle + single-survivor guard, validated positively.

Two things are being established here, and they are separate claims.

(1) THE ORACLE IS INDEPENDENT AND CORRECT. Which keys should be flagged is fixed by the generator's
    parameters before any file is written, and is computed in closed form -- no table read, no engine
    readback, no shared code with the mechanism. The derivation is checked against the engine on a
    quantity the mechanism does not touch: the surviving row count. If the closed form predicts the
    engine's live_rows exactly, the model of Iceberg's strictly-lower suppression that the oracle rests
    on is the one the engine actually implements.

(2) THE GUARD IS LOAD-BEARING, NOT VACUOUS. The mechanism reports a key only when it has exactly ONE
    survivor. A key with two survivors and a higher-ordered discarded version would otherwise be a
    false positive -- and false positives are the only failure mode that costs the mechanism its
    one-sidedness. The delete rotation alone does NOT produce that shape (the oracle reports the risky
    D-class is {C} only, i.e. single-survivor keys), so a table built from the rotation cannot test the
    guard: "no false positives" would be indistinguishable from "no trap was ever set".

    So the generator injects the FLINK-38450 shape explicitly: for a slice of keys, two rows written in
    ONE commit as two data files in a single RowDelta -- same sequence number, so the co-committed
    equality delete suppresses neither -- plus a very high ordering value in commit 1 that IS discarded.
    Those keys have two survivors and a discarded version out-ordering both.

    The trap is then shown to fire: with the guard disabled, the mechanism reports exactly those keys.
    With it enabled, it reports none of them. Exclusion is verified by intersecting the captured set
    with the duplicate set and requiring an empty intersection, not by noticing nothing went wrong.
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

WH = os.path.join(tempfile.gettempdir(), "mor_oracle")
JAR = os.environ.get("MOR_ICEBERG_JAR", os.path.expanduser(
    "~/IdeaProjects/iceberg-mor-fork/spark/v3.5/spark-runtime/build/libs/"
    "iceberg-spark-runtime-3.5_2.12-1.11.0-SNAPSHOT.jar"))
COLS = [{"name": "id", "type": "int"}, {"name": "val", "type": "string"},
        {"name": "lsn", "type": "int"}]

# Small enough that every version of a key lands in one file group, so within-group detection sees the
# whole picture and a miss cannot be blamed on straddling. Straddling is Phase 5's subject, not this
# script's; conflating them would make a guard failure look like a straddle and vice versa.
BASE = {"commits": 8, "rows_per_commit": 20_000, "payload_bytes": 200, "delete_frac": 0.2}

ARMS = [
    # label,                 ordering,      dup_frac, guard, what it establishes
    ("inverted_dup_guardON",  "inverted",   0.25,     True,
     "guard on: duplicates excluded, real violations still all found"),
    ("inverted_dup_guardOFF", "inverted",   0.25,     False,
     "guard off: the same duplicates become false positives -- the trap fires"),
    ("inverted_nodup",        "inverted",   0.0,      True,
     "no injection: pure stale-wins recall"),
    ("contiguous_dup",        "contiguous", 0.25,     True,
     "faithful ordering: nothing to report even with duplicates present"),
    ("inverted_dup_split4",   "inverted",   0.25,     True,
     "files_per_commit=4: bytes decoupled from commit depth, closed form must be unaffected"),
]
# arms whose table is built with the rows of each commit split across several data files
SPLIT = {"inverted_dup_split4": 4}


def one(label, ordering, dup_frac, guard):
    name = f"orc_{label}"
    tdir = os.path.join(WH, "db", name)
    shutil.rmtree(tdir, ignore_errors=True)
    plan = WritePlan(checkpoints=[], key_columns=["id"], payload_columns=["val"],
                     version_column="lsn", enforcement_mode="unsafe_compact")
    pj = serialize_plan(plan, name, tdir, WH, "lsn", COLS)
    pj["synth"] = dict(BASE, ordering=ordering, dup_frac=dup_frac,
                       files_per_commit=SPLIT.get(label, 1))
    os.environ["MOR_ICEBERG_JAR"] = JAR
    os.environ["MOR_BULK_INGEST"] = "1"
    os.environ["MOR_AUDIT"] = "1"
    os.environ["MOR_AUDIT_CROSS_GROUP"] = "0"
    # The gate is sound but conservative; leaving it on in the contiguous arm is the point of that arm.
    os.environ["MOR_REWRITE_OPTS"] = "" if guard else "audit-require-single-survivor=false"
    res = run_driver("iceberg_driver.py", pj, os.path.join(WH, "_io", name))
    shutil.rmtree(tdir, ignore_errors=True)
    return res.get("oracle") or {}


out, failures = {}, []
for label, ordering, dup_frac, guard, _why in ARMS:
    o = one(label, ordering, dup_frac, guard)
    out[label] = o
    print(f"\n=== {label} ===", flush=True)
    if not o:
        failures.append(f"{label}: driver returned no oracle block")
        print("  NO ORACLE BLOCK", flush=True)
        continue
    print(f"  live rows   expected={o['expected_live_rows']:>9,}  measured={o['measured_live_rows']:>9,}"
          f"  match={o['live_rows_match']}")
    print(f"  expected    stale={o['expected_stale_wins']:>6,}  dup_risky={o['expected_dup_risky']:>6,}"
          f"  riskyD={o['risky_D_values']}")
    print(f"  captured    {o['captured']:>6,}   TP={o['true_positives']:>6,}  misses={o['misses']:>6,}")
    print(f"  FP from duplicates={o['false_positives_from_duplicates']:>6,}   "
          f"FP other={o['false_positives_other']:>6,}")

    # --- claims, asserted per arm ---
    if not o["live_rows_match"]:
        failures.append(f"{label}: closed form {o['expected_live_rows']} != engine "
                        f"{o['measured_live_rows']}")
    if o["misses"]:
        failures.append(f"{label}: {o['misses']} real violations missed")
    if o["false_positives_other"]:
        failures.append(f"{label}: {o['false_positives_other']} unexplained false positives")
    if label.endswith("guardON") or label in ("inverted_nodup", "contiguous_dup",
                                             "inverted_dup_split4"):
        if o["false_positives_from_duplicates"]:
            failures.append(f"{label}: GUARD FAILED -- {o['false_positives_from_duplicates']} "
                            f"duplicate keys reported as stale wins")
    if label.endswith("guardOFF"):
        # the trap must actually fire, else the guard-on result proves nothing
        if o["expected_dup_risky"] == 0:
            failures.append(f"{label}: no duplicates were injected -- test is vacuous")
        elif o["false_positives_from_duplicates"] != o["expected_dup_risky"]:
            failures.append(f"{label}: trap did not fire as predicted "
                            f"({o['false_positives_from_duplicates']}/{o['expected_dup_risky']})")

print("\n" + "=" * 78)
g_on = out.get("inverted_dup_guardON", {})
g_off = out.get("inverted_dup_guardOFF", {})
if g_on and g_off:
    print(f"GUARD IS LOAD-BEARING: same table, same {g_on.get('expected_dup_risky')} injected "
          f"same-sequence duplicates.")
    print(f"  guard off -> {g_off.get('false_positives_from_duplicates')} false positives")
    print(f"  guard on  -> {g_on.get('false_positives_from_duplicates')} false positives, "
          f"{g_on.get('misses')} misses")
print("PASS" if not failures else "FAIL:\n  " + "\n  ".join(failures))

dst = os.path.join(os.path.dirname(os.path.abspath(__file__)), "validate_oracle_guard.json")
json.dump({"arms": out, "failures": failures}, open(dst, "w"), indent=1)
print(f"evidence -> {dst}")
sys.exit(1 if failures else 0)
