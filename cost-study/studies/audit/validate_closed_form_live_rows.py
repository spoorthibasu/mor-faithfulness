#!/usr/bin/env python3
"""The closed form's surviving-row prediction, checked against the engine.

Section 6.1 claims that "the closed form predicts 1,699,998 surviving rows for one configuration and
the engine reports exactly that number". That is the check which licenses treating the construction
oracle as an oracle at all: the derivation is over generator parameters and shares no code with the
mechanism, so if it also predicts a quantity the ENGINE independently reports, the model of Iceberg's
strict-suppression rule the oracle rests on is the one the engine actually implements.

Until now only the prediction was reproducible. Recomputing `construction_oracle()` gives 1,699,998
for `commits=8, rows_per_commit=500000, delete_frac=0.2` and for no other configuration in the swept
space, but no artifact recorded a run of it, so the second half of the claim -- that the engine agrees
-- had no committed evidence. This script records both halves.

The payload width is deliberately small. `expected_live_rows` is a function of commits, rows per
commit and delete fraction only, so narrowing the rows changes the bytes on disk and not the quantity
under test, and keeps the run to a few minutes.

POSITIVE CONTROLS. A run that fails to build the table, or builds one the planner declines to rewrite,
would report no live-row count at all rather than a wrong one; both are asserted. The entropy guard is
kept because a payload that compresses would change the file sizes the planner sees, and with them
whether a rewrite happens at all.
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

WH = os.path.join(tempfile.gettempdir(), "mor_closedform")
JAR = os.environ.get("MOR_ICEBERG_JAR", os.path.expanduser(
    "~/IdeaProjects/iceberg-mor-fork/spark/v3.5/spark-runtime/build/libs/"
    "iceberg-spark-runtime-3.5_2.12-1.11.0-SNAPSHOT.jar"))
COLS = [{"name": "id", "type": "int"}, {"name": "val", "type": "string"},
        {"name": "lsn", "type": "int"}]

# The configuration §6.1's figure comes from.
SYNTH = {"commits": 8, "rows_per_commit": 500_000, "payload_bytes": 100,
         "delete_frac": 0.2, "ordering": "inverted"}
NAME = "closedform"

tdir = os.path.join(WH, "db", NAME)
shutil.rmtree(tdir, ignore_errors=True)
plan = WritePlan(checkpoints=[], key_columns=["id"], payload_columns=["val"],
                 version_column="lsn", enforcement_mode="unsafe_compact")
pj = serialize_plan(plan, NAME, tdir, WH, "lsn", COLS)
pj["synth"] = SYNTH
os.environ.update({"MOR_ICEBERG_JAR": JAR, "MOR_BULK_INGEST": "1", "MOR_AUDIT": "1",
                   "MOR_AUDIT_CROSS_GROUP": "0", "MOR_REWRITE_OPTS": "audit-cache-scan=false"})

print(f"closed-form live-row check: {SYNTH['commits']} commits x "
      f"{SYNTH['rows_per_commit']:,} rows, delete_frac {SYNTH['delete_frac']}", flush=True)
res = run_driver("iceberg_driver.py", pj, os.path.join(WH, "_io", NAME))
on_disk = 0
ddir = os.path.join(tdir, "data")
if os.path.isdir(ddir):
    on_disk = sum(os.path.getsize(os.path.join(ddir, f)) for f in os.listdir(ddir))
shutil.rmtree(tdir, ignore_errors=True)

fail = []
if res.get("error"):
    fail.append(f"driver failed: {res['error'][-800:]}")
orc = res.get("oracle") or {}
stats = res.get("stats") or {}
predicted = orc.get("expected_live_rows")
measured = stats.get("live_rows")

if predicted is None:
    fail.append("no closed-form prediction in the result; the oracle did not run")
if measured is None:
    fail.append("no live_rows from the engine; a missing count must not read as agreement")
if not (res.get("audit_summary") or {}).get("mor.audit.groups-total"):
    fail.append("no file group was planned, so no rewrite happened and the run measures nothing")
agree = (predicted is not None and measured is not None and predicted == measured)
if predicted is not None and measured is not None and not agree:
    fail.append(f"PREDICTION AND ENGINE DISAGREE: closed form {predicted:,} vs engine {measured:,}")

out = {
    "what": "the closed form's expected_live_rows against the engine's reported live_rows",
    "why": "Section 6.1 cites this agreement as what licenses treating the construction oracle "
           "as an oracle; only the prediction half was previously reproducible",
    "config": SYNTH,
    "closed_form_expected_live_rows": predicted,
    "engine_reported_live_rows": measured,
    "agree": agree,
    "expected_stale_wins": orc.get("expected_stale_wins"),
    "captured": orc.get("captured"),
    "false_positives_other": orc.get("false_positives_other"),
    "misses": orc.get("misses"),
    "bytes_on_disk": on_disk,
    "failures": fail,
}
dst = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                   "validate_closed_form_live_rows.json")
with open(dst, "w") as f:
    json.dump(out, f, indent=1)

print(f"  closed form predicts : {predicted:,}" if predicted is not None else "  closed form: n/a")
print(f"  engine reports       : {measured:,}" if measured is not None else "  engine: n/a")
print(f"  agree                : {agree}")
print(f"  stale wins expected  : {orc.get('expected_stale_wins')}  captured {orc.get('captured')}")
print(f"\nevidence -> {dst}")
print("\nPASS" if not fail else "\nFAIL:\n  " + "\n  ".join(fail))
sys.exit(1 if fail else 0)
