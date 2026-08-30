#!/usr/bin/env python3
"""Gate CLEARANCE as a function of how much a commit interleaves keys across the ordering domain.

The paper measures the gate's COST thoroughly and its CLEARANCE RATE not at all, while resting its
value proposition on "free unless you need it". Section 5.3 says selectivity is workload-dependent and
that clearance needs each commit to write "a contiguous, advancing window of ordering values". That is
a claim about workloads, and nothing in the study tests it. This does.

WHY THIS IS CHEAP. Clearance is decided from manifest metadata alone -- per-file [min, max] ordering
bounds and data sequence numbers -- and is a deterministic function of the data. No timing, so no cloud
host, no paired rounds, no repeats for noise, no cold-cache control. Small tables, run locally.

WHAT IS SWEPT. `interleave_frac`: the fraction of each commit's rows whose ordering value is drawn from
a different commit's window instead of this commit's own contiguous one. 0.0 is the shape the gate is
designed for; 1.0 scatters every row. This is a different axis from the existing `inverted` flag, which
relocates WHOLE COMMITS and so is a single extreme point rather than a curve.

WHAT THE GATE'S OWN LOGIC PREDICTS, written down before running so the measurement can contradict it.
`mayContainStaleWins` sorts a group's files by data sequence number, tracks the running maximum ordering
upper bound, and declares an inversion the moment some later file's ordering LOWER bound falls below it.
The test is on per-file interval endpoints, not on row counts -- so ONE out-of-window row anywhere in a
group is enough to widen an interval and defeat the gate for that whole group. Clearance should
therefore not decay gradually with `interleave_frac`; it should hold at 1.0 and then collapse once
groups typically contain at least one interleaved row, i.e. near frac ~ 1 / (rows per group). The sweep
is spaced logarithmically to resolve that region rather than the uninteresting flat parts either side.

If the measurement shows a gradual decay instead, the prediction above is wrong and that is the finding.

POSITIVE CONTROL, non-negotiable. A run in which no group was formed, or the gate never executed, would
report zero audited groups -- which reads identically to perfect clearance. Every cell asserts that
groups were formed, that gated + audited accounts for every group, and that the audit actually ran. The
two endpoints are additionally checked against what is already known: fully contiguous must clear
everything, and the existing `inverted` configuration clears nothing (Section 6.4).
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

WH = os.path.join(tempfile.gettempdir(), "mor_gate_sweep")
JAR = os.environ.get("MOR_ICEBERG_JAR", os.path.expanduser(
    "~/IdeaProjects/iceberg-mor-fork/spark/v3.5/spark-runtime/build/libs/"
    "iceberg-spark-runtime-3.5_2.12-1.11.0-SNAPSHOT.jar"))
COLS = [{"name": "id", "type": "int"}, {"name": "val", "type": "string"},
        {"name": "lsn", "type": "int"}]

# Layout defaults to one data file per commit. That was written for the ORIGINAL per-file gate, where
# files of one commit share a sequence number and the sort's tie order could manufacture an inversion
# unrelated to the property being swept; one file per commit removed that confound. Under the fixed
# per-sequence gate the confound cannot arise at all, so FPC>1 is now a meaningful axis rather than a
# hazard -- see MOR_SWEEP_FPC / MOR_SWEEP_SCATTER below.
COMMITS, RPC, PAYLOAD = 40, 1_500, 200          # 60,000 rows, ~12 MB, 40 data files
# Group size is forced small so the run yields MANY groups rather than one: a single group can only
# report 0% or 100% and no rate is observable. min-input-files is lowered to match, otherwise a small
# group is skipped and never counted.
GROUP_BYTES, MIN_INPUT = 1_500_000, 2
OPTS = (f"max-file-group-size-bytes={GROUP_BYTES},min-input-files={MIN_INPUT},"
        "audit-cache-scan=false")

# Logarithmic, dense through the predicted collapse region (~1/rows-per-group), with both endpoints.
FRACS = [0.0, 1e-5, 2e-5, 5e-5, 1e-4, 1.5e-4, 2e-4, 3e-4, 5e-4, 1e-3, 2e-3, 5e-3,
         1e-2, 5e-2, 0.1, 0.25, 0.5, 1.0]
# Anchors-first smoke test: MOR_SWEEP_FRACS=0,1 runs only the two endpoints, which is the cheapest
# way to find out that the rig is wired up before spending the full sweep on it.
if os.environ.get("MOR_SWEEP_FRACS"):
    FRACS = [float(x) for x in os.environ["MOR_SWEEP_FRACS"].split(",")]

# Clearance is a PROPORTION estimated from however many groups the run forms, so one pass over 10
# groups resolves it only to the nearest 10 percentage points -- far too coarse to say where the cliff
# is. Extra seeds are independent realisations of the WORKLOAD (which rows land out of window), not
# repeats against measurement noise: the quantity is deterministic given the data, and what varies
# between seeds is the data. Groups are pooled across seeds before the rate is taken.
SEEDS = [int(x) for x in os.environ.get("MOR_SWEEP_SEEDS", "1").split(",")]

# Intra-commit file layout. The default (1 file/commit, contiguous key blocks) is the configuration
# the sweep was written for, and there each sequence number maps to exactly ONE file -- so the
# per-sequence union IS that file's interval and the fixed gate reduces exactly to the old one. At
# FPC>1 with roundrobin scatter the two differ, which is the only configuration where this sweep can
# see the gate change at all, and the one a real hash-partitioned CDC sink is in.
FPC = int(os.environ.get("MOR_SWEEP_FPC", "1"))
SCATTER = os.environ.get("MOR_SWEEP_SCATTER", "block")
OUT_NAME = os.environ.get("MOR_SWEEP_OUT", "sweep_gate_selectivity.json")


class ControlFailure(RuntimeError):
    """A guard tripped: the cell measured nothing, which is different from measuring zero."""


def one(frac, seed=1):
    name = f"gs_{str(frac).replace('.','p').replace('-','m')}_{FPC}{SCATTER}"
    tdir = os.path.join(WH, "db", name)
    shutil.rmtree(tdir, ignore_errors=True)
    plan = WritePlan(checkpoints=[], key_columns=["id"], payload_columns=["val"],
                     version_column="lsn", enforcement_mode="unsafe_compact")
    pj = serialize_plan(plan, name, tdir, WH, "lsn", COLS)
    pj["synth"] = {"commits": COMMITS, "rows_per_commit": RPC, "payload_bytes": PAYLOAD,
                   "delete_frac": 0.2, "ordering": "contiguous",
                   "files_per_commit": FPC, "key_scatter": SCATTER,
                   "interleave_frac": frac, "interleave_seed": seed}
    os.environ.update({"MOR_ICEBERG_JAR": JAR, "MOR_BULK_INGEST": "1", "MOR_AUDIT": "1",
                       "MOR_AUDIT_CROSS_GROUP": "0", "MOR_REWRITE_OPTS": OPTS})
    os.environ.pop("MOR_DROP_CACHE", None)
    os.environ.pop("MOR_DROP_CACHE_MODE", None)
    res = run_driver("iceberg_driver.py", pj, os.path.join(WH, "_io", name))
    shutil.rmtree(tdir, ignore_errors=True)
    if res.get("error"):
        raise ControlFailure(f"frac={frac}: driver failed: ...{res['error'][-1200:]}")
    s = res.get("audit_summary") or {}
    total = int(s.get("mor.audit.groups-total", 0))
    gated = int(s.get("mor.audit.groups-gated", 0))
    audited = int(s.get("mor.audit.groups-audited", 0))

    # ---- positive controls ----
    if total == 0:
        raise ControlFailure(
            f"frac={frac}: no audit summary / no group formed. Nothing was gated because nothing was "
            f"planned -- this is NOT 'the gate cleared everything'")
    if gated + audited != total:
        raise ControlFailure(
            f"frac={frac}: gated({gated}) + audited({audited}) != total({total}); the counters do not "
            f"account for every group, so the rate is not trustworthy")
    if total < 3:
        raise ControlFailure(
            f"frac={frac}: only {total} group(s) formed; a clearance RATE is not resolvable and the "
            f"cell would report 0% or 100% as an artifact of group count")
    return {"frac": frac, "groups_total": total, "groups_gated": gated,
            "groups_audited": audited, "clearance": gated / total,
            "live_rows": res["stats"]["live_rows"],
            "oracle_valid": (res.get("oracle") or {}).get("oracle_valid")}


# Print the ACTUAL layout, not a hardcoded one: a header that says "1 file/commit" whatever the
# config is would make a silently-ignored MOR_SWEEP_FPC indistinguishable from a real result.
print(f"gate selectivity sweep: {COMMITS} commits x {RPC:,} rows, payload {PAYLOAD}B, "
      f"files/commit={FPC}, key_scatter={SCATTER}")
print(f"group cap {GROUP_BYTES/1e6:.1f} MB, min-input-files {MIN_INPUT}")
print(f"predicted collapse near frac ~ 1/(rows per group); measuring {len(FRACS)} points\n", flush=True)
print(f"{'frac':>10} {'groups':>7} {'gated':>6} {'audited':>8} {'clearance':>10}   "
      f"{'rows/group':>10}   {'binom':>10}")

rows, failures = [], []
for f in FRACS:
    per_seed = []
    for sd in SEEDS:
        try:
            per_seed.append(one(f, seed=sd))
        except ControlFailure as e:
            failures.append(str(e)); print(f"{f:>10} CONTROL FAILURE: {e}", flush=True)
    if not per_seed:
        continue
    tot = sum(x["groups_total"] for x in per_seed)
    gat = sum(x["groups_gated"] for x in per_seed)
    aud = sum(x["groups_audited"] for x in per_seed)
    rpg = (COMMITS * RPC) / (tot / len(per_seed))
    r = {"frac": f, "seeds": SEEDS, "groups_total": tot, "groups_gated": gat, "groups_audited": aud,
         "clearance": gat / tot, "rows_per_group": round(rpg),
         "expected_interleaved_per_group": round(f * rpg, 3),
         "per_seed_clearance": [round(x["clearance"], 3) for x in per_seed],
         "binomial_prediction": round((1.0 - f) ** rpg, 4)}
    rows.append(r)
    print(f"{f:>10} {tot:>7} {gat:>6} {aud:>8} {r['clearance']:>9.1%}   {rpg:>10,.0f}   "
          f"pred {r['binomial_prediction']:>6.1%}", flush=True)

# ---- endpoint anchors, checked against what the study already knows ----
print("\n" + "=" * 88)
anchors = []
z = next((r for r in rows if r["frac"] == 0.0), None)
o = next((r for r in rows if r["frac"] == 1.0), None)
if z and z["clearance"] < 1.0:
    anchors.append(f"ANCHOR FAILED: fully contiguous cleared only {z['clearance']:.1%}, not 100%. "
                   f"Either the gate is weaker than Section 6.4 reports or the generator is not "
                   f"producing contiguous windows; the whole sweep is suspect until this is resolved.")
if o and o["clearance"] > 0.0:
    anchors.append(f"ANCHOR FAILED: fully interleaved cleared {o['clearance']:.1%}, not 0%.")
for a in anchors:
    print("  " + a)
if z and o and not anchors:
    print(f"  anchors hold: contiguous {z['clearance']:.0%} cleared, fully interleaved "
          f"{o['clearance']:.0%} cleared")

# ---- where the cliff is ----
cliff = None
for a, b in zip(rows, rows[1:]):
    if a["clearance"] >= 0.5 > b["clearance"]:
        cliff = (a, b); break
if cliff:
    a, b = cliff
    print(f"\n  clearance crosses 50% between frac={a['frac']:g} ({a['clearance']:.1%}) and "
          f"frac={b['frac']:g} ({b['clearance']:.1%})")
    print(f"  in workload terms: between {a['expected_interleaved_per_group']:g} and "
          f"{b['expected_interleaved_per_group']:g} out-of-window rows per file group")
    print(f"  i.e. roughly one out-of-window row in every "
          f"{1/b['frac']:,.0f} to {1/a['frac']:,.0f} rows" if a["frac"] else
          f"  i.e. any out-of-window rate at or above {b['frac']:g}")
else:
    print("\n  no 50% crossing within the swept range")

dst = os.path.join(os.path.dirname(__file__), OUT_NAME)
json.dump({"config": {"commits": COMMITS, "rows_per_commit": RPC, "payload_bytes": PAYLOAD,
                      "files_per_commit": FPC, "key_scatter": SCATTER,
                      "group_cap_bytes": GROUP_BYTES,
                      "min_input_files": MIN_INPUT, "rewrite_opts": OPTS},
           "points": rows, "anchor_failures": anchors, "control_failures": failures},
          open(dst, "w"), indent=1)
print(f"\nevidence -> {dst}")
print("\nPASS" if not (failures or anchors) else "\nFAIL")
sys.exit(1 if (failures or anchors) else 0)
