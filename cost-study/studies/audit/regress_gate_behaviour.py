#!/usr/bin/env python3
"""Regression: did the per-sequence gate fix break the gate results the paper already reports?

The fix replaced the gate's per-FILE inversion test with a per-SEQUENCE one (union the ordering bounds
of all files sharing a data sequence number, compare only across distinct sequences). That is licensed
by `discarded_seq_lt_visible_seq` in `lean/MorFaithful/GateSoundness.lean`. The risk it introduces is
not unsoundness -- the argument covers that -- but OVER-clearing: a gate that now skips groups it
should audit would show up as lost recall, and lost recall is the failure mode that costs the
mechanism its one-sidedness.

So this checks the two behaviours the paper reports, and checks them against the construction oracle
rather than against remembered numbers:

  clean contiguous   the gate SKIPS      -> gated == groups, audited == 0, verdict == 0
  inverted ordering  the gate AUDITS     -> gated == 0, audited == groups,
                                            and the captured key set EQUALS the oracle's expected set

The second is the one that matters. "audited == groups" alone would be satisfied by a gate that audits
and then finds nothing; requiring the captured set to equal the oracle's expected stale-wins set makes
this a recall test, not just a routing test.

SCALE. The paper's figures come from an ~11 GB run (32 commits x 900K rows) whose purpose was TIMING.
Nothing here is a timing claim, and gate routing plus capture recall are properties of the ordering
configuration rather than of the byte count, so this runs the same configurations small. The numbers
reported here are therefore behavioural, and are NOT a re-measurement of the paper's 11 GB timings.

REPEATS. The paper says "in every repeat", so each arm runs several times and every repeat must agree;
a single passing run would not establish the claim as stated.
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

WH = os.path.join(tempfile.gettempdir(), "mor_gate_regress")
JAR = os.environ.get("MOR_ICEBERG_JAR", os.path.expanduser(
    "~/IdeaProjects/iceberg-mor-fork/spark/v3.5/spark-runtime/build/libs/"
    "iceberg-spark-runtime-3.5_2.12-1.11.0-SNAPSHOT.jar"))
COLS = [{"name": "id", "type": "int"}, {"name": "val", "type": "string"},
        {"name": "lsn", "type": "int"}]
REPEATS = int(os.environ.get("MOR_REGRESS_REPEATS", "3"))
COMMITS, RPC, PAYLOAD = 8, 20_000, 200

# Single group, as in the configuration the paper reports: every version of a key is co-resident, so a
# miss cannot be blamed on straddling.
OPTS = "audit-cache-scan=false"


def one(ordering, i):
    name = f"rg_{ordering}_{i}"
    tdir = os.path.join(WH, "db", name)
    shutil.rmtree(tdir, ignore_errors=True)
    plan = WritePlan(checkpoints=[], key_columns=["id"], payload_columns=["val"],
                     version_column="lsn", enforcement_mode="unsafe_compact")
    pj = serialize_plan(plan, name, tdir, WH, "lsn", COLS)
    pj["synth"] = {"commits": COMMITS, "rows_per_commit": RPC, "payload_bytes": PAYLOAD,
                   "delete_frac": 0.2, "ordering": ordering}
    os.environ.update({"MOR_ICEBERG_JAR": JAR, "MOR_BULK_INGEST": "1", "MOR_AUDIT": "1",
                       "MOR_AUDIT_CROSS_GROUP": "0", "MOR_REWRITE_OPTS": OPTS})
    res = run_driver("iceberg_driver.py", pj, os.path.join(WH, "_io", name))
    shutil.rmtree(tdir, ignore_errors=True)
    if res.get("error"):
        raise RuntimeError(f"{name}: ...{res['error'][-1200:]}")  # tail: the Java exception lives there
    s = res.get("audit_summary") or {}
    total = int(s.get("mor.audit.groups-total", 0))
    if total == 0:
        raise RuntimeError(f"{name}: no group formed; nothing was gated because nothing was planned")
    gated = int(s.get("mor.audit.groups-gated", 0))
    audited = int(s.get("mor.audit.groups-audited", 0))
    if gated + audited != total:
        raise RuntimeError(f"{name}: counters do not account for every group")
    o = res.get("oracle") or {}
    return {"ordering": ordering, "repeat": i, "groups": total, "gated": gated, "audited": audited,
            "verdict_count": s.get("mor.audit.stale-wins-count"),
            "captured": o.get("captured"), "expected": o.get("expected_stale_wins"),
            # two false-positive families are tracked separately by the scorer; a guard failure shows
            # up as `false_positives_from_duplicates` and must be 0 as well
            "fp": (None if o.get("false_positives_other") is None else
                   o["false_positives_other"] + o.get("false_positives_from_duplicates", 0)),
            "miss": o.get("misses"),
            "oracle": {k: v for k, v in o.items() if k in
                       ("captured", "expected_stale_wins", "false_positives_other",
                        "false_positives_from_duplicates", "misses", "true_positives",
                        "live_rows_match", "scored_property", "scored")},
            "live_rows": res["stats"]["live_rows"],
            "oracle_expected_live_rows": o.get("expected_live_rows"),
            "live_rows_match": o.get("live_rows_match")}


print(f"gate behaviour regression, per-sequence gate: {COMMITS} commits x {RPC:,} rows, "
      f"{REPEATS} repeats/arm")
print(f"{'arm':<12} {'rep':>4} {'groups':>7} {'gated':>6} {'audited':>8} {'verdict':>9} "
      f"{'fp':>5} {'miss':>6}")

out, fail = {}, []
for ordering in ("contiguous", "inverted"):
    out[ordering] = []
    for i in range(REPEATS):
        try:
            r = one(ordering, i)
        except RuntimeError as e:
            fail.append(str(e)); print(f"{ordering:<12} {i:>4} FAILED: {e}", flush=True); continue
        out[ordering].append(r)
        print(f"{ordering:<12} {i:>4} {r['groups']:>7} {r['gated']:>6} {r['audited']:>8} "
              f"{str(r['verdict_count']):>9} {str(r['fp']):>5} {str(r['miss']):>6}", flush=True)

print("\n" + "=" * 92)

# ---- clean-table case: the gate must SKIP, in every repeat ----
c = out.get("contiguous", [])
if not c:
    fail.append("contiguous arm produced no runs")
else:
    bad = [r for r in c if not (r["gated"] == r["groups"] and r["audited"] == 0)]
    if bad:
        fail.append(f"CLEAN-TABLE CASE BROKEN: {len(bad)}/{len(c)} repeats did not skip "
                    f"(gated,audited,groups) = {[(r['gated'], r['audited'], r['groups']) for r in bad]}")
    v = {str(r["verdict_count"]) for r in c}
    if v - {"0", "None"}:
        fail.append(f"clean contiguous reported a non-zero verdict {v}; the gate skipped but "
                    f"something was still captured")
    if not bad:
        print(f"  clean contiguous: gate SKIPS in {len(c)}/{len(c)} repeats "
              f"(gated={c[0]['gated']}, audited={c[0]['audited']}, verdict={c[0]['verdict_count']})")

# ---- inverted case: the gate must AUDIT, and capture exactly what the oracle expects ----
inv = out.get("inverted", [])
if not inv:
    fail.append("inverted arm produced no runs")
else:
    bad = [r for r in inv if not (r["gated"] == 0 and r["audited"] == r["groups"])]
    if bad:
        fail.append(f"INVERTED CASE BROKEN: {len(bad)}/{len(inv)} repeats did not audit "
                    f"(gated,audited,groups) = {[(r['gated'], r['audited'], r['groups']) for r in bad]}")
    else:
        print(f"  inverted ordering: gate AUDITS in {len(inv)}/{len(inv)} repeats "
              f"(gated=0, audited={inv[0]['audited']})")
    # recall, against the oracle -- routing alone is not enough
    for r in inv:
        if r["fp"] is None or r["miss"] is None:
            fail.append(f"inverted repeat {r['repeat']}: oracle did not score this run "
                        f"({r['oracle']}), so recall is unverified and 'audited' proves nothing")
        elif r["fp"] != 0 or r["miss"] != 0:
            fail.append(f"RECALL REGRESSION, inverted repeat {r['repeat']}: fp={r['fp']} "
                        f"miss={r['miss']} (captured {r['captured']} of {r['expected']})")
    ok = [r for r in inv if r["fp"] == 0 and r["miss"] == 0]
    if len(ok) == len(inv) and inv:
        print(f"  capture recall: {len(ok)}/{len(inv)} repeats exact against the construction oracle "
              f"({inv[0]['captured']} captured, {inv[0]['expected']} expected, 0 FP, 0 miss)")

# ---- the arms must be the tables the oracle thinks they are ----
for arm, runs in out.items():
    for r in runs:
        if r["oracle_expected_live_rows"] is not None and \
                r["live_rows"] != r["oracle_expected_live_rows"]:
            fail.append(f"{arm} repeat {r['repeat']}: live_rows {r['live_rows']:,} != oracle "
                        f"{r['oracle_expected_live_rows']:,}; the table is not what the oracle models")

dst = os.path.join(os.path.dirname(__file__), "regress_gate_behaviour.json")
json.dump({"config": {"commits": COMMITS, "rows_per_commit": RPC, "payload_bytes": PAYLOAD,
                      "repeats": REPEATS, "gate": "per-sequence union intervals"},
           "arms": out, "failures": fail}, open(dst, "w"), indent=1)
print(f"\nevidence -> {dst}")
print("\nPASS" if not fail else "\nFAIL:\n  " + "\n  ".join(fail))
sys.exit(1 if fail else 0)
