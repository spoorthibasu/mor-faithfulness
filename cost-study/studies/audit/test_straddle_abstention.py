#!/usr/bin/env python3
"""Fail-closed on straddling: the mechanism must abstain rather than publish a verdict it cannot justify.

The single-survivor guard is evaluated WITHIN a file group, so it is sound only while every surviving
version of a key is co-resident there. Once the rewrite forms more than one group that precondition is
not established, and per-group detection can report keys that are not violations. That is measured, not
hypothetical: 180,000 false positives in one local run, 1 in one cloud run, both on tables bin-packed
into several groups.

So the runner now abstains in that regime. No key list, no count, an explicit `undecidable` in the
snapshot summary instead. This is the same discipline the checker follows with NEEDS_CONTEXT and the
formal development follows in refusing to certify what physical state cannot establish, and it makes
one-sidedness hold unconditionally rather than subject to a precondition a reader has to remember.

Both directions are tested, because only one of them is interesting on its own:
  * groups > 1, cross-group off  -> MUST abstain. Without this the mechanism is unsound.
  * groups = 1                   -> MUST NOT abstain. An abstention that always fires is not a
                                    safeguard, it is a broken feature, and it would silently delete
                                    every result in the paper.
  * groups > 1, cross-group ON   -> MUST decide, because the merge makes the survivor count global.
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

WH = os.path.join(tempfile.gettempdir(), "mor_abstain")
JAR = os.environ.get("MOR_ICEBERG_JAR", os.path.expanduser(
    "~/IdeaProjects/iceberg-mor-fork/spark/v3.5/spark-runtime/build/libs/"
    "iceberg-spark-runtime-3.5_2.12-1.11.0-SNAPSHOT.jar"))
COLS = [{"name": "id", "type": "int"}, {"name": "val", "type": "string"},
        {"name": "lsn", "type": "int"}]
# 40 files of ~9.8 MB, ~390 MB total. The group cap below has to exceed one file (or no valid group
# forms) and leave each group at least `min-input-files` (default 5) files, or the group is skipped
# and the run reports nothing at all rather than reporting an abstention.
SYNTH = {"commits": 10, "rows_per_commit": 100_000, "payload_bytes": 400, "delete_frac": 0.2,
         "ordering": "inverted", "dup_frac": 0.05, "files_per_commit": 4}

# label, rewrite options, cross-group, what must happen
CASES = [
    ("multigroup_base", "max-file-group-size-bytes=100000000", False, "undecidable"),
    ("singlegroup_base", "", False, "decided"),
    ("multigroup_cross", "max-file-group-size-bytes=100000000", True, "decided"),
]


def run(label, opts, cross):
    name = f"ab_{label}"
    tdir = os.path.join(WH, "db", name)
    shutil.rmtree(tdir, ignore_errors=True)
    plan = WritePlan(checkpoints=[], key_columns=["id"], payload_columns=["val"],
                     version_column="lsn", enforcement_mode="unsafe_compact")
    pj = serialize_plan(plan, name, tdir, WH, "lsn", COLS)
    pj["synth"] = SYNTH
    os.environ.update({"MOR_ICEBERG_JAR": JAR, "MOR_BULK_INGEST": "1", "MOR_AUDIT": "1",
                       "MOR_AUDIT_CROSS_GROUP": "1" if cross else "0",
                       "MOR_REWRITE_OPTS": opts})
    os.environ.pop("MOR_DROP_CACHE_MODE", None)
    res = run_driver("iceberg_driver.py", pj, os.path.join(WH, "_io", name))
    shutil.rmtree(tdir, ignore_errors=True)
    return res.get("audit_summary") or {}, res.get("oracle") or {}


failures = []
out = {}
for label, opts, cross, expect in CASES:
    s, o = run(label, opts, cross)
    verdict = s.get("mor.audit.verdict")
    groups = int(s.get("mor.audit.groups-total") or 0)
    has_keys = bool(s.get("mor.audit.stale-wins-keys")) or \
        s.get("mor.audit.stale-wins-keys-spilled") == "true" or \
        bool(s.get("mor.audit.cross-group-keys"))
    out[label] = {"verdict": verdict, "groups": groups, "count": s.get("mor.audit.stale-wins-count"),
                  "reason": s.get("mor.audit.undecidable-reason"), "has_keys": has_keys,
                  "expected_stale": o.get("expected_stale_wins")}
    print(f"\n=== {label} (expect {expect}) ===")
    print(f"  groups={groups}  verdict={verdict}  count={out[label]['count']}  keys_present={has_keys}")
    if out[label]["reason"]:
        print(f"  reason: {out[label]['reason'][:150]}")

    if verdict != expect:
        failures.append(f"{label}: verdict is {verdict!r}, expected {expect!r}")

    if expect == "undecidable":
        # the abstention has to be real: no verdict may leak out alongside it
        if groups < 2:
            failures.append(f"{label}: only {groups} group, so this case did not test straddling "
                            f"at all -- the abstention would be vacuous")
        if has_keys:
            failures.append(f"{label}: ABSTAINED BUT STILL PUBLISHED KEYS -- the point of failing "
                            f"closed is that no verdict escapes")
        if out[label]["count"] is not None:
            failures.append(f"{label}: emitted a stale-wins count ({out[label]['count']}) while "
                            f"abstaining")
        if not out[label]["reason"]:
            failures.append(f"{label}: abstained without recording a reason")
    else:
        # and the safeguard must not fire when it should not, or it deletes every real result
        if not has_keys and (out[label]["expected_stale"] or 0) > 0:
            failures.append(f"{label}: decided but published no keys, while the oracle expects "
                            f"{out[label]['expected_stale']} -- the abstention is over-firing")

print("\n" + "=" * 80)
m, s1, mc = out.get("multigroup_base", {}), out.get("singlegroup_base", {}), out.get("multigroup_cross", {})
print(f"straddling, per-group : {m.get('groups')} groups -> {m.get('verdict')}")
print(f"single group          : {s1.get('groups')} group  -> {s1.get('verdict')}")
print(f"straddling, cross-group: {mc.get('groups')} groups -> {mc.get('verdict')}")
print("\nPASS" if not failures else "\nFAIL:\n  " + "\n  ".join(failures))
dst = os.path.join(os.path.dirname(os.path.abspath(__file__)), "test_straddle_abstention.json")
json.dump({"cases": out, "failures": failures}, open(dst, "w"), indent=1)
print(f"evidence -> {dst}")
sys.exit(1 if failures else 0)
