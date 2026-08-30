#!/usr/bin/env python3
"""The gate at data-dominated scale, on commit-contiguous ordering.

The gate is now the mechanism's load-bearing component (Entry 33: capture costs +51%, the gate makes it
conditional), but every selectivity result so far was toy scale. This runs the SAME ~11 GB configuration
as the intermediate overhead run (32 commits x 900K rows, files ~207 MB, below the 384 MB floor) with
commit-contiguous ordering, and reports, per the methodological rule, BOTH:
  - gate ON  : does it skip, and what does the audit cost against flag-off?
  - gate OFF : the capture cost on the same data, so "free" is never reported without its counterfactual.
An INVERTED-ordering arm is included as the contrast: there the gate cannot rule the group out.
"""
import json
import os
import shutil
import statistics
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "src"))
from mor_harness.adapters.base import run_driver, serialize_plan  # noqa: E402
from mor_harness.model import WritePlan                            # noqa: E402

WH = os.path.join(tempfile.gettempdir(), "mor_gate_dd")
REPEATS = int(sys.argv[1]) if len(sys.argv) > 1 else 3
COMMITS, RPC, PAYLOAD = 32, 900_000, 400
COLS = [{"name": "id", "type": "int"}, {"name": "val", "type": "string"},
        {"name": "lsn", "type": "int"}]
# (label, audit?, extra rewrite opts, ordering)
ARMS = [
    ("off/contig",      False, "",                 "contiguous"),
    ("gateON/contig",   True,  "",                 "contiguous"),
    ("gateOFF/contig",  True,  "audit-gate=false", "contiguous"),
    ("off/inverted",    False, "",                 "inverted"),
    ("gateON/inverted", True,  "",                 "inverted"),
]


def one(label, audit, opts, ordering, i):
    name = f"g_{label.replace('/','_')}_{i}"
    tdir = os.path.join(WH, "db", name)
    plan = WritePlan(checkpoints=[], key_columns=["id"], payload_columns=["val"],
                     version_column="lsn", enforcement_mode="unsafe_compact")
    pj = serialize_plan(plan, name, tdir, WH, "lsn", COLS)
    pj["synth"] = {"commits": COMMITS, "rows_per_commit": RPC, "payload_bytes": PAYLOAD,
                   "delete_frac": 0.2, "ordering": ordering}
    os.environ["MOR_BULK_INGEST"] = "1"
    os.environ["MOR_AUDIT"] = "1" if audit else "0"
    os.environ["MOR_AUDIT_CROSS_GROUP"] = "0"
    os.environ["MOR_REWRITE_OPTS"] = opts
    res = run_driver("iceberg_driver.py", pj, os.path.join(WH, "_io", name))
    s = res["stats"]
    ddir = os.path.join(tdir, "data")
    pre = sum(os.path.getsize(os.path.join(ddir, f)) for f in os.listdir(ddir)
              if f.startswith("synth") and f.endswith("data.parquet")) if os.path.isdir(ddir) else 0
    summ = res.get("audit_summary") or {}
    shutil.rmtree(tdir, ignore_errors=True)
    return {"compact_s": s["compact_time_s"], "apply_s": s["apply_time_s"], "pre_gb": pre / 1024 ** 3,
            "rows": s["live_rows"], "groups": summ.get("mor.audit.groups-total"),
            "gated": summ.get("mor.audit.groups-gated"),
            "audited": summ.get("mor.audit.groups-audited"),
            "verdict": summ.get("mor.audit.stale-wins-count")}


out = {}
print(f"{COMMITS} commits x {RPC:,} rows, payload {PAYLOAD}B, {REPEATS} repeats/arm\n")
for label, audit, opts, ordering in ARMS:
    runs = [one(label, audit, opts, ordering, i) for i in range(REPEATS)]
    out[label] = runs
    c = [r["compact_s"] for r in runs]
    r0 = runs[0]
    print(f"{label:16} pre={r0['pre_gb']:5.2f}GB compact med={statistics.median(c):7.2f}s "
          f"ingest med={statistics.median([r['apply_s'] for r in runs]):6.1f}s "
          f"groups={r0['groups']} gated={r0['gated']} audited={r0['audited']} verdict={r0['verdict']}",
          flush=True)

print()
for base_arm, arms in (("off/contig", ["gateON/contig", "gateOFF/contig"]),
                       ("off/inverted", ["gateON/inverted"])):
    b = statistics.median([r["compact_s"] for r in out[base_arm]])
    for a in arms:
        m = statistics.median([r["compact_s"] for r in out[a]])
        print(f"  {a:16} vs {base_arm:14}: {m-b:+7.2f}s  {((m/b-1)*100):+7.1f}%")
ing = {k: statistics.median([r["apply_s"] for r in v]) for k, v in out.items()}
print(f"\ningest control spread: {(max(ing.values())/min(ing.values())-1)*100:.1f}% "
      f"({ {k: round(v,1) for k,v in ing.items()} })")
dst = os.path.join(os.path.dirname(__file__), "bench_gate_contiguous.json")
json.dump(out, open(dst, "w"), indent=1)
print(f"evidence -> {dst}")
