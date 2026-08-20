#!/usr/bin/env python3
"""Overhead measurement in the DATA-DOMINATED regime (local, intermediate scale).

Calibration (NOTES Entry 32): with realistic small files (~207 MB, below Iceberg's 384 MB rewritable
floor) compaction costs ~2.4 s per GB of pre-compaction data on this machine. So ~12 GB gives a ~30 s
baseline -- data-dominated rather than job-launch bound (the toy cells were ~2 s, i.e. pure overhead).

This is NOT a production number (single laptop, local[2], 8 GB heap). It is a directional test of the
PRE-REGISTERED prediction (NOTES Entry 30): the fixed-cost model says absolute audit overhead stays
roughly constant as the baseline grows, so relative overhead should collapse from the +51%/+92% measured
at toy scale into single digits here.

Usage: bench_overhead_datadominated.py [repeats] [commits] [rows_per_commit]
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

WH = os.path.join(tempfile.gettempdir(), "mor_dd_bench")
REPEATS = int(sys.argv[1]) if len(sys.argv) > 1 else 4
COMMITS = int(sys.argv[2]) if len(sys.argv) > 2 else 32
RPC = int(sys.argv[3]) if len(sys.argv) > 3 else 900_000
PAYLOAD = 400
ARMS = [("off", False, False), ("base", True, False), ("cross", True, True)]
COLS = [{"name": "id", "type": "int"}, {"name": "val", "type": "string"},
        {"name": "lsn", "type": "int"}]


def one(arm, audit, cross, i):
    name = f"dd_{arm}_{i}"
    tdir = os.path.join(WH, "db", name)
    plan = WritePlan(checkpoints=[], key_columns=["id"], payload_columns=["val"],
                     version_column="lsn", enforcement_mode="unsafe_compact")
    pj = serialize_plan(plan, name, tdir, WH, "lsn", COLS)
    pj["synth"] = {"commits": COMMITS, "rows_per_commit": RPC,
                   "payload_bytes": PAYLOAD, "delete_frac": 0.2}
    os.environ["MOR_BULK_INGEST"] = "1"
    os.environ["MOR_AUDIT"] = "1" if audit else "0"
    os.environ["MOR_AUDIT_CROSS_GROUP"] = "1" if cross else "0"
    os.environ["MOR_REWRITE_OPTS"] = ""
    res = run_driver("iceberg_driver.py", pj, os.path.join(WH, "_io", name))
    s = res["stats"]
    ddir = os.path.join(tdir, "data")
    pre = 0
    if os.path.isdir(ddir):
        pre = sum(os.path.getsize(os.path.join(ddir, f)) for f in os.listdir(ddir)
                  if f.startswith("synth") and f.endswith("data.parquet"))
    summ = res.get("audit_summary") or {}
    shutil.rmtree(tdir, ignore_errors=True)
    return {"compact_s": s["compact_time_s"], "apply_s": s["apply_time_s"],
            "pre_gb": pre / 1024 ** 3, "live_rows": s["live_rows"],
            "verdict": summ.get("mor.audit.stale-wins-count"),
            "groups": summ.get("mor.audit.groups-total"),
            "gated": summ.get("mor.audit.groups-gated")}


print(f"commits={COMMITS} rows/commit={RPC:,} total={COMMITS*RPC:,} rows  repeats={REPEATS}\n")
out = {}
for arm, audit, cross in ARMS:
    runs = [one(arm, audit, cross, i) for i in range(REPEATS)]
    out[arm] = runs
    c = [r["compact_s"] for r in runs]
    a = [r["apply_s"] for r in runs]
    print(f"{arm:6} pre={runs[0]['pre_gb']:.2f}GB compact med={statistics.median(c):7.2f}s "
          f"(min {min(c):.2f} max {max(c):.2f})  ingest med={statistics.median(a):7.2f}s  "
          f"groups={runs[0]['groups']} gated={runs[0]['gated']} verdict={runs[0]['verdict']}",
          flush=True)

off = statistics.median([r["compact_s"] for r in out["off"]])
print(f"\n{'arm':6} {'compact med':>12} {'abs overhead':>13} {'rel overhead':>13}")
for arm in ("off", "base", "cross"):
    m = statistics.median([r["compact_s"] for r in out[arm]])
    print(f"{arm:6} {m:>11.2f}s {m-off:>+12.2f}s {((m/off-1)*100):>+12.1f}%")
ing = {arm: statistics.median([r["apply_s"] for r in out[arm]]) for arm in out}
spread = (max(ing.values()) / min(ing.values()) - 1) * 100
print(f"\ningest control (audit must not touch the write path): "
      + " ".join(f"{k}={v:.1f}s" for k, v in ing.items()) + f"  spread={spread:.1f}%")
dst = os.path.join(os.path.dirname(__file__), "bench_overhead_datadominated.json")
json.dump({"commits": COMMITS, "rows_per_commit": RPC, "repeats": REPEATS, "arms": out},
          open(dst, "w"), indent=1)
print(f"evidence -> {dst}")
