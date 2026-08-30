#!/usr/bin/env python3
"""Item 1: is the compaction baseline stable once the page cache is controlled?

Two changes against the run that drifted 2.1x within a session:
  (a) COLD CACHE. Before each compaction the driver streams a junk file larger than the free page
      cache, evicting the table the ingest just wrote. macOS `purge` needs root, so this is the
      userspace substitute. It runs outside the compaction timer.
  (b) A SECOND SCALE the machine cannot cache regardless: ~22 GB against 16 GB of RAM with an 8 GB
      heap, so at most a quarter of the table can ever be resident.

Three arms, round-robin interleaved, so any residual drift lands on every arm equally.
Files stay under the rewrite planner's 384 MB selection floor at both scales, so every file is
genuinely rewritten (the Entry-32 trap).
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

WH = os.path.join(tempfile.gettempdir(), "mor_cold")
JUNK = os.path.join(WH, "cache_buster.bin")
JUNK_GB = 12
COLS = [{"name": "id", "type": "int"}, {"name": "val", "type": "string"},
        {"name": "lsn", "type": "int"}]
ARMS = [("off", False, ""), ("gateON", True, ""), ("gateOFF", True, "audit-gate=false")]
# (label, commits, rows/commit, repeats) -- 900K rows/commit keeps files ~350 MB, under the floor
SCALES = [("11GB", 32, 900_000, 5), ("22GB", 64, 900_000, 3)]


def make_junk():
    os.makedirs(WH, exist_ok=True)
    if os.path.exists(JUNK) and os.path.getsize(JUNK) >= JUNK_GB * (1 << 30):
        return
    print(f"creating {JUNK_GB} GB cache-buster ...", flush=True)
    block = os.urandom(1 << 26)                      # 64 MB, incompressible
    with open(JUNK, "wb") as f:
        for _ in range(JUNK_GB * 16):
            f.write(block)
    print(f"  junk file: {os.path.getsize(JUNK)/2**30:.1f} GB", flush=True)


def one(scale, commits, rpc, label, audit, opts, i):
    name = f"cc_{scale}_{label}_{i}"
    tdir = os.path.join(WH, "db", name)
    plan = WritePlan(checkpoints=[], key_columns=["id"], payload_columns=["val"],
                     version_column="lsn", enforcement_mode="unsafe_compact")
    pj = serialize_plan(plan, name, tdir, WH, "lsn", COLS)
    pj["synth"] = {"commits": commits, "rows_per_commit": rpc, "payload_bytes": 400,
                   "delete_frac": 0.2, "ordering": "contiguous"}
    os.environ["MOR_BULK_INGEST"] = "1"
    os.environ["MOR_AUDIT"] = "1" if audit else "0"
    os.environ["MOR_AUDIT_CROSS_GROUP"] = "0"
    os.environ["MOR_REWRITE_OPTS"] = opts
    os.environ["MOR_DROP_CACHE"] = JUNK
    res = run_driver("iceberg_driver.py", pj, os.path.join(WH, "_io", name))
    s = res["stats"]
    ddir = os.path.join(tdir, "data")
    pre = sum(os.path.getsize(os.path.join(ddir, f)) for f in os.listdir(ddir)
              if f.startswith("synth") and f.endswith("data.parquet")) if os.path.isdir(ddir) else 0
    summ = res.get("audit_summary") or {}
    shutil.rmtree(tdir, ignore_errors=True)
    return {"compact_s": s["compact_time_s"], "apply_s": s["apply_time_s"],
            "evict_s": s.get("evict_s"), "pre_gb": pre / 1024 ** 3,
            "gated": summ.get("mor.audit.groups-gated")}


make_junk()
out = {}
for scale, commits, rpc, reps in SCALES:
    out[scale] = {a: [] for a, _, _ in ARMS}
    print(f"\n===== {scale}: {commits} commits x {rpc:,} rows, {reps} repeats, interleaved =====",
          flush=True)
    for i in range(reps):
        for label, audit, opts in ARMS:
            r = one(scale, commits, rpc, label, audit, opts, i)
            out[scale][label].append(r)
            print(f"  r{i} {label:8} compact={r['compact_s']:8.2f}s ingest={r['apply_s']:6.1f}s "
                  f"evict={r['evict_s']}s pre={r['pre_gb']:5.2f}GB gated={r['gated']}", flush=True)
    print(f"  --- {scale} summary ---")
    med = {}
    for label, _, _ in ARMS:
        c = [x["compact_s"] for x in out[scale][label]]
        med[label] = statistics.median(c)
        cv = statistics.stdev(c) / statistics.mean(c) if len(c) > 1 else 0
        print(f"  {label:8} median={statistics.median(c):8.2f}s  CV={cv:5.1%}  "
              f"raw={[round(x,1) for x in c]}", flush=True)
    b = med["off"]
    for label in ("gateON", "gateOFF"):
        ratios = [x["compact_s"] / y["compact_s"]
                  for x, y in zip(out[scale][label], out[scale]["off"])]
        print(f"  {label:8} vs off: median ratio {statistics.median(ratios):.2f} "
              f"(range {min(ratios):.2f}-{max(ratios):.2f})", flush=True)

dst = os.path.join(os.path.dirname(__file__), "bench_coldcache.json")
json.dump(out, open(dst, "w"), indent=1)
print(f"\nevidence -> {dst}")
print("prior warm-cache run at 11GB: baseline drifted 36.4->76.3s within one session (2.1x)")
