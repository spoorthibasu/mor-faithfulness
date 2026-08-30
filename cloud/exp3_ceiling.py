#!/usr/bin/env python3
"""Experiment 3 (priority 3) -- where cross-group mode runs out of heap, and how that scales.

Cross-group mode accumulates a per-key candidate map on the driver, so its memory is O(distinct keys)
and independent of table bytes. Locally, on an 8 GB heap, it survived 20M keys (397 s) and died at 50M
with `java.lang.OutOfMemoryError: Java heap space` inside `rewrite_data_files`. The pre-registered
prediction said 100M, so it was optimistic by at least 2x.

One number is not a scaling law. This sweeps keys at TWO heap sizes: 8 GB to check whether the local
ceiling reproduces on different hardware at all, and 24 GB to see whether the ceiling moves with heap
as the O(distinct keys) story predicts. If it does not move roughly proportionally, the story is wrong
and that is the finding.

An OOM here is the measurement, not an error, and is recorded as a data point. A run that exceeds the
per-point timeout is recorded as "beyond practical use at this heap" and is NOT reported as a success --
at 20M keys locally the compaction already took 6.6 minutes, so the ceiling in practice arrives before
the ceiling in memory.

The overall budget is enforced and anything skipped is named. A silent truncation would read as
"we covered the range" when we did not.
"""
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import err_excerpt, ControlFailure, check_rewrote, emit, hostinfo, preflight, run_one  # noqa: E402

BUDGET_S = float(os.environ.get("MOR_EXP3_BUDGET_S", "4500"))     # 75 min
TIMEOUT_S = float(os.environ.get("MOR_EXP3_TIMEOUT_S", "1500"))   # 25 min per point
PAYLOAD = 24          # small: distinct keys is the axis, bytes are not
COMMITS = 3

# (heap, distinct keys, files per commit) -- files/commit chosen to stay under the 384 MB floor
POINTS = [("8g", 20_000_000, 4), ("8g", 35_000_000, 6), ("8g", 50_000_000, 8),
          ("24g", 50_000_000, 8), ("24g", 100_000_000, 16)]

print(f"exp3: budget {BUDGET_S/60:.0f} min, per-point timeout {TIMEOUT_S/60:.0f} min", flush=True)
print(f"host: {hostinfo()}", flush=True)

out, failures, skipped = [], [], []
t_start = time.time()
dead_heaps = set()

for heap, keys, fpc in POINTS:
    elapsed = time.time() - t_start
    if elapsed + 120 > BUDGET_S:
        skipped.append({"heap": heap, "keys": keys, "why": "overall budget exhausted"})
        print(f"  SKIP {keys:,} keys @ {heap}: budget exhausted after {elapsed/60:.0f} min",
              flush=True)
        continue
    if heap in dead_heaps:
        skipped.append({"heap": heap, "keys": keys,
                        "why": "a smaller key count already failed at this heap"})
        print(f"  SKIP {keys:,} keys @ {heap}: already failed lower down", flush=True)
        continue
    synth = {"commits": COMMITS, "rows_per_commit": keys, "payload_bytes": PAYLOAD,
             "delete_frac": 0.2, "ordering": "inverted", "dup_frac": 0.0, "files_per_commit": fpc}
    try:
        p = preflight(f"exp3/{keys}@{heap}", COMMITS, keys, fpc, PAYLOAD)
    except ControlFailure as e:
        failures.append(str(e))
        print(f"  SKIP {keys:,} keys @ {heap}: {e}", flush=True)
        continue
    print(f"\n--- {keys:,} distinct keys, heap {heap}: {p['rows_total']:,} rows, "
          f"~{p['bytes_total']/2**30:.1f} GB, {p['files_total']} files of "
          f"{p['bytes_per_file']/2**20:.0f} MB ---", flush=True)
    res, wall = run_one(f"e3_{heap}_{keys//1000000}m", synth, heap=heap, cross=True)
    err = res.get("error", "")
    oom = "OutOfMemoryError" in err or "OutOfMemoryError" in str(res.get("traceback", ""))
    rec = {"heap": heap, "keys": keys, "wall_s": round(wall, 1), "rows": p["rows_total"],
           "gb": round(p["bytes_total"] / 2**30, 1)}
    if err:
        rec.update({"outcome": "OOM" if oom else "error", "detail": err_excerpt(err)})
        dead_heaps.add(heap)
        print(f"  {'OOM' if oom else 'ERROR'} after {wall:.0f}s -- this is the measurement: "
              f"{err[:200]}", flush=True)
    elif wall > TIMEOUT_S:
        rec.update({"outcome": "beyond-practical-use",
                    "compact_s": res["stats"]["compact_time_s"]})
        print(f"  COMPLETED but took {wall/60:.1f} min, past the {TIMEOUT_S/60:.0f} min bar -- "
              f"recorded as beyond practical use, not as a success", flush=True)
    else:
        st, s = res["stats"], (res.get("audit_summary") or {})
        try:
            check_rewrote(f"exp3/{keys}@{heap}", res)
        except ControlFailure as e:
            failures.append(str(e))
            print(f"  CONTROL FAILURE: {e}", flush=True)
        rec.update({"outcome": "ok", "compact_s": st["compact_time_s"],
                    "ingest_s": st["apply_time_s"],
                    "straddle_candidates": s.get("mor.audit.straddle-candidates"),
                    "groups_total": s.get("mor.audit.groups-total")})
        print(f"  ok: compact={rec['compact_s']}s ingest={rec['ingest_s']}s "
              f"candidates={rec['straddle_candidates']} groups={rec['groups_total']}", flush=True)
    out.append(rec)

print("\n" + "=" * 92)
for heap in ("8g", "24g"):
    ok = [r["keys"] for r in out if r["heap"] == heap and r["outcome"] == "ok"]
    bad = [r["keys"] for r in out if r["heap"] == heap and r["outcome"] in ("OOM", "error")]
    if ok and bad:
        print(f"heap {heap}: ceiling between {max(ok):,} and {min(bad):,} distinct keys")
    elif ok:
        print(f"heap {heap}: survived up to {max(ok):,} keys; ceiling NOT bracketed in this run")
    elif bad:
        print(f"heap {heap}: failed at the smallest point tried ({min(bad):,}); ceiling is BELOW the "
              f"range swept")
    else:
        print(f"heap {heap}: no usable points")
print(f"local reference: 8 GB heap, ok at 20M (397 s compaction), OOM at 50M")
if skipped:
    print(f"SKIPPED {len(skipped)} point(s), named so this is not read as full coverage:")
    for s in skipped:
        print(f"  {s['keys']:,} keys @ {s['heap']}: {s['why']}")

print("\nPASS" if not failures else "\nFAIL:\n  " + "\n  ".join(failures))
emit("exp3_ceiling.json", {"points": out, "skipped": skipped, "host": hostinfo(),
                           "budget_s": BUDGET_S, "timeout_s": TIMEOUT_S, "failures": failures})
sys.exit(1 if failures else 0)
