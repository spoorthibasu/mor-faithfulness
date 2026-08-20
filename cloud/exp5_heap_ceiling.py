#!/usr/bin/env python3
"""Item 2 -- does the cross-group ceiling actually scale with heap?

At an 8 GB heap the ceiling is a genuine OutOfMemoryError between 20 and 35 million distinct keys. At
24 GB it was never established: the 100M point died on Spark's default 1 GB
`spark.driver.maxResultSize` before heap became the binding constraint, and a configuration cap is not
a memory ceiling. Raising that cap turns the question back into the one worth asking.

The O(distinct keys) story predicts the heap ceiling moves roughly in proportion to the heap. If 24 GB
(3x the 8 GB heap) does NOT get roughly 3x the keys, the story is wrong -- and that matters well
beyond this table, because the same story is the stated reason for declining the combined pass.

Two rules, both learned the hard way in this study:
  * Never infer OOM from a truncated error string. The failure class is decided by searching the
    driver's own result file for the exception, and anything unrecognised is reported as "other",
    never folded into a memory ceiling.
  * A point that completes is not automatically a success. If it takes longer than the bar, it is
    recorded as beyond practical use, because a ceiling in practice arrives before a ceiling in memory.

Written after every point, so a truncated session keeps what completed.
"""
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import (ControlFailure, WAREHOUSE, check_rewrote, emit, hostinfo,  # noqa: E402
                    preflight, run_one)

HEAP = os.environ.get("MOR_EXP5_HEAP", "24g")
MAXRESULT = os.environ.get("MOR_EXP5_MAXRESULT", "16g")   # was the 1 GB default that bound first
BUDGET_S = float(os.environ.get("MOR_EXP5_BUDGET_S", "2700"))
TIMEOUT_S = float(os.environ.get("MOR_EXP5_TIMEOUT_S", "1500"))
PAYLOAD, COMMITS = 24, 3

# Start at the point that previously failed on the config cap. Escalate only if it now succeeds --
# there is no value in a higher point until the lower one is known to clear.
LADDER = [(100_000_000, 16), (140_000_000, 22), (180_000_000, 28)]


def classify(res, name):
    """Decide the failure class from the driver's own result file, not from a truncated string."""
    err = res.get("error", "") or ""
    blob = err
    rp = os.path.join(WAREHOUSE, "_io", name, "result.json")
    if os.path.exists(rp):
        try:
            blob += json.load(open(rp)).get("traceback", "")
        except Exception:
            pass
    if "OutOfMemoryError" in blob or "GC overhead limit" in blob:
        return "OOM", blob
    if "maxResultSize" in blob:
        return "maxResultSize", blob
    if not err:
        return "ok", ""
    return "other", blob


print(f"exp5: heap {HEAP}, maxResultSize {MAXRESULT}, budget {BUDGET_S/60:.0f} min", flush=True)
print(f"host: {hostinfo()}", flush=True)

out, failures, skipped = [], [], []
t0 = time.time()
for keys, fpc in LADDER:
    if time.time() - t0 + 180 > BUDGET_S:
        skipped.append({"keys": keys, "why": "budget exhausted"})
        print(f"  SKIP {keys:,}: budget exhausted", flush=True)
        continue
    synth = {"commits": COMMITS, "rows_per_commit": keys, "payload_bytes": PAYLOAD,
             "delete_frac": 0.2, "ordering": "inverted", "dup_frac": 0.0, "files_per_commit": fpc}
    try:
        p = preflight(f"exp5/{keys}", COMMITS, keys, fpc, PAYLOAD)
    except ControlFailure as e:
        failures.append(str(e)); print(f"  SKIP {keys:,}: {e}", flush=True); continue
    name = f"e5_{keys//1000000}m"
    print(f"\n--- {keys:,} distinct keys @ {HEAP}: {p['rows_total']:,} rows, "
          f"~{p['bytes_total']/2**30:.1f} GB, {p['files_total']} files ---", flush=True)
    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        f"--driver-memory {HEAP} --conf spark.driver.maxResultSize={MAXRESULT} pyspark-shell")
    res, wall = run_one(name, synth, heap=HEAP, cross=True)
    kind, blob = classify(res, name)
    rec = {"keys": keys, "heap": HEAP, "maxResultSize": MAXRESULT, "wall_s": round(wall, 1),
           "outcome": kind, "detail": blob[-400:] if blob else ""}
    if kind == "ok":
        st, s = res["stats"], (res.get("audit_summary") or {})
        try:
            check_rewrote(name, res)
        except ControlFailure as e:
            failures.append(str(e))
        rec.update({"compact_s": st["compact_time_s"], "ingest_s": st["apply_time_s"],
                    "straddle_candidates": s.get("mor.audit.straddle-candidates")})
        if wall > TIMEOUT_S:
            rec["outcome"] = "beyond-practical-use"
        print(f"  {rec['outcome']}: compact={rec['compact_s']}s candidates="
              f"{rec['straddle_candidates']}", flush=True)
    else:
        print(f"  {kind.upper()} after {wall:.0f}s -- this is the measurement", flush=True)
        print(f"    {blob.strip().splitlines()[-1][:160] if blob.strip() else ''}", flush=True)
    out.append(rec)
    emit("exp5_heap_ceiling.json", {"points": out, "skipped": skipped, "host": hostinfo(),
                                    "failures": failures})
    if rec["outcome"] != "ok":
        skipped += [{"keys": k, "why": f"a lower point already ended in {rec['outcome']}"}
                    for k, _ in LADDER if k > keys]
        break

print("\n" + "=" * 92)
ok = [r["keys"] for r in out if r["outcome"] == "ok"]
oom = [r["keys"] for r in out if r["outcome"] == "OOM"]
cap = [r["keys"] for r in out if r["outcome"] == "maxResultSize"]
oth = [r["keys"] for r in out if r["outcome"] == "other"]
print(f"heap {HEAP}, maxResultSize {MAXRESULT}:")
if ok and oom:
    print(f"  HEAP ceiling between {max(ok):,} and {min(oom):,} distinct keys")
    print(f"  8 GB heap ceiling was 20M-35M. Scaling {HEAP} / 8g = 3x predicts ~60M-105M.")
    lo, hi = max(ok), min(oom)
    print(f"  measured {lo/1e6:.0f}M-{hi/1e6:.0f}M => the O(distinct keys) story "
          f"{'HOLDS' if lo >= 50e6 else 'DOES NOT HOLD'} at this heap")
elif ok and not oom:
    print(f"  survived to {max(ok):,} keys; heap ceiling NOT bracketed within the ladder")
if cap:
    print(f"  still bound by maxResultSize at {min(cap):,} keys even at {MAXRESULT} -- raise further")
if oth:
    print(f"  UNCLASSIFIED failure at {min(oth):,} keys; not counted as a memory ceiling")
for s in skipped:
    print(f"  SKIPPED {s['keys']:,}: {s['why']}")
emit("exp5_heap_ceiling.json", {"points": out, "skipped": skipped, "host": hostinfo(),
                                "failures": failures})
print("\nPASS" if not failures else "\nFAIL:\n  " + "\n  ".join(failures))
