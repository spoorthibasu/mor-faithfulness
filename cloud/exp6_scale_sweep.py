#!/usr/bin/env python3
"""Item 3 -- is the capture overhead scale-invariant?

The pre-registration was a claim about how overhead behaves as the baseline grows: it predicted a
fixed stage whose relative cost falls away. That was falsified at one scale. One point cannot say
whether the replacement figure is a constant, a trend, or an artifact of the size we happened to pick,
and a reviewer is entitled to ask. Three or four points on a curve is a much stronger claim than one
number, in either direction -- if the ratio drifts with scale, that is the finding and the paper says
so rather than quoting 1.91x as a constant.

DESIGN NOTE, and it is the part that makes the comparison mean anything. Bytes are scaled by widening
each commit, NOT by adding commits. Commit depth is held at 32 throughout, because the first data file
of a rewrite must load every later commit's equality deletes: adding commits inflates the baseline
with delete-set construction, which flatters the audited ratio for a reason that has nothing to do
with the mechanism. Rows per file is pinned at 900K (~335 MB) at every scale, so every cell sits in the
same compaction regime and below the planner's 384 MB selection floor. What does vary with bytes is
the distinct key count, and that is the honest confound: a bigger table has more keys, and the
aggregation's shuffle grows with them. It is reported alongside so the reader can see it.

Written after every round of every scale, so a truncated session keeps whole cells.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import (ControlFailure, check_entropy, check_rewrote, cv, emit, hostinfo,  # noqa: E402
                    median, preflight, run_one, spread)

HEAP = os.environ.get("MOR_EXP6_HEAP", "32g")
ROUNDS = int(os.environ.get("MOR_EXP6_ROUNDS", "3"))
COMMITS, PAYLOAD = 32, 400
CAPTURE = "audit-gate=false,audit-cache-scan=false"

# label, rows/commit, files/commit -> rows per file is 900K in every row of this table
SCALES = [
    ("14GB",  900_000,  1),
    ("28GB",  1_800_000, 2),
    ("114GB", 7_200_000, 8),
]
# 57 GB (3.6M x 4) is omitted: it is the configuration already measured at five rounds, and its
# paired median of 1.91x is carried into the curve rather than re-spent.

print(f"exp6: commit depth fixed at {COMMITS}, 900K rows/file at every scale, heap {HEAP}",
      flush=True)
print(f"host: {hostinfo()}", flush=True)

out, failures = {}, []


def save():
    emit("exp6_scale_sweep.json", {"scales": out, "host": hostinfo(), "failures": failures,
                                   "note": "57GB point carried from exp1 (5 rounds, 1.91x)",
                                   "config": {"commits": COMMITS, "heap": HEAP, "rounds": ROUNDS}})


for label, rpc, fpc in SCALES:
    try:
        p = preflight(f"exp6/{label}", COMMITS, rpc, fpc, PAYLOAD)
    except ControlFailure as e:
        failures.append(str(e)); print(f"SKIP {label}: {e}", flush=True); save(); continue
    synth = {"commits": COMMITS, "rows_per_commit": rpc, "payload_bytes": PAYLOAD,
             "delete_frac": 0.2, "ordering": "contiguous", "files_per_commit": fpc}
    print(f"\n===== {label}: {p['rows_total']:,} rows, ~{p['bytes_total']/2**30:.1f} GB, "
          f"{p['files_total']} files of {p['bytes_per_file']/2**20:.0f} MB, "
          f"{rpc:,} distinct keys =====", flush=True)
    cell = {"off": [], "audited": [], "plan": p, "distinct_keys": rpc}
    out[label] = cell
    for i in range(ROUNDS):
        for arm, audit, opts in (("off", False, ""), ("audited", True, CAPTURE)):
            os.environ.pop("PYSPARK_SUBMIT_ARGS", None)   # plain run; event log not needed here
            res, wall = run_one(f"e6_{label}_{arm}_{i}", synth, heap=HEAP, audit=audit, opts=opts)
            if res.get("error"):
                failures.append(f"exp6/{label}/{arm}/r{i}: {res['error'][:250]}")
                print(f"  r{i} {arm:8} FAILED: {res['error'][:150]}", flush=True)
                cell[arm].append({"error": res["error"][:1200]}); save(); continue
            st = res["stats"]
            try:
                check_entropy(f"exp6/{label}/{arm}/r{i}", res, synth)
                if audit:
                    check_rewrote(f"exp6/{label}/{arm}/r{i}", res)
            except ControlFailure as e:
                failures.append(str(e))
            rec = {"compact_s": st["compact_time_s"], "ingest_s": st["apply_time_s"],
                   "on_disk_gb": round(res["_on_disk_bytes"] / 2**30, 2), "wall_s": round(wall, 1)}
            cell[arm].append(rec)
            print(f"  r{i} {arm:8} compact={rec['compact_s']:8.2f}s ingest={rec['ingest_s']:7.1f}s "
                  f"disk={rec['on_disk_gb']:6.1f}GB", flush=True)
            save()
    b = [r["compact_s"] for r in cell["off"] if r.get("compact_s")]
    ing = [r["ingest_s"] for a in ("off", "audited") for r in cell[a] if r.get("ingest_s")]
    pairs = [(x["compact_s"], y["compact_s"]) for x, y in zip(cell["audited"], cell["off"])
             if x.get("compact_s") and y.get("compact_s")]
    cell["ratios"] = [round(a / b_, 3) for a, b_ in pairs]
    cell["baseline_median_s"] = round(median(b), 1) if b else None
    cell["ingest_spread"] = round(spread(ing), 3) if ing else None
    if ing and spread(ing) > 1.10:
        failures.append(f"exp6/{label}: ingest varies {spread(ing):.2f}x within the cell; the "
                        f"control did not hold, so this cell's ratio is not attributable")
    if b and spread(b) > 1.5:
        failures.append(f"exp6/{label}: baseline spread {spread(b):.2f}x -- the machine, not the "
                        f"mechanism, is being measured at this scale")
    print(f"  -- {label}: baseline median {cell['baseline_median_s']}s (spread "
          f"{spread(b):.2f}x, CV {cv(b):.1%}), ingest spread {cell['ingest_spread']}x, "
          f"paired ratios {cell['ratios']}", flush=True)
    save()

print("\n" + "=" * 96)
print(f"{'scale':>7} {'disk_GB':>8} {'keys':>11} {'baseline_s':>11} {'ratio':>7} {'ingest':>8}")
rows = []
for label, _, _ in SCALES:
    c = out.get(label) or {}
    if c.get("ratios"):
        m = median(c["ratios"])
        rows.append((c["plan"]["bytes_total"] / 2**30, m))
        print(f"{label:>7} {c['off'][0].get('on_disk_gb', 0):>8} {c['distinct_keys']:>11,} "
              f"{c['baseline_median_s']:>11} {m:>7.2f} {c['ingest_spread']:>8}")
print(f"{'57GB':>7} {'53.1':>8} {3_600_000:>11,} {'137.2':>11} {1.91:>7.2f} {'1.005':>8}   "
      f"(from the five-round experiment, not re-run)")
rows.append((53.1, 1.91))
if len(rows) >= 3:
    rs = [r for _, r in sorted(rows)]
    print(f"\nratio across {len(rs)} scales: {min(rs):.2f} to {max(rs):.2f} "
          f"(spread {max(rs)/min(rs):.2f}x)")
    if max(rs) / min(rs) < 1.15:
        print("  => SCALE-INVARIANT within the range measured. The overhead is a property of the")
        print("     mechanism, not of the size chosen, and a single figure is defensible.")
    else:
        print("  => THE RATIO DRIFTS WITH SCALE. A single figure is NOT defensible and the paper")
        print("     must report the curve. Direction of drift:",
              "rising with size" if rs[-1] > rs[0] else "falling with size")
save()
print("\nPASS" if not failures else "\nFAIL:\n  " + "\n  ".join(failures))
