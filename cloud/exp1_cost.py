#!/usr/bin/env python3
"""Experiment 1 (priority 1) -- cost, at a scale where the machine is not the subject.

Locally this measurement kept measuring the laptop. Warm, the baseline drifted 2.1x within one
session (page cache). Cold, it settled to a 1.44x spread at 11 GB but blew up to 9.14x at 22 GB with
ingest itself degrading 61% -- a RAM ceiling, not a cost. Here the table is ~42 GB against 128 GB of
RAM with a 32 GB heap, so ~54 GB stays free for page cache and the OS, and the page cache is dropped
exactly through the kernel rather than approximately by memory pressure.

Three arms, round-robin interleaved so residual drift lands on all three equally:
  off      -- stock rewrite, audit disabled: the baseline
  gateON   -- audit enabled with the metadata gate: on contiguous ordering the gate should clear the
              table and skip capture, so this should be indistinguishable from `off`
  gateOFF  -- audit enabled, gate forced off: the cost of capture when it cannot be avoided

Ordering is contiguous, so there are no violations to find. That is deliberate: it prices capture at
its worst, on a table where the verdict is empty and every byte of the work is pure overhead.

Controls, all fatal:
  * ingest time must not vary with the arm -- the mechanism changes only the rewrite. The first run of
    the session is a stated warmup exclusion; the rest must hold within a few percent.
  * gateON must actually gate (groups-gated > 0) and gateOFF must actually audit (groups-audited > 0).
    Without these, "the gate is free" is indistinguishable from "the audit never ran".
  * file size, file count, and payload entropy are checked before and after ingest.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import (err_excerpt, ControlFailure, check_entropy, check_rewrote, cv, emit, hostinfo,  # noqa: E402
                    median, preflight, run_one, spread)

HEAP = os.environ.get("MOR_EXP1_HEAP", "32g")
REPEATS = int(os.environ.get("MOR_EXP1_REPEATS", "5"))
COMMITS = int(os.environ.get("MOR_EXP1_COMMITS", "32"))
RPC = int(os.environ.get("MOR_EXP1_RPC", "3600000"))
FPC = int(os.environ.get("MOR_EXP1_FPC", "4"))          # 900K rows/file ~ 335 MB, under the 384 MB floor
PAYLOAD = 400

SYNTH = {"commits": COMMITS, "rows_per_commit": RPC, "payload_bytes": PAYLOAD,
         "delete_frac": 0.2, "ordering": "contiguous", "files_per_commit": FPC}
# The 1.96x measured previously is close to what double materialisation alone predicts: without a
# cache, the aggregation and the survivor write are two actions over the same scan, so Spark re-reads
# the Parquet and re-applies the delete filter. Caching between them is the fix. Both arms are run so
# the cost of the DESIGN can be separated from the cost of the missing cache -- and so that "caching
# did not help" is reportable if that is what happens, since the cached representation of wide rows
# can exceed the Parquet it came from and spill.
ARMS = [
    ("off", False, ""),
    ("capture_cached", True, "audit-gate=false,audit-cache-scan=true"),
    ("capture_uncached", True, "audit-gate=false,audit-cache-scan=false"),
]

p = preflight("exp1", COMMITS, RPC, FPC, PAYLOAD)
print(f"exp1: {p['rows_total']:,} rows, ~{p['bytes_total']/2**30:.1f} GB, "
      f"{p['files_total']} files of ~{p['bytes_per_file']/2**20:.0f} MB, heap {HEAP}", flush=True)
print(f"host: {hostinfo()}", flush=True)

out = {a: [] for a, _, _ in ARMS}
failures = []
for i in range(REPEATS):
    for label, audit, opts in ARMS:
        res, wall = run_one(f"e1_{label}_{i}", SYNTH, heap=HEAP, audit=audit, opts=opts)
        if res.get("error"):
            failures.append(f"exp1/{label}/r{i}: {err_excerpt(res['error'], 200, 800)}")
            print(f"  r{i} {label:8} FAILED: {res['error'][:200]}", flush=True)
            out[label].append({"error": err_excerpt(res["error"])})
            continue
        st, summ = res["stats"], (res.get("audit_summary") or {})
        try:
            per_row = check_entropy(f"exp1/{label}/r{i}", res, SYNTH)
            groups = check_rewrote(f"exp1/{label}/r{i}", res) if audit else None
        except ControlFailure as e:
            failures.append(str(e))
            per_row, groups = None, None
            print(f"  r{i} {label:8} CONTROL FAILURE: {e}", flush=True)
        rec = {"compact_s": st["compact_time_s"], "ingest_s": st["apply_time_s"],
               "evict_s": st.get("evict_s"), "on_disk_gb": res["_on_disk_bytes"] / 2**30,
               "bytes_per_row": per_row, "live_rows": st.get("live_rows"),
               "groups_total": summ.get("mor.audit.groups-total"),
               "groups_gated": summ.get("mor.audit.groups-gated"),
               "groups_audited": summ.get("mor.audit.groups-audited"),
               "wall_s": round(wall, 1)}
        out[label].append(rec)
        print(f"  r{i} {label:8} compact={rec['compact_s']:8.2f}s ingest={rec['ingest_s']:7.1f}s "
              f"evict={rec['evict_s']}s disk={rec['on_disk_gb']:5.1f}GB "
              f"B/row={per_row and round(per_row)} groups tot/gated/aud="
              f"{rec['groups_total']}/{rec['groups_gated']}/{rec['groups_audited']}", flush=True)

# ---- positive controls: did each arm exercise what it claims to? ----
def latest(label, key):
    return [r.get(key) for r in out[label] if not r.get("error")]

for arm in ("capture_cached", "capture_uncached"):
    aud = [int(x) for x in latest(arm, "groups_audited") if x is not None]
    if aud and not any(a > 0 for a in aud):
        failures.append(f"exp1: {arm} never audited a group -- capture did not actually run, so its "
                        f"cost is not being measured")

# ---- ingest control, with the warmup exclusion stated rather than assumed ----
ing_all = [r["ingest_s"] for a, _, _ in ARMS for r in out[a] if r.get("ingest_s")]
order = []
for i in range(REPEATS):
    for label, _, _ in ARMS:
        if i < len(out[label]) and out[label][i].get("ingest_s"):
            order.append(out[label][i]["ingest_s"])
ing_excl = order[1:]
print("\n" + "=" * 92)
print(f"INGEST CONTROL (must not vary with arm): all {len(order)} runs span "
      f"{spread(order):.3f}x; excluding the first (session warmup) {len(ing_excl)} runs span "
      f"{spread(ing_excl):.3f}x")
if ing_excl and spread(ing_excl) > 1.10:
    failures.append(f"exp1: ingest varies {spread(ing_excl):.2f}x after the warmup exclusion; the "
                    f"control did not hold, so compaction deltas are not attributable to the audit")

base = [r["compact_s"] for r in out["off"] if r.get("compact_s")]
print(f"BASELINE STABILITY: median {median(base):.1f}s  spread {spread(base):.2f}x  CV {cv(base):.1%}"
      f"   (local cold 11 GB was 1.44x / 15%; local 22 GB was 9.14x and unusable)")
if spread(base) > 2.0:
    failures.append(f"exp1: baseline spread {spread(base):.2f}x -- the machine is still the subject; "
                    f"treat the ratios below as unreportable until this is understood")

for label in ("capture_cached", "capture_uncached"):
    pairs = [(x["compact_s"], y["compact_s"]) for x, y in zip(out[label], out["off"])
             if x.get("compact_s") and y.get("compact_s")]
    if pairs:
        rat = [a / b for a, b in pairs]
        above = sum(1 for r in rat if r > 1)
        print(f"{label:8} vs off: paired median {median(rat):.2f} "
              f"(range {min(rat):.2f}-{max(rat):.2f}, {above}/{len(rat)} above 1)")

print("\nPASS" if not failures else "\nFAIL:\n  " + "\n  ".join(failures))
emit("exp1_cost.json", {"config": {**SYNTH, "heap": HEAP, "repeats": REPEATS}, "plan": p,
                        "host": hostinfo(), "arms": out, "failures": failures})
sys.exit(1 if failures else 0)
