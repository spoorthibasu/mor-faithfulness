#!/usr/bin/env python3
"""Item 1 -- does giving the final aggregation more than one task recover the ~38 s it costs?

Stage attribution of the audited rewrite found 130 s of overhead, of which ~38 s sits in a FINAL
AGGREGATION THAT RUNS IN ONE TASK. The cause is not the mechanism: the harness sets
`spark.sql.shuffle.partitions=1`, which is right for the KB-scale cells it was written for and forces
an entire GB-scale aggregation shuffle through a single core. That is a property of our measurement
configuration, so roughly 29% of the reported overhead may be an artifact rather than a cost.

Three arms, round-robin interleaved, paired within each round:
  off        stock rewrite, the baseline
  audited    capture with the gate off and the scan uncached, at the harness default of 1 partition
  audited_fx the same with a partition count matched to the machine

POSITIVE CONTROL, and it is the whole point of running this rather than assuming it. A config that
silently fails to take effect is indistinguishable from one that takes effect and does not help: both
show an unchanged ratio. So the final aggregation's TASK COUNT is read out of the event log and
asserted. If it is still 1 in the fixed arm, the arm did not test anything and the run says so rather
than reporting "no improvement".

Results are written after every round, so a truncated session keeps what completed.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import (err_excerpt, ControlFailure, check_entropy, check_rewrote, emit, event_log_args,  # noqa: E402
                    hostinfo, median, newest_event_log, parse_event_log, preflight,
                    print_stages, run_one, spread, EVENT_DIR)

HEAP = os.environ.get("MOR_EXP4_HEAP", "32g")
REPEATS = int(os.environ.get("MOR_EXP4_REPEATS", "5"))
COMMITS, RPC, FPC, PAYLOAD = 32, 3_600_000, 4, 400

SYNTH = {"commits": COMMITS, "rows_per_commit": RPC, "payload_bytes": PAYLOAD,
         "delete_frac": 0.2, "ordering": "contiguous", "files_per_commit": FPC}
CAPTURE = "audit-gate=false,audit-cache-scan=false"
# FIRST ATTEMPT, AND WHY IT WAS WRONG: we disabled AQE partition coalescing, on the assumption that
# the single-task final aggregation was AQE collapsing 200 partitions. The config took effect and
# changed nothing, because the harness sets `spark.sql.shuffle.partitions=1` -- sized for the KB-scale
# cells it was written for. There was never more than one partition to coalesce. The positive control
# caught this: the fixed arm still reported one task. The knob that matters is the partition count.
SHUFFLE_PARTS = os.environ.get("MOR_EXP4_PARTITIONS", "64")   # 16 vCPU; ~15 MB per partition here
ARMS = [("off", False, "", None), ("audited", True, CAPTURE, None),
        ("audited_fx", True, CAPTURE, SHUFFLE_PARTS)]

p = preflight("exp4", COMMITS, RPC, FPC, PAYLOAD)
print(f"exp4: {p['rows_total']:,} rows, ~{p['bytes_total']/2**30:.1f} GB, heap {HEAP}", flush=True)
print(f"host: {hostinfo()}", flush=True)

out = {a: [] for a, _, _, _ in ARMS}
failures = []


def save():
    emit("exp4_shuffle_fix.json", {"config": {**SYNTH, "heap": HEAP}, "host": hostinfo(),
                                   "arms": out, "failures": failures})


for i in range(REPEATS):
    for label, audit, opts, extra in ARMS:
        os.makedirs(EVENT_DIR, exist_ok=True)
        before = set(os.listdir(EVENT_DIR))
        os.environ["PYSPARK_SUBMIT_ARGS"] = event_log_args(HEAP)
        if extra:
            os.environ["MOR_SHUFFLE_PARTITIONS"] = extra
        else:
            os.environ.pop("MOR_SHUFFLE_PARTITIONS", None)
        res, wall = run_one(f"e4_{label}_{i}", SYNTH, heap=HEAP, audit=audit, opts=opts)
        if res.get("error"):
            failures.append(f"exp4/{label}/r{i}: {err_excerpt(res['error'], 200, 800)}")
            print(f"  r{i} {label:11} FAILED: {res['error'][:160]}", flush=True)
            out[label].append({"error": err_excerpt(res["error"])}); save(); continue
        st = res["stats"]
        stages = parse_event_log(newest_event_log(before))
        try:
            check_entropy(f"exp4/{label}/r{i}", res, SYNTH)
            if audit:
                check_rewrote(f"exp4/{label}/r{i}", res)
        except ControlFailure as e:
            failures.append(str(e))

        # the aggregation stages are the ones that read far less than the table
        aggs = [s for s in stages if s["input_gb"] < p["bytes_total"] / 2**30 * 0.5
                and s["wall_s"] and s["wall_s"] > 1]
        final = min(aggs, key=lambda s: s["tasks"]) if aggs else None
        rec = {"compact_s": st["compact_time_s"], "ingest_s": st["apply_time_s"],
               "evict_s": st.get("evict_s"), "wall_s": round(wall, 1), "stages": stages,
               "final_agg_tasks": final["tasks"] if final else None,
               "final_agg_wall_s": final["wall_s"] if final else None}
        out[label].append(rec)
        print(f"  r{i} {label:11} compact={rec['compact_s']:8.2f}s ingest={rec['ingest_s']:7.1f}s "
              f"final_agg_tasks={rec['final_agg_tasks']} ({rec['final_agg_wall_s']}s)", flush=True)
        if i == 0 and stages:
            print_stages(stages)
        save()

# ---- positive controls ----
fx = [r for r in out["audited_fx"] if r.get("final_agg_tasks")]
au = [r for r in out["audited"] if r.get("final_agg_tasks")]
if au and not any(r["final_agg_tasks"] == 1 for r in au):
    failures.append("exp4: the unfixed arm never showed a single-task final aggregation, so the "
                    "condition this experiment exists to fix was not reproduced")
if fx and not any((r["final_agg_tasks"] or 0) > 1 for r in fx):
    failures.append("exp4: CONFIG DID NOT TAKE EFFECT -- the fixed arm's final aggregation still ran "
                    "in one task. Any 'no improvement' reading here is void, not negative.")

print("\n" + "=" * 92)
base = [r["compact_s"] for r in out["off"] if r.get("compact_s")]
print(f"baseline: median {median(base):.1f}s  spread {spread(base):.2f}x")
ing = [r["ingest_s"] for a in out for r in out[a] if r.get("ingest_s")]
print(f"ingest control: {len(ing)} runs span {spread(ing):.3f}x")
if ing and spread(ing) > 1.10:
    failures.append(f"exp4: ingest varies {spread(ing):.2f}x; the control did not hold")
for label in ("audited", "audited_fx"):
    pairs = [(x["compact_s"], y["compact_s"]) for x, y in zip(out[label], out["off"])
             if x.get("compact_s") and y.get("compact_s")]
    if pairs:
        r = [a / b for a, b in pairs]
        print(f"{label:11} vs off: paired median {median(r):.2f} (range {min(r):.2f}-{max(r):.2f}, "
              f"{sum(1 for x in r if x > 1)}/{len(r)} above 1)")
if au and fx:
    a_t = median([r["final_agg_wall_s"] for r in au if r.get("final_agg_wall_s")])
    f_t = median([r["final_agg_wall_s"] for r in fx if r.get("final_agg_wall_s")])
    print(f"final aggregation: {a_t:.1f}s in {au[0]['final_agg_tasks']} task(s) -> "
          f"{f_t:.1f}s in {fx[0]['final_agg_tasks']} task(s)   (recovered {a_t - f_t:+.1f}s)")
save()
print("\nPASS" if not failures else "\nFAIL:\n  " + "\n  ".join(failures))
sys.exit(1 if failures else 0)
