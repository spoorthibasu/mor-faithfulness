#!/usr/bin/env python3
"""Item 4 -- what is the partial aggregation spending 91 seconds on?

It is the largest unexplained component of the headline overhead. The obvious answer, that it
re-applies the equality deletes, was refuted by the isolation probe: on a plain read of the same data
at the same scale, applying the deletes costs 17.3 s against 18.3 s without them. So the explanation
has to come from the stage itself.

This runs nothing. Spark's event log already records, per task, the record count, CPU time against
wall time, GC time, spill, shuffle write time and deserialisation time; Exp 4 writes those logs as a
side effect. Parsing them costs no instance time, which is the right price for a question that is
about interpretation rather than about a missing measurement.

THE HYPOTHESIS THIS IS BUILT TO KILL. "91 s for 1.0 GB of input" is how we described it, and that
framing may be the whole confusion. `Input Bytes` falls with column pruning; `Records Read` does not.
The aggregation must see EVERY row, including the ones the delete filter marks, because a discarded
version is exactly what it is looking for. If its record count equals the write stage's, then it is
not processing 1 GB of work at all -- it is processing the same 115 million rows the write processes,
over three columns instead of all of them, and 91 s against the write's 136 s is unremarkable rather
than mysterious.

The write stage is the control: same run, same rows, same delete application, different output. Every
difference between them is what the aggregation does extra.
"""
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import EVENT_DIR, emit, hostinfo  # noqa: E402
from collections import defaultdict  # noqa: E402


def detail(path):
    """Per-stage task-level detail. Everything summed from the log; nothing estimated."""
    stages, agg = {}, defaultdict(lambda: defaultdict(float))
    durations = defaultdict(list)
    with open(path) as f:
        for line in f:
            try:
                e = json.loads(line)
            except Exception:
                continue
            ev = e.get("Event", "")
            if ev == "SparkListenerStageCompleted":
                si = e["Stage Info"]
                sub, comp = si.get("Submission Time"), si.get("Completion Time")
                stages[si["Stage ID"]] = {
                    "name": si.get("Stage Name", "?")[:60], "tasks": si.get("Number of Tasks", 0),
                    "wall_s": round((comp - sub) / 1000.0, 2) if sub and comp else None}
            elif ev == "SparkListenerTaskEnd":
                m = e.get("Task Metrics") or {}
                sid = e.get("Stage ID")
                a = agg[sid]
                a["run_ms"] += m.get("Executor Run Time", 0)
                a["cpu_ns"] += m.get("Executor CPU Time", 0)
                a["gc_ms"] += m.get("JVM GC Time", 0)
                a["deser_ms"] += m.get("Executor Deserialize Time", 0)
                a["resser_ms"] += m.get("Result Serialization Time", 0)
                a["spill_disk"] += m.get("Disk Bytes Spilled", 0)
                a["spill_mem"] += m.get("Memory Bytes Spilled", 0)
                a["peak_mem"] = max(a["peak_mem"], m.get("Peak Execution Memory", 0))
                im = m.get("Input Metrics") or {}
                a["in_bytes"] += im.get("Bytes Read", 0)
                a["in_records"] += im.get("Records Read", 0)
                om = m.get("Output Metrics") or {}
                a["out_records"] += om.get("Records Written", 0)
                sw = m.get("Shuffle Write Metrics") or {}
                a["sw_bytes"] += sw.get("Shuffle Bytes Written", 0)
                a["sw_records"] += sw.get("Shuffle Records Written", 0)
                a["sw_time_ns"] += sw.get("Shuffle Write Time", 0)
                durations[sid].append(m.get("Executor Run Time", 0))
    out = []
    for sid, st in sorted(stages.items()):
        if (st["wall_s"] or 0) < 1:
            continue
        a, d = agg[sid], sorted(durations.get(sid, []))
        n = len(d)
        out.append({
            "stage": sid, "name": st["name"], "tasks": st["tasks"], "wall_s": st["wall_s"],
            "run_s": round(a["run_ms"] / 1000, 1), "cpu_s": round(a["cpu_ns"] / 1e9, 1),
            "gc_s": round(a["gc_ms"] / 1000, 1), "deser_s": round(a["deser_ms"] / 1000, 1),
            "shuffle_write_s": round(a["sw_time_ns"] / 1e9, 1),
            "in_records": int(a["in_records"]), "in_gb": round(a["in_bytes"] / 2**30, 2),
            "out_records": int(a["out_records"]),
            "shuffle_records": int(a["sw_records"]),
            "shuffle_gb": round(a["sw_bytes"] / 2**30, 3),
            "spill_gb": round((a["spill_disk"] + a["spill_mem"]) / 2**30, 2),
            "peak_mem_gb": round(a["peak_mem"] / 2**30, 2),
            "task_ms_p50": d[n // 2] if n else 0, "task_ms_max": d[-1] if n else 0,
            "skew": round(d[-1] / d[n // 2], 2) if n and d[n // 2] else None})
    return out


logs = sorted(os.listdir(EVENT_DIR)) if os.path.isdir(EVENT_DIR) else []
logs = [os.path.join(EVENT_DIR, f) for f in logs if not f.endswith(".inprogress")]
if not logs:
    print("no completed event logs found", flush=True); sys.exit(2)

# The audited runs are the ones with an aggregation stage: more than one substantial stage.
picked, best = None, 0
for lg in reversed(logs):
    st = detail(lg)
    big = [s for s in st if s["wall_s"] > 5]
    if len(big) >= 2 and len(big) > best:
        picked, best, stages = lg, len(big), st
    if best >= 3:
        break
if not picked:
    print("no audited run found among the event logs", flush=True); sys.exit(2)

print(f"log: {picked}", flush=True)
print(f"host: {hostinfo()}\n", flush=True)
big = [s for s in stages if s["wall_s"] > 5]
for s in big:
    print(f"stage {s['stage']}  {s['name']}", flush=True)
    print(f"   tasks {s['tasks']:>4}  wall {s['wall_s']:>7}s   run {s['run_s']:>7}s   "
          f"cpu {s['cpu_s']:>7}s   gc {s['gc_s']:>6}s   deser {s['deser_s']:>5}s")
    print(f"   in {s['in_records']:>12,} rec / {s['in_gb']:>6} GB     out {s['out_records']:>12,} rec"
          f"     shuffle {s['shuffle_records']:>11,} rec / {s['shuffle_gb']} GB in "
          f"{s['shuffle_write_s']}s")
    print(f"   spill {s['spill_gb']} GB   peak_exec_mem {s['peak_mem_gb']} GB   "
          f"task ms p50 {s['task_ms_p50']} max {s['task_ms_max']} (skew {s['skew']})\n", flush=True)

# ---- the comparison that answers the question ----
scan_like = [s for s in big if s["in_records"] > 0]
if len(scan_like) >= 2:
    a = min(scan_like, key=lambda s: s["in_gb"])      # pruned: the aggregation
    w = max(scan_like, key=lambda s: s["in_gb"])      # full width: the write
    print("=" * 92)
    print(f"aggregation  stage {a['stage']}: {a['in_records']:,} records over {a['in_gb']} GB, "
          f"{a['wall_s']}s")
    print(f"write        stage {w['stage']}: {w['in_records']:,} records over {w['in_gb']} GB, "
          f"{w['wall_s']}s")
    if a["in_records"] and w["in_records"]:
        rr = a["in_records"] / w["in_records"]
        print(f"\nrecord ratio {rr:.2f}, byte ratio {a['in_gb']/w['in_gb']:.3f}")
        if rr > 0.95:
            print("  => THE FRAMING WAS WRONG. The aggregation reads the SAME NUMBER OF ROWS as the")
            print("     write; only the bytes differ, because pruning narrows columns and not rows.")
            print(f"     It is not '{a['wall_s']}s for {a['in_gb']} GB' -- it is {a['wall_s']}s for")
            print(f"     {a['in_records']:,} records, against the write's {w['wall_s']}s for the same")
            print("     rows plus the payload and the output. Nothing is unexplained.")
        else:
            print("  => the aggregation genuinely reads fewer ROWS, so the time is not accounted for")
            print("     by row count and the per-task detail above is where to look next.")
    gc_frac = a["gc_s"] / a["run_s"] if a["run_s"] else 0
    print(f"\nGC is {gc_frac:.1%} of the aggregation's executor time; spill {a['spill_gb']} GB; "
          f"skew {a['skew']}")
    if gc_frac > 0.2:
        print("  => GC is a material share: the group-by's hash maps are the pressure.")

emit("exp7_stage_detail.json", {"log": picked, "stages": stages, "host": hostinfo()})
