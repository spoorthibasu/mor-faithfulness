#!/usr/bin/env python3
"""Attribute the ~1.9x: where does the audited rewrite actually spend its extra time?

The isolation probe could not answer this. It measured plain reads with a noop sink, so it omitted the
write from both sides; against the real 137 s stock rewrite its 27.9 s second traversal predicts 1.20x,
leaving roughly 100 s unexplained. This measures the rewrite itself, with Spark's event log on, and
attributes the difference by stage.

Two runs at the cost experiment's scale, paired: a stock rewrite and an audited one with the gate off
and the scan uncached (the arm that measured 1.91x). Each builds its own table, because compaction is
destructive and the arms must not share one.

What the stage breakdown can settle:

  * Does the audited run scan the table TWICE? Then the aggregation stage's input bytes will be
    comparable to the write stage's, and the second traversal is real -- which is what the combined-pass
    reasoning in the future-work section assumes.
  * Does column pruning survive the rewrite's scan-task-set data source? The probe showed pruning on a
    plain read (`BatchScan [id, lsn, _deleted]`), but the rewrite reads through a different path. If the
    aggregation stage reads the full 53 GB rather than three columns' worth, pruning does NOT survive,
    and that alone is a large chunk of the missing time.
  * Is the cost in the shuffle instead? Shuffle write bytes and the post-Exchange stage's duration say
    so directly. If that is where the time is, the future-work claim needs rewriting: fusing the passes
    removes a scan, not a shuffle, and an accumulator-based fusion removes the shuffle but buys the
    O(distinct keys) memory profile we rejected it for.

Nothing here is inferred from a ratio. Every number below is read out of the event log.
"""
import json
import os
import shutil
import sys
import time
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import ControlFailure, check_rewrote, emit, hostinfo, preflight, run_one  # noqa: E402

HEAP = os.environ.get("MOR_ATTR_HEAP", "32g")
COMMITS = int(os.environ.get("MOR_ATTR_COMMITS", "32"))
RPC = int(os.environ.get("MOR_ATTR_RPC", "3600000"))
FPC = int(os.environ.get("MOR_ATTR_FPC", "4"))
PAYLOAD = 400
EVDIR = os.environ.get("MOR_ATTR_EVENTDIR", "/mnt/nvme/spark-events")

SYNTH = {"commits": COMMITS, "rows_per_commit": RPC, "payload_bytes": PAYLOAD,
         "delete_frac": 0.2, "ordering": "contiguous", "files_per_commit": FPC}

p = preflight("attribute", COMMITS, RPC, FPC, PAYLOAD)
print(f"attribute: {p['rows_total']:,} rows, ~{p['bytes_total']/2**30:.1f} GB, heap {HEAP}", flush=True)
print(f"host: {hostinfo()}", flush=True)
os.makedirs(EVDIR, exist_ok=True)


def parse_event_log(path):
    """Aggregate per stage straight out of the event log. No sampling, no estimation."""
    stages = {}
    tasks = defaultdict(lambda: {"run_ms": 0, "input_bytes": 0, "shuffle_write": 0,
                                 "shuffle_read": 0, "output_bytes": 0, "n": 0,
                                 "spill_disk": 0, "spill_mem": 0})
    with open(path) as f:
        for line in f:
            try:
                e = json.loads(line)
            except Exception:
                continue
            ev = e.get("Event", "")
            if ev == "SparkListenerStageCompleted":
                si = e["Stage Info"]
                sid = si["Stage ID"]
                sub, comp = si.get("Submission Time"), si.get("Completion Time")
                stages[sid] = {"name": si.get("Stage Name", "?"),
                               "tasks": si.get("Number of Tasks", 0),
                               "wall_s": round((comp - sub) / 1000.0, 2) if sub and comp else None}
            elif ev == "SparkListenerTaskEnd":
                sid = e.get("Stage ID")
                m = e.get("Task Metrics") or {}
                t = tasks[sid]
                t["n"] += 1
                t["run_ms"] += m.get("Executor Run Time", 0)
                t["input_bytes"] += (m.get("Input Metrics") or {}).get("Bytes Read", 0)
                t["output_bytes"] += (m.get("Output Metrics") or {}).get("Bytes Written", 0)
                t["shuffle_write"] += (m.get("Shuffle Write Metrics") or {}).get(
                    "Shuffle Bytes Written", 0)
                srm = m.get("Shuffle Read Metrics") or {}
                t["shuffle_read"] += (srm.get("Remote Bytes Read", 0)
                                      + srm.get("Local Bytes Read", 0))
                t["spill_disk"] += m.get("Disk Bytes Spilled", 0)
                t["spill_mem"] += m.get("Memory Bytes Spilled", 0)
    out = []
    for sid, s in sorted(stages.items()):
        t = tasks.get(sid, {})
        out.append({"stage": sid, "name": s["name"][:70], "tasks": s["tasks"],
                    "wall_s": s["wall_s"], "cpu_s": round(t.get("run_ms", 0) / 1000.0, 1),
                    "input_gb": round(t.get("input_bytes", 0) / 2**30, 2),
                    "output_gb": round(t.get("output_bytes", 0) / 2**30, 2),
                    "shuffle_write_gb": round(t.get("shuffle_write", 0) / 2**30, 3),
                    "shuffle_read_gb": round(t.get("shuffle_read", 0) / 2**30, 3),
                    "spill_disk_gb": round(t.get("spill_disk", 0) / 2**30, 2)})
    return out


def newest_log(before):
    now = set(os.listdir(EVDIR))
    fresh = [f for f in now - before if not f.endswith(".inprogress")]
    if not fresh:
        fresh = [f for f in now - before]
    return os.path.join(EVDIR, sorted(fresh)[-1]) if fresh else None


ARMS = [("stock", False, ""), ("audited", True, "audit-gate=false,audit-cache-scan=false")]
out, failures = {}, []
for label, audit, opts in ARMS:
    before = set(os.listdir(EVDIR))
    # setdefault in the driver means an externally-set value wins, so event logging goes on without
    # modifying the driver at all.
    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        f"--driver-memory {HEAP} "
        f"--conf spark.eventLog.enabled=true "
        f"--conf spark.eventLog.dir=file://{EVDIR} "
        f"pyspark-shell")
    print(f"\n=== {label} ===", flush=True)
    res, wall = run_one(f"attr_{label}", SYNTH, heap=HEAP, audit=audit, opts=opts)
    if res.get("error"):
        failures.append(f"{label}: {res['error'][:300]}")
        print(f"  FAILED: {res['error'][:200]}", flush=True)
        continue
    st = res["stats"]
    if audit:
        try:
            check_rewrote(label, res)
        except ControlFailure as e:
            failures.append(str(e))
    log = newest_log(before)
    print(f"  compact={st['compact_time_s']}s ingest={st['apply_time_s']}s  eventlog={log}",
          flush=True)
    stages = parse_event_log(log) if log and os.path.exists(log) else []
    out[label] = {"compact_s": st["compact_time_s"], "ingest_s": st["apply_time_s"],
                  "event_log": log, "stages": stages}
    print(f"  {'stg':>3} {'tasks':>6} {'wall_s':>8} {'cpu_s':>9} {'in_GB':>8} {'out_GB':>8} "
          f"{'shufW_GB':>9} {'spill_GB':>9}  name")
    for s in stages:
        print(f"  {s['stage']:>3} {s['tasks']:>6} {str(s['wall_s']):>8} {s['cpu_s']:>9} "
              f"{s['input_gb']:>8} {s['output_gb']:>8} {s['shuffle_write_gb']:>9} "
              f"{s['spill_disk_gb']:>9}  {s['name']}", flush=True)

print("\n" + "=" * 100)
s_st, s_au = out.get("stock", {}), out.get("audited", {})
if s_st and s_au:
    ds, da = s_st["stages"], s_au["stages"]
    tot_in_st = sum(x["input_gb"] for x in ds)
    tot_in_au = sum(x["input_gb"] for x in da)
    print(f"compaction: stock {s_st['compact_s']}s -> audited {s_au['compact_s']}s "
          f"({s_au['compact_s']/s_st['compact_s']:.2f}x)")
    print(f"bytes read: stock {tot_in_st:.1f} GB -> audited {tot_in_au:.1f} GB "
          f"({tot_in_au/tot_in_st:.2f}x)" if tot_in_st else "")
    print(f"shuffle written: stock {sum(x['shuffle_write_gb'] for x in ds):.2f} GB -> "
          f"audited {sum(x['shuffle_write_gb'] for x in da):.2f} GB")
    print(f"disk spill: stock {sum(x['spill_disk_gb'] for x in ds):.2f} GB -> "
          f"audited {sum(x['spill_disk_gb'] for x in da):.2f} GB")
    print()
    if tot_in_st and tot_in_au / tot_in_st > 1.6:
        print("=> the audited run READS THE TABLE ROUGHLY TWICE. The second traversal is real and")
        print("   full-width, so pruning does NOT survive the rewrite's scan path. Fusing the passes")
        print("   would remove a genuine second scan.")
    elif tot_in_st and tot_in_au / tot_in_st < 1.25:
        print("=> the audited run reads barely more than the stock one, so the extra time is NOT a")
        print("   second scan. The future-work reasoning about fusing away a traversal does not hold")
        print("   as stated and must be rewritten around whatever the stage table shows instead.")
    else:
        print("=> partial re-read: pruning survives in part. Attribute from the per-stage rows above")
        print("   rather than from this ratio.")

emit("attribute_overhead.json", {"arms": out, "host": hostinfo(), "failures": failures,
                                 "config": {**SYNTH, "heap": HEAP}})
print("\nPASS" if not failures else "\nFAIL:\n  " + "\n  ".join(failures))
