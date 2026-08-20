#!/usr/bin/env python3
"""Shared rig for the cloud experiments.

Every guard in here exists because a local run once produced a clean-looking number that was wrong.
They are cheap; a wasted instance-hour is not.

  * FILE SIZE. Iceberg's bin-pack only selects files below 0.75x the 512 MB target = 384 MB. A run
    whose files sit above that reports a fast "compaction" that rewrote nothing (Entry 32).
  * MIN INPUT FILES. Below `min-input-files` (default 5) no rewrite is planned at all, and the audit
    summary is simply absent -- which reads as a very fast run rather than as a skipped one (Entry 46).
  * PAYLOAD ENTROPY. A generator that emits compressible payload silently shrinks the table by two
    orders of magnitude; the row count still looks right (Entry 21). Checked against bytes ON DISK.
  * POSITIVE CONTROL. Every arm asserts the thing it is supposed to exercise actually ran -- the gate
    actually gated, capture actually captured, the duplicate trap was actually set.
"""
import json
import os
import shutil
import subprocess
import sys
import time

REPO = os.environ.get("MOR_REPO", "/opt/mor/mor-faithfulness")
sys.path.insert(0, os.path.join(REPO, "cost-study/src"))
from mor_harness.adapters.base import run_driver, serialize_plan  # noqa: E402
from mor_harness.model import WritePlan                            # noqa: E402

WAREHOUSE = os.environ["MOR_WAREHOUSE"]          # must be on the NVMe; run.sh has already checked
JAR = os.environ["MOR_ICEBERG_JAR"]
RESULTS = os.environ.get("MOR_RESULTS", "/opt/mor/results")
COLS = [{"name": "id", "type": "int"}, {"name": "val", "type": "string"},
        {"name": "lsn", "type": "int"}]

SELECTION_FLOOR = 384 * 1024 ** 2                # 0.75 x 512 MB target
MIN_INPUT_FILES = 5


class ControlFailure(RuntimeError):
    """A guard tripped. The measurement is void, not merely disappointing."""


def plan_bytes(commits, rows_per_commit, files_per_commit, payload_bytes):
    """Predicted on-disk size, and the per-file size the planner will see."""
    per_row = payload_bytes * 0.975                      # measured: ~390 B on disk at payload 400
    rows_per_file = rows_per_commit // files_per_commit
    return {"rows_total": commits * rows_per_commit,
            "bytes_total": int(commits * rows_per_commit * per_row),
            "rows_per_file": rows_per_file,
            "bytes_per_file": int(rows_per_file * per_row),
            "files_total": commits * files_per_commit}


def preflight(label, commits, rows_per_commit, files_per_commit, payload_bytes):
    """Refuse to run a configuration that cannot measure what it claims to."""
    p = plan_bytes(commits, rows_per_commit, files_per_commit, payload_bytes)
    if p["bytes_per_file"] >= SELECTION_FLOOR:
        raise ControlFailure(
            f"{label}: {p['bytes_per_file']/2**20:.0f} MB per file is at or above the "
            f"{SELECTION_FLOOR/2**20:.0f} MB selection floor -- the planner would skip these files and "
            f"the run would report a rewrite that never happened")
    if p["files_total"] < MIN_INPUT_FILES:
        raise ControlFailure(
            f"{label}: {p['files_total']} files is below min-input-files={MIN_INPUT_FILES}; no rewrite "
            f"would be planned and the run would look instantaneous")
    return p


def run_one(name, synth, *, heap, cross=False, audit=True, opts="", drop_cache=True,
            timeout=None):
    """One fresh-JVM run. Returns (result, wall_seconds). Never reuses a table."""
    tdir = os.path.join(WAREHOUSE, "db", name)
    shutil.rmtree(tdir, ignore_errors=True)
    plan = WritePlan(checkpoints=[], key_columns=["id"], payload_columns=["val"],
                     version_column="lsn", enforcement_mode="unsafe_compact")
    pj = serialize_plan(plan, name, tdir, WAREHOUSE, "lsn", COLS)
    pj["synth"] = synth
    env = {"MOR_ICEBERG_JAR": JAR, "MOR_BULK_INGEST": "1",
           "MOR_AUDIT": "1" if audit else "0",
           "MOR_AUDIT_CROSS_GROUP": "1" if cross else "0",
           "MOR_REWRITE_OPTS": opts, "MOR_DRIVER_MEM": heap}
    if drop_cache:
        env["MOR_DROP_CACHE_MODE"] = "sysctl"     # exact, kernel-level; not the approximate substitute
    os.environ.update(env)
    os.environ.pop("MOR_DROP_CACHE", None)
    if not drop_cache:
        os.environ.pop("MOR_DROP_CACHE_MODE", None)
    t0 = time.time()
    try:
        res = run_driver("iceberg_driver.py", pj, os.path.join(WAREHOUSE, "_io", name))
    except Exception as e:
        res = {"error": f"{type(e).__name__}: {str(e)[:2000]}"}
    wall = time.time() - t0
    on_disk = 0
    ddir = os.path.join(tdir, "data")
    if os.path.isdir(ddir):
        on_disk = sum(os.path.getsize(os.path.join(ddir, f)) for f in os.listdir(ddir))
    res["_on_disk_bytes"] = on_disk
    shutil.rmtree(tdir, ignore_errors=True)       # one table at a time; the NVMe is big but not free
    return res, wall


def check_entropy(label, res, synth, tol=(0.55, 1.45)):
    """The payload must actually be incompressible. Measured against bytes on disk, not row counts."""
    rows = synth["commits"] * synth["rows_per_commit"]
    got = res.get("_on_disk_bytes", 0)
    if not got:
        raise ControlFailure(f"{label}: no data files on disk after ingest")
    per_row = got / rows
    expect = synth["payload_bytes"] * 0.975
    if not (expect * tol[0] <= per_row <= expect * tol[1]):
        raise ControlFailure(
            f"{label}: {per_row:.0f} B/row on disk against {expect:.0f} B/row expected. Payload is "
            f"compressing, so the table is not the size the experiment thinks it is")
    return per_row


def check_rewrote(label, res):
    """A rewrite that never happened is the fastest possible result and the least useful."""
    s = res.get("audit_summary") or {}
    if not s.get("mor.audit.groups-total"):
        raise ControlFailure(
            f"{label}: no audit summary in the snapshot -- no file group was planned, so nothing was "
            f"rewritten. Check file sizes and min-input-files before trusting any timing here")
    return int(s["mor.audit.groups-total"])


def spread(xs):
    xs = [x for x in xs if x]
    return (max(xs) / min(xs)) if len(xs) > 1 and min(xs) else float("nan")


def cv(xs):
    xs = [x for x in xs if x]
    if len(xs) < 2:
        return float("nan")
    m = sum(xs) / len(xs)
    return (sum((x - m) ** 2 for x in xs) / (len(xs) - 1)) ** 0.5 / m


def median(xs):
    xs = sorted(x for x in xs if x)
    n = len(xs)
    return float("nan") if not n else (xs[n // 2] if n % 2 else (xs[n // 2 - 1] + xs[n // 2]) / 2)


def emit(name, payload):
    os.makedirs(RESULTS, exist_ok=True)
    dst = os.path.join(RESULTS, name)
    with open(dst, "w") as f:
        json.dump(payload, f, indent=1, default=str)
    print(f"\nevidence -> {dst}", flush=True)


def hostinfo():
    def sh(c):
        try:
            return subprocess.run(c, shell=True, capture_output=True, text=True).stdout.strip()
        except Exception:
            return "?"
    return {"nproc": sh("nproc"), "mem_total": sh("free -g | awk '/Mem:/{print $2\" GiB\"}'"),
            "warehouse_dev": sh(f"findmnt -no SOURCE --target {WAREHOUSE}"),
            "root_dev": sh("findmnt -no SOURCE --target /"),
            "kernel": sh("uname -r"), "instance": sh(
                "curl -s --max-time 2 -H 'X-aws-ec2-metadata-token: '"
                "\"$(curl -s --max-time 2 -X PUT http://169.254.169.254/latest/api/token "
                "-H 'X-aws-ec2-metadata-token-ttl-seconds: 60')\" "
                "http://169.254.169.254/latest/meta-data/instance-type")}


# ---------------------------------------------------------------------------------------------
# Spark event-log attribution. Reading stage timings out of the log is the only way to say WHERE
# time goes; every attempt in this study to infer it from a ratio instead has been wrong at least
# once. Nothing here estimates -- each field is summed from the log's own task metrics.
from collections import defaultdict  # noqa: E402

EVENT_DIR = os.environ.get("MOR_EVENT_DIR", "/mnt/nvme/spark-events")


def event_log_args(heap):
    """PYSPARK_SUBMIT_ARGS with event logging on. The driver uses setdefault, so this wins."""
    os.makedirs(EVENT_DIR, exist_ok=True)
    return (f"--driver-memory {heap} --conf spark.eventLog.enabled=true "
            f"--conf spark.eventLog.dir=file://{EVENT_DIR} pyspark-shell")


def newest_event_log(before):
    now = set(os.listdir(EVENT_DIR)) if os.path.isdir(EVENT_DIR) else set()
    fresh = sorted(now - before)
    return os.path.join(EVENT_DIR, fresh[-1]) if fresh else None


def parse_event_log(path, min_wall_s=0.5):
    """Per-stage wall, CPU, bytes read/written, shuffle and spill, straight from the log."""
    if not path or not os.path.exists(path):
        return []
    stages, tasks = {}, defaultdict(lambda: defaultdict(int))
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
                    "name": si.get("Stage Name", "?")[:70],
                    "tasks": si.get("Number of Tasks", 0),
                    "wall_s": round((comp - sub) / 1000.0, 2) if sub and comp else None}
            elif ev == "SparkListenerTaskEnd":
                m = e.get("Task Metrics") or {}
                t = tasks[e.get("Stage ID")]
                t["run_ms"] += m.get("Executor Run Time", 0)
                t["input"] += (m.get("Input Metrics") or {}).get("Bytes Read", 0)
                t["output"] += (m.get("Output Metrics") or {}).get("Bytes Written", 0)
                t["sw"] += (m.get("Shuffle Write Metrics") or {}).get("Shuffle Bytes Written", 0)
                srm = m.get("Shuffle Read Metrics") or {}
                t["sr"] += srm.get("Remote Bytes Read", 0) + srm.get("Local Bytes Read", 0)
                t["spill"] += m.get("Disk Bytes Spilled", 0)
    out = []
    for sid, st in sorted(stages.items()):
        if (st["wall_s"] or 0) < min_wall_s:
            continue
        t = tasks.get(sid, {})
        out.append({"stage": sid, "name": st["name"], "tasks": st["tasks"], "wall_s": st["wall_s"],
                    "cpu_s": round(t.get("run_ms", 0) / 1000.0, 1),
                    "input_gb": round(t.get("input", 0) / 2**30, 2),
                    "output_gb": round(t.get("output", 0) / 2**30, 2),
                    "shuffle_write_gb": round(t.get("sw", 0) / 2**30, 3),
                    "shuffle_read_gb": round(t.get("sr", 0) / 2**30, 3),
                    "spill_gb": round(t.get("spill", 0) / 2**30, 2)})
    return out


def print_stages(stages, indent="    "):
    print(f"{indent}{'stg':>3} {'tasks':>6} {'wall_s':>8} {'in_GB':>7} {'out_GB':>7} "
          f"{'shufW':>7} {'shufR':>7}  name")
    for s in stages:
        print(f"{indent}{s['stage']:>3} {s['tasks']:>6} {str(s['wall_s']):>8} {s['input_gb']:>7} "
              f"{s['output_gb']:>7} {s['shuffle_write_gb']:>7} {s['shuffle_read_gb']:>7}  "
              f"{s['name']}", flush=True)
