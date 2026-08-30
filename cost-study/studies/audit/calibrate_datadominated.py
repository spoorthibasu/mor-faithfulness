#!/usr/bin/env python3
"""Find the smallest LOCAL dataset where flag-off compaction is data-dominated (30-60 s), not
job-launch bound (~2 s).

Uses the driver's synth mode (rows generated in-driver, no full readback), sweeping total table bytes.
Reports for each size: on-disk bytes, flag-off compaction seconds, and effective MB/s, so the
data-dominated point can be read off directly. Also reports disk headroom.

Usage: calibrate_datadominated.py [commits] [payload_bytes]
"""
import os
import shutil
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "src"))
from mor_harness.adapters.base import run_driver, serialize_plan  # noqa: E402
from mor_harness.model import WritePlan                            # noqa: E402

WH = os.path.join(tempfile.gettempdir(), "mor_calib")
COMMITS = int(sys.argv[1]) if len(sys.argv) > 1 else 8
PAYLOAD = int(sys.argv[2]) if len(sys.argv) > 2 else 400
ROWS_PER_COMMIT = [200_000, 600_000, 1_500_000, 2_500_000]


def run(rpc, audit=False, cross=False):
    name = f"calib_{rpc}_{int(audit)}{int(cross)}"
    tdir = os.path.join(WH, "db", name)
    plan = WritePlan(checkpoints=[], key_columns=["id"], payload_columns=["val"],
                     version_column="lsn", enforcement_mode="unsafe_compact")
    cols = [{"name": "id", "type": "int"}, {"name": "val", "type": "string"},
            {"name": "lsn", "type": "int"}]
    plan_json = serialize_plan(plan, name, tdir, WH, "lsn", cols)
    plan_json["synth"] = {"commits": COMMITS, "rows_per_commit": rpc,
                          "payload_bytes": PAYLOAD, "delete_frac": 0.2}
    os.environ["MOR_BULK_INGEST"] = "1"
    os.environ["MOR_AUDIT"] = "1" if audit else "0"
    os.environ["MOR_AUDIT_CROSS_GROUP"] = "1" if cross else "0"
    os.environ["MOR_REWRITE_OPTS"] = ""
    res = run_driver("iceberg_driver.py", plan_json, os.path.join(WH, "_io", name))
    s = res["stats"]
    shutil.rmtree(tdir, ignore_errors=True)
    return s


def gb(n):
    return n / (1024 ** 3)


print(f"commits={COMMITS} payload_bytes={PAYLOAD}\n")
print(f"{'rows/commit':>12} {'total rows':>12} {'table GB':>9} {'ingest s':>9} "
      f"{'COMPACT s':>10} {'MB/s':>8} {'live rows':>11}")
target = None
for rpc in ROWS_PER_COMMIT:
    free = shutil.disk_usage(tempfile.gettempdir()).free
    if gb(free) < 20:
        print(f"  stopping: only {gb(free):.1f} GB free")
        break
    s = run(rpc)
    total_rows = rpc * COMMITS
    tb = s["bytes_total"]
    mbps = (tb / (1024 ** 2)) / s["compact_time_s"] if s["compact_time_s"] else 0
    print(f"{rpc:>12,} {total_rows:>12,} {gb(tb):>9.2f} {s['apply_time_s']:>9.1f} "
          f"{s['compact_time_s']:>10.1f} {mbps:>8.0f} {s['live_rows']:>11,}", flush=True)
    if target is None and s["compact_time_s"] >= 30:
        target = (rpc, gb(tb), s["compact_time_s"])
        print(f"  --> DATA-DOMINATED at {gb(tb):.2f} GB "
              f"(compaction {s['compact_time_s']:.1f}s); stopping sweep")
        break

free_gb = gb(shutil.disk_usage(tempfile.gettempdir()).free)
print(f"\ndisk free on scratch volume: {free_gb:.0f} GB")
if target:
    rpc, size_gb, secs = target
    print(f"smallest data-dominated size: ~{size_gb:.2f} GB (rows/commit={rpc:,}, compaction {secs:.0f}s)")
    print(f"3 arms x 5 repeats would need ~{size_gb*2*1:.1f} GB peak (table + rewrite output), "
          f"re-created per run -> fits in {free_gb:.0f} GB")
else:
    print("no size in the sweep reached 30 s of compaction; increase rows/commit or payload_bytes")
