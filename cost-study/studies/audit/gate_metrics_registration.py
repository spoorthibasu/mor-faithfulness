#!/usr/bin/env python3
"""GATE: verify that bulk-registered files carry CORRECT per-column bounds.

Why this is a gate and not a test. The audit's metadata gate reads per-file lower/upper bounds for the
ordering column. If bulk registration loses or corrupts those bounds, `mayContainStaleWins()` hits its
missing-bounds fallback and audits EVERY group -- selectivity silently collapses to 0% while every
correctness number still looks right. That exact failure already happened once (NOTES Entry 18, scan-task
DataFiles have stats stripped), and at GB scale it would invalidate every selectivity figure in a way
that looks entirely plausible.

So: build a small table on the BULK path, then assert against ground truth computed in Python:
  1. bounds are PRESENT for every column of every data file,
  2. bounds are CORRECT (== true per-file min/max of the rows written),
  3. record counts and equality-delete bounds are correct,
  4. the audit gate actually discriminates (skips a clean table, audits a corrupted one)
     -- i.e. the bounds are not just present but usable.
Exits non-zero and prints FAIL on any violation.
"""
import json
import os
import subprocess
import sys
import tempfile

HARNESS = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "src")
sys.path.insert(0, HARNESS)
CHECKER_PY = os.environ.get("MOR_CHECKER_PY", os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(
        os.path.dirname(os.path.abspath(__file__))))), "checker/.venv/bin/python"))

from mor_harness.adapters.base import run_driver, iceberg_columns, serialize_plan  # noqa: E402
from mor_harness.model import Checkpoint, WritePlan                                 # noqa: E402

WH = os.path.join(tempfile.gettempdir(), "mor_metrics_gate")
os.makedirs(WH, exist_ok=True)
FAILURES = []


def check(cond, msg):
    print(("  ok   " if cond else "  FAIL ") + msg)
    if not cond:
        FAILURES.append(msg)


def build_plan(name, checkpoints, mode="unsafe"):
    # NOTE: the driver decides whether to compact from plan.enforcement_mode, NOT from any config
    # passed alongside it (NOTES Entry 22). Gate 4 needs "unsafe_compact" or no rewrite runs at all.
    plan = WritePlan(checkpoints=checkpoints, key_columns=["id"], payload_columns=["val"],
                     version_column="lsn", enforcement_mode=mode)
    cols = iceberg_columns(plan)
    tdir = os.path.join(WH, "db", name)
    return serialize_plan(plan, name, tdir, WH, "lsn", cols), tdir


def run(name, checkpoints, env_extra, mode="unsafe"):
    plan_json, tdir = build_plan(name, checkpoints, mode)
    env = dict(os.environ)
    env.update(env_extra)
    old = os.environ.copy()
    os.environ.update(env_extra)
    try:
        res = run_driver("iceberg_driver.py", plan_json, os.path.join(WH, "_io", name))
    finally:
        os.environ.clear()
        os.environ.update(old)
    return res, tdir


# --- workload: 6 commits, keys 1..20, lsn ascending; ground truth kept in Python ---
def mk_checkpoints(corrupt=False):
    cks, truth = [], {}
    for c in range(1, 7):
        rows = []
        for k in range(1, 21):
            lsn = k + c * 1000
            if corrupt and c == 5 and k == 7:
                lsn = 99999          # a version that out-orders its own later survivor
            rows.append({"id": k, "val": f"k{k}c{c}", "lsn": lsn})
        truth[c] = rows
        cks.append(Checkpoint(index=c, data=rows,
                              deletes=[(k,) for k in range(1, 21)] if c > 1 else [],
                              schema_flush=False))
    return cks, truth


print("=== GATE 1-3: bounds present and CORRECT on bulk-registered files ===")
cks, truth = mk_checkpoints(corrupt=False)
res, tdir = run("gate_clean", cks, {"MOR_BULK_INGEST": "1", "MOR_AUDIT": "0"})

# read back per-file bounds through the checker's read-only adapter
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "..", "checker", "src"))
from pyiceberg.table import StaticTable          # noqa: E402
from pyiceberg.conversions import from_bytes     # noqa: E402
from mor_checker.adapters.iceberg import resolve_metadata_location  # noqa: E402

tbl = StaticTable.from_metadata(resolve_metadata_location(res["table_dir"] or tdir))
schema = tbl.schema()
fid = {n: schema.find_field(n).field_id for n in ("id", "val", "lsn")}
ftype = {n: schema.find_field(n).field_type for n in ("id", "val", "lsn")}

entries = [e for e in tbl.inspect.entries().to_pylist() if e["status"] != 2]
data_entries = [e for e in entries if e["data_file"]["content"] == 0]
del_entries = [e for e in entries if e["data_file"]["content"] == 2]
check(len(data_entries) == 6, f"6 data files registered (got {len(data_entries)})")
check(len(del_entries) == 5, f"5 equality-delete files registered (got {len(del_entries)})")

# map each data file to its commit via record content: commits are disjoint in lsn ranges
expected = {c: (min(r["lsn"] for r in rows), max(r["lsn"] for r in rows), len(rows))
            for c, rows in truth.items()}
matched = 0
for e in data_entries:
    df = e["data_file"]
    lo = {k: v for k, v in (df.get("lower_bounds") or [])}
    hi = {k: v for k, v in (df.get("upper_bounds") or [])}
    present = all(fid[n] in lo and fid[n] in hi for n in ("id", "val", "lsn"))
    check(present, f"all 3 columns have bounds in {os.path.basename(df['file_path'])}")
    if not present:
        continue
    lsn_lo = from_bytes(ftype["lsn"], lo[fid["lsn"]])
    lsn_hi = from_bytes(ftype["lsn"], hi[fid["lsn"]])
    id_lo = from_bytes(ftype["id"], lo[fid["id"]])
    id_hi = from_bytes(ftype["id"], hi[fid["id"]])
    hit = [c for c, (elo, ehi, n) in expected.items()
           if elo == lsn_lo and ehi == lsn_hi and df["record_count"] == n]
    check(bool(hit), f"lsn bounds [{lsn_lo},{lsn_hi}] + count {df['record_count']} match a commit")
    check((id_lo, id_hi) == (1, 20), f"id bounds are [1,20] (got [{id_lo},{id_hi}])")
    matched += bool(hit)
check(matched == 6, f"all 6 data files matched their commit's true min/max lsn (got {matched})")

for e in del_entries:
    df = e["data_file"]
    lo = {k: v for k, v in (df.get("lower_bounds") or [])}
    hi = {k: v for k, v in (df.get("upper_bounds") or [])}
    ok = fid["id"] in lo and fid["id"] in hi
    check(ok, f"equality-delete file has id bounds ({os.path.basename(df['file_path'])})")
    if ok:
        check((from_bytes(ftype["id"], lo[fid["id"]]),
               from_bytes(ftype["id"], hi[fid["id"]])) == (1, 20), "delete-file id bounds are [1,20]")
    check(df.get("equality_ids") == [fid["id"]], f"equality_ids == [{fid['id']}]")

print("\n=== GATE 4: bounds are USABLE -- the audit gate discriminates on bulk-registered files ===")
cks_clean, _ = mk_checkpoints(corrupt=False)
res_c, _ = run("gate_g_clean", cks_clean,
               {"MOR_BULK_INGEST": "1", "MOR_AUDIT": "1", "MOR_REWRITE_OPTS": ""},
               mode="unsafe_compact")
cks_bad, _ = mk_checkpoints(corrupt=True)
res_b, _ = run("gate_g_bad", cks_bad,
               {"MOR_BULK_INGEST": "1", "MOR_AUDIT": "1", "MOR_REWRITE_OPTS": ""},
               mode="unsafe_compact")


def gsum(res, k):
    return (res.get("audit_summary") or {}).get(k)


print(f"  clean : groups={gsum(res_c,'mor.audit.groups-total')} "
      f"gated={gsum(res_c,'mor.audit.groups-gated')} audited={gsum(res_c,'mor.audit.groups-audited')} "
      f"stale_wins={gsum(res_c,'mor.audit.stale-wins-count')}")
print(f"  corrupt: groups={gsum(res_b,'mor.audit.groups-total')} "
      f"gated={gsum(res_b,'mor.audit.groups-gated')} audited={gsum(res_b,'mor.audit.groups-audited')} "
      f"stale_wins={gsum(res_b,'mor.audit.stale-wins-count')}")
check(gsum(res_c, "mor.audit.groups-gated") == "1",
      "clean table: gate SKIPS the group (bounds prove no inversion)")
check(gsum(res_b, "mor.audit.groups-audited") == "1",
      "corrupted table: gate AUDITS the group (bounds expose the inversion)")
check(gsum(res_b, "mor.audit.stale-wins-count") == "1",
      "corrupted table: exactly 1 stale-win captured")

print("\n" + ("GATE PASSED" if not FAILURES else f"GATE FAILED ({len(FAILURES)}): {FAILURES}"))
sys.exit(1 if FAILURES else 0)
