#!/usr/bin/env python3
"""M2b: Puffin spill, and the decisive format-reachability test.

Builds a table with ~100K keys at a ~10% violation rate so the verdict crosses the 64 KB spill threshold
(~6.1 B/key => ~10K keys => ~61 KB), then:
  1. asserts the verdict actually SPILLED (not inline) and that the checker reads it back correctly;
  2. runs remove_orphan_files and asserts the REGISTERED Puffin blob SURVIVES;
  3. writes a NAIVE sidecar -- a file referenced only from a property string, never registered -- runs the
     same cleanup, and asserts it is DELETED.
(2)+(3) together demonstrate that registration, not merely writing the bytes down, is what makes the audit
record survive routine maintenance: the paper's own thesis one level up.
"""
import json
import os
import shutil
import sys
import tempfile
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "src"))
from mor_harness import check, imperfections, tpcds          # noqa: E402
from mor_harness.adapters import make_adapter                # noqa: E402
from mor_harness.batching import build_write_plan            # noqa: E402
from mor_harness.config import RunConfig                     # noqa: E402
from mor_harness.rng import SeededRng                        # noqa: E402
from mor_harness.stream import synthesize                    # noqa: E402

WH = os.path.join(tempfile.gettempdir(), "mor_spill")
shutil.rmtree(WH, ignore_errors=True)
FAIL = []


def check_(cond, msg):
    print(("  ok   " if cond else "  FAIL ") + msg)
    if not cond:
        FAIL.append(msg)


BASE = dict(keys_sampled=1.0, versions_per_key_mean=4, op_mix=(0.8, 0.15, 0.05),
            key_columns=("id",), payload_columns=("val",), ts_step_ms=1,
            commit_coarsening=1, clock_skew_ms=0, format="iceberg")

cfg = RunConfig(**{**BASE, "ooo_rate": 0.50, "dup_rate": 0.0, "schema_change_freq": 0.0,
                   "base_keys": 100_000, "seed": 101,
                   "enforcement_mode": "unsafe_compact", "keep_tables": True})
seeded = SeededRng(cfg.seed)
base = tpcds.base_customer(cfg, os.path.join(WH, "_io"))
stream = synthesize(base, cfg, seeded)
imperfections.apply(stream, cfg, seeded)
plan = build_write_plan(stream, cfg, seeded)
os.environ["MOR_BULK_INGEST"] = "1"
os.environ["MOR_AUDIT"] = "1"
os.environ["MOR_AUDIT_CROSS_GROUP"] = "0"
os.environ["MOR_REWRITE_OPTS"] = "audit-gate=false"     # force capture; the workload is disordered anyway

t0 = time.time()
res = make_adapter(cfg.format).apply(plan, "spill", os.path.join(WH, "db", "spill"), WH,
                                     cfg.precombine_field(), os.path.join(WH, "_io", "spill"))
print(f"\nbuilt in {time.time()-t0:.0f}s; compaction {res.stats['compact_time_s']}s")

summ = res.audit_summary or {}
kcols, vcol = stream.key_columns, stream.version_column
oracle = check.oracle_verdicts(res.materialized, stream.truth, kcols, vcol)
oracle_stale = {k for k, v in oracle.items() if v == check.OracleVerdict.STALE_WINS}
captured = {tuple(k) for k in json.loads(summ.get("mor.audit.stale-wins-keys", "[]"))}

print("\n=== 1. spill happened, and the checker reads it back ===")
print(f"  oracle STALE_WINS={len(oracle_stale)}  verdict count prop={summ.get('mor.audit.stale-wins-count')}")
check_(summ.get("mor.audit.stale-wins-keys-spilled") == "true",
       f"verdict SPILLED (flag={summ.get('mor.audit.stale-wins-keys-spilled')})")
check_(summ.get("mor.audit.spill-source") == "puffin-statistics-file",
       f"read back via the registered statistics file (source={summ.get('mor.audit.spill-source')})")
check_(not summ.get("mor.audit.spill-read-error"), f"no spill read error ({summ.get('mor.audit.spill-read-error')})")
check_(captured == oracle_stale,
       f"spilled verdict == oracle ({len(captured)} vs {len(oracle_stale)}, "
       f"FP={len(captured-oracle_stale)} miss={len(oracle_stale-captured)})")
est = len(json.dumps([list(k) for k in sorted(captured)]))
print(f"  verdict JSON ~{est} bytes ({est/max(1,len(captured)):.1f} B/key), threshold 65536")

TBL = res.table_dir or os.path.join(WH, "db", "spill")
meta = os.path.join(TBL, "metadata")
puffins = [f for f in os.listdir(meta) if f.endswith(".puffin")]
check_(len(puffins) == 1, f"exactly one puffin blob written: {puffins}")

# ---- 2 + 3: the reachability pair ----
naive = os.path.join(meta, "naive-audit-sidecar.json")
with open(naive, "w") as f:
    json.dump([list(k) for k in sorted(captured)], f)
print(f"\n=== 2+3. remove_orphan_files: registered blob vs naive sidecar ===")
print(f"  before: puffin={puffins[0][:40]}... exists={os.path.exists(os.path.join(meta, puffins[0]))}"
      f"  naive sidecar exists={os.path.exists(naive)}")

os.environ["MOR_ORPHAN_TABLE_DIR"] = TBL
os.environ["MOR_ORPHAN_NAIVE"] = naive
rc = os.system(
    f'JAVA_HOME={os.environ.get("JAVA_HOME","")} '
    f'MOR_ICEBERG_JAR={os.environ.get("MOR_ICEBERG_JAR","")} '
    f'{sys.executable} '
    f'{os.path.join(os.path.dirname(os.path.abspath(__file__)), "run_orphan_cleanup.py")} '
    f'"{TBL}" "{naive}" > /tmp/orphan_out.txt 2>&1')
print(open("/tmp/orphan_out.txt").read().strip()[-1500:])

puffin_alive = os.path.exists(os.path.join(meta, puffins[0]))
naive_alive = os.path.exists(naive)
check_(puffin_alive, "REGISTERED puffin blob SURVIVES remove_orphan_files")
check_(not naive_alive, "NAIVE sidecar (path only in a property string) is DELETED by remove_orphan_files")

print("\n" + ("ALL PASS" if not FAIL else f"FAILURES ({len(FAIL)}): {FAIL}"))
sys.exit(1 if FAIL else 0)
