#!/usr/bin/env python3
"""Bulk-ingest rework: (a) semantic EQUIVALENCE to the per-record path, (b) the speedup.

Runs cell ooo50_sf1_s101 both ways and compares what must not change -- the engine oracle's verdicts, the
materialized content, and the audit's captured stale-wins set -- then reports ingest rows/s for each.
"""
import json
import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "src"))
from mor_harness import check, imperfections, tpcds          # noqa: E402
from mor_harness.adapters import make_adapter                # noqa: E402
from mor_harness.batching import build_write_plan            # noqa: E402
from mor_harness.config import RunConfig                     # noqa: E402
from mor_harness.rng import SeededRng                        # noqa: E402
from mor_harness.stream import synthesize                    # noqa: E402

WH = os.path.join(tempfile.gettempdir(), "mor_bulk_equiv")
BASE = dict(keys_sampled=1.0, versions_per_key_mean=4, op_mix=(0.8, 0.15, 0.05),
            key_columns=("id",), payload_columns=("val",), ts_step_ms=1,
            commit_coarsening=1, clock_skew_ms=0, format="iceberg")
KNOBS = dict(ooo_rate=0.50, dup_rate=0.0, schema_change_freq=0.0)


def run(label, bulk):
    cfg = RunConfig(**{**BASE, **KNOBS, "base_keys": 1200, "seed": 101,
                       "enforcement_mode": "unsafe_compact", "keep_tables": False})
    seeded = SeededRng(cfg.seed)
    base = tpcds.base_customer(cfg, os.path.join(WH, "_io"))
    stream = synthesize(base, cfg, seeded)
    imperfections.apply(stream, cfg, seeded)
    plan = build_write_plan(stream, cfg, seeded)
    os.environ["MOR_BULK_INGEST"] = "1" if bulk else "0"
    os.environ["MOR_AUDIT"] = "1"
    os.environ["MOR_AUDIT_CROSS_GROUP"] = "0"
    os.environ["MOR_REWRITE_OPTS"] = ""
    res = make_adapter(cfg.format).apply(
        plan, label, os.path.join(WH, "db", label), WH, cfg.precombine_field(),
        os.path.join(WH, "_io", label))
    kcols, vcol = stream.key_columns, stream.version_column
    oracle = check.oracle_verdicts(res.materialized, stream.truth, kcols, vcol)
    summ = res.audit_summary or {}
    captured = {tuple(k) for k in json.loads(summ.get("mor.audit.stale-wins-keys", "[]"))}
    content = {tuple(r[c] for c in kcols): r.get(vcol) for r in res.materialized}
    n_rows = sum(len(c.data) for c in plan.checkpoints)
    return {
        "oracle_tally": {v.value: sum(1 for x in oracle.values() if x == v)
                         for v in set(oracle.values())},
        "stale": {k for k, v in oracle.items() if v == check.OracleVerdict.STALE_WINS},
        "captured": captured, "content": content,
        "apply_s": res.stats["apply_time_s"], "rows_written": n_rows,
        "data_files": res.stats["data_files"], "delete_files": res.stats["delete_files"],
    }


a = run("perrecord", False)
b = run("bulk", True)

print("=== semantic equivalence (per-record vs bulk) ===")
print(f"  oracle tally      : {a['oracle_tally']}  vs  {b['oracle_tally']}")
print(f"  oracle STALE_WINS : {len(a['stale'])} vs {len(b['stale'])}  identical={a['stale']==b['stale']}")
print(f"  captured verdict  : {len(a['captured'])} vs {len(b['captured'])}  "
      f"identical={a['captured']==b['captured']}")
print(f"  materialized rows : {len(a['content'])} vs {len(b['content'])}  "
      f"identical={a['content']==b['content']}")
print(f"  files after       : data {a['data_files']}/{b['data_files']} "
      f"delete {a['delete_files']}/{b['delete_files']}")

print("\n=== ingest throughput ===")
for tag, r in (("per-record", a), ("bulk", b)):
    print(f"  {tag:11} {r['rows_written']:6d} rows in {r['apply_s']:7.2f}s "
          f"= {r['rows_written']/r['apply_s']:8.0f} rows/s")
print(f"  speedup: {(a['apply_s']/b['apply_s']):.1f}x")

ok = (a["stale"] == b["stale"] and a["captured"] == b["captured"] and a["content"] == b["content"])
print(f"\nEQUIVALENT: {ok}")
sys.exit(0 if ok else 1)
