#!/usr/bin/env python3
"""Overhead benchmark for the compaction-time stale-wins audit.

Three arms on an identical workload (cell ooo50_sf1_s101), N fresh-JVM repeats each:
  off    MOR_AUDIT=0                       -- forked jar, stock rewrite path (the control)
  base   audit-stale-wins=true             -- per-group verdict, metadata gate ON
  cross  + audit-cross-group=true          -- table-level merge (gate forced off)

Each repeat runs in its own Spark subprocess (mor_harness spawns one per apply()), so every repeat is a
fresh JVM with no warm code cache carried across arms.

Reported per arm: compaction wall time (the cost the audit adds), ingest apply time (a CONTROL -- the
audit does not touch the write path, so this should not move), and derived compaction throughput
(rows/s). Also a verdict-size sweep: bytes of the persisted verdict as a fraction of table bytes across
violation rates (ooo_rate).

Usage: bench_audit_overhead.py [repeats]      (default 10)
"""
import json
import os
import statistics
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "src"))
from mor_harness import check, imperfections, tpcds          # noqa: E402
from mor_harness.adapters import make_adapter                # noqa: E402
from mor_harness.batching import build_write_plan            # noqa: E402
from mor_harness.config import RunConfig                     # noqa: E402
from mor_harness.rng import SeededRng                        # noqa: E402
from mor_harness.stream import synthesize                    # noqa: E402

WH = os.path.join(tempfile.gettempdir(), "mor_bench")
BASE = dict(keys_sampled=1.0, versions_per_key_mean=4, op_mix=(0.8, 0.15, 0.05),
            key_columns=("id",), payload_columns=("val",), ts_step_ms=1,
            commit_coarsening=1, clock_skew_ms=0, format="iceberg")
REPEATS = int(sys.argv[1]) if len(sys.argv) > 1 else 10
BASE_KEYS = int(sys.argv[2]) if len(sys.argv) > 2 else 1200
SKIP_VERDICT_SWEEP = os.environ.get("BENCH_SKIP_VERDICT_SWEEP") == "1"
ARMS = [("off", False, False), ("base", True, False), ("cross", True, True)]


def one_run(label, ooo_rate, audit, cross):
    cfg = RunConfig(**{**BASE, "ooo_rate": ooo_rate, "dup_rate": 0.0, "schema_change_freq": 0.0,
                       "base_keys": BASE_KEYS, "seed": 101,
                       "enforcement_mode": "unsafe_compact", "keep_tables": False})
    seeded = SeededRng(cfg.seed)
    base = tpcds.base_customer(cfg, os.path.join(WH, "_io"))
    stream = synthesize(base, cfg, seeded)
    imperfections.apply(stream, cfg, seeded)
    plan = build_write_plan(stream, cfg, seeded)
    os.environ["MOR_AUDIT"] = "1" if audit else "0"
    os.environ["MOR_AUDIT_CROSS_GROUP"] = "1" if cross else "0"
    os.environ["MOR_REWRITE_OPTS"] = ""
    res = make_adapter(cfg.format).apply(
        plan, label, os.path.join(WH, "db", label), WH, cfg.precombine_field(),
        os.path.join(WH, "_io", label))
    kcols, vcol = stream.key_columns, stream.version_column
    oracle = check.oracle_verdicts(res.materialized, stream.truth, kcols, vcol)
    n_stale = sum(1 for v in oracle.values() if v == check.OracleVerdict.STALE_WINS)
    summ = res.audit_summary or {}
    prop = "mor.audit.cross-group-keys" if cross else "mor.audit.stale-wins-keys"
    verdict_json = summ.get(prop, "")
    return {
        "compact_s": res.stats.get("compact_time_s"),
        "apply_s": res.stats.get("apply_time_s"),
        "rows": len(res.materialized),
        "bytes_total": res.stats.get("bytes_total"),
        "verdict_bytes": len(verdict_json.encode()),
        "verdict_keys": len(json.loads(verdict_json)) if verdict_json else 0,
        "oracle_stale": n_stale,
        "n_keys": len(oracle),
    }


def summarize(vals):
    return {
        "median": round(statistics.median(vals), 3),
        "mean": round(statistics.mean(vals), 3),
        "stdev": round(statistics.stdev(vals), 3) if len(vals) > 1 else 0.0,
        "min": round(min(vals), 3), "max": round(max(vals), 3),
    }


def main():
    out = {"repeats": REPEATS, "base_keys": BASE_KEYS, "cell": "ooo50_s101", "arms": {}, "verdict_size": []}

    print(f"=== overhead: {REPEATS} fresh-JVM repeats per arm (cell ooo50_sf1_s101) ===", flush=True)
    for arm, audit, cross in ARMS:
        runs = []
        for i in range(REPEATS):
            r = one_run(f"bench_{arm}_{i}", 0.50, audit, cross)
            runs.append(r)
            print(f"  {arm:6} rep{i:02d} compact={r['compact_s']:7.3f}s apply={r['apply_s']:8.3f}s",
                  flush=True)
        out["arms"][arm] = {
            "compact_s": summarize([r["compact_s"] for r in runs]),
            "apply_s": summarize([r["apply_s"] for r in runs]),
            "rows": runs[0]["rows"],
            "compact_rows_per_s": round(runs[0]["rows"] / statistics.median(
                [r["compact_s"] for r in runs]), 1),
            "verdict_keys": runs[0]["verdict_keys"],
            "verdict_bytes": runs[0]["verdict_bytes"],
            "runs": runs,
        }

    base_med = out["arms"]["off"]["compact_s"]["median"]
    print("\n=== compaction overhead vs flag-off ===")
    for arm in ("off", "base", "cross"):
        m = out["arms"][arm]["compact_s"]["median"]
        print(f"  {arm:6} median={m:7.3f}s  stdev={out['arms'][arm]['compact_s']['stdev']:6.3f}  "
              f"overhead={((m / base_med - 1) * 100):+7.1f}%")
    print("  apply (ingest) times -- CONTROL, audit must not touch the write path:")
    for arm in ("off", "base", "cross"):
        a = out["arms"][arm]["apply_s"]
        print(f"    {arm:6} median={a['median']:8.3f}s stdev={a['stdev']:6.3f}")

    if SKIP_VERDICT_SWEEP:
        dst = os.path.join(os.path.dirname(__file__), f"bench_audit_overhead_{BASE_KEYS}.json")
        with open(dst, "w") as f:
            json.dump(out, f, indent=1)
        print(f"\nevidence -> {dst}")
        return

    # ---- verdict size as a fraction of table size, across violation rates ----
    print("\n=== verdict size vs table size, across violation rates ===")
    print(f"  {'ooo_rate':>9} {'stale_wins':>11} {'viol_rate':>10} {'verdict_B':>10} "
          f"{'table_B':>10} {'fraction':>12}")
    for rate in (0.0, 0.05, 0.10, 0.25, 0.50):
        r = one_run(f"bench_vs_{int(rate * 100)}", rate, True, False)
        frac = r["verdict_bytes"] / r["bytes_total"] if r["bytes_total"] else 0.0
        row = {"ooo_rate": rate, "oracle_stale": r["oracle_stale"], "n_keys": r["n_keys"],
               "violation_rate": round(r["oracle_stale"] / r["n_keys"], 4) if r["n_keys"] else 0,
               "verdict_bytes": r["verdict_bytes"], "verdict_keys": r["verdict_keys"],
               "table_bytes": r["bytes_total"], "fraction_of_table": round(frac, 8)}
        out["verdict_size"].append(row)
        print(f"  {rate:9.2f} {r['oracle_stale']:11d} {row['violation_rate']:10.4f} "
              f"{r['verdict_bytes']:10d} {r['bytes_total']:10d} {frac:12.6%}", flush=True)

    dst = os.path.join(os.path.dirname(__file__), "bench_audit_overhead.json")
    with open(dst, "w") as f:
        json.dump(out, f, indent=1)
    print(f"\nevidence -> {dst}")


if __name__ == "__main__":
    main()
