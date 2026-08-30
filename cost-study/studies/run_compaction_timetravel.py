"""Does compaction ERASE the evidence, or only hide it from the current snapshot?

`run_compaction_masking_sweep.py` shows every checker STALE_WINS verdict becomes FAITHFUL
after `rewrite_data_files`. That is measured against the table's *current* snapshot, which is
what an audit reads by default. But `rewrite_data_files` commits a NEW snapshot; the previous
one, and the data files it references, remain in the table until `expire_snapshots` drops them.
So the honest objection is: the evidence is not erased, it is one time-travel query away.

This script measures that directly, over three observation states:

  S1  pre-compaction                        current snapshot = the violating layout
  S2  after rewrite_data_files              current snapshot laundered, history RETAINED
  S3  after expire_snapshots                history dropped, unreferenced files deleted

and at S2 and S3 it runs the checker twice: once on the table's current metadata (what an
auditor sees) and once on the S1 metadata file (an auditor who knows to time-travel). No
checker changes are needed: `IcebergAdapter` resolves a metadata location and opens a
read-only `StaticTable`, so handing it the pre-compaction `vN.metadata.json` is exactly a
time-travel read through the existing read path.

Expected shape of the result, and the reason the two-stage framing matters:

  S1 current      -> STALE_WINS            (violation visible)
  S2 current      -> FAITHFUL              (laundered from the default audit)
  S2 time-travel  -> STALE_WINS            (evidence still recoverable)
  S3 current      -> FAITHFUL              (laundered)
  S3 time-travel  -> unreadable            (evidence irrecoverable)

If that holds, `rewrite_data_files` launders current-snapshot audits and `expire_snapshots`
makes the laundering permanent, which is a stronger claim than "compaction erases evidence"
and is not vulnerable to the time-travel objection.

Usage: JAVA_HOME=<jdk17> PYTHONPATH=src python studies/run_compaction_timetravel.py [cell ...]
Emits results/compaction_timetravel.json.
"""

import dataclasses
import json
import os
import subprocess
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
sys.path.insert(0, os.path.dirname(__file__))

from mor_harness import check, imperfections, tpcds                 # noqa: E402
from mor_harness.adapters import make_adapter, spark_env            # noqa: E402
from mor_harness.batching import build_write_plan                   # noqa: E402
from mor_harness.config import RunConfig                            # noqa: E402
from mor_harness.rng import SeededRng                               # noqa: E402
from mor_harness.stream import synthesize                           # noqa: E402
from run_cost import HARNESS                                        # noqa: E402
from mor_checker.adapters.iceberg import (                          # noqa: E402
    IcebergAdapter, resolve_metadata_location,
)
from mor_checker.core.classify import classify                      # noqa: E402

WH = os.environ.get("MOR_TT_WH",
                    os.path.join(tempfile.gettempdir(), "mor_harness", "timetravel_wh"))

BASE = dict(keys_sampled=1.0, versions_per_key_mean=4, op_mix=(0.8, 0.15, 0.05),
            key_columns=("id",), payload_columns=("val",), ts_step_ms=1,
            commit_coarsening=1, clock_skew_ms=0, format="iceberg",
            enforcement_mode="unsafe", keep_tables=True)

CELLS = {
    "ooo50_sf1_s101":  (1200, 101, dict(ooo_rate=0.50, dup_rate=0.0,  schema_change_freq=0.0)),
    "ooo50_sf1_s202":  (1200, 202, dict(ooo_rate=0.50, dup_rate=0.0,  schema_change_freq=0.0)),
    "ooo50_sf10_s101": (4000, 101, dict(ooo_rate=0.50, dup_rate=0.0,  schema_change_freq=0.0)),
    "mixed_sf1_s101":  (1200, 101, dict(ooo_rate=0.50, dup_rate=0.15, schema_change_freq=0.0)),
}

# Spark maintenance ops, run in a fresh JVM against the already-written table.
SPARK_OP = r'''
import sys, json
from pyspark.sql import SparkSession
WAREHOUSE, NAME, OP, IVY, ADD_OPENS = sys.argv[1:6]
spark = (SparkSession.builder.appName("mor-tt").master("local[2]")
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1")
    .config("spark.jars.ivy", IVY)
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", WAREHOUSE)
    .config("spark.sql.catalog.local.cache-enabled", "false")
    .config("spark.sql.catalogImplementation", "in-memory")
    .config("spark.driver.host", "localhost").config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "1")
    .config("spark.driver.extraJavaOptions", ADD_OPENS)
    .config("spark.executor.extraJavaOptions", ADD_OPENS)
    .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
if OP == "rewrite":
    spark.sql("CALL local.system.rewrite_data_files(table => 'db.%s')" % NAME)
elif OP == "expire":
    # Drop every snapshot but the current one and delete the files they alone referenced.
    spark.sql("CALL local.system.expire_snapshots("
              "table => 'db.%s', older_than => TIMESTAMP '2100-01-01 00:00:00', "
              "retain_last => 1)" % NAME)
n = spark.sql("SELECT count(*) c FROM local.db.%s.snapshots" % NAME).collect()[0]["c"]
print("SNAPSHOTS=%d" % n)
spark.stop()
'''


def spark_op(name, op):
    script = os.path.join(WH, f"_op_{op}.py")
    os.makedirs(WH, exist_ok=True)
    with open(script, "w") as f:
        f.write(SPARK_OP)
    r = subprocess.run([sys.executable, script, WH, name, op,
                        spark_env.resolve_ivy(), spark_env.add_opens()],
                       capture_output=True, text=True, env=spark_env.subprocess_env())
    if r.returncode != 0:
        raise RuntimeError(f"spark {op} failed:\n{r.stdout[-3000:]}\n{r.stderr[-3000:]}")
    n = [l for l in r.stdout.splitlines() if l.startswith("SNAPSHOTS=")]
    return int(n[-1].split("=")[1]) if n else None


def current_metadata(tdir):
    """The table's current metadata file. Delegates to the checker's own resolver, which
    orders vN.metadata.json numerically; a plain lexicographic sort puts v9 after v52."""
    return resolve_metadata_location(tdir)


def verdicts(source, kcols, vcol):
    """Run the checker over one metadata location. Returns (tally, error)."""
    try:
        ad = IcebergAdapter(source, key_columns=list(kcols), version_column=vcol,
                            upsert_only=False)
        t = {}
        for _, lay in ad.layouts().items():
            v = classify(lay).value
            t[v] = t.get(v, 0) + 1
        return t, None
    except Exception as e:  # evidence gone: files the old snapshot referenced are deleted
        return None, f"{type(e).__name__}: {str(e)[:200]}"


def run_cell(label):
    base_keys, seed, knobs = CELLS[label]
    cfg = RunConfig(**{**BASE, **knobs, "base_keys": base_keys, "seed": seed})
    print(f"\n===== cell {label} (base_keys={base_keys}, seed={seed}, {knobs}) =====",
          flush=True)

    seeded = SeededRng(cfg.seed)
    base = tpcds.base_customer(cfg, os.path.join(WH, "_io"))
    stream = synthesize(base, cfg, seeded)
    imperfections.apply(stream, cfg, seeded)
    plan = build_write_plan(stream, cfg, seeded)
    name = f"tt_{label}"
    tdir = os.path.join(WH, "db", name)
    res = make_adapter(cfg.format).apply(plan, name, tdir, WH, cfg.precombine_field(),
                                         os.path.join(WH, "_io", name))
    kcols, vcol = stream.key_columns, stream.version_column
    oracle = check.oracle_verdicts(res.materialized, stream.truth, kcols, vcol)
    oracle_viol = sum(1 for v in oracle.values() if v in check.VIOLATIONS)
    tdir = res.table_dir or tdir

    # ---- S1: pre-compaction ------------------------------------------------------
    s1_meta = current_metadata(tdir)
    s1_cur, _ = verdicts(s1_meta, kcols, vcol)
    print(f"  S1 current      {json.dumps(s1_cur)}", flush=True)

    # ---- S2: after rewrite_data_files, history retained ---------------------------
    n_snap_s2 = spark_op(name, "rewrite")
    s2_cur, _ = verdicts(tdir, kcols, vcol)
    s2_tt, s2_tt_err = verdicts(s1_meta, kcols, vcol)
    print(f"  S2 current      {json.dumps(s2_cur)}   (snapshots={n_snap_s2})", flush=True)
    print(f"  S2 time-travel  {json.dumps(s2_tt) if s2_tt else 'UNREADABLE: ' + str(s2_tt_err)}",
          flush=True)

    # ---- S3: after expire_snapshots ----------------------------------------------
    n_snap_s3 = spark_op(name, "expire")
    s3_cur, _ = verdicts(tdir, kcols, vcol)
    s3_tt, s3_tt_err = verdicts(s1_meta, kcols, vcol)
    print(f"  S3 current      {json.dumps(s3_cur)}   (snapshots={n_snap_s3})", flush=True)
    print(f"  S3 time-travel  {json.dumps(s3_tt) if s3_tt else 'UNREADABLE: ' + str(s3_tt_err)}",
          flush=True)

    sw1 = (s1_cur or {}).get("STALE_WINS", 0)
    out = {
        "config": {"base_keys": base_keys, "seed": seed, **knobs},
        "oracle_violations": oracle_viol,
        "s1_pre_compaction": {"metadata": os.path.basename(s1_meta), "verdicts": s1_cur},
        "s2_after_rewrite": {
            "snapshots_in_table": n_snap_s2,
            "current_verdicts": s2_cur,
            "timetravel_verdicts": s2_tt,
            "timetravel_error": s2_tt_err,
            "stale_wins_recoverable_by_timetravel": (s2_tt or {}).get("STALE_WINS", 0),
        },
        "s3_after_expire": {
            "snapshots_in_table": n_snap_s3,
            "current_verdicts": s3_cur,
            "timetravel_verdicts": s3_tt,
            "timetravel_error": s3_tt_err,
            "s1_metadata_file_still_on_disk": os.path.exists(s1_meta),
            "stale_wins_recoverable_by_timetravel": (s3_tt or {}).get("STALE_WINS", 0),
        },
        "stale_wins_s1": sw1,
        "stale_wins_masked_from_current_s2": sw1 - (s2_cur or {}).get("STALE_WINS", 0),
    }
    return out


def main(labels):
    out = {"what": ("Iceberg compaction: is the evidence erased, or only hidden from the "
                    "current snapshot until expire_snapshots?"),
           "observation_states": {
               "S1": "pre-compaction current snapshot",
               "S2": "after rewrite_data_files, snapshot history retained",
               "S3": "after expire_snapshots (retain_last=1)"},
           "method": ("The checker is unchanged; time travel is performed by pointing its "
                      "read-only StaticTable at the pre-compaction vN.metadata.json. Oracle "
                      "violation counts come from the engine readback."),
           "cells": {}}
    for label in labels:
        out["cells"][label] = run_cell(label)

    c = out["cells"].values()
    out["totals"] = {
        "cells": len(out["cells"]),
        "stale_wins_s1": sum(x["stale_wins_s1"] for x in c),
        "stale_wins_masked_from_current_after_rewrite":
            sum(x["stale_wins_masked_from_current_s2"] for x in c),
        "stale_wins_recoverable_by_timetravel_at_s2":
            sum(x["s2_after_rewrite"]["stale_wins_recoverable_by_timetravel"] for x in c),
        "stale_wins_recoverable_by_timetravel_at_s3":
            sum(x["s3_after_expire"]["stale_wins_recoverable_by_timetravel"] for x in c),
        "timetravel_unreadable_at_s3":
            all(x["s3_after_expire"]["timetravel_verdicts"] is None for x in c),
    }
    print("\n================ TOTALS ================")
    print(json.dumps(out["totals"], indent=1))
    dst = os.path.join(HARNESS, "results", "compaction_timetravel.json")
    with open(dst, "w") as f:
        json.dump(out, f, indent=1)
        f.write("\n")
    print(f"\nevidence -> {dst}")


if __name__ == "__main__":
    sel = sys.argv[1:] or list(CELLS)
    bad = [s for s in sel if s not in CELLS]
    if bad:
        sys.exit(f"unknown cell(s) {bad}; known: {list(CELLS)}")
    main(sel)
