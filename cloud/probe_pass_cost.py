#!/usr/bin/env python3
"""Where does the second traversal's cost actually live?

Capture costs ~1.9x the stock rewrite, and the audited path consumes the marked scan twice. That is
close enough to double materialisation to look like a complete explanation, but it is not one: it does
not say WHICH part of the traversal is being paid for twice. Two candidates, with opposite
implications:

  PAYLOAD I/O. The aggregation touches only the key, the ordering column and _deleted. If Catalyst
  prunes the projection for that branch, the second traversal reads three columns rather than the whole
  400-byte row, and should be far cheaper than the write. If so, ~1.9x cannot be explained by the
  second pass alone and something else dominates.

  DELETE-SET RECONSTRUCTION. Applying the equality deletes has to happen on both traversals regardless
  of which columns survive pruning. If that is the expensive part, the two passes cost nearly the same
  no matter how few columns the second one reads, and fusing them would recover most of the overhead.

This measures the two branches in isolation at the same scale as the cost experiment, so the numbers
are directly comparable to it. It writes nothing to the table and does not compact: each arm is a read
with one action, so nothing here is destructive and the arms can share a table.

It also dumps the physical plan of the aggregation branch, because "did pruning happen" is answerable
directly rather than by inference from a timing.
"""
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import RESULTS, WAREHOUSE, emit, hostinfo, preflight  # noqa: E402

_REPO = os.environ.get("MOR_REPO", "/opt/mor/mor-faithfulness")
sys.path.insert(0, os.path.join(_REPO, "cost-study/src"))
from mor_harness.adapters.base import run_driver, serialize_plan  # noqa: E402
from mor_harness.model import WritePlan                            # noqa: E402

# Same shape as exp1 so the timings sit alongside its 1.91x.
COMMITS = int(os.environ.get("MOR_PROBE_COMMITS", "32"))
RPC = int(os.environ.get("MOR_PROBE_RPC", "3600000"))
FPC = int(os.environ.get("MOR_PROBE_FPC", "4"))
PAYLOAD = 400
HEAP = os.environ.get("MOR_PROBE_HEAP", "32g")
TABLE = os.environ.get("MOR_PROBE_TABLE", "probe_pass")


def drop_caches():
    os.sync()
    try:
        with open("/proc/sys/vm/drop_caches", "w") as f:
            f.write("3\n")
        return True
    except PermissionError:
        return False


p = preflight("probe", COMMITS, RPC, FPC, PAYLOAD)
print(f"probe: {p['rows_total']:,} rows, ~{p['bytes_total']/2**30:.1f} GB, heap {HEAP}", flush=True)
print(f"host: {hostinfo()}", flush=True)

# Build the table in a separate driver process, exactly as the cost experiment builds its own, but
# WITHOUT compacting -- enforcement_mode outside {safe,unsafe}_compact skips the rewrite. The table is
# deliberately left on disk so the read arms below measure the same bytes the audited rewrite would.
TDIR = os.path.join(WAREHOUSE, "db", TABLE)
if not os.path.isdir(TDIR):
    print(f"building {TABLE} (uncompacted) ...", flush=True)
    plan = WritePlan(checkpoints=[], key_columns=["id"], payload_columns=["val"],
                     version_column="lsn", enforcement_mode="unsafe")
    pj = serialize_plan(plan, TABLE, TDIR, WAREHOUSE, "lsn",
                        [{"name": "id", "type": "int"}, {"name": "val", "type": "string"},
                         {"name": "lsn", "type": "int"}])
    pj["synth"] = {"commits": COMMITS, "rows_per_commit": RPC, "payload_bytes": PAYLOAD,
                   "delete_frac": 0.2, "ordering": "contiguous", "files_per_commit": FPC}
    os.environ.update({"MOR_BULK_INGEST": "1", "MOR_AUDIT": "0", "MOR_DRIVER_MEM": HEAP})
    t0 = time.time()
    run_driver("iceberg_driver.py", pj, os.path.join(WAREHOUSE, "_io", TABLE))
    print(f"  built in {time.time()-t0:.0f}s", flush=True)

os.environ.setdefault("PYSPARK_SUBMIT_ARGS", f"--driver-memory {HEAP} pyspark-shell")
from pyspark.sql import SparkSession, functions as F  # noqa: E402

jar = os.environ["MOR_ICEBERG_JAR"]
spark = (SparkSession.builder.appName("probe_pass_cost")
         .config("spark.jars", jar)
         .config("spark.sql.extensions",
                 "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
         .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
         .config("spark.sql.catalog.local.type", "hadoop")
         .config("spark.sql.catalog.local.warehouse", WAREHOUSE)
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

tbl = f"local.db.{TABLE}"
n = spark.sql(f"SELECT count(*) c FROM {tbl}").collect()[0]["c"]
print(f"table {tbl}: {n:,} live rows", flush=True)

marked = spark.read.format("iceberg").load(tbl).select(
    "*", F.col("_deleted").alias("_del"))

def _noop(df):
    """Force full materialisation in the JVM and discard the rows.

    NOT `foreach(lambda ...)`: a Python lambda ships every row to a Python worker, so the timing would
    be dominated by serialisation round-trips that the real write path never pays. The write branch we
    are modelling is entirely JVM-side, so the probe has to be too.
    """
    df.write.format("noop").mode("overwrite").save()


ARMS = {
    # three columns only: what the aggregation branch needs. Already JVM-only -- no Python UDF.
    "aggregate_only": lambda: marked.groupBy("id").agg(
        F.max(F.when(F.col("_del"), F.col("lsn"))).alias("dmax"),
        F.max(F.when(~F.col("_del"), F.col("lsn"))).alias("smax"),
        F.count(F.when(~F.col("_del"), F.lit(1))).alias("scnt")).write.format("noop").mode(
            "overwrite").save(),
    # every column: what the write branch has to materialise, delete filter applied
    "full_scan": lambda: _noop(marked.where(~F.col("_del")).select("id", "val", "lsn")),
    # key+ordering only, delete filter still applied: separates column width from delete application
    "narrow_scan": lambda: _noop(marked.where(~F.col("_del")).select("id", "lsn")),
    # no delete marking at all: the floor, what a plain columnar read of two columns costs
    "no_deletes": lambda: _noop(spark.read.format("iceberg").load(tbl).select("id", "lsn")),
}

out = {}
for label, fn in ARMS.items():
    cold = drop_caches()
    t0 = time.time()
    fn()
    dt = time.time() - t0
    out[label] = round(dt, 2)
    print(f"  {label:16} {dt:8.2f}s   (cache dropped: {cold})", flush=True)

print("\n=== physical plan of the aggregation branch (is the projection pruned?) ===", flush=True)
plan = marked.groupBy("id").agg(
    F.max(F.when(F.col("_del"), F.col("lsn"))).alias("dmax"))._jdf.queryExecution().toString()
out["aggregate_plan"] = plan[:4000]
for ln in plan.splitlines():
    if any(k in ln for k in ("ReadSchema", "BatchScan", "Project", "Scan ")):
        print("  " + ln.strip()[:200], flush=True)

agg = out.get("aggregate_only", 0); full = out.get("full_scan", 0)
narrow = out.get("narrow_scan", 0); nodel = out.get("no_deletes", 0)
print("\n" + "=" * 84)
# Consult the floor arm. An earlier version compared only aggregate/full and concluded that
# delete-set reconstruction dominated whenever that ratio was high -- which is unsound, because a
# high ratio is equally consistent with the aggregation paying for a shuffle. The no_deletes arm
# exists precisely to tell those apart, so the verdict has to use it.
if narrow and nodel:
    dcost = narrow - nodel
    print(f"delete application: {narrow:.1f}s with vs {nodel:.1f}s without = {dcost:+.1f}s")
    if abs(dcost) < 0.15 * nodel:
        print("  => applying the equality deletes is FREE within noise. It is not what the second")
        print("     traversal is paying for, and fusing the passes would not recover it.")
    else:
        print("  => delete application is a real cost and fusing would recover it.")
if agg and narrow:
    print(f"aggregation over a comparable scan: {agg:.1f}s vs {narrow:.1f}s = {agg-narrow:+.1f}s")
    print("  => that delta is the group-by shuffle, not column width or delete handling.")
if full and narrow:
    print(f"payload width: {full:.1f}s (all cols) vs {narrow:.1f}s (two cols) = {full-narrow:+.1f}s")
print()
print("CAVEAT, and it bounds what any of this supports: these are plain table reads with a noop sink.")
print("The audited rewrite reads through a scan-task-set data source and also WRITES files, so its")
print("stock baseline is far larger than full_scan here. Do not attribute the measured overhead ratio")
print("to this decomposition without measuring the rewrite path itself.")
emit("probe_pass_cost.json", {"timings_s": out, "host": hostinfo(), "config":
                              {"commits": COMMITS, "rows_per_commit": RPC, "heap": HEAP}})
spark.stop()
