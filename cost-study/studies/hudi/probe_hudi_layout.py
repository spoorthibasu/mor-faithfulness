#!/usr/bin/env python3
"""Phase 6 probe 1: what does Hudi MOR physically keep, and what does compaction remove?

Iceberg's checker works because every version is a physical data record and the delete files mark
suppression, so a reader sees survivor AND losers. The Hudi analogue depends on where losing versions
live: in-batch preCombine resolves them in memory (never persisted), while across delta commits they
land in .log files and survive until compaction merges them into a new base file.

So this writes ONE DELTA COMMIT PER VERSION (not the harness's single bulk upsert) and inspects, before
and after compaction:
  - file layout: base .parquet vs .log files, per file group
  - read_optimized query (base files of the current slice only)
  - snapshot query (base + log merged -> one row per key)
  - direct spark.read.parquet on the base files (what is physically in base)
No assumptions: everything below is read off the table.
"""
import glob
import os
import shutil
import sys

WH = sys.argv[1]
NKEYS = 50
NVERSIONS = 6
shutil.rmtree(WH, ignore_errors=True)
os.makedirs(WH, exist_ok=True)
TBL = os.path.join(WH, "h_mor")

ADD_OPENS = " ".join(
    f"--add-opens=java.base/{p}=ALL-UNNAMED"
    for p in ["java.lang", "java.lang.invoke", "java.lang.reflect", "java.io", "java.net", "java.nio",
              "java.util", "java.util.concurrent", "java.util.concurrent.atomic", "sun.nio.ch",
              "sun.nio.cs", "sun.security.action", "sun.util.calendar"]
) + " --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"
os.environ.setdefault("PYSPARK_SUBMIT_ARGS", "--driver-memory 4g pyspark-shell")

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql.types import IntegerType, LongType, StructField, StructType  # noqa: E402

spark = (SparkSession.builder.appName("hudi-layout-probe").master("local[2]")
         .config("spark.jars.packages", "org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0")
         .config("spark.jars.ivy", os.path.join(WH, "_ivy"))
         .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
         .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
         .config("spark.driver.host", "localhost").config("spark.ui.enabled", "false")
         .config("spark.sql.shuffle.partitions", "1")
         .config("spark.driver.extraJavaOptions", ADD_OPENS)
         .config("spark.executor.extraJavaOptions", ADD_OPENS)
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

SCHEMA = StructType([StructField("id", IntegerType(), True),
                     StructField("lsn", IntegerType(), True),
                     StructField("ts_ms", LongType(), True)])


def opts(op, compact_inline, max_delta=1):
    return {
        "hoodie.table.name": "h_mor",
        "hoodie.datasource.write.recordkey.field": "id",
        "hoodie.datasource.write.precombine.field": "ts_ms",
        "hoodie.payload.ordering.field": "ts_ms",
        "hoodie.record.merge.mode": "EVENT_TIME_ORDERING",
        "hoodie.datasource.write.table.type": "MERGE_ON_READ",
        "hoodie.datasource.write.operation": op,
        "hoodie.datasource.write.payload.class":
            "org.apache.hudi.common.model.DefaultHoodieRecordPayload",
        "hoodie.compact.inline": "true" if compact_inline else "false",
        "hoodie.compact.inline.max.delta.commits": str(max_delta),
        "hoodie.metadata.enable": "false",
        "hoodie.datasource.write.hive_style_partitioning": "false",
    }


def layout(tag):
    # NB: Hudi log files are HIDDEN (".<fileId>_<instant>.log.<n>_<token>"), so glob("*.log.*")
    # silently returns zero. Walk the directory and include dotfiles instead.
    base, logs = [], []
    for dp, dns, fns in os.walk(TBL):
        if ".hoodie" in dp.split(os.sep):
            continue
        for fn in fns:
            p = os.path.join(dp, fn)
            if ".log." in fn:
                logs.append(p)
            elif fn.endswith(".parquet"):
                base.append(p)
    base, logs = sorted(base), sorted(logs)
    print(f"\n--- {tag} ---")
    print(f"  base parquet files: {len(base)}")
    for b in base:
        n = spark.read.parquet(b).count()
        print(f"     {os.path.basename(b)[:58]}  rows={n}")
    print(f"  log files: {len(logs)}")
    for lg in logs:
        print(f"     {os.path.basename(lg)[:58]}  bytes={os.path.getsize(lg)}")
    for qt in ("read_optimized", "snapshot"):
        try:
            df = (spark.read.format("hudi").option("hoodie.datasource.query.type", qt)
                  .option("hoodie.payload.ordering.field", "ts_ms").load(TBL))
            print(f"  query {qt:15}: rows={df.count()}  distinct_ids={df.select('id').distinct().count()}")
        except Exception as e:
            print(f"  query {qt:15}: FAILED {str(e)[:120]}")
    # instants on the timeline
    inst = sorted(os.path.basename(p) for p in glob.glob(os.path.join(TBL, ".hoodie", "*"))
                  if not os.path.isdir(p))
    kinds = {}
    for i in inst:
        ext = i.split(".")[-1]
        kinds[ext] = kinds.get(ext, 0) + 1
    print(f"  timeline: {kinds}")


# one delta commit per version; ts_ms SKEWED so version 3 of every key looks newest
for v in range(1, NVERSIONS + 1):
    rows = []
    for k in range(1, NKEYS + 1):
        ts = 1_700_000_000_000 + v * 1000
        if v == 3:
            ts += 10_000_000          # version 3 gets a far-future timestamp -> it wins on precombine
        rows.append((k, v, ts))
    mode = "overwrite" if v == 1 else "append"
    (spark.createDataFrame(rows, SCHEMA).write.format("hudi")
     .options(**opts("upsert", compact_inline=False)).mode(mode).save(TBL))

layout("AFTER 6 delta commits, BEFORE compaction")

# what does the snapshot view actually return per key?
snap = (spark.read.format("hudi").option("hoodie.datasource.query.type", "snapshot")
        .option("hoodie.payload.ordering.field", "ts_ms").load(TBL)
        .selectExpr("id", "lsn", "ts_ms").orderBy("id").limit(3).collect())
print("\n  snapshot sample (id, lsn, ts_ms):", [(r["id"], r["lsn"], r["ts_ms"]) for r in snap])
print("  ^ lsn=3 would mean the SKEWED (stale-by-lsn) version won on precombine")

# now force a compaction: one more upsert with inline compaction enabled
rows = [(k, NVERSIONS + 1, 1_700_000_000_000 + (NVERSIONS + 1) * 1000) for k in range(1, NKEYS + 1)]
(spark.createDataFrame(rows, SCHEMA).write.format("hudi")
 .options(**opts("upsert", compact_inline=True, max_delta=1)).mode("append").save(TBL))

layout("AFTER inline compaction")
spark.stop()
