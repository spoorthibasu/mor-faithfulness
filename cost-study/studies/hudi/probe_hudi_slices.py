#!/usr/bin/env python3
"""Phase 6 probe 2: which files belong to the CURRENT file slice, before vs after compaction?

Hudi encodes slice membership in the file names, so this needs no API guesswork:
  base file : <fileId>_<writeToken>_<instant>.parquet
  log file  : .<fileId>_<baseInstant>.log.<version>_<writeToken>
A log file belongs to the slice whose base instant equals the <baseInstant> in its name. So if compaction
writes a new base at a later instant, the pre-existing logs stay attached to the OLD slice and are not
part of the current one -- which is exactly the Iceberg pattern (new snapshot; old data retained but no
longer current) and is what a current-state checker would or would not be able to see.

Then runs the cleaner to see whether the superseded slice is deleted (the expire_snapshots analogue).
"""
import os
import re
import shutil
import sys
from collections import defaultdict

WH = sys.argv[1]
NKEYS, NVERSIONS = 50, 6
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

spark = (SparkSession.builder.appName("hudi-slice-probe").master("local[2]")
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
BASE_RE = re.compile(r"^(?P<fid>[0-9a-f\-]+)-\d+_[^_]+_(?P<instant>\d+)\.parquet$")
LOG_RE = re.compile(r"^\.(?P<fid>[0-9a-f\-]+)-\d+_(?P<baseinstant>\d+)\.log\.(?P<ver>\d+)_")


def opts(op, compact_inline, clean=False, retained=10):
    o = {
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
        "hoodie.compact.inline.max.delta.commits": "1",
        "hoodie.metadata.enable": "false",
        "hoodie.datasource.write.hive_style_partitioning": "false",
        "hoodie.clean.automatic": "true" if clean else "false",
    }
    if clean:
        o["hoodie.cleaner.policy"] = "KEEP_LATEST_COMMITS"
        o["hoodie.cleaner.commits.retained"] = str(retained)
    return o


def slices(tag):
    files = []
    for dp, _, fns in os.walk(TBL):
        if ".hoodie" in dp.split(os.sep):
            continue
        for fn in fns:
            if fn.endswith(".crc"):
                continue
            files.append(fn)
    bases, logs = {}, defaultdict(list)
    for fn in files:
        m = BASE_RE.match(fn)
        if m:
            bases[m.group("instant")] = fn
            continue
        m = LOG_RE.match(fn)
        if m:
            logs[m.group("baseinstant")].append(fn)
    print(f"\n--- {tag} ---")
    for inst in sorted(bases):
        n = spark.read.parquet(os.path.join(TBL, bases[inst])).count()
        cur = "  <== CURRENT SLICE" if inst == max(bases) else "  (superseded)"
        print(f"  slice base_instant={inst}  base_rows={n}  attached_logs={len(logs.get(inst, []))}{cur}")
    orphan = set(logs) - set(bases)
    for inst in sorted(orphan):
        print(f"  logs for base_instant={inst} with NO base file present: {len(logs[inst])}")
    return bases, logs


# 6 delta commits, one per version; version 3 gets a far-future ts_ms so it wins on precombine
for v in range(1, NVERSIONS + 1):
    rows = [(k, v, 1_700_000_000_000 + v * 1000 + (10_000_000 if v == 3 else 0))
            for k in range(1, NKEYS + 1)]
    (spark.createDataFrame(rows, SCHEMA).write.format("hudi")
     .options(**opts("upsert", compact_inline=False)).mode("overwrite" if v == 1 else "append")
     .save(TBL))
b1, l1 = slices("BEFORE compaction (6 delta commits)")

snap = (spark.read.format("hudi").option("hoodie.datasource.query.type", "snapshot")
        .option("hoodie.payload.ordering.field", "ts_ms").load(TBL)
        .selectExpr("id", "lsn").orderBy("id").limit(3).collect())
print(f"  snapshot winner per key: lsn={[r['lsn'] for r in snap]} (3 = the skewed stale version wins)")

# compaction only: a no-op-ish upsert of one key, with inline compaction on
(spark.createDataFrame([(1, NVERSIONS, 1_700_000_000_000 + NVERSIONS * 1000)], SCHEMA)
 .write.format("hudi").options(**opts("upsert", compact_inline=True)).mode("append").save(TBL))
b2, l2 = slices("AFTER compaction")

snap2 = (spark.read.format("hudi").option("hoodie.datasource.query.type", "snapshot")
         .option("hoodie.payload.ordering.field", "ts_ms").load(TBL)
         .selectExpr("id", "lsn").orderBy("id").limit(3).collect())
print(f"  snapshot winner per key: lsn={[r['lsn'] for r in snap2]} (unchanged = content preserved)")

# now run the cleaner aggressively (retain 1 commit) -- the expire_snapshots analogue
(spark.createDataFrame([(2, NVERSIONS, 1_700_000_000_000 + NVERSIONS * 1000)], SCHEMA)
 .write.format("hudi").options(**opts("upsert", compact_inline=False, clean=True, retained=1))
 .mode("append").save(TBL))
slices("AFTER cleaner (KEEP_LATEST_COMMITS, retained=1)")
spark.stop()
