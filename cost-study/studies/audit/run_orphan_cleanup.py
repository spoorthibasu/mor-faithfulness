#!/usr/bin/env python3
"""Run Iceberg's remove_orphan_files over a table, in its own Spark session.

`older_than` must be passed explicitly: the procedure defaults to a 3-day age floor, so without it a
freshly written file is never considered an orphan and the test would silently pass for the wrong reason.
"""
import os
import sys
import time

TBL, NAIVE = sys.argv[1], sys.argv[2]
JAR = os.environ.get("MOR_ICEBERG_JAR")
WH = os.path.dirname(os.path.dirname(TBL))
os.environ.setdefault("PYSPARK_SUBMIT_ARGS", "--driver-memory 4g pyspark-shell")
ADD = " ".join(f"--add-opens=java.base/{p}=ALL-UNNAMED" for p in
               ["java.lang", "java.util", "java.nio", "sun.nio.ch", "java.lang.invoke", "java.io",
                "java.net", "java.util.concurrent"])

from pyspark.sql import SparkSession  # noqa: E402

b = (SparkSession.builder.appName("orphan").master("local[2]")
     .config("spark.jars.ivy", os.path.join(WH, "_ivy"))
     .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
     .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
     .config("spark.sql.catalog.local.type", "hadoop")
     .config("spark.sql.catalog.local.warehouse", WH)
     .config("spark.sql.catalog.local.cache-enabled", "false")
     .config("spark.sql.catalogImplementation", "in-memory")
     .config("spark.driver.host", "localhost").config("spark.ui.enabled", "false")
     .config("spark.driver.extraJavaOptions", ADD).config("spark.executor.extraJavaOptions", ADD))
b = b.config("spark.jars", JAR) if JAR else b.config(
    "spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.2")
spark = b.getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
name = os.path.basename(TBL)

# The SQL procedure refuses an interval under 24 hours ("may corrupt the table if other operations are
# happening"), and passing a FUTURE timestamp trips the same guard -- so a naive call silently does
# nothing and every survival assertion passes for the wrong reason. The error message itself points at
# the Action API for arbitrary intervals; use it, with a cutoff just in the future so every file is old.
jvm = spark._jvm
tbl = jvm.org.apache.iceberg.hadoop.HadoopTables(spark._jsc.hadoopConfiguration()).load(TBL)
cutoff = int(time.time() * 1000) + 60_000
action = jvm.org.apache.iceberg.spark.actions.SparkActions.get(spark._jsparkSession)
result = action.deleteOrphanFiles(tbl).olderThan(cutoff).execute()
deleted = []
it = result.orphanFileLocations().iterator()
while it.hasNext():
    deleted.append(it.next())
print(f"  remove_orphan_files (Action API, cutoff=now+60s) deleted {len(deleted)} file(s)")
assert deleted, "cleanup deleted NOTHING -- the guard blocked it again; survival proves nothing"
for d in deleted:
    tag = "  <== NAIVE SIDECAR" if os.path.basename(NAIVE) in d else (
        "  <== PUFFIN BLOB" if d.endswith(".puffin") else "")
    print(f"    {os.path.basename(d)[:70]}{tag}")
spark.stop()
