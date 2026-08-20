#!/usr/bin/env python3
"""Self-contained Hudi MOR write driver (own Spark subprocess).

Writes each checkpoint as one Hudi upsert commit into a MERGE_ON_READ table whose
precombine field is `lsn` (safe) or `ts_ms` (unsafe), then reads the snapshot (MOR
merge) current view. Reproduces probe_hudi.py: precombine monotone with logical order
-> current wins (faithful); precombine backwards vs logical order -> stale wins.

The Hudi checker cross-check does NOT read raw files; it is computed in the main
process from the known stream versions (argmax precombine), and the materialized
winner returned here must match it. So this driver only needs the current view + stats.
"""
import json
import os
import resource
import sys
import time
import traceback

IN, OUT = sys.argv[1], sys.argv[2]
with open(IN) as f:
    PLAN = json.load(f)

IVY = PLAN["ivy"]
NAME = PLAN["table_name"]
TABLE_DIR = PLAN["table_dir"]
COLUMNS = PLAN["columns"]
KEY_COLUMNS = PLAN["key_columns"]
PRECOMBINE = PLAN["precombine_field"]

ADD_OPENS = " ".join(
    f"--add-opens=java.base/{p}=ALL-UNNAMED"
    for p in ["java.lang", "java.lang.invoke", "java.lang.reflect", "java.io", "java.net",
              "java.nio", "java.util", "java.util.concurrent", "java.util.concurrent.atomic",
              "sun.nio.ch", "sun.nio.cs", "sun.security.action", "sun.util.calendar"]
) + " --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"

DEFAULT_PAYLOAD = "org.apache.hudi.common.model.DefaultHoodieRecordPayload"


def peak_rss_mb():
    r = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return round(r / (1024 * 1024), 1) if sys.platform == "darwin" else round(r / 1024, 1)


def dir_stats(root):
    """Base parquet files vs MOR log files, byte totals, and commit count."""
    data_files = log_files = bytes_data = bytes_log = commits = 0
    for dp, _, fns in os.walk(root):
        for fn in fns:
            fp = os.path.join(dp, fn)
            try:
                sz = os.path.getsize(fp)
            except OSError:
                sz = 0
            if os.sep + ".hoodie" + os.sep in fp or fp.endswith(".hoodie"):
                if fn.endswith(".deltacommit") or fn.endswith(".commit"):
                    commits += 1
                continue
            if ".log." in fn:
                log_files += 1
                bytes_log += sz
            elif fn.endswith(".parquet"):
                data_files += 1
                bytes_data += sz
    return data_files, log_files, bytes_data, bytes_log, commits


def main():
    from pyspark.sql import SparkSession
    from pyspark.sql.types import (StructType, StructField, IntegerType, LongType, StringType)

    spark = (SparkSession.builder.appName(f"mor-harness-hudi-{NAME}").master("local[2]")
        .config("spark.jars.packages", "org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0")
        .config("spark.jars.ivy", IVY)
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
        .config("spark.driver.host", "localhost").config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.driver.extraJavaOptions", ADD_OPENS)
        .config("spark.executor.extraJavaOptions", ADD_OPENS)
        .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")

    tmap = {"int": IntegerType(), "long": LongType(), "string": StringType()}
    schema = StructType([StructField(c["name"], tmap[c["type"]], True) for c in COLUMNS])
    names = [c["name"] for c in COLUMNS]

    # safe_compact / unsafe_compact fold in the identical inline compaction; only layout differs.
    compacting = PLAN.get("enforcement_mode") in ("safe_compact", "unsafe_compact")

    def opts(op):
        return {
            "hoodie.table.name": NAME,
            "hoodie.datasource.write.recordkey.field": ",".join(KEY_COLUMNS),
            "hoodie.datasource.write.precombine.field": PRECOMBINE,
            "hoodie.payload.ordering.field": PRECOMBINE,
            # Force EVENT_TIME (precombine) ordering at READ-time MOR merge. Without this
            # Hudi 0.15 defaults to COMMIT_TIME ordering (last-write-wins) across the many
            # delta-commits, so precombine would be ignored and skew would have no effect.
            "hoodie.record.merge.mode": "EVENT_TIME_ORDERING",
            "hoodie.datasource.write.table.type": "MERGE_ON_READ",
            "hoodie.datasource.write.operation": op,
            "hoodie.datasource.write.payload.class": DEFAULT_PAYLOAD,
            # safe_compact folds an inline compaction of the MOR log into the write.
            "hoodie.compact.inline": "true" if compacting else "false",
            "hoodie.compact.inline.max.delta.commits": "1",
            "hoodie.metadata.enable": "false",
            "hoodie.datasource.write.hive_style_partitioning": "false",
        }

    # Hudi arbitrates by precombine over ALL versions regardless of commit structure, and
    # its in-batch preCombine honors precombine reliably (unlike read-time merge across
    # many small delta-commits, which falls back to last-write-wins). So write ALL data
    # versions in ONE bulk upsert (in-batch arbitration -> argmax precombine per key), then
    # delete only the keys whose terminal op is a delete (delete-tail), leaving reinserted
    # keys present. This isolates the precombine mechanism the sensitivity study measures.
    kidx = [names.index(k) for k in KEY_COLUMNS]

    def keyof(row):
        return tuple(row.get(k) for k in KEY_COLUMNS)

    all_data, last_data_ck, last_del_ck = [], {}, {}
    for ck in PLAN["checkpoints"]:
        for r in ck["data"]:
            all_data.append(tuple(r.get(n) for n in names))
            last_data_ck[keyof(r)] = ck["index"]
        for kv in ck["deletes"]:
            last_del_ck[tuple(kv)] = ck["index"]
    terminally_deleted = [list(k) for k, dc in last_del_ck.items()
                          if dc > last_data_ck.get(k, -1)]

    t0 = time.time()
    if all_data:
        spark.createDataFrame(all_data, schema).write.format("hudi") \
            .options(**opts("upsert")).mode("overwrite").save(TABLE_DIR)
    if terminally_deleted:
        drows = []
        for kv in terminally_deleted:
            d = {n: None for n in names}
            for kc, v in zip(KEY_COLUMNS, kv):
                d[kc] = v
            d[PRECOMBINE] = 1 << 62
            drows.append(tuple(d.get(n) for n in names))
        spark.createDataFrame(drows, schema).write.format("hudi") \
            .options(**opts("delete")).mode("append").save(TABLE_DIR)
    apply_time = time.time() - t0

    t1 = time.time()
    cols = ", ".join(names)
    rows = (spark.read.format("hudi")
            .option("hoodie.datasource.query.type", "snapshot")
            .option("hoodie.payload.ordering.field", PRECOMBINE)
            .load(TABLE_DIR).selectExpr(*names).collect())
    materialized = [r.asDict() for r in rows]
    readback_time = time.time() - t1

    data_files, log_files, bytes_data, bytes_log, commits = dir_stats(TABLE_DIR)
    result = {
        "materialized": materialized,
        "stats": {
            "apply_time_s": round(apply_time, 3),
            "compact_time_s": 0.0,  # Hudi inline compaction is folded into apply_time_s
            "readback_time_s": round(readback_time, 3),
            "commit_count": commits,
            "data_files": data_files,
            "delete_files": log_files,   # MOR log files carry updates/deletes
            "bytes_data": bytes_data,
            "bytes_delete": bytes_log,
            "bytes_total": bytes_data + bytes_log,
            "peak_rss_mb": peak_rss_mb(),
        },
    }
    with open(OUT, "w") as f:
        json.dump(result, f)
    spark.stop()


try:
    main()
except Exception:
    with open(OUT, "w") as f:
        json.dump({"error": "hudi driver failed", "traceback": traceback.format_exc()}, f)
    sys.exit(1)
