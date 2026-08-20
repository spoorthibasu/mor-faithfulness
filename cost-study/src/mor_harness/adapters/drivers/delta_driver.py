#!/usr/bin/env python3
"""Self-contained Delta write driver (own Spark subprocess), deletion vectors on.

Applies each checkpoint as a MERGE upsert (+ optional DELETE) into a Delta table with
deletion vectors enabled, then reads the current view. Oracle-only in v1: Delta's
positional + log-order suppression has no equal-seq rule, so it is a control that
should hold violation_rate ~= 0 on the same streams (probe_delta.py). Needs the Delta
jar, resolved from maven central.
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

ADD_OPENS = " ".join(
    f"--add-opens=java.base/{p}=ALL-UNNAMED"
    for p in ["java.lang", "java.lang.invoke", "java.lang.reflect", "java.io", "java.net",
              "java.nio", "java.util", "java.util.concurrent", "java.util.concurrent.atomic",
              "sun.nio.ch", "sun.nio.cs", "sun.security.action", "sun.util.calendar"]
) + " --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"

_SQL_TYPE = {"int": "INT", "long": "BIGINT", "string": "STRING"}


def peak_rss_mb():
    r = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return round(r / (1024 * 1024), 1) if sys.platform == "darwin" else round(r / 1024, 1)


def main():
    from pyspark.sql import SparkSession
    from pyspark.sql.types import (StructType, StructField, IntegerType, LongType, StringType)

    spark = (SparkSession.builder.appName(f"mor-harness-delta-{NAME}").master("local[2]")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
        .config("spark.jars.ivy", IVY)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.properties.defaults.enableDeletionVectors", "true")
        .config("spark.driver.host", "localhost").config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.extraJavaOptions", ADD_OPENS)
        .config("spark.executor.extraJavaOptions", ADD_OPENS)
        .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")

    tmap = {"int": IntegerType(), "long": LongType(), "string": StringType()}
    schema = StructType([StructField(c["name"], tmap[c["type"]], True) for c in COLUMNS])
    names = [c["name"] for c in COLUMNS]
    ddl = ", ".join(f"{c['name']} {_SQL_TYPE[c['type']]}" for c in COLUMNS)
    set_clause = ", ".join(f"t.{n} = s.{n}" for n in names)
    on_clause = " AND ".join(f"t.{k} = s.{k}" for k in KEY_COLUMNS)

    import shutil
    shutil.rmtree(TABLE_DIR, ignore_errors=True)
    spark.sql(f"CREATE TABLE delta.`{TABLE_DIR}` ({ddl}) USING delta "
              "TBLPROPERTIES('delta.enableDeletionVectors'='true')")

    kidx = [names.index(k) for k in KEY_COLUMNS]

    def dedup(rows):
        # Delta MERGE errors if the source has >1 row per key; keep the last occurrence.
        # (Delta structurally cannot represent a same-key duplicate: it is the control.)
        seen = {}
        for r in rows:
            seen[tuple(r[i] for i in kidx)] = r
        return list(seen.values())

    t0 = time.time()
    for ck in PLAN["checkpoints"]:
        if ck["data"]:
            rows = dedup([tuple(r.get(n) for n in names) for r in ck["data"]])
            spark.createDataFrame(rows, schema).createOrReplaceTempView("src")
            spark.sql(f"""MERGE INTO delta.`{TABLE_DIR}` t USING src s ON {on_clause}
                          WHEN MATCHED THEN UPDATE SET {set_clause}
                          WHEN NOT MATCHED THEN INSERT *""")
        if ck["deletes"]:
            conds = []
            for kv in ck["deletes"]:
                conds.append("(" + " AND ".join(f"{k} = {v}" for k, v in zip(KEY_COLUMNS, kv)) + ")")
            spark.sql(f"DELETE FROM delta.`{TABLE_DIR}` WHERE " + " OR ".join(conds))
    apply_time = time.time() - t0

    compact_time = 0.0
    # safe_compact / unsafe_compact run the identical OPTIMIZE; only pre-compaction layout differs.
    if PLAN.get("enforcement_mode") in ("safe_compact", "unsafe_compact"):
        tc = time.time()
        spark.sql(f"OPTIMIZE delta.`{TABLE_DIR}`")
        compact_time = time.time() - tc

    t1 = time.time()
    rows = spark.sql(f"SELECT {', '.join(names)} FROM delta.`{TABLE_DIR}`").collect()
    materialized = [r.asDict() for r in rows]
    readback_time = time.time() - t1

    detail = spark.sql(f"DESCRIBE DETAIL delta.`{TABLE_DIR}`").collect()[0].asDict()
    commit_count = spark.sql(f"DESCRIBE HISTORY delta.`{TABLE_DIR}`").count()
    result = {
        "materialized": materialized,
        "stats": {
            "apply_time_s": round(apply_time, 3),
            "compact_time_s": round(compact_time, 3),
            "readback_time_s": round(readback_time, 3),
            "commit_count": int(commit_count),
            "data_files": int(detail.get("numFiles") or 0),
            "delete_files": 0,
            "bytes_data": int(detail.get("sizeInBytes") or 0),
            "bytes_delete": 0,
            "bytes_total": int(detail.get("sizeInBytes") or 0),
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
        json.dump({"error": "delta driver failed", "traceback": traceback.format_exc()}, f)
    sys.exit(1)
