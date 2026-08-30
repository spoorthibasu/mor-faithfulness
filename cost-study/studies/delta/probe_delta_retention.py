#!/usr/bin/env python3
"""Phase 7: Delta characterization — what survives OPTIMIZE, what VACUUM removes, and for how long.

Source-and-docs question, not a replication. Two parts:
  A. DEFAULT CONSTANTS read from the shipped delta-spark classes (not documentation claims), the way
     Hudi's hoodie.clean.automatic was verified in Entry 35.
  B. A small decisive probe: after OPTIMIZE, does _delta_log still name the superseded files and the
     commit versions they belonged to? And with Change Data Feed on, what is physically written?

NB on false zeros (this pattern has bitten three times today): _delta_log holds .json commits,
.checkpoint.parquet files, _last_checkpoint, and possibly .crc files; CDF data lives OUTSIDE it in
_change_data/. Listings below walk the tree and count by extension rather than globbing one pattern.
"""
import json
import os
import shutil
import sys
from collections import Counter

WH = sys.argv[1]
shutil.rmtree(WH, ignore_errors=True)
os.makedirs(WH, exist_ok=True)
os.environ.setdefault("PYSPARK_SUBMIT_ARGS", "--driver-memory 4g pyspark-shell")

ADD_OPENS = " ".join(
    f"--add-opens=java.base/{p}=ALL-UNNAMED"
    for p in ["java.lang", "java.lang.invoke", "java.lang.reflect", "java.io", "java.net", "java.nio",
              "java.util", "java.util.concurrent", "java.util.concurrent.atomic", "sun.nio.ch",
              "sun.nio.cs", "sun.security.action", "sun.util.calendar"]
) + " --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"

from pyspark.sql import SparkSession  # noqa: E402

spark = (SparkSession.builder.appName("delta-retention-probe").master("local[2]")
         .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
         .config("spark.jars.ivy", os.path.join(WH, "_ivy"))
         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
         .config("spark.driver.host", "localhost").config("spark.ui.enabled", "false")
         .config("spark.sql.shuffle.partitions", "2")
         .config("spark.driver.extraJavaOptions", ADD_OPENS)
         .config("spark.executor.extraJavaOptions", ADD_OPENS)
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
jvm = spark._jvm

print("=== A. DEFAULTS read from the shipped delta-spark 3.2.0 classes ===")
CFG = jvm.org.apache.spark.sql.delta.DeltaConfigs
for field in ["LOG_RETENTION", "TOMBSTONE_RETENTION", "CHECKPOINT_RETENTION_DURATION",
              "CHECKPOINT_INTERVAL", "CHANGE_DATA_FEED", "ENABLE_EXPIRED_LOG_CLEANUP",
              "DATA_SKIPPING_NUM_INDEXED_COLS"]:
    try:
        c = getattr(CFG, field)()
        print(f"  {c.key():55} = {c.defaultValue()}")
    except Exception as e:
        print(f"  {field:55} : NOT FOUND ({str(e)[:60]})")
for prop, desc in [("spark.databricks.delta.retentionDurationCheck.enabled",
                    "guard against VACUUM with short retention"),
                   ("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "CDF default")]:
    print(f"  [session] {prop} = {spark.conf.get(prop, '<unset>')}   ({desc})")

TBL = os.path.join(WH, "t")


def tree(tag):
    counts, log_files = Counter(), []
    for dp, _, fns in os.walk(TBL):
        rel = os.path.relpath(dp, TBL)
        for fn in fns:
            key = "_delta_log" if rel == "_delta_log" else ("_change_data" if "_change_data" in rel
                                                            else "data")
            ext = ("crc" if fn.endswith(".crc") else
                   "checkpoint.parquet" if "checkpoint" in fn and fn.endswith(".parquet") else
                   "json" if fn.endswith(".json") else
                   "parquet" if fn.endswith(".parquet") else "other")
            counts[f"{key}/{ext}"] += 1
            if key == "_delta_log" and ext in ("json", "checkpoint.parquet"):
                log_files.append(fn)
    print(f"\n--- {tag} ---")
    print(f"  file inventory: {dict(sorted(counts.items()))}")
    print(f"  log files: {sorted(log_files)[:14]}")
    return counts


# ---- build: 6 commits that update the same keys, so each commit supersedes the previous file ----
spark.sql(f"CREATE TABLE delta.`{TBL}` (id INT, lsn INT) USING delta")
for v in range(1, 7):
    spark.sql(f"CREATE OR REPLACE TEMP VIEW src AS "
              f"SELECT explode(sequence(1,50)) AS id, {v} AS lsn")
    spark.sql(f"MERGE INTO delta.`{TBL}` t USING src s ON t.id = s.id "
              f"WHEN MATCHED THEN UPDATE SET t.lsn = s.lsn "
              f"WHEN NOT MATCHED THEN INSERT *")
tree("after 6 MERGE commits (before OPTIMIZE)")

hist = spark.sql(f"DESCRIBE HISTORY delta.`{TBL}`").select("version", "operation").collect()
print(f"  history versions: {[(r['version'], r['operation']) for r in hist][:8]}")

# does time travel still reach the superseded versions, i.e. is ordering evidence recoverable?
tt = [(v, spark.read.format("delta").option("versionAsOf", v).load(TBL)
       .selectExpr("min(lsn) mn", "max(lsn) mx").collect()[0]) for v in (1, 3, 6)]
print(f"  time travel lsn ranges: {[(v, r['mn'], r['mx']) for v, r in tt]}")

# ---- OPTIMIZE ----
spark.sql(f"OPTIMIZE delta.`{TBL}`")
tree("after OPTIMIZE")

# what does the OPTIMIZE commit record? read the newest JSON commit's actions
logdir = os.path.join(TBL, "_delta_log")
newest = sorted(f for f in os.listdir(logdir) if f.endswith(".json"))[-1]
acts = Counter()
removed_with_version = 0
for line in open(os.path.join(logdir, newest)):
    a = json.loads(line)
    for k in a:
        acts[k] += 1
    if "remove" in a:
        # does the remove action carry enough to identify the superseded file?
        r = a["remove"]
        if r.get("path") and ("deletionTimestamp" in r or "dataChange" in r):
            removed_with_version += 1
print(f"\n  newest commit ({newest}) actions: {dict(acts)}")
print(f"  remove actions naming a superseded file: {removed_with_version}")
print(f"  OPTIMIZE dataChange flag: "
      f"{[json.loads(l).get('add', {}).get('dataChange') for l in open(os.path.join(logdir, newest)) if 'add' in json.loads(l)][:3]}")

# after OPTIMIZE, is the pre-OPTIMIZE data still readable by time travel?
tt2 = spark.read.format("delta").option("versionAsOf", 3).load(TBL) \
    .selectExpr("min(lsn) mn", "max(lsn) mx").collect()[0]
print(f"  time travel to v3 AFTER optimize: lsn [{tt2['mn']},{tt2['mx']}] (evidence still reachable)")
spark.stop()
