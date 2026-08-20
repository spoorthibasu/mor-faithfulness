#!/usr/bin/env python3
"""Phase 7 probe 2: what an OPTIMIZE commit actually records, and what CDF adds.

Probe 1 silently measured nothing: after 6 MERGEs the current version held ONE data file, so OPTIMIZE had
nothing to bin-pack and produced no commit at all -- and the script then read the last MERGE commit as if
it were the OPTIMIZE (reporting dataChange=True, which OPTIMIZE never sets). Same class as the Iceberg
file-size-band trap. So this version builds MULTIPLE current files first and ASSERTS that OPTIMIZE ran.
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
ADD_OPENS = " ".join(f"--add-opens=java.base/{p}=ALL-UNNAMED" for p in
                     ["java.lang", "java.util", "java.nio", "sun.nio.ch", "java.lang.invoke", "java.io",
                      "java.net", "java.util.concurrent"])

from pyspark.sql import SparkSession  # noqa: E402

spark = (SparkSession.builder.appName("delta-optimize-probe").master("local[2]")
         .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
         .config("spark.jars.ivy", os.path.join(WH, "_ivy"))
         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
         .config("spark.driver.host", "localhost").config("spark.ui.enabled", "false")
         .config("spark.sql.shuffle.partitions", "2")
         .config("spark.driver.extraJavaOptions", ADD_OPENS)
         .config("spark.executor.extraJavaOptions", ADD_OPENS).getOrCreate())
spark.sparkContext.setLogLevel("ERROR")


def commits(tbl):
    d = os.path.join(tbl, "_delta_log")
    return sorted(f for f in os.listdir(d) if f.endswith(".json"))


def actions(tbl, fname):
    out, flags = Counter(), []
    for line in open(os.path.join(tbl, "_delta_log", fname)):
        a = json.loads(line)
        for k, v in a.items():
            out[k] += 1
            if k in ("add", "remove") and isinstance(v, dict):
                flags.append((k, v.get("dataChange"), os.path.basename(v.get("path", ""))[:28]))
    return out, flags


# ---------- table A: OPTIMIZE with real work to do ----------
A = os.path.join(WH, "ta")
spark.sql(f"CREATE TABLE delta.`{A}` (id INT, lsn INT) USING delta")
for v in range(1, 7):                       # 6 appends -> 6 CURRENT files
    spark.sql(f"INSERT INTO delta.`{A}` SELECT explode(sequence({(v-1)*50+1},{v*50})) AS id, {v} AS lsn")
for v in (7, 8):                            # 2 MERGEs -> supersede some files (tombstones)
    spark.sql(f"CREATE OR REPLACE TEMP VIEW s AS SELECT explode(sequence(1,50)) AS id, {v} AS lsn")
    spark.sql(f"MERGE INTO delta.`{A}` t USING s ON t.id=s.id WHEN MATCHED THEN UPDATE SET t.lsn=s.lsn")
before = commits(A)
n_data_before = len([f for f in os.listdir(A) if f.endswith(".parquet")])

res = spark.sql(f"OPTIMIZE delta.`{A}`").collect()
hist = [(r["version"], r["operation"]) for r in
        spark.sql(f"DESCRIBE HISTORY delta.`{A}`").select("version", "operation").collect()]
opt = [h for h in hist if h[1] == "OPTIMIZE"]
assert opt, f"OPTIMIZE produced no commit -- nothing was bin-packed. history={hist[:4]}"
after = commits(A)
new_commits = [c for c in after if c not in before]
print(f"=== OPTIMIZE ran: version {opt[0][0]}; new commit files {new_commits} ===")
m = res[0].asDict().get("metrics")
print(f"  metrics: filesAdded={getattr(m,'numFilesAdded',None)} filesRemoved={getattr(m,'numFilesRemoved',None)}")
acts, flags = actions(A, new_commits[-1])
print(f"  actions in the OPTIMIZE commit: {dict(acts)}")
print(f"  (action, dataChange, file) sample: {flags[:6]}")
print(f"  data parquet files on disk: before={n_data_before} "
      f"after={len([f for f in os.listdir(A) if f.endswith('.parquet')])} (superseded files NOT deleted)")

# do earlier commits still name the files they superseded, with their commit version?
print("\n  per-commit remove actions (version -> #removes naming a path):")
for c in commits(A):
    a, fl = actions(A, c)
    nrem = sum(1 for k, _, _ in fl if k == "remove")
    if nrem:
        print(f"    {c}: removes={nrem}")
tt = spark.read.format("delta").option("versionAsOf", 3).load(A).selectExpr("count(*) c").collect()[0]
print(f"  time travel to v3 after OPTIMIZE: rows={tt['c']} (pre-OPTIMIZE state still reconstructible)")

# ---------- table B: Change Data Feed ----------
B = os.path.join(WH, "tb")
spark.sql(f"CREATE TABLE delta.`{B}` (id INT, lsn INT) USING delta "
          f"TBLPROPERTIES (delta.enableChangeDataFeed = true)")
spark.sql(f"INSERT INTO delta.`{B}` SELECT explode(sequence(1,50)) AS id, 1 AS lsn")
for v in (2, 3):
    spark.sql(f"CREATE OR REPLACE TEMP VIEW s AS SELECT explode(sequence(1,50)) AS id, {v} AS lsn")
    spark.sql(f"MERGE INTO delta.`{B}` t USING s ON t.id=s.id WHEN MATCHED THEN UPDATE SET t.lsn=s.lsn")
cdc_dir = os.path.join(B, "_change_data")
print(f"\n=== CDF table ===")
print(f"  _change_data exists={os.path.isdir(cdc_dir)} "
      f"files={len([f for f in os.listdir(cdc_dir) if f.endswith('.parquet')]) if os.path.isdir(cdc_dir) else 0}")
ch = spark.read.format("delta").option("readChangeFeed", "true") \
    .option("startingVersion", 0).load(B)
agg = ch.groupBy("_change_type").count().collect()
print(f"  table_changes by type: {[(r['_change_type'], r['count']) for r in agg]}")
pre = ch.selectExpr("id", "lsn", "_change_type", "_commit_version").where("id = 1").orderBy("_commit_version").collect()
print(f"  key id=1 full history from CDF: {[(r['_commit_version'], r['_change_type'], r['lsn']) for r in pre]}")
spark.sql(f"OPTIMIZE delta.`{B}`")
ch2 = spark.read.format("delta").option("readChangeFeed", "true").option("startingVersion", 0).load(B)
print(f"  CDF rows before OPTIMIZE={ch.count()} after OPTIMIZE={ch2.count()} (OPTIMIZE is dataChange=false)")
spark.stop()
