#!/usr/bin/env python3
"""NEW MEASUREMENT (2026-08-25). Does the `no_deletes` arm of cloud/probe_pass_cost.py actually
omit delete application?

THIS IS NOT A REGENERATION of cloud/results2/results/probe_pass_cost.json. It writes its own
artifact, NEW_probe_pass_cost_control.json, and touches nothing committed.

WHY. §6.4 says "on a plain read of the same data, applying them costs 17.3 s against 18.3 s
without", sourced from that artifact's `narrow_scan` (17.3) and `no_deletes` (18.27) arms. The
artifact carries no control that the delete filter was exercised. Reading the script, the two arms
are:

    "narrow_scan": _noop(marked.where(~F.col("_del")).select("id", "lsn"))
    "no_deletes":  _noop(spark.read.format("iceberg").load(tbl).select("id", "lsn"))

where `marked = spark.read.format("iceberg").load(tbl).select("*", F.col("_deleted").alias("_del"))`.

Both arms read the SAME merge-on-read table through the SAME Iceberg reader. Projecting `_deleted`
flips the delete filter from dropping to marking; it does not switch it off. Iceberg exposes no read
option to skip equality deletes. So the hypothesis under test here is that the `no_deletes` arm
applies the deletes too, and the near-equal timings compare marking-plus-filtering against dropping
rather than delete application against its absence.

THE CONTROL the original lacks, and the one this run turns on. For each arm, read from the run:
  * how many equality-delete files are in scope for the table
  * how many rows the arm's own read returns
  * how many rows the table physically holds (data records, deletes not applied)
If the `no_deletes` arm returns fewer rows than the table physically holds, the deletes were applied
in that arm, and the arm is misnamed.

A THIRD ARM the original does not have: `true_floor` reads a second table built with the same row
count and NO deletes at all. That is the only arm in which delete application genuinely does not
happen, and it is what the 18.3 s number was taken to be.

Scale is far below the cloud run (that used 32 commits x 3,600,000 rows on an i4i.4xlarge). These
timings are NOT comparable to 17.3/18.3 in magnitude and no claim here rests on them. What is
comparable is the ROW-COUNT control, which is scale-independent.
"""
import json
import os
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))
JAR = os.environ.get("MOR_ICEBERG_JAR", os.path.expanduser(
    "~/IdeaProjects/iceberg-mor-fork/spark/v3.5/spark-runtime/build/libs/"
    "iceberg-spark-runtime-3.5_2.12-1.11.0-SNAPSHOT.jar"))
JAVA_HOME = "/Library/Java/JavaVirtualMachines/temurin-17.jdk/Contents/Home"
PY = os.environ.get("MOR_PY", os.path.join(REPO, "checker", ".venv", "bin", "python"))
OUT = os.path.join(HERE, "NEW_probe_pass_cost_control.json")
WH = tempfile.mkdtemp(prefix="mor_probe_control_")

SCRIPT = r'''
import json, os, sys, time
from pyspark.sql import SparkSession, functions as F

WH = sys.argv[1]; JAR = sys.argv[2]
spark = (SparkSession.builder.appName("probe_control").master("local[2]")
         .config("spark.jars", JAR)
         .config("spark.sql.extensions",
                 "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
         .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
         .config("spark.sql.catalog.local.type", "hadoop")
         .config("spark.sql.catalog.local.warehouse", WH)
         .config("spark.sql.shuffle.partitions", "2").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
jvm = spark.sparkContext._jvm; gw = spark.sparkContext._gateway
res = {}

import pyarrow as pa, pyarrow.parquet as pq
Files = jvm.org.apache.iceberg.Files
DataFiles = jvm.org.apache.iceberg.DataFiles
FileMetadata = jvm.org.apache.iceberg.FileMetadata
FileFormat = jvm.org.apache.iceberg.FileFormat
ParquetUtil = jvm.org.apache.iceberg.parquet.ParquetUtil
MetricsConfig = jvm.org.apache.iceberg.MetricsConfig
MappingUtil = jvm.org.apache.iceberg.mapping.MappingUtil

def build(name, with_deletes):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db")
    spark.sql(f"DROP TABLE IF EXISTS local.db.{name}")
    spark.sql(f"""CREATE TABLE local.db.{name} (id bigint NOT NULL, val string, lsn bigint)
                  USING iceberg TBLPROPERTIES ('format-version'='2')""")
    spark.sql(f"ALTER TABLE local.db.{name} SET IDENTIFIER FIELDS id")
    cat = jvm.org.apache.iceberg.hadoop.HadoopCatalog(
            jvm.org.apache.hadoop.conf.Configuration(), WH)
    ns = gw.new_array(jvm.java.lang.String, 1); ns[0] = "db"
    tid = jvm.org.apache.iceberg.catalog.TableIdentifier.of(
            jvm.org.apache.iceberg.catalog.Namespace.of(ns), name)
    t = cat.loadTable(tid)
    d = os.path.join(WH, "_f_" + name); os.makedirs(d, exist_ok=True)
    def metrics(p):
        return ParquetUtil.fileMetrics(Files.localInput(p), MetricsConfig.forTable(t),
                                       MappingUtil.create(t.schema()))
    def datafile(p, ids, lsns):
        pq.write_table(pa.table({"id": pa.array(ids, pa.int64()),
                                 "val": pa.array(["x"*180]*len(ids), pa.string()),
                                 "lsn": pa.array(lsns, pa.int64())}), p)
        return (DataFiles.builder(t.spec()).withPath(p).withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(os.path.getsize(p)).withMetrics(metrics(p)).build())
    def eqdel(p, ids):
        eq = gw.new_array(jvm.int, 1); eq[0] = t.schema().findField("id").fieldId()
        pq.write_table(pa.table({"id": pa.array(ids, pa.int64())}), p)
        return (FileMetadata.deleteFileBuilder(t.spec()).ofEqualityDeletes(eq)
                .withPath(p).withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(os.path.getsize(p)).withMetrics(metrics(p)).build())
    COMMITS, RPC, DEL = 12, 120000, 24000
    written = 0
    for c in range(1, COMMITS + 1):
        ids = list(range(1, RPC + 1))
        lsns = [c * 10_000_000 + i for i in ids]
        t.refresh()
        if with_deletes and c >= 2:
            st = ((c - 2) * DEL) % max(1, RPC - DEL + 1) + 1
            t.newRowDelta() \
             .addRows(datafile(os.path.join(d, f"d{c}.parquet"), ids, lsns)) \
             .addDeletes(eqdel(os.path.join(d, f"x{c}.parquet"), list(range(st, st + DEL)))) \
             .commit()
        else:
            t.newAppend().appendFile(datafile(os.path.join(d, f"d{c}.parquet"), ids, lsns)).commit()
        written += RPC
    t.refresh()
    ndel = sum(1 for _ in t.currentSnapshot().addedDeleteFiles(t.io())) if False else None
    return written

res["rows_written_mor"]   = build("probe_mor", True)
res["rows_written_clean"] = build("probe_clean", False)

# ---- CONTROLS, read from the run ----
mor, clean = "local.db.probe_mor", "local.db.probe_clean"
res["delete_files_in_scope"] = spark.sql(
    f"SELECT count(*) c FROM {mor}.files WHERE content != 0").collect()[0]["c"]
res["data_records_physical"] = spark.sql(
    f"SELECT sum(record_count) s FROM {mor}.files WHERE content = 0").collect()[0]["s"]
res["plain_read_rows"] = spark.read.format("iceberg").load(mor).count()
marked = spark.read.format("iceberg").load(mor).select("*", F.col("_deleted").alias("_del"))
res["marked_read_rows"]   = marked.count()
res["marked_rows_flagged"] = marked.where(F.col("_del")).count()
res["clean_table_rows"]   = spark.read.format("iceberg").load(clean).count()

def timed(fn):
    t0 = time.time(); fn(); return round(time.time() - t0, 2)
def noop(df): df.write.format("noop").mode("overwrite").save()

res["timings_s"] = {
  "narrow_scan": timed(lambda: noop(marked.where(~F.col("_del")).select("id", "lsn"))),
  "no_deletes":  timed(lambda: noop(spark.read.format("iceberg").load(mor).select("id","lsn"))),
  "true_floor":  timed(lambda: noop(spark.read.format("iceberg").load(clean).select("id","lsn"))),
}
print("PROBE_JSON " + json.dumps(res))
'''

sp = os.path.join(WH, "p.py")
open(sp, "w").write(SCRIPT)
p = subprocess.run([PY, sp, WH, JAR], capture_output=True, text=True,
                   env={**os.environ, "JAVA_HOME": JAVA_HOME})
payload = None
for line in p.stdout.splitlines():
    if line.startswith("PROBE_JSON "):
        payload = json.loads(line[len("PROBE_JSON "):])
if payload is None:
    print("FAILED\n", p.stdout[-2500:], "\n", p.stderr[-2500:])
    sys.exit(2)

r = payload
fail = []
if r["delete_files_in_scope"] < 1:
    fail.append("no equality-delete files in the MOR table; the arms cannot differ on delete "
                "application and nothing was measured")
if r["plain_read_rows"] >= r["data_records_physical"]:
    fail.append("the plain read returned every physical data record, so no deletes were applied "
                "anywhere and this run cannot speak to the question")
if r["marked_rows_flagged"] < 1:
    fail.append("the marked read flagged no rows; the _deleted projection did not engage")

r["deletes_applied_in_no_deletes_arm"] = r["plain_read_rows"] < r["data_records_physical"]
r["failures"] = fail
r["what"] = ("NEW measurement; does cloud/probe_pass_cost.py's `no_deletes` arm omit delete "
             "application? Not a regeneration of probe_pass_cost.json.")

print(f"  delete files in scope        : {r['delete_files_in_scope']}")
print(f"  data records physically held : {r['data_records_physical']:,}")
print(f"  plain read ('no_deletes')    : {r['plain_read_rows']:,} rows")
print(f"  marked read total / flagged  : {r['marked_read_rows']:,} / {r['marked_rows_flagged']:,}")
print(f"  clean table (true floor)     : {r['clean_table_rows']:,} rows")
print(f"  => deletes applied in the 'no_deletes' arm? {r['deletes_applied_in_no_deletes_arm']}")
print(f"  timings: {json.dumps(r['timings_s'])}")
json.dump(r, open(OUT, "w"), indent=1)
print(f"\n  -> {OUT}")
print("\nCONTROLS FAILED:\n  " + "\n  ".join(fail) if fail else "\ncontrols passed")
import shutil; shutil.rmtree(WH, ignore_errors=True)
sys.exit(1 if fail else 0)
