#!/usr/bin/env python3
"""Isolated probe: does `rewrite_data_files(remove-dangling-deletes=true)` strip the
orphaned equality delete that carries a fully-suppressed (NEEDS_CONTEXT) key?

Builds the minimal NC scenario twice and compares delete-file counts:
  commit1: append data id=1(lsn1), id=2(lsn1)      -> seq 1
  commit2: rowDelta eq-delete id=1                  -> seq 2  (suppresses id=1's only row)
id=1 has 0 survivors (NC); id=2 survives. Then:
  table A: rewrite_data_files DEFAULT
  table B: rewrite_data_files remove-dangling-deletes=true  (exception surfaced, not swallowed)
Prints delete_files before/after, readback, and whether id=1 is still visible in metadata.
"""
import os, sys, traceback

VER = os.environ.get("MOR_ICEBERG_VERSION", "1.10.2")
WH = sys.argv[1]
IVY = os.path.join(WH, "_ivy")
os.makedirs(WH, exist_ok=True)

ADD_OPENS = " ".join(
    f"--add-opens=java.base/{p}=ALL-UNNAMED"
    for p in ["java.lang","java.lang.invoke","java.lang.reflect","java.io","java.net",
              "java.nio","java.util","java.util.concurrent","java.util.concurrent.atomic",
              "sun.nio.ch","sun.nio.cs","sun.security.action","sun.util.calendar"]
) + " --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"

from pyspark.sql import SparkSession
spark = (SparkSession.builder.appName("dangling-probe").master("local[2]")
    .config("spark.jars.packages", f"org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:{VER}")
    .config("spark.jars.ivy", IVY)
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", WH)
    .config("spark.sql.catalogImplementation", "in-memory")
    .config("spark.driver.host", "localhost").config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "1")
    .config("spark.driver.extraJavaOptions", ADD_OPENS)
    .config("spark.executor.extraJavaOptions", ADD_OPENS)
    .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
jvm = spark._jvm
Files = jvm.org.apache.iceberg.Files
FileFormat = jvm.org.apache.iceberg.FileFormat
GenRecord = jvm.org.apache.iceberg.data.GenericRecord
GenAppFac = jvm.org.apache.iceberg.data.GenericAppenderFactory
EncFiles = jvm.org.apache.iceberg.encryption.EncryptedFiles
EMPTY_KEY = jvm.org.apache.iceberg.encryption.EncryptionKeyMetadata.EMPTY

def enc(o): return EncFiles.encryptedOutput(o, EMPTY_KEY)
def load(path): return jvm.org.apache.iceberg.hadoop.HadoopTables(spark._jsc.hadoopConfiguration()).load(path)

def mk(schema, d, names):
    r = GenRecord.create(schema)
    for n in names: r.setField(n, d.get(n))
    return r

def write_data(t, path, rows):
    af = GenAppFac(t.schema(), t.spec())
    w = af.newDataWriter(enc(Files.localOutput(path)), FileFormat.PARQUET, None)
    for row in rows: w.write(mk(t.schema(), row, ["id","val","lsn"]))
    w.close(); return w.toDataFile()

def write_eqdel(t, path, ids):
    eq_schema = t.schema().select(["id"])
    gw = spark.sparkContext._gateway
    eq_ids = gw.new_array(jvm.int, 1); eq_ids[0] = t.schema().findField("id").fieldId()
    af = GenAppFac(t.schema(), t.spec(), eq_ids, eq_schema, None)
    w = af.newEqDeleteWriter(enc(Files.localOutput(path)), FileFormat.PARQUET, None)
    for i in ids: w.write(mk(eq_schema, {"id": i}, ["id"]))
    w.close(); return w.toDeleteFile()

def delete_file_count(name):
    n = 0
    for r in spark.sql(f"SELECT content, count(*) c FROM local.db.{name}.files GROUP BY content").collect():
        if r["content"] == 2: n = r["c"]
    return n

def build(name):
    tdir = os.path.join(WH, "db", name); ddir = os.path.join(tdir, "data"); os.makedirs(ddir, exist_ok=True)
    spark.sql(f"DROP TABLE IF EXISTS local.db.{name}")
    spark.sql(f"CREATE TABLE local.db.{name} (id INT, val STRING, lsn INT) USING iceberg "
              "TBLPROPERTIES('format-version'='2','write.delete.mode'='merge-on-read')")
    t = load(tdir)
    df = write_data(t, os.path.join(ddir, "c1.parquet"), [{"id":1,"val":"v1","lsn":1},{"id":2,"val":"a","lsn":1}])
    t.newAppend().appendFile(df).commit()
    t = load(tdir)
    ddf = write_eqdel(t, os.path.join(ddir, "c2-del.parquet"), [1])
    t.newRowDelta().addDeletes(ddf).commit()
    return tdir

def readback(name):
    return sorted([r["id"] for r in spark.sql(f"SELECT id FROM local.db.{name}").collect()])

for name, opt in [("probe_default", ""), ("probe_dangling", ", options => map('remove-dangling-deletes','true')")]:
    build(name)
    before = delete_file_count(name); rb_before = readback(name)
    sql = f"CALL local.system.rewrite_data_files(table => 'db.{name}'{opt})"
    err = None
    try:
        spark.sql(sql)
    except Exception as e:
        err = str(e)[:300]
    spark.sql(f"REFRESH TABLE local.db.{name}")
    after = delete_file_count(name); rb_after = readback(name)
    print(f"\n=== {name} ===")
    print(f"  SQL: {sql}")
    print(f"  exception: {err}")
    print(f"  delete_files: {before} -> {after}")
    print(f"  readback ids: {rb_before} -> {rb_after}")

spark.stop()
