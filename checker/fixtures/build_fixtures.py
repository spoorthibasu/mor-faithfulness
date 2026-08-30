#!/usr/bin/env python3
"""Build the four gating fixtures as real Iceberg v2 merge-on-read tables.

Spark is used ONLY here, to drive the Iceberg Java API and write equality-delete files
at controlled sequence numbers (Spark SQL alone writes position deletes and cannot
reproduce the equality-delete-same-sequence case). The checker itself never needs Spark.

Fixtures (persisted under fixtures/wh/db/<name>):
  bad_equal_seq          data+eqdel in ONE snapshot, equal seq -> mult_phys 2  (DUPLICATE)
  good_ascending         ascending snapshots + a version column -> mult_phys 1 (FAITHFUL)
  undecidable_no_version same layout, NO version column -> mult_phys 1          (UNDECIDABLE)
  wrongly_suppressed     stale delete at higher seq -> mult_phys 0             (WRONGLY_SUPPRESSED)

Run:
  JAVA_HOME=<jdk17> fixtures/build_fixtures.py
"""
import json
import os
import shutil
import sys
import traceback

HERE = os.path.dirname(os.path.abspath(__file__))
WAREHOUSE = os.path.join(HERE, "wh")
# Local ivy cache so the Iceberg jar resolves offline; override with MOR_IVY_DIR.
# Falls back to the standard ~/.ivy2 cache, then to maven central if neither exists.
IVY = os.environ.get("MOR_IVY_DIR") or os.path.expanduser("~/.ivy2")

ADD_OPENS = " ".join(
    f"--add-opens=java.base/{p}=ALL-UNNAMED"
    for p in [
        "java.lang", "java.lang.invoke", "java.lang.reflect", "java.io", "java.net",
        "java.nio", "java.util", "java.util.concurrent", "java.util.concurrent.atomic",
        "sun.nio.ch", "sun.nio.cs", "sun.security.action", "sun.util.calendar",
    ]
) + " --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"

shutil.rmtree(WAREHOUSE, ignore_errors=True)
os.makedirs(WAREHOUSE, exist_ok=True)

from pyspark.sql import SparkSession  # noqa: E402

spark = (
    SparkSession.builder.appName("mor-checker-fixtures")
    .master("local[2]")
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1")
    .config("spark.jars.ivy", IVY)
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", WAREHOUSE)
    .config("spark.sql.catalog.local.cache-enabled", "false")
    .config("spark.sql.catalogImplementation", "in-memory")
    .config("spark.driver.host", "localhost")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "1")
    .config("spark.driver.extraJavaOptions", ADD_OPENS)
    .config("spark.executor.extraJavaOptions", ADD_OPENS)
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")
jvm = spark._jvm
gw = spark.sparkContext._gateway

Files = jvm.org.apache.iceberg.Files
FileFormat = jvm.org.apache.iceberg.FileFormat
GenRecord = jvm.org.apache.iceberg.data.GenericRecord
GenAppFac = jvm.org.apache.iceberg.data.GenericAppenderFactory
EncFiles = jvm.org.apache.iceberg.encryption.EncryptedFiles
EMPTY_KEY = jvm.org.apache.iceberg.encryption.EncryptionKeyMetadata.EMPTY


def load_table(path):
    tables = jvm.org.apache.iceberg.hadoop.HadoopTables(spark._jsc.hadoopConfiguration())
    return tables.load(path)


def enc(output_file):
    return EncFiles.encryptedOutput(output_file, EMPTY_KEY)


def rec(schema, values: dict):
    r = GenRecord.create(schema)
    for k, v in values.items():
        r.setField(k, v)
    return r


def write_data_file(table, path, rows):
    af = GenAppFac(table.schema(), table.spec())
    w = af.newDataWriter(enc(Files.localOutput(path)), FileFormat.PARQUET, None)
    for values in rows:
        w.write(rec(table.schema(), values))
    w.close()
    return w.toDataFile()


def write_eq_delete_file(table, path, ids):
    eq_schema = table.schema().select(["id"])
    id_fid = table.schema().findField("id").fieldId()
    eq_ids = gw.new_array(jvm.int, 1)
    eq_ids[0] = id_fid
    af = GenAppFac(table.schema(), table.spec(), eq_ids, eq_schema, None)
    w = af.newEqDeleteWriter(enc(Files.localOutput(path)), FileFormat.PARQUET, None)
    for idv in ids:
        w.write(rec(eq_schema, {"id": idv}))
    w.close()
    return w.toDeleteFile()


def tbl_path(name):
    return os.path.join(WAREHOUSE, "db", name)


def create(name, columns):
    spark.sql(f"DROP TABLE IF EXISTS local.db.{name}")
    spark.sql(
        f"CREATE TABLE local.db.{name} ({columns}) USING iceberg "
        "TBLPROPERTIES('format-version'='2','write.delete.mode'='merge-on-read')"
    )
    return load_table(tbl_path(name))


fixtures = {}
try:
    # 1. BAD / DUPLICATE: delete + both versions of key 1 in ONE snapshot (equal seq).
    t = create("bad_equal_seq", "id INT, val STRING")
    df = write_data_file(
        t, os.path.join(tbl_path("bad_equal_seq"), "data", "bug-data.parquet"),
        [{"id": 1, "val": "a_stale"}, {"id": 1, "val": "b_current"}],
    )
    ddf = write_eq_delete_file(
        t, os.path.join(tbl_path("bad_equal_seq"), "data", "bug-eqdel.parquet"), [1]
    )
    t.newRowDelta().addRows(df).addDeletes(ddf).commit()
    fixtures["bad_equal_seq"] = dict(
        key_columns=["id"], version_column=None,
        expected_verdict="DUPLICATE", expected_mult_phys=2, key={"id": 1},
    )

    # 2. GOOD / FAITHFUL: ascending snapshots WITH a version column.
    t = create("good_ascending", "id INT, val STRING, ver INT")
    dfa = write_data_file(
        t, os.path.join(tbl_path("good_ascending"), "data", "a.parquet"),
        [{"id": 1, "val": "a_stale", "ver": 1}],
    )
    t.newAppend().appendFile(dfa).commit()  # seq 1
    t = load_table(tbl_path("good_ascending"))
    dfb = write_data_file(
        t, os.path.join(tbl_path("good_ascending"), "data", "b.parquet"),
        [{"id": 1, "val": "b_current", "ver": 2}],
    )
    ddf = write_eq_delete_file(
        t, os.path.join(tbl_path("good_ascending"), "data", "eqdel.parquet"), [1]
    )
    t.newRowDelta().addRows(dfb).addDeletes(ddf).commit()  # seq 2
    fixtures["good_ascending"] = dict(
        key_columns=["id"], version_column="ver",
        expected_verdict="FAITHFUL", expected_mult_phys=1, key={"id": 1},
    )

    # 3. UNDECIDABLE: same ascending layout, NO version column.
    t = create("undecidable_no_version", "id INT, val STRING")
    dfa = write_data_file(
        t, os.path.join(tbl_path("undecidable_no_version"), "data", "a.parquet"),
        [{"id": 1, "val": "a_stale"}],
    )
    t.newAppend().appendFile(dfa).commit()  # seq 1
    t = load_table(tbl_path("undecidable_no_version"))
    dfb = write_data_file(
        t, os.path.join(tbl_path("undecidable_no_version"), "data", "b.parquet"),
        [{"id": 1, "val": "b_current"}],
    )
    ddf = write_eq_delete_file(
        t, os.path.join(tbl_path("undecidable_no_version"), "data", "eqdel.parquet"), [1]
    )
    t.newRowDelta().addRows(dfb).addDeletes(ddf).commit()  # seq 2
    fixtures["undecidable_no_version"] = dict(
        key_columns=["id"], version_column=None,
        expected_verdict="UNDECIDABLE", expected_mult_phys=1, key={"id": 1},
    )

    # 4. WRONGLY_SUPPRESSED: current data at seq 1, stale delete at seq 2, no re-insert.
    t = create("wrongly_suppressed", "id INT, val STRING")
    dcur = write_data_file(
        t, os.path.join(tbl_path("wrongly_suppressed"), "data", "cur.parquet"),
        [{"id": 1, "val": "b_current"}],
    )
    t.newAppend().appendFile(dcur).commit()  # seq 1
    t = load_table(tbl_path("wrongly_suppressed"))
    dlate = write_eq_delete_file(
        t, os.path.join(tbl_path("wrongly_suppressed"), "data", "latedel.parquet"), [1]
    )
    t.newRowDelta().addDeletes(dlate).commit()  # seq 2
    # Default (no delete-context signal) verdict is NEEDS_CONTEXT; with --upsert-only it
    # escalates to the WRONGLY_SUPPRESSED_CURRENT violation. The fixture test checks both.
    fixtures["wrongly_suppressed"] = dict(
        key_columns=["id"], version_column=None,
        expected_verdict="NEEDS_CONTEXT", expected_mult_phys=0, key={"id": 1},
        expected_verdict_upsert_only="WRONGLY_SUPPRESSED_CURRENT",
    )

    for name, meta in fixtures.items():
        meta["table_dir"] = tbl_path(name)
    with open(os.path.join(HERE, "expected.json"), "w") as f:
        json.dump(fixtures, f, indent=2)
    print("BUILT FIXTURES:")
    print(json.dumps(fixtures, indent=2))
except Exception:
    traceback.print_exc()
    sys.exit(1)
finally:
    spark.stop()
