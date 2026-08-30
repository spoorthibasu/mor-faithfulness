#!/usr/bin/env python3
"""Does Iceberg format-version 3 row lineage survive the equality-delete write path?

THE QUESTION. §4.7 argues that v3 row lineage does not answer this paper's problem, because
lineage is not carried for rows updated through equality deletes: an engine writing them does not
read existing data first, so it cannot put the original identifier on the replacement row, and the
replacement is treated as an unrelated insertion. That is load-bearing motivation, and until this
probe no v3 table had been built. This settles it by measurement.

THE SHAPE. Three v3 merge-on-read tables. In every one, commit 1 is a PLAIN INSERT (the positive
control -- lineage must be present there) and commit 2 updates key 2:

  eqdel_javaapi : the harness's NON-BULK writer path verbatim --
                  GenericAppenderFactory.newDataWriter / newEqDeleteWriter, one RowDelta per
                  checkpoint carrying data + equality delete at the same sequence number.
  eqdel_bulk    : the harness's BULK writer path verbatim -- pyarrow parquet registered through
                  DataFiles / FileMetadata.ofEqualityDeletes.
  control_sql   : engine-managed -- Spark SQL INSERT, then an engine-native Spark SQL UPDATE.

control_sql is the discriminator. Without it, an absence on the equality-delete arms cannot be told
apart from a build in which lineage never survives any update at all.

  LINEAGE CARRIED     => the replacement row keeps its original _row_id. §4.7 is WRONG.
  LINEAGE NOT CARRIED => the replacement row gets a fresh _row_id, or none. §4.7 stands.

POSITIVE CONTROLS, all hard failures -- a run that measured something else answers a different
question, and this project has had seven measurements report a clean result while doing nothing:

  * format-version 3 is READ BACK from the table's own metadata.json, never trusted from the DDL.
    A v2 table would show absent lineage for reasons having nothing to do with equality deletes.
  * plain-insert rows must carry a non-null _row_id in every table. If the control path shows no
    lineage, the read is wrong or the table is not v3, and no absence measured here means anything.
  * the equality delete must actually suppress the old version of key 2 (3 live rows, key 2 holding
    the new payload). An eq-delete that did not apply is not the path under study.
  * the untouched rows (keys 1 and 3) must keep their identifiers and sequence numbers unchanged.

ENGINE. Deliberately STOCK Iceberg (default 1.10.2), NOT the forked jar the audit runs use: the
question is what the published format does, and the fork touches only the rewrite runner. This
probe therefore ignores MOR_ICEBERG_JAR and resolves the published package from the local ivy cache.

Run:  JAVA_HOME=<jdk17> ../../../checker/.venv/bin/python probe_v3_row_lineage.py [warehouse]
"""
import glob
import json
import os
import shutil
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "probe_v3_row_lineage.json")
# Published package, not the fork. Local ivy cache so it resolves offline; override with MOR_IVY_DIR.
VER = os.environ.get("MOR_ICEBERG_VERSION", "1.10.2")
IVY = os.environ.get("MOR_IVY_DIR") or os.path.expanduser("~/.ivy2")
WH = sys.argv[1] if len(sys.argv) > 1 else os.path.join(tempfile.gettempdir(), "mor_v3_lineage")
shutil.rmtree(WH, ignore_errors=True)
os.makedirs(WH)

ADD_OPENS = " ".join(
    f"--add-opens=java.base/{p}=ALL-UNNAMED"
    for p in ["java.lang", "java.lang.invoke", "java.lang.reflect", "java.io", "java.net",
              "java.nio", "java.util", "java.util.concurrent", "java.util.concurrent.atomic",
              "sun.nio.ch", "sun.nio.cs", "sun.security.action", "sun.util.calendar"]
) + " --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"

from pyspark.sql import SparkSession                                          # noqa: E402

spark = (SparkSession.builder.appName("v3-row-lineage").master("local[2]")
    .config("spark.jars.packages", f"org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:{VER}")
    .config("spark.jars.ivy", IVY)
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", WH)
    .config("spark.sql.catalog.local.cache-enabled", "false")
    .config("spark.sql.catalogImplementation", "in-memory")
    .config("spark.driver.host", "localhost").config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "1")
    .config("spark.driver.extraJavaOptions", ADD_OPENS)
    .config("spark.executor.extraJavaOptions", ADD_OPENS)
    .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
jvm, gw = spark._jvm, spark.sparkContext._gateway

Files = jvm.org.apache.iceberg.Files
FileFormat = jvm.org.apache.iceberg.FileFormat
GenRecord = jvm.org.apache.iceberg.data.GenericRecord
GenAppFac = jvm.org.apache.iceberg.data.GenericAppenderFactory
EncFiles = jvm.org.apache.iceberg.encryption.EncryptedFiles
EMPTY_KEY = jvm.org.apache.iceberg.encryption.EncryptionKeyMetadata.EMPTY
DataFiles = jvm.org.apache.iceberg.DataFiles
FileMetadata = jvm.org.apache.iceberg.FileMetadata
ParquetUtil = jvm.org.apache.iceberg.parquet.ParquetUtil
MetricsConfig = jvm.org.apache.iceberg.MetricsConfig
MappingUtil = jvm.org.apache.iceberg.mapping.MappingUtil
NameMappingParser = jvm.org.apache.iceberg.mapping.NameMappingParser
MC = jvm.org.apache.iceberg.MetadataColumns
# The lineage column names come from the library, not from a string in this file.
RID, LUS = MC.ROW_ID.name(), MC.LAST_UPDATED_SEQUENCE_NUMBER.name()

COLUMNS = [{"name": "id", "type": "int"}, {"name": "lsn", "type": "int"},
           {"name": "payload", "type": "string"}]
KEY_COLUMNS = ["id"]
coltype = {c["name"]: c["type"] for c in COLUMNS}
FAIL = []


def check_(cond, msg):
    print(("  ok   " if cond else "  FAIL ") + msg)
    if not cond:
        FAIL.append(msg)
    return cond


def load(path):
    return jvm.org.apache.iceberg.hadoop.HadoopTables(
        spark._jsc.hadoopConfiguration()).load(path)


def enc(o):
    return EncFiles.encryptedOutput(o, EMPTY_KEY)


def box(name, v):
    if v is None:
        return None
    return int(v) if coltype[name] == "int" else str(v)


def mk_record(schema, row, names):
    r = GenRecord.create(schema)
    for n in names:
        r.setField(n, box(n, row.get(n)))
    return r


# ---- the harness's NON-BULK writers, verbatim from iceberg_driver.py --------------------------
def write_data_file(t, path, rows):
    af = GenAppFac(t.schema(), t.spec())
    w = af.newDataWriter(enc(Files.localOutput(path)), FileFormat.PARQUET, None)
    names = [c["name"] for c in COLUMNS]
    for row in rows:
        w.write(mk_record(t.schema(), row, names))
    w.close()
    return w.toDataFile()


def write_eq_delete_file(t, path, key_rows):
    eq_schema = t.schema().select(KEY_COLUMNS)
    eq_ids = gw.new_array(jvm.int, len(KEY_COLUMNS))
    for i, kc in enumerate(KEY_COLUMNS):
        eq_ids[i] = t.schema().findField(kc).fieldId()
    af = GenAppFac(t.schema(), t.spec(), eq_ids, eq_schema, None)
    w = af.newEqDeleteWriter(enc(Files.localOutput(path)), FileFormat.PARQUET, None)
    for kv in key_rows:
        w.write(mk_record(eq_schema, {kc: v for kc, v in zip(KEY_COLUMNS, kv)}, KEY_COLUMNS))
    w.close()
    return w.toDeleteFile()


# ---- the harness's BULK writers, verbatim from iceberg_driver.py ------------------------------
import pyarrow as pa                                                          # noqa: E402
import pyarrow.parquet as pq                                                  # noqa: E402

_PA_TYPE = {"int": "int32", "string": "string"}


def pa_schema(t, names):
    return pa.schema([
        pa.field(n, getattr(pa, _PA_TYPE[coltype[n]])(), nullable=True,
                 metadata={b"PARQUET:field_id": str(t.schema().findField(n).fieldId()).encode()})
        for n in names])


def pa_write(t, path, names, cols):
    pq.write_table(pa.table(cols, schema=pa_schema(t, names)), path)


def metrics_of(t, path):
    return ParquetUtil.fileMetrics(Files.localInput(path), MetricsConfig.forTable(t),
                                   MappingUtil.create(t.schema()))


def bulk_write_data_file(t, path, rows):
    names = [c["name"] for c in COLUMNS]
    pa_write(t, path, names, {n: [r.get(n) for r in rows] for n in names})
    return (DataFiles.builder(t.spec()).withPath(path).withFormat(FileFormat.PARQUET)
            .withFileSizeInBytes(os.path.getsize(path))
            .withMetrics(metrics_of(t, path)).build())


def bulk_write_eq_delete_file(t, path, key_rows):
    eq_ids = gw.new_array(jvm.int, len(KEY_COLUMNS))
    for i, kc in enumerate(KEY_COLUMNS):
        eq_ids[i] = t.schema().findField(kc).fieldId()
    pa_write(t, path, list(KEY_COLUMNS),
             {kc: [kv[i] for kv in key_rows] for i, kc in enumerate(KEY_COLUMNS)})
    return (FileMetadata.deleteFileBuilder(t.spec()).ofEqualityDeletes(eq_ids)
            .withPath(path).withFormat(FileFormat.PARQUET)
            .withFileSizeInBytes(os.path.getsize(path))
            .withMetrics(metrics_of(t, path)).build())


# ---- readers ---------------------------------------------------------------------------------
def metadata_json(tdir):
    """format-version READ BACK from the table's own metadata, never trusted from the DDL."""
    ms = sorted(glob.glob(os.path.join(tdir, "metadata", "*.metadata.json")))
    with open(ms[-1]) as f:
        m = json.load(f)
    return m.get("format-version"), m.get("next-row-id")


def lineage(tbl):
    rows = spark.sql(f"SELECT id, lsn, payload, {RID} AS row_id, {LUS} AS last_updated_seq "
                     f"FROM local.db.{tbl} ORDER BY id").collect()
    return [r.asDict() for r in rows]


def files_meta(tbl):
    cols = [f.name for f in spark.table(f"local.db.{tbl}.files").schema.fields]
    want = [c for c in ("content", "file_path", "record_count", "first_row_id") if c in cols]
    rs = spark.sql(f"SELECT {', '.join(want)} FROM local.db.{tbl}.files").collect()
    return [{**r.asDict(), "file_path": os.path.basename(r.asDict()["file_path"])} for r in rs]


def parquet_columns(path):
    """Does the replacement data file physically carry a _row_id column? This is the mechanism."""
    return list(pq.read_table(path).schema.names)


def create_v3(tbl):
    tdir = os.path.join(WH, "db", tbl)
    ddl = ", ".join(f"{c['name']} {'INT' if c['type'] == 'int' else 'STRING'}" for c in COLUMNS)
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db")
    spark.sql(f"DROP TABLE IF EXISTS local.db.{tbl}")
    spark.sql(f"CREATE TABLE local.db.{tbl} ({ddl}) USING iceberg "
              "TBLPROPERTIES('format-version'='3','write.delete.mode'='merge-on-read',"
              "'write.update.mode'='merge-on-read')")
    fv, nrid = metadata_json(tdir)
    check_(fv == 3, f"{tbl}: format-version read back from metadata.json is 3 (got {fv})")
    return tdir, {"format_version_read_back": fv, "next_row_id_at_create": nrid}


ROWS_C1 = [{"id": 1, "lsn": 10, "payload": "v1-k1"},
           {"id": 2, "lsn": 11, "payload": "v1-k2"},
           {"id": 3, "lsn": 12, "payload": "v1-k3"}]

RESULT = {"iceberg_build_version": jvm.org.apache.iceberg.IcebergBuild.version(),
          "engine_is_stock_not_fork": True,
          "lineage_columns": {"row_id": RID, "last_updated_sequence_number": LUS}}


# ==============================================================================================
# Arms 1 and 2: the harness's own equality-delete write path, against a v3 table.
# ==============================================================================================
def run_eqdel_arm(tbl, bulk):
    print(f"\n[{tbl}] {'bulk' if bulk else 'java-api'} writer, equality-delete update")
    wdata = bulk_write_data_file if bulk else write_data_file
    wdel = bulk_write_eq_delete_file if bulk else write_eq_delete_file
    tdir, arm = create_v3(tbl)
    arm["writer_path"] = "bulk" if bulk else "java-api"
    ddir = os.path.join(tdir, "data")
    os.makedirs(ddir, exist_ok=True)
    if bulk:  # name mapping for externally written files, exactly as the driver sets it
        t0 = load(tdir)
        t0.updateProperties().set(
            "schema.name-mapping.default",
            NameMappingParser.toJson(MappingUtil.create(t0.schema()))).commit()

    # ---- commit 1: PLAIN INSERT (positive control) -------------------------------------------
    t = load(tdir)
    d1 = wdata(t, os.path.join(ddir, "c1-data.parquet"), ROWS_C1)
    t.newAppend().appendFile(d1).commit()
    before = lineage(tbl)
    arm["after_insert"] = {"lineage": before, "next_row_id": metadata_json(tdir)[1]}
    check_(len(before) == 3, f"{tbl}: control insert wrote 3 live rows (got {len(before)})")
    missing = [r["id"] for r in before if r["row_id"] is None]
    check_(not missing, f"{tbl}: plain-insert rows all carry a non-null {RID} "
                        f"(null for {missing or 'none'})")

    # ---- commit 2: the CDC update -- eq-delete on key 2 + replacement row, ONE RowDelta -------
    t = load(tdir)
    c2_data = os.path.join(ddir, "c2-data.parquet")
    d2 = wdata(t, c2_data, [{"id": 2, "lsn": 20, "payload": "v2-k2"}])
    x2 = wdel(t, os.path.join(ddir, "c2-eqdel.parquet"), [(2,)])
    t.newRowDelta().addRows(d2).addDeletes(x2).commit()

    after = lineage(tbl)
    arm["after_eq_delete_update"] = {
        "lineage": after, "next_row_id": metadata_json(tdir)[1], "files": files_meta(tbl),
        "replacement_file_parquet_columns": parquet_columns(c2_data)}
    k2 = [r for r in after if r["id"] == 2]
    check_(len(after) == 3, f"{tbl}: 3 live rows after the update (got {len(after)})")
    check_(len(k2) == 1 and k2[0]["payload"] == "v2-k2",
           f"{tbl}: the equality delete suppressed the old version of key 2")
    b = {r["id"]: r for r in before}
    check_(all(r["row_id"] == b[r["id"]]["row_id"]
               and r["last_updated_seq"] == b[r["id"]]["last_updated_seq"]
               for r in after if r["id"] in (1, 3)),
           f"{tbl}: untouched rows 1 and 3 kept their identifiers and sequence numbers")

    arm["verdict"] = {
        "key2_row_id_before": b[2]["row_id"],
        "key2_row_id_after": k2[0]["row_id"] if k2 else None,
        "key2_row_id_null_after": bool(k2) and k2[0]["row_id"] is None,
        "key2_row_id_preserved": bool(k2) and k2[0]["row_id"] == b[2]["row_id"],
        "key2_last_updated_seq_after": k2[0]["last_updated_seq"] if k2 else None,
    }
    return arm


RESULT["eqdel_javaapi"] = run_eqdel_arm("eqdel_javaapi", bulk=False)
RESULT["eqdel_bulk"] = run_eqdel_arm("eqdel_bulk", bulk=True)

# ==============================================================================================
# Arm 3: the discriminating control -- SQL INSERT, then an engine-native SQL UPDATE.
# Tells "lineage never survives an update here" apart from "not on THIS path".
# ==============================================================================================
TBL = "control_sql"
print(f"\n[{TBL}] engine-managed writer, native SQL UPDATE")
tdir, arm = create_v3(TBL)
arm["writer_path"] = "spark-sql"
spark.sql(f"INSERT INTO local.db.{TBL} VALUES (1,10,'v1-k1'),(2,11,'v1-k2'),(3,12,'v1-k3')")
before = lineage(TBL)
arm["after_insert"] = {"lineage": before, "next_row_id": metadata_json(tdir)[1]}
missing = [r["id"] for r in before if r["row_id"] is None]
check_(not missing, f"{TBL}: plain-insert rows all carry a non-null {RID} "
                    f"(null for {missing or 'none'})")

spark.sql(f"UPDATE local.db.{TBL} SET payload='v2-k2', lsn=20 WHERE id=2")
after = lineage(TBL)
files = files_meta(TBL)
# the file the UPDATE wrote: the data file whose first_row_id is the highest
rewritten = max([f for f in files if f["content"] == 0], key=lambda f: f["first_row_id"] or -1)
arm["after_sql_update"] = {
    "lineage": after, "next_row_id": metadata_json(tdir)[1], "files": files,
    "rewritten_file_parquet_columns": parquet_columns(
        os.path.join(tdir, "data", rewritten["file_path"]))}
k2 = [r for r in after if r["id"] == 2]
b = {r["id"]: r for r in before}
check_(len(k2) == 1 and k2[0]["payload"] == "v2-k2", f"{TBL}: the SQL UPDATE applied")
arm["verdict"] = {
    "key2_row_id_before": b[2]["row_id"],
    "key2_row_id_after": k2[0]["row_id"] if k2 else None,
    "key2_row_id_preserved": bool(k2) and k2[0]["row_id"] == b[2]["row_id"],
    "key2_last_updated_seq_after": k2[0]["last_updated_seq"] if k2 else None,
}
RESULT["control_sql"] = arm
check_(arm["verdict"]["key2_row_id_preserved"],
       f"{TBL}: engine-native UPDATE PRESERVES {RID} -- lineage works where it should")

# ==============================================================================================
print()
if FAIL:
    print("CONTROLS FAILED -- the run did not measure the intended shape:")
    for f in FAIL:
        print("   -", f)
else:
    for arm_name in ("eqdel_javaapi", "eqdel_bulk", "control_sql"):
        v = RESULT[arm_name]["verdict"]
        print(f"  {arm_name:15s} key 2 _row_id {v['key2_row_id_before']} -> "
              f"{v['key2_row_id_after']}  "
              f"{'PRESERVED' if v['key2_row_id_preserved'] else 'NOT preserved'}"
              f"   last_updated_seq={v['key2_last_updated_seq_after']}")
    print()
    eq = RESULT["eqdel_javaapi"]["verdict"]
    if eq["key2_row_id_preserved"]:
        print("  => lineage IS carried across an equality-delete update. §4.7 IS WRONG.")
    else:
        print("  => the replacement row does NOT keep its identifier. §4.7 stands, with the")
        print("     wording correction that the identifier is PRESENT and FRESH, not absent:")
        print(f"     _row_id is {'null' if eq['key2_row_id_null_after'] else 'a new value'}, "
              "and next-row-id advances as for an unrelated insertion.")
        print("     Mechanism, at the file level:")
        print("       eq-delete replacement file :",
              RESULT["eqdel_javaapi"]["after_eq_delete_update"]["replacement_file_parquet_columns"])
        print("       SQL UPDATE rewritten file  :",
              RESULT["control_sql"]["after_sql_update"]["rewritten_file_parquet_columns"])

RESULT["failures"] = FAIL
with open(OUT, "w") as f:
    json.dump(RESULT, f, indent=1)
print(f"\n  -> {OUT}")
spark.stop()
shutil.rmtree(WH, ignore_errors=True)
sys.exit(1 if FAIL else 0)
