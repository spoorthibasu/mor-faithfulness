#!/usr/bin/env python3
"""M3 sanity check (SYNTHETIC, small): the gate is selective when the ordering column advances with
commit order — the property real CDC has and the random-merge harness lacks (NOT a substitute for
Phase 8). Two hand-built 6-commit tables, ordering = lsn:
  clean-contiguous : commit c writes lsn window [(c-1)*K+1 .. c*K]  -> no file-level inversion -> gate SKIPS
  corrupted-contig : one key's last two versions are commit-swapped  -> one inversion -> gate AUDITS + captures
Reads the gate instrumentation + verdict from the snapshot summary.
"""
import os
import sys

VER = os.environ.get("MOR_ICEBERG_VERSION", "1.10.2")
JAR = os.environ.get("MOR_ICEBERG_JAR")
WH = sys.argv[1]
IVY = os.path.join(WH, "_ivy")
os.makedirs(WH, exist_ok=True)
K = 5  # keys per commit
C = 6  # commits (>= bin-pack min-input-files=5, so the rewrite actually runs)

ADD_OPENS = " ".join(
    f"--add-opens=java.base/{p}=ALL-UNNAMED"
    for p in ["java.lang", "java.lang.invoke", "java.lang.reflect", "java.io", "java.net", "java.nio",
              "java.util", "java.util.concurrent", "java.util.concurrent.atomic", "sun.nio.ch",
              "sun.nio.cs", "sun.security.action", "sun.util.calendar"]
) + " --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"

from pyspark.sql import SparkSession
b = (SparkSession.builder.appName("m3-contiguous").master("local[2]")
     .config("spark.jars.ivy", IVY)
     .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
     .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
     .config("spark.sql.catalog.local.type", "hadoop")
     .config("spark.sql.catalog.local.warehouse", WH)
     .config("spark.sql.catalog.local.cache-enabled", "false")
     .config("spark.sql.catalogImplementation", "in-memory")
     .config("spark.driver.host", "localhost").config("spark.ui.enabled", "false")
     .config("spark.sql.shuffle.partitions", "1")
     .config("spark.driver.extraJavaOptions", ADD_OPENS).config("spark.executor.extraJavaOptions", ADD_OPENS))
b = b.config("spark.jars", JAR) if JAR else b.config(
    "spark.jars.packages", f"org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:{VER}")
spark = b.getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
jvm = spark._jvm
Files = jvm.org.apache.iceberg.Files
FileFormat = jvm.org.apache.iceberg.FileFormat
GenRecord = jvm.org.apache.iceberg.data.GenericRecord
GenAppFac = jvm.org.apache.iceberg.data.GenericAppenderFactory
EncFiles = jvm.org.apache.iceberg.encryption.EncryptedFiles
EMPTY = jvm.org.apache.iceberg.encryption.EncryptionKeyMetadata.EMPTY


def enc(o):
    return EncFiles.encryptedOutput(o, EMPTY)


def load(p):
    return jvm.org.apache.iceberg.hadoop.HadoopTables(spark._jsc.hadoopConfiguration()).load(p)


def mk(schema, d, names):
    r = GenRecord.create(schema)
    for n in names:
        r.setField(n, d[n])
    return r


def wdata(t, path, rows):
    w = GenAppFac(t.schema(), t.spec()).newDataWriter(enc(Files.localOutput(path)), FileFormat.PARQUET, None)
    for row in rows:
        w.write(mk(t.schema(), row, ("id", "val", "lsn")))
    w.close()
    return w.toDataFile()


def wdel(t, path, ids):
    eqs = t.schema().select(["id"])
    gw = spark.sparkContext._gateway
    a = gw.new_array(jvm.int, 1)
    a[0] = t.schema().findField("id").fieldId()
    w = GenAppFac(t.schema(), t.spec(), a, eqs, None).newEqDeleteWriter(
        enc(Files.localOutput(path)), FileFormat.PARQUET, None)
    for i in ids:
        w.write(mk(eqs, {"id": i}, ("id",)))
    w.close()
    return w.toDeleteFile()


def create(name):
    tdir = os.path.join(WH, "db", name)
    os.makedirs(os.path.join(tdir, "data"), exist_ok=True)
    spark.sql(f"DROP TABLE IF EXISTS local.db.{name}")
    spark.sql(f"CREATE TABLE local.db.{name} (id INT, val STRING, lsn INT) USING iceberg "
              "TBLPROPERTIES('format-version'='2','write.delete.mode'='merge-on-read')")
    return tdir


def commit(tdir, tag, rows, del_ids=None):
    """One commit: append `rows` (list of {id,lsn}); optionally eq-delete `del_ids`. Files stay live
    (each holds a distinct key's current row), so bin-pack rewrites them all into one group."""
    t = load(tdir)
    df = wdata(t, os.path.join(tdir, "data", f"{tag}-d.parquet"),
               [{"id": r["id"], "val": f"v{r['lsn']}", "lsn": r["lsn"]} for r in rows])
    if del_ids:
        ddf = wdel(t, os.path.join(tdir, "data", f"{tag}-x.parquet"), del_ids)
        t.newRowDelta().addRows(df).addDeletes(ddf).commit()
    else:
        t.newAppend().appendFile(df).commit()


def build_clean(name):
    # 10 distinct keys, one per commit, lsn advancing with commit order: contiguous, non-inverting, live.
    tdir = create(name)
    for c in range(1, 11):
        commit(tdir, f"c{c}", [{"id": c, "lsn": c}])
    return tdir


def build_corrupt(name):
    # keys 1..8 clean (lsn 1..8); key 10's HIGH-lsn version (12) rides in commit 9 next to live key 9;
    # commit 10 writes key 10's LOW-lsn survivor (11) + eq-delete -> stale-win, and commit 9 stays live.
    tdir = create(name)
    for c in range(1, 9):
        commit(tdir, f"c{c}", [{"id": c, "lsn": c}])
    commit(tdir, "c9", [{"id": 9, "lsn": 9}, {"id": 10, "lsn": 12}])  # ord [9,12]
    commit(tdir, "c10", [{"id": 10, "lsn": 11}], del_ids=[10])         # ord [11,11]; suppresses id10@12
    return tdir


def audit_compact(name):
    opts = ("map('audit-stale-wins','true','audit-ordering-column','lsn',"
            "'audit-key-columns','id','audit-gate','true')")
    r = spark.sql(f"CALL local.system.rewrite_data_files(table => 'db.{name}', options => {opts})").collect()
    print(f"  [{name}] rewrite result:", r[0].asDict() if r else None)
    spark.sql(f"REFRESH TABLE local.db.{name}")
    snaps = spark.sql(
        f"SELECT operation, summary FROM local.db.{name}.snapshots ORDER BY committed_at").collect()
    print(f"  [{name}] snapshots:", [(s['operation'],
          {k: v for k, v in (s['summary'] or {}).items() if k.startswith('mor.audit.')}) for s in snaps])
    s = snaps[-1]["summary"] if snaps else {}
    return {k: v for k, v in (s or {}).items() if k.startswith("mor.audit.")}


# diagnostic: per-file sequence number + lsn bounds on a fresh clean table (gate inputs)
build_clean("m3_plain")
seqs = spark.sql("SELECT sequence_number FROM local.db.m3_plain.entries WHERE status != 2 "
                 "ORDER BY sequence_number").collect()
mets = spark.sql("SELECT readable_metrics.lsn.lower_bound lo, readable_metrics.lsn.upper_bound hi "
                 "FROM local.db.m3_plain.files").collect()
print("  [diag] seqs:", [r["sequence_number"] for r in seqs])
print("  [diag] lsn bounds:", sorted((r["lo"], r["hi"]) for r in mets))

build_clean("m3_clean")
clean = audit_compact("m3_clean")

build_corrupt("m3_corrupt")
corrupt = audit_compact("m3_corrupt")

print("clean-contiguous  :", {k: clean.get(k) for k in
      ["mor.audit.groups-total", "mor.audit.groups-gated", "mor.audit.groups-audited", "mor.audit.stale-wins-count"]})
print("corrupt-contiguous:", {k: corrupt.get(k) for k in
      ["mor.audit.groups-total", "mor.audit.groups-gated", "mor.audit.groups-audited", "mor.audit.stale-wins-count"]},
      "keys=", corrupt.get("mor.audit.stale-wins-keys"))
ok = (clean.get("mor.audit.groups-gated") == "1" and clean.get("mor.audit.stale-wins-count") == "0"
      and corrupt.get("mor.audit.groups-audited") == "1" and corrupt.get("mor.audit.stale-wins-count") == "1")
print(f"\nGATE SELECTIVE under commit-contiguous ordering: {ok}")
spark.stop()
sys.exit(0 if ok else 1)
