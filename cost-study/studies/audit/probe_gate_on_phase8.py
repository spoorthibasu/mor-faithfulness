#!/usr/bin/env python3
"""Does the metadata gate clear the REAL CDC table of §6.6, or does it audit it?

THE QUESTION. §6.4 reports gate clearance falling to zero at about one out-of-window row per file
group. The standing objection is that the gate may clear exactly where the problem does not occur --
a busy multi-writer CDC table is the interleaving case -- so "the gate removes essentially all of the
capture cost" could be true only of tables that never needed auditing. The synthetic sweep cannot
close that. The Phase 8 table can bound it: Postgres 14 / Debezium 2.7.3 / Kafka / the stock Flink
CDC Iceberg writer, ordered by the WAL LSN, and it carries a real STALE_WINS violation.

WHAT IS MEASURED. groups total / gated / audited, read from the audit's own snapshot summary
(`mor.audit.groups-*`), on the PRE-COMPACTION state of that table.

THE TABLE IS NOT REBUILT. All five snapshots survive in the committed run's warehouse, so the
pre-compaction state (snapshot 3253760871779525870, sequence number 4: 4 data files, 4 equality
deletes, 200 rows) is reachable by rollback. No Docker, no Maven, no Flink. The warehouse embeds
absolute paths, so it cannot be relocated; this probe therefore TARS IT FIRST and RESTORES IT after,
and verifies the restore byte-for-byte. Compaction mutates a table in place -- that is already one of
this project's recorded silent-success failures -- so the artifact is protected rather than trusted.

POSITIVE CONTROLS, all hard failures.

  C1  the rollback must land on the pre-compaction state: 4 data files, 4 equality deletes, 200 rows.
      Measuring the post-compaction leftover would be measuring one data file and nothing to gate.
  C2  the rewrite must actually rewrite. Four data files is below `min-input-files` default 5, so the
      original run passed min-input-files=2 and so does this; a run that planned nothing is the
      fastest possible way to produce a clean-looking gate result.
  C3  the audit must actually have run: `mor.audit.*` must be PRESENT in the resulting snapshot
      summary. An absent summary is the flag being off, not the gate deciding anything.
  C4  the gate must have BOUNDS TO READ. This is the Entry 18 failure mode: the rewrite scan task's
      DataFile carries null column bounds, so a gate reading bounds off the task always hits its
      missing-bounds fallback and audits everything while appearing to work. The probe asserts the
      manifests carry non-null lower/upper bounds for the ordering column before believing any
      "audited" result.
  C5  the per-sequence comparison must be exercised: the group must span at least two distinct data
      sequence numbers, or there is nothing to compare across and "audited" is vacuous.
  C6  the gate must be capable of clearing SOMETHING. A gate stuck at audit-everything looks
      identical to a gate correctly auditing a violating table. So the same code path is run against
      a synthetic CLEAN table of the same shape -- four commits, ascending LSN, no inversion -- and
      that one must come back GATED. Without this arm the headline number cannot be interpreted.

Run:  JAVA_HOME=<jdk17> ../../../checker/.venv/bin/python probe_gate_on_phase8.py
Writes probe_gate_on_phase8.json alongside. Exits non-zero if any control fails.
"""
import json
import os
import shutil
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))
OUT = os.path.join(HERE, "probe_gate_on_phase8.json")
P8 = os.path.join(REPO, "phase8-cdc", "results", "phase8_cdc_wh")
TBL_DIR = os.path.join(P8, "realworld", "phase8_cdc")
PRE_SNAPSHOT = 3253760871779525870          # sequence number 4, the last CDC write
JAR = os.environ.get("MOR_ICEBERG_JAR", os.path.expanduser(
    "~/IdeaProjects/iceberg-mor-fork/spark/v3.5/spark-runtime/build/libs/"
    "iceberg-spark-runtime-3.5_2.12-1.11.0-SNAPSHOT.jar"))
assert os.path.exists(JAR), f"forked jar not found: {JAR}"
assert os.path.isdir(TBL_DIR), f"phase 8 warehouse not found: {TBL_DIR}"

SCRATCH = tempfile.mkdtemp(prefix="mor_gate_p8_")
BACKUP = os.path.join(SCRATCH, "phase8_cdc_wh.tar")
CLEAN_WH = os.path.join(SCRATCH, "clean_wh")
import atexit
FAIL, R = [], {"jar": os.path.basename(JAR), "pre_snapshot": PRE_SNAPSHOT}


def check_(cond, msg):
    print(("  ok   " if cond else "  FAIL ") + msg)
    if not cond:
        FAIL.append(msg)
    return cond


print("=== protecting the artifact ===")
subprocess.run(["tar", "-cf", BACKUP, "-C", os.path.dirname(P8), os.path.basename(P8)], check=True)
print(f"  tarred {P8} -> {BACKUP} ({os.path.getsize(BACKUP)} bytes)")
_restored = {"done": False}
@atexit.register
def _safety_restore():
    if not _restored["done"]:
        print("\n  !! run did not reach its own restore -- restoring the artifact now")
        try:
            shutil.rmtree(P8, ignore_errors=True)
            subprocess.run(["tar", "-xf", BACKUP, "-C", os.path.dirname(P8)], check=True)
            print("  artifact restored by the safety hook")
        except Exception as e:
            print(f"  RESTORE FAILED: {e}; backup is at {BACKUP}")

ADD_OPENS = " ".join(
    f"--add-opens=java.base/{p}=ALL-UNNAMED"
    for p in ["java.lang", "java.lang.invoke", "java.lang.reflect", "java.io", "java.net",
              "java.nio", "java.util", "java.util.concurrent", "java.util.concurrent.atomic",
              "sun.nio.ch", "sun.nio.cs", "sun.security.action", "sun.util.calendar"]
) + " --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"

from pyspark.sql import SparkSession                                          # noqa: E402

spark = (SparkSession.builder.appName("gate-on-phase8").master("local[2]")
    .config("spark.jars", JAR)
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.h", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.h.type", "hadoop")
    .config("spark.sql.catalog.h.warehouse", P8)
    .config("spark.sql.catalog.h.cache-enabled", "false")
    .config("spark.sql.catalog.c", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.c.type", "hadoop")
    .config("spark.sql.catalog.c.warehouse", CLEAN_WH)
    .config("spark.sql.catalog.c.cache-enabled", "false")
    .config("spark.sql.catalogImplementation", "in-memory")
    .config("spark.driver.host", "localhost").config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "1")
    .config("spark.driver.extraJavaOptions", ADD_OPENS)
    .config("spark.executor.extraJavaOptions", ADD_OPENS).getOrCreate())
spark.sparkContext.setLogLevel("ERROR")


def audit_summary(cat, tbl):
    row = spark.sql(f"SELECT summary FROM {cat}.{tbl}.snapshots ORDER BY committed_at DESC "
                    f"LIMIT 1").collect()[0]
    return {k: v for k, v in row["summary"].items() if k.startswith("mor.audit.")}


def inventory(cat, tbl):
    rows = spark.sql(f"SELECT data_file.content AS content, data_file.file_path AS p, "
                     f"sequence_number FROM {cat}.{tbl}.entries WHERE status < 2").collect()
    return ([r for r in rows if r["content"] == 0], [r for r in rows if r["content"] != 0])


def ordering_bounds(cat, tbl, field_id):
    """Do the MANIFESTS carry lower/upper bounds for the ordering column? Entry 18's failure mode is
    a gate that finds none and silently audits everything."""
    rows = spark.sql(f"SELECT data_file.content AS content, data_file.lower_bounds AS lo, "
                     f"data_file.upper_bounds AS hi FROM {cat}.{tbl}.entries "
                     f"WHERE status < 2").collect()
    have = 0
    for r in rows:
        if r["content"] != 0:
            continue
        lo, hi = r["lo"] or {}, r["hi"] or {}
        if field_id in lo and field_id in hi:
            have += 1
    return have


def restore_artifact():
    """Always run. A crash between the rollback and the end would otherwise leave the committed
    Phase 8 warehouse in a rolled-back, recompacted state -- the exact in-place mutation this
    project already recorded as a silent-success failure."""
    shutil.rmtree(P8, ignore_errors=True)
    subprocess.run(["tar", "-xf", BACKUP, "-C", os.path.dirname(P8)], check=True)
    tmp = tempfile.mkdtemp()
    subprocess.run(["tar", "-xf", BACKUP, "-C", tmp], check=True)
    def tree_sha(root):
        import hashlib
        h = hashlib.sha1()
        for f in sorted(os.path.relpath(os.path.join(dp, fn), root)
                        for dp, _, fns in os.walk(root) for fn in fns):
            h.update(f.encode())
            with open(os.path.join(root, f), "rb") as fh:
                h.update(hashlib.sha1(fh.read()).digest())
        return h.hexdigest()
    same = tree_sha(P8) == tree_sha(os.path.join(tmp, os.path.basename(P8)))
    shutil.rmtree(tmp, ignore_errors=True)
    return same


AUDIT_OPTS = ("map('min-input-files','2','audit-stale-wins','true',"
              "'audit-ordering-column','lsn','audit-key-columns','id','audit-gate','true')")

# ============================================================================================
# ARM 1 — the real Phase 8 CDC table, rolled back to its pre-compaction state
# ============================================================================================
print("\n=== arm 1: the real Phase 8 CDC table ===")
try:
    spark.sql(f"CALL h.system.rollback_to_snapshot('realworld.phase8_cdc', {PRE_SNAPSHOT})")
    R["rollback"] = "ok"
except Exception as e:
    R["rollback"] = f"FAILED: {str(e)[:200]}"
print(f"  rollback to pre-compaction snapshot: {R['rollback']}")

data, dels = inventory("h", "realworld.phase8_cdc")
rows = spark.sql("SELECT count(*) c FROM h.realworld.phase8_cdc").collect()[0]["c"]
seqs = sorted({r["sequence_number"] for r in data})
R["pre"] = {"data_files": len(data), "delete_files": len(dels), "rows": rows,
            "distinct_data_sequence_numbers": seqs}
print(f"  pre-compaction: {len(data)} data files, {len(dels)} delete files, {rows} rows, "
      f"data seqs {seqs}")
check_(len(data) == 4 and len(dels) == 4 and rows == 200,
       f"C1: rolled back to the pre-compaction state (4 data / 4 delete / 200 rows; got "
       f"{len(data)}/{len(dels)}/{rows})")
check_(len(seqs) >= 2, f"C5: the group spans >1 data sequence number ({seqs})")

lsn_fid = spark.sql("SELECT * FROM h.realworld.phase8_cdc.entries LIMIT 0")  # force schema load
fid = None
import re as _re
sch = json.loads(open(os.path.join(TBL_DIR, "metadata", "v6.metadata.json")).read())
for f in sch["schemas"][0]["fields"]:
    if f["name"] == "lsn":
        fid = f["id"]
R["ordering_column_field_id"] = fid
with_bounds = ordering_bounds("h", "realworld.phase8_cdc", fid)
R["pre_data_files_with_ordering_bounds"] = with_bounds
print(f"  data files carrying lsn bounds in the manifests: {with_bounds}/{len(data)}")
check_(with_bounds == len(data),
       f"C4: every data file carries lower/upper bounds for the ordering column in the manifests "
       f"({with_bounds}/{len(data)}) -- the gate has bounds to read")

res = spark.sql(f"CALL h.system.rewrite_data_files(table => 'realworld.phase8_cdc', "
                f"options => {AUDIT_OPTS})").collect()[0]
R["rewrite_result"] = {k: res[k] for k in res.asDict()}
print(f"  rewrite: {R['rewrite_result']}")
check_(R["rewrite_result"].get("rewritten_data_files_count", 0) > 0,
       f"C2: the rewrite actually rewrote "
       f"({R['rewrite_result'].get('rewritten_data_files_count')} data files)")

summ = audit_summary("h", "realworld.phase8_cdc")
R["phase8_audit_summary"] = summ
print(f"  audit summary: {json.dumps(summ, indent=2)}")
check_(bool(summ), "C3: the audit wrote a summary (mor.audit.* present)")
R["phase8_groups"] = {k: summ.get(f"mor.audit.groups-{k}") for k in ("total", "gated", "audited")}

# ============================================================================================
# ARM 2 — synthetic CLEAN control of the same shape: the gate must CLEAR this one
# ============================================================================================
print("\n=== arm 2: clean control, same shape, ascending LSN (the gate must clear it) ===")
# The clean control must have the SAME SHAPE as Phase 8: four data files and four EQUALITY
# deletes. Spark SQL MERGE does not produce equality deletes -- it rewrites copy-on-write and left
# the first attempt at one data file and nothing to gate -- so the control is written through the
# Java API, the same path the harness and the Flink sink use, with strictly ascending LSN so there
# is no inversion for the gate to find.
import pyarrow as pa
import pyarrow.parquet as pq
jvm, gw = spark._jvm, spark.sparkContext._gateway
Files = jvm.org.apache.iceberg.Files
FileFormat = jvm.org.apache.iceberg.FileFormat
DataFiles = jvm.org.apache.iceberg.DataFiles
FileMetadata = jvm.org.apache.iceberg.FileMetadata
ParquetUtil = jvm.org.apache.iceberg.parquet.ParquetUtil
MetricsConfig = jvm.org.apache.iceberg.MetricsConfig
MappingUtil = jvm.org.apache.iceberg.mapping.MappingUtil
NameMappingParser = jvm.org.apache.iceberg.mapping.NameMappingParser

spark.sql("CREATE NAMESPACE IF NOT EXISTS c.db")
spark.sql("DROP TABLE IF EXISTS c.db.clean")
spark.sql("CREATE TABLE c.db.clean (id INT, balance INT, note STRING, lsn BIGINT) USING iceberg "
          "TBLPROPERTIES('format-version'='2','write.delete.mode'='merge-on-read')")
CDIR = os.path.join(CLEAN_WH, "db", "clean")
ctab = jvm.org.apache.iceberg.hadoop.HadoopTables(
    spark._jsc.hadoopConfiguration()).load(CDIR)
ctab.updateProperties().set("schema.name-mapping.default",
                            NameMappingParser.toJson(MappingUtil.create(ctab.schema()))).commit()
cdd = os.path.join(CDIR, "data")
os.makedirs(cdd, exist_ok=True)


def _csch(t, names, types):
    m = {"int": pa.int32(), "long": pa.int64(), "string": pa.string()}
    return pa.schema([pa.field(n, m[ty], nullable=True,
                     metadata={b"PARQUET:field_id": str(t.schema().findField(n).fieldId()).encode()})
                      for n, ty in zip(names, types)])


def _cmetrics(t, path):
    return ParquetUtil.fileMetrics(Files.localInput(path), MetricsConfig.forTable(t),
                                   MappingUtil.create(t.schema()))


for c in range(1, 5):
    t = jvm.org.apache.iceberg.hadoop.HadoopTables(
        spark._jsc.hadoopConfiguration()).load(CDIR)
    ids = list(range(1, 51))
    base = 24000000 + c * 1000
    dp = os.path.join(cdd, f"clean-c{c}-data.parquet")
    pq.write_table(pa.table({"id": ids, "balance": [c] * 50, "note": [f"v{c}"] * 50,
                             "lsn": [base + i for i in ids]},
                            schema=_csch(t, ["id", "balance", "note", "lsn"],
                                         ["int", "int", "string", "long"])), dp)
    df = (DataFiles.builder(t.spec()).withPath(dp).withFormat(FileFormat.PARQUET)
          .withFileSizeInBytes(os.path.getsize(dp)).withMetrics(_cmetrics(t, dp)).build())
    if c == 1:
        t.newAppend().appendFile(df).commit()
    else:
        eq = gw.new_array(jvm.int, 1)
        eq[0] = t.schema().findField("id").fieldId()
        xp = os.path.join(cdd, f"clean-c{c}-del.parquet")
        pq.write_table(pa.table({"id": ids}, schema=_csch(t, ["id"], ["int"])), xp)
        ddf = (FileMetadata.deleteFileBuilder(t.spec()).ofEqualityDeletes(eq)
               .withPath(xp).withFormat(FileFormat.PARQUET)
               .withFileSizeInBytes(os.path.getsize(xp))
               .withMetrics(_cmetrics(t, xp)).build())
        t.newRowDelta().addRows(df).addDeletes(ddf).commit()

cdata, cdels = inventory("c", "db.clean")
print(f"  clean control: {len(cdata)} data files, {len(cdels)} delete files")
cres = spark.sql(f"CALL c.system.rewrite_data_files(table => 'db.clean', "
                 f"options => {AUDIT_OPTS})").collect()[0]
R["clean_rewrite_result"] = {k: cres[k] for k in cres.asDict()}
csumm = audit_summary("c", "db.clean")
R["clean_audit_summary"] = csumm
R["clean_groups"] = {k: csumm.get(f"mor.audit.groups-{k}") for k in ("total", "gated", "audited")}
print(f"  clean control audit summary: {json.dumps(csumm, indent=2)}")
check_(bool(csumm), "C6a: the clean control produced an audit summary")
cg, ct = csumm.get("mor.audit.groups-gated"), csumm.get("mor.audit.groups-total")
check_(cg is not None and ct is not None and int(cg) == int(ct) and int(ct) > 0,
       f"C6b: the gate CLEARS the clean control ({cg}/{ct} groups gated) -- so it is not stuck "
       f"auditing everything")

# ============================================================================================
print("\n=== restoring the artifact ===")
spark.stop()
R["artifact_restored_identical"] = restore_artifact()
_restored["done"] = True
print(f"  restored, tree checksum identical: {R['artifact_restored_identical']}")
check_(R["artifact_restored_identical"], "the Phase 8 warehouse is byte-identical after the run")

R["failures"] = FAIL
print()
if FAIL:
    print("CONTROLS FAILED -- the run did not measure the intended shape:")
    for f in FAIL:
        print("   -", f)
else:
    g = R["phase8_groups"]
    print(f"  => Phase 8 CDC table: groups total {g['total']}, gated {g['gated']}, "
          f"audited {g['audited']}")
    print(f"     clean control:      groups total {R['clean_groups']['total']}, "
          f"gated {R['clean_groups']['gated']}, audited {R['clean_groups']['audited']}")
with open(OUT, "w") as f:
    json.dump(R, f, indent=1)
print(f"\n  -> {OUT}")

sys.exit(1 if FAIL else 0)
