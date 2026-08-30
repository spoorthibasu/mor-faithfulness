#!/usr/bin/env python3
"""Does a rewrite group elect a survivor from among globally-discarded versions?

THE QUESTION. `lean/MorFaithful/Grouped.lean` models a group's suppression threshold as the max
sequence number over the GROUP'S OWN CONTENTS (`groupSD = G.sup M.s`) and group-relative visibility
as filtering against it. Under that model a group holding only globally-discarded versions still
elects one of them -- the highest-seq one -- as a local survivor, and can therefore witness a stale
win. Two Lean results (T2, and the refutation of T3) turn on exactly that.

Real Iceberg may not behave that way: the equality deletes applicable to a data file are matched to
it table-wide, not group-wide, so a delete written in a commit whose data file is OUTSIDE the group
is still in scope for files INSIDE it. If so, every version in such a group is marked deleted, no
survivor is elected, and no verdict is possible.

THE SHAPE, matching Grouped.lean's `Mfp` witness. One key, three versions in three commits:

    commit 1 (seq 1): id=1, lsn=50    <- inside the rewrite group
    commit 2 (seq 2): id=1, lsn=10    <- inside the rewrite group, + eq delete on id=1
    commit 3 (seq 3): id=1, lsn=100   <- OUTSIDE the group,       + eq delete on id=1

Globally the seq-3 version alone survives and it out-orders both discarded versions (100 > 50),
so there is no genuine stale win. The group {seq 1, seq 2} is exactly the non-co-resident group of
T2: under Grouped.lean's model its threshold is max(1,2)=2, so the seq-2 version (lsn 10) is elected
survivor, the seq-1 version (lsn 50) is discarded, and 50 > 10 is a stale win.

  MODEL FAITHFUL      => the mechanism reports a violation for key 1.
  MODEL NOT FAITHFUL  => it reports nothing, because both versions are marked deleted.

POSITIVE CONTROLS, all hard failures -- a run that forms different groups answers a different
question:
  * the intended group composition was actually produced (exactly the two low-lsn files, verified
    from the manifest, not assumed from the where-clause)
  * the rewrite planned and rewrote files rather than selecting none
  * key 1 was present in the group being rewritten
  * the table really is in the intended global state (one live row, lsn=100) before the rewrite
  * the audit actually ran on the group rather than being skipped by the metadata gate
"""
import json
import os
import shutil
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))  # repo root, not cost-study/
JAR = os.environ.get("MOR_ICEBERG_JAR", os.path.expanduser(
    "~/IdeaProjects/iceberg-mor-fork/spark/v3.5/spark-runtime/build/libs/"
    "iceberg-spark-runtime-3.5_2.12-1.11.0-SNAPSHOT.jar"))
JAVA_HOME = "/Library/Java/JavaVirtualMachines/temurin-17.jdk/Contents/Home"
# pyspark must be 3.5.x to match the Spark 3.5 runtime jar; the system interpreter carries 4.x,
# which fails at analysis time with an unrelated-looking error.
PY = os.environ.get("MOR_PY", os.path.join(REPO, "checker", ".venv", "bin", "python"))
if not os.path.exists(PY):
    PY = sys.executable
OUT = os.path.join(HERE, "probe_group_survivor_semantics"
                   + ("" if os.environ.get("MOR_SS", "true") == "true" else "_noguard") + ".json")

WH = tempfile.mkdtemp(prefix="mor_survivor_probe_")

SCRIPT = r'''
import json, os, sys
from pyspark.sql import SparkSession

WH = sys.argv[1]; JAR = sys.argv[2]
spark = (SparkSession.builder.appName("survivor_probe").master("local[2]")
         .config("spark.jars", JAR)
         .config("spark.sql.extensions",
                 "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
         .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
         .config("spark.sql.catalog.local.type", "hadoop")
         .config("spark.sql.catalog.local.warehouse", WH)
         .config("spark.sql.shuffle.partitions", "1").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
jvm = spark.sparkContext._jvm
gw = spark.sparkContext._gateway
res = {}

spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db")
spark.sql("DROP TABLE IF EXISTS local.db.t")
spark.sql("""CREATE TABLE local.db.t (id bigint NOT NULL, val string, lsn bigint)
             USING iceberg TBLPROPERTIES ('format-version'='2')""")
spark.sql("ALTER TABLE local.db.t SET IDENTIFIER FIELDS id")

cat = jvm.org.apache.iceberg.hadoop.HadoopCatalog(jvm.org.apache.hadoop.conf.Configuration(), WH)
_ns = gw.new_array(jvm.java.lang.String, 1); _ns[0] = "db"   # Namespace.of is varargs
tid = jvm.org.apache.iceberg.catalog.TableIdentifier.of(
        jvm.org.apache.iceberg.catalog.Namespace.of(_ns), "t")
t = cat.loadTable(tid)

import pyarrow as pa, pyarrow.parquet as pq
Files = jvm.org.apache.iceberg.Files
DataFiles = jvm.org.apache.iceberg.DataFiles
FileMetadata = jvm.org.apache.iceberg.FileMetadata
FileFormat = jvm.org.apache.iceberg.FileFormat
ParquetUtil = jvm.org.apache.iceberg.parquet.ParquetUtil
MetricsConfig = jvm.org.apache.iceberg.MetricsConfig
MappingUtil = jvm.org.apache.iceberg.mapping.MappingUtil

def metrics(path):
    return ParquetUtil.fileMetrics(Files.localInput(path), MetricsConfig.forTable(t),
                                   MappingUtil.create(t.schema()))

def data_file(path, ids, vals, lsns):
    pq.write_table(pa.table({"id": pa.array(ids, pa.int64()),
                             "val": pa.array(vals, pa.string()),
                             "lsn": pa.array(lsns, pa.int64())}), path)
    return (DataFiles.builder(t.spec()).withPath(path).withFormat(FileFormat.PARQUET)
            .withFileSizeInBytes(os.path.getsize(path)).withMetrics(metrics(path)).build())

def eq_delete_file(path, ids):
    eq = gw.new_array(jvm.int, 1)
    eq[0] = t.schema().findField("id").fieldId()
    pq.write_table(pa.table({"id": pa.array(ids, pa.int64())}), path)
    return (FileMetadata.deleteFileBuilder(t.spec()).ofEqualityDeletes(eq)
            .withPath(path).withFormat(FileFormat.PARQUET)
            .withFileSizeInBytes(os.path.getsize(path)).withMetrics(metrics(path)).build())

d = os.path.join(WH, "_files"); os.makedirs(d, exist_ok=True)

# commit 1 (seq 1): the version that will be group-local "discarded" under the Lean model
t.newAppend().appendFile(data_file(os.path.join(d, "v1.parquet"), [1], ["v1"], [50])).commit()
# commit 2 (seq 2): the version the Lean model would elect as the group's survivor
t.refresh()
t.newRowDelta().addRows(data_file(os.path.join(d, "v2.parquet"), [1], ["v2"], [10])) \
               .addDeletes(eq_delete_file(os.path.join(d, "d2.parquet"), [1])).commit()
# commit 3 (seq 3): the true global survivor, OUTSIDE the group
t.refresh()
t.newRowDelta().addRows(data_file(os.path.join(d, "v3.parquet"), [1], ["v3"], [100])) \
               .addDeletes(eq_delete_file(os.path.join(d, "d3.parquet"), [1])).commit()
t.refresh()

# ---- control: the global state is the intended one ----
live = [(r[0], r[1], r[2]) for r in spark.sql("SELECT id, val, lsn FROM local.db.t").collect()]
res["live_before"] = live

# ---- record data files and their sequence numbers ----
files = [(r[0], r[1], r[2], r[3]) for r in spark.sql(
    "SELECT file_path, record_count, file_size_in_bytes, content FROM local.db.t.files").collect()]
res["files_before"] = [{"path": os.path.basename(f[0]), "records": f[1], "content": f[3]}
                       for f in files]
try:
    seqs = [(os.path.basename(r[0]), r[1]) for r in spark.sql(
        "SELECT data_file.file_path, sequence_number FROM local.db.t.entries").collect()]
except Exception as e:
    seqs = [("entries-unavailable", str(e)[:120])]
res["sequence_numbers"] = seqs

# ---- the rewrite: force the group to the two low-lsn data files ----
opts = ("map('audit-stale-wins','true','audit-ordering-column','lsn',"
        "'audit-key-columns','id','audit-gate','false','audit-cross-group','false',"
        f"'audit-require-single-survivor','{os.environ.get('MOR_SS','true')}',"
        "'min-input-files','1','rewrite-all','true')")
row = spark.sql(f"CALL local.system.rewrite_data_files(table => 'db.t', "
                f"where => 'lsn <= 50', options => {opts})").collect()[0]
res["rewrite"] = {"rewritten_data_files": int(row[0]), "added_data_files": int(row[1])}

t.refresh()
snap = t.currentSnapshot()
res["summary"] = {k: v for k, v in dict(snap.summary()).items() if k.startswith("mor.audit")}
res["single_survivor_guard"] = os.environ.get("MOR_SS", "true")
res["live_after"] = [(r[0], r[1], r[2]) for r in
                     spark.sql("SELECT id, val, lsn FROM local.db.t").collect()]
print("PROBE_JSON " + json.dumps(res))
'''

sp = os.path.join(WH, "probe.py")
with open(sp, "w") as f:
    f.write(SCRIPT)

p = subprocess.run([PY, sp, WH, JAR], capture_output=True, text=True,
                   env={**os.environ, "JAVA_HOME": JAVA_HOME,
                        "MOR_SS": os.environ.get("MOR_SS", "true")})
payload = None
for line in p.stdout.splitlines():
    if line.startswith("PROBE_JSON "):
        payload = json.loads(line[len("PROBE_JSON "):])
if payload is None:
    print("DRIVER FAILED\n", p.stdout[-3000:], "\n", p.stderr[-3000:])
    shutil.rmtree(WH, ignore_errors=True)
    sys.exit(2)

fail = []
r = payload

# ---- positive controls ----
live = r.get("live_before") or []
if len(live) != 1 or live[0][2] != 100:
    fail.append(f"global state is not the intended one: live rows {live}; expected exactly "
                f"[(1,'v3',100)]. The three commits did not produce one survivor at lsn 100, so "
                f"the group under test is not the T2 shape")
rw = r.get("rewrite") or {}
if rw.get("rewritten_data_files", 0) < 2:
    fail.append(f"the rewrite selected {rw.get('rewritten_data_files')} data file(s); the intended "
                f"group holds TWO. Nothing was measured about a two-file non-co-resident group")
summ = r.get("summary") or {}
total = int(summ.get("mor.audit.groups-total", 0))
gated = int(summ.get("mor.audit.groups-gated", 0))
audited = int(summ.get("mor.audit.groups-audited", 0))
if total < 1:
    fail.append("no rewrite group was formed; a zero verdict count here means nothing")
if audited < 1:
    fail.append(f"the audit did not run on any group (gated={gated}, audited={audited}); a zero "
                f"verdict count would be the gate skipping, not the survivor logic")

count = int(summ.get("mor.audit.stale-wins-count", 0))
keys = summ.get("mor.audit.stale-wins-keys")

r["controls"] = {"live_before_ok": len(live) == 1 and live and live[0][2] == 100,
                 "rewritten_data_files": rw.get("rewritten_data_files"),
                 "groups_total": total, "groups_gated": gated, "groups_audited": audited}
r["verdict_count"] = count
r["verdict_keys"] = keys
r["model_faithful"] = count >= 1

print(f"  live before rewrite      : {live}")
print(f"  data files before        : {[f['path'] for f in r.get('files_before', [])]}")
print(f"  sequence numbers         : {r.get('sequence_numbers')}")
print(f"  rewrite                  : {rw}")
print(f"  groups total/gated/audited: {total}/{gated}/{audited}")
print(f"  stale-wins-count         : {count}")
print(f"  stale-wins-keys          : {keys}")
print(f"  live after rewrite       : {r.get('live_after')}")
print()
if fail:
    print("CONTROLS FAILED -- the run did not measure the intended shape:")
    for f in fail:
        print("   -", f)
else:
    if count >= 1:
        print("  => the mechanism REPORTED a violation for the key.")
        print("     groupSD as modelled in Grouped.lean is FAITHFUL to the code.")
    else:
        print("  => the mechanism reported NOTHING for the key.")
        print("     groupSD as modelled in Grouped.lean is NOT faithful: no survivor is elected")
        print("     from among globally-discarded versions, so T2's group cannot witness anything.")

r["failures"] = fail
with open(OUT, "w") as f:
    json.dump(r, f, indent=1)
print(f"\n  -> {OUT}")
shutil.rmtree(WH, ignore_errors=True)
sys.exit(1 if fail else 0)
