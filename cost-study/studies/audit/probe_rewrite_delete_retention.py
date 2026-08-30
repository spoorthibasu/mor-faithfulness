#!/usr/bin/env python3
"""Why does default `rewrite_data_files` remove exactly 8 delete files, whatever the total?

THE QUESTION. Entry 14 established the constant is engine bin-pack behaviour, not a generator
artifact: checkpoints-with-deletes matches the observed pre-compaction delete count in every cell
(50, 27, 42, 50, 28, 35, 50, 28), yet the removal is 8 regardless. Entry 6 then read the source and
explained why the `remove-dangling-deletes` OPTION removes nothing extra -- the action short-circuits
on a single unpartitioned spec -- but that does not explain the 8. Entry 6 named the probe that
would settle it, and this is that probe.

THE COMMIT-TIME FILTER, which is the hypothesis under test. `ManifestFilterManager` drops a live
delete entry when

    entry.dataSequenceNumber() > 0 && entry.dataSequenceNumber() < minSequenceNumber

where `minSequenceNumber` comes from `MergingSnapshotProducer.apply`: the minimum
`ManifestFile::minSequenceNumber` over the surviving data manifests, floored by
`base.lastSequenceNumber()`. This probe reads every quantity in that expression directly instead of
reading the filter forward, and then checks set equality against what actually disappeared.

THE SHAPE. 50 commits over one unpartitioned table, each a RowDelta carrying one data file and one
equality delete at the same sequence number -- the cell shape the 8-cell sweep uses. Then a default
`rewrite_data_files`, and the same metadata read again.

POSITIVE CONTROLS, all hard failures. The standalone probe this replaces was inconclusive because a
single data file no-ops bin-pack under `min-input-files` (Entry 6's method note), which is exactly
the shape that reports success while doing nothing:

  C1  the table must hold >1 data file and >1 delete file BEFORE the rewrite, or bin-pack has
      nothing to do and the run measures a no-op.
  C2  the rewrite must actually rewrite: data files must FALL to fewer than before, in this run,
      measured before and after rather than assumed from a table found in that state.
  C3  delete files must actually be removed in this run -- a removal of zero would make the
      comparison vacuous.
  C4  every delete file counted must be an equality delete (content 2); a position delete or DV in
      the mix would put a different branch of the filter in play.

Run:  JAVA_HOME=<jdk17> ../../../checker/.venv/bin/python probe_rewrite_delete_retention.py
Writes probe_rewrite_delete_retention.json alongside. Exits non-zero if any control fails.
"""
import glob
import json
import os
import shutil
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "probe_rewrite_delete_retention.json")
VER = os.environ.get("MOR_ICEBERG_VERSION", "1.10.2")
IVY = os.environ.get("MOR_IVY_DIR") or os.path.expanduser("~/.ivy2")
WH = sys.argv[1] if len(sys.argv) > 1 else os.path.join(tempfile.gettempdir(), "mor_del_retention")
N_COMMITS = int(os.environ.get("MOR_COMMITS", "50"))
ROWS = int(os.environ.get("MOR_ROWS", "200"))
shutil.rmtree(WH, ignore_errors=True)
os.makedirs(WH)

ADD_OPENS = " ".join(
    f"--add-opens=java.base/{p}=ALL-UNNAMED"
    for p in ["java.lang", "java.lang.invoke", "java.lang.reflect", "java.io", "java.net",
              "java.nio", "java.util", "java.util.concurrent", "java.util.concurrent.atomic",
              "sun.nio.ch", "sun.nio.cs", "sun.security.action", "sun.util.calendar"]
) + " --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"

from pyspark.sql import SparkSession                                          # noqa: E402

spark = (SparkSession.builder.appName("rewrite-delete-retention").master("local[2]")
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
DataFiles = jvm.org.apache.iceberg.DataFiles
FileMetadata = jvm.org.apache.iceberg.FileMetadata
ParquetUtil = jvm.org.apache.iceberg.parquet.ParquetUtil
MetricsConfig = jvm.org.apache.iceberg.MetricsConfig
MappingUtil = jvm.org.apache.iceberg.mapping.MappingUtil
NameMappingParser = jvm.org.apache.iceberg.mapping.NameMappingParser
import pyarrow as pa                                                          # noqa: E402
import pyarrow.parquet as pq                                                  # noqa: E402

TBL, TDIR = "cell", os.path.join(WH, "db", "cell")
FAIL, R = [], {"iceberg_version": VER, "commits": N_COMMITS, "rows_per_commit": ROWS}


def check_(cond, msg):
    print(("  ok   " if cond else "  FAIL ") + msg)
    if not cond:
        FAIL.append(msg)
    return cond


def load(path):
    return jvm.org.apache.iceberg.hadoop.HadoopTables(
        spark._jsc.hadoopConfiguration()).load(path)


def pa_schema(t, names, types):
    m = {"int": pa.int32(), "string": pa.string()}
    return pa.schema([pa.field(n, m[ty], nullable=True,
                     metadata={b"PARQUET:field_id": str(t.schema().findField(n).fieldId()).encode()})
                      for n, ty in zip(names, types)])


def metrics_of(t, path):
    return ParquetUtil.fileMetrics(Files.localInput(path), MetricsConfig.forTable(t),
                                   MappingUtil.create(t.schema()))


def write_data(t, path, ids, lsns, payload):
    pq.write_table(pa.table({"id": ids, "lsn": lsns, "payload": payload},
                            schema=pa_schema(t, ["id", "lsn", "payload"],
                                             ["int", "int", "string"])), path)
    return (DataFiles.builder(t.spec()).withPath(path).withFormat(FileFormat.PARQUET)
            .withFileSizeInBytes(os.path.getsize(path))
            .withMetrics(metrics_of(t, path)).build())


def write_eqdel(t, path, ids):
    eq = gw.new_array(jvm.int, 1)
    eq[0] = t.schema().findField("id").fieldId()
    pq.write_table(pa.table({"id": ids}, schema=pa_schema(t, ["id"], ["int"])), path)
    return (FileMetadata.deleteFileBuilder(t.spec()).ofEqualityDeletes(eq)
            .withPath(path).withFormat(FileFormat.PARQUET)
            .withFileSizeInBytes(os.path.getsize(path))
            .withMetrics(metrics_of(t, path)).build())


def entries():
    """Live manifest entries of the CURRENT snapshot: content, path, data sequence number."""
    rows = spark.sql(
        f"SELECT status, sequence_number, data_file.content AS content, "
        f"data_file.file_path AS path FROM local.db.{TBL}.entries WHERE status < 2").collect()
    data = {os.path.basename(r["path"]): r["sequence_number"] for r in rows if r["content"] == 0}
    dels = {os.path.basename(r["path"]): (r["sequence_number"], r["content"])
            for r in rows if r["content"] != 0}
    return data, dels


def delete_manifests(t):
    """Delete-manifest inventory: granularity matters, because the commit path filters per manifest."""
    snap = t.currentSnapshot()
    return [{"path": os.path.basename(m.path()), "min_sequence_number": m.minSequenceNumber(),
             "added": m.addedFilesCount(), "existing": m.existingFilesCount(),
             "deleted": m.deletedFilesCount()} for m in snap.deleteManifests(t.io())]


def manifest_min_seqs(t):
    """ManifestFile::minSequenceNumber over the current snapshot's DATA manifests -- the quantity
    MergingSnapshotProducer feeds to dropDeleteFilesOlderThan."""
    snap = t.currentSnapshot()
    out = []
    for m in snap.dataManifests(t.io()):
        out.append({"path": os.path.basename(m.path()), "min_sequence_number": m.minSequenceNumber(),
                    "added_snapshot_id": m.snapshotId()})
    return out


# ---- build ---------------------------------------------------------------------------------
print(f"\n=== build: {N_COMMITS} commits, one data file + one equality delete each ===")
spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db")
spark.sql(f"DROP TABLE IF EXISTS local.db.{TBL}")
spark.sql(f"CREATE TABLE local.db.{TBL} (id INT, lsn INT, payload STRING) USING iceberg "
          "TBLPROPERTIES('format-version'='2','write.delete.mode'='merge-on-read')")
t0 = load(TDIR)
t0.updateProperties().set("schema.name-mapping.default",
                          NameMappingParser.toJson(MappingUtil.create(t0.schema()))).commit()
ddir = os.path.join(TDIR, "data")
os.makedirs(ddir, exist_ok=True)
WIN_DIV = int(os.environ.get("MOR_WIN_DIV", "10"))   # delete window = ROWS/WIN_DIV
win = max(1, ROWS // WIN_DIV)
R["delete_window"] = win
R["rotation_period_commits"] = max(1, ROWS // win)
for c in range(1, N_COMMITS + 1):
    t = load(TDIR)
    ids = list(range(1, ROWS + 1))
    df = write_data(t, os.path.join(ddir, f"c{c}-data.parquet"), ids,
                    [c * 1000 + i for i in ids], [f"v{c}"] * ROWS)
    if c == 1:
        t.newAppend().appendFile(df).commit()
    else:
        start = ((c - 2) * win) % ROWS + 1
        dele = write_eqdel(t, os.path.join(ddir, f"c{c}-del.parquet"),
                           [((start + k - 1) % ROWS) + 1 for k in range(win)])
        t.newRowDelta().addRows(df).addDeletes(dele).commit()

t = load(TDIR)
data_before, dels_before = entries()
R["data_files_before"] = len(data_before)
R["delete_files_before"] = len(dels_before)
R["data_seq_before"] = {"min": min(data_before.values()), "max": max(data_before.values())}
R["delete_seq_before"] = {"min": min(s for s, _ in dels_before.values()),
                          "max": max(s for s, _ in dels_before.values())}
R["snapshot_sequence_number_before"] = t.currentSnapshot().sequenceNumber()
R["manifests_before"] = manifest_min_seqs(t)
R["delete_manifests_before"] = delete_manifests(t)
print(f"  data files {len(data_before)}  delete files {len(dels_before)}")
print(f"  data seq {R['data_seq_before']}   delete seq {R['delete_seq_before']}")
print(f"  snapshot sequence number: {R['snapshot_sequence_number_before']}")

print("\n=== controls before rewrite ===")
check_(len(data_before) > 1, f"C1a: >1 data file before the rewrite ({len(data_before)})")
check_(len(dels_before) > 1, f"C1b: >1 delete file before the rewrite ({len(dels_before)})")
check_(all(c == 2 for _, c in dels_before.values()),
       f"C4: every delete file is an equality delete (contents: "
       f"{sorted({c for _, c in dels_before.values()})})")

# ---- the operation under test: DEFAULT rewrite_data_files -----------------------------------
print("\n=== default rewrite_data_files ===")
res = spark.sql(f"CALL local.system.rewrite_data_files(table => 'db.{TBL}')").collect()[0]
R["rewrite_result"] = {k: res[k] for k in res.asDict()}
print(f"  {R['rewrite_result']}")

t = load(TDIR)
data_after, dels_after = entries()
R["data_files_after"] = len(data_after)
R["delete_files_after"] = len(dels_after)
R["snapshot_sequence_number_after"] = t.currentSnapshot().sequenceNumber()
R["manifests_after"] = manifest_min_seqs(t)
R["delete_manifests_after"] = delete_manifests(t)
R["data_files_after_detail"] = [{"file": f, "data_sequence_number": s}
                                for f, s in sorted(data_after.items())]
print(f"  data files {len(data_before)} -> {len(data_after)}   "
      f"delete files {len(dels_before)} -> {len(dels_after)}")
print(f"  surviving data file sequence numbers: {sorted(data_after.values())}")

print("\n=== controls after rewrite ===")
check_(len(data_after) < len(data_before),
       f"C2: the rewrite actually rewrote ({len(data_before)} -> {len(data_after)} data files)")
removed = sorted(set(dels_before) - set(dels_after))
check_(len(removed) > 0, f"C3: delete files were removed in THIS run ({len(removed)})")

# ---- the comparison the filter would make ----------------------------------------------------
min_data_seq_after = min(data_after.values()) if data_after else None
manifest_min = min(m["min_sequence_number"] for m in R["manifests_after"]) if R["manifests_after"] else None
R["min_data_sequence_number_after"] = min_data_seq_after
R["min_manifest_sequence_number_after"] = manifest_min
R["removed_delete_files"] = [{"file": f, "data_sequence_number": dels_before[f][0]} for f in removed]
R["retained_delete_files"] = [{"file": f, "data_sequence_number": s}
                              for f, (s, _) in sorted(dels_after.items())]
removed_seqs = sorted(dels_before[f][0] for f in removed)
retained_seqs = sorted(s for s, _ in dels_after.values())
R["removed_sequence_numbers"] = removed_seqs
R["retained_sequence_numbers_range"] = {"min": min(retained_seqs), "max": max(retained_seqs)} if retained_seqs else None
print(f"  removed {len(removed)} delete files, sequence numbers: {removed_seqs}")
print(f"  retained sequence numbers span: {R['retained_sequence_numbers_range']}")
print(f"  min data sequence number after: {min_data_seq_after}")
print(f"  min manifest minSequenceNumber after: {manifest_min}")
print(f"  delete manifests before: {len(R['delete_manifests_before'])}  "
      f"after: {len(R['delete_manifests_after'])}")
for m in R["delete_manifests_after"]:
    print(f"    {m}")

for label, threshold in (("min_live_data_seq", min_data_seq_after),
                         ("min_manifest_seq", manifest_min)):
    if threshold is None:
        continue
    predicted = sorted(f for f, (s, _) in dels_before.items() if 0 < s < threshold)
    R[f"filter_prediction_{label}"] = {
        "threshold": threshold, "predicted_removed_count": len(predicted),
        "matches_actual": sorted(predicted) == removed,
        "predicted_not_removed": sorted(set(predicted) - set(removed))[:8],
        "removed_not_predicted": sorted(set(removed) - set(predicted))[:8],
    }
    print(f"  filter with threshold={threshold} ({label}): predicts {len(predicted)} removed, "
          f"actual {len(removed)}, match={sorted(predicted) == removed}")

R["failures"] = FAIL
print()
if FAIL:
    print("CONTROLS FAILED -- the run did not measure the intended shape:")
    for f in FAIL:
        print("   -", f)
else:
    m = any(R.get(f"filter_prediction_{l}", {}).get("matches_actual")
            for l in ("min_live_data_seq", "min_manifest_seq"))
    print(f"  => {len(dels_before)} delete files -> {len(dels_after)}; {len(removed)} removed.")
    print(f"     The commit-time filter's condition {'REPRODUCES' if m else 'does NOT reproduce'} "
          f"the removed set.")
with open(OUT, "w") as f:
    json.dump(R, f, indent=1)
print(f"\n  -> {OUT}")
spark.stop()
shutil.rmtree(WH, ignore_errors=True)
sys.exit(1 if FAIL else 0)
