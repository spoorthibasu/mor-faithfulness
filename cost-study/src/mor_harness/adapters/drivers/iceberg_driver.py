#!/usr/bin/env python3
"""Self-contained Iceberg write driver (runs in its own Spark subprocess).

Reads a plan JSON, writes each checkpoint as ONE Iceberg v2 RowDelta commit (data +
equality delete share the snapshot's sequence number), reads back the merge-on-read
current view, and writes a result JSON. Only pyspark + stdlib are imported here so the
driver has no dependency on the mor_harness package.

Uses a Java-API equality-delete writer:
data via GenericAppenderFactory.newDataWriter, equality deletes via newEqDeleteWriter
at controlled sequence numbers. Same-commit RowDelta => equal seq => the delete cannot
suppress same-seq data => the FLINK-38450 duplicate. Ascending commits => faithful.
"""
import json
import math
import os
import random
import resource
import zlib
import shutil
import sys
import time
import traceback

IN, OUT = sys.argv[1], sys.argv[2]
with open(IN) as f:
    PLAN = json.load(f)

# Iceberg engine version. Default 1.6.1 preserves the committed baseline exactly; set
# MOR_ICEBERG_VERSION=1.10.2 for the pinned-engine (post-refactor FileRewriteRunner) runs.
ICEBERG_VERSION = os.environ.get("MOR_ICEBERG_VERSION", "1.6.1")
# Optional: load a locally-built runtime jar (the fork) via spark.jars instead of the
# published Maven package. Set MOR_ICEBERG_JAR=/path/to/iceberg-spark-runtime-*.jar.
ICEBERG_JAR = os.environ.get("MOR_ICEBERG_JAR")
# Bulk ingest (MOR_BULK_INGEST=1): write data and equality-delete parquet with pyarrow in one shot and
# register the files via the Iceberg metadata API, instead of writing records one at a time through py4j
# (measured 292 rows/s, which caps any GB-scale study). Default off so committed results reproduce byte
# for byte on the original path.
BULK_INGEST = os.environ.get("MOR_BULK_INGEST") == "1"
# Synthetic in-driver generation. The normal path ships every row through the plan JSON and collects the
# whole table back into Python, neither of which survives GB-scale data. With PLAN["synth"] the driver
# generates each commit's rows itself (column-wise, via pyarrow) and skips the full materialization,
# reporting only counts and timings. Implies bulk ingest.
SYNTH = PLAN.get("synth")
# Spark runs in local mode here, so the driver JVM is also the executor. The default heap (1 GB) is fine
# for the KB-scale cells but not for GB-scale synth runs -- symptom is a bare Py4JError with no Java
# traceback (a JVM-level fatal), not a clean OOM message. This must be set BEFORE the JVM is launched,
# and in local mode `spark.driver.memory` in the builder is too late, so it goes through submit args.
if SYNTH:
    os.environ.setdefault(
        "PYSPARK_SUBMIT_ARGS",
        f"--driver-memory {os.environ.get('MOR_DRIVER_MEM', '8g')} pyspark-shell")

WAREHOUSE = PLAN["warehouse"]
IVY = PLAN["ivy"]
NAME = PLAN["table_name"]
TABLE_DIR = PLAN["table_dir"]
COLUMNS = PLAN["columns"]                 # [{name,type}]
KEY_COLUMNS = PLAN["key_columns"]

ADD_OPENS = " ".join(
    f"--add-opens=java.base/{p}=ALL-UNNAMED"
    for p in ["java.lang", "java.lang.invoke", "java.lang.reflect", "java.io", "java.net",
              "java.nio", "java.util", "java.util.concurrent", "java.util.concurrent.atomic",
              "sun.nio.ch", "sun.nio.cs", "sun.security.action", "sun.util.calendar"]
) + " --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"

_SQL_TYPE = {"int": "INT", "long": "BIGINT", "string": "STRING"}

# ---------------------------------------------------------------------------------------------
# Construction oracle. Everything below is derived from the generator's PARAMETERS -- no table is
# read, no file is opened, and the mechanism under test contributes nothing. The generator assigns
# every sequence number and every ordering value itself, so which keys should be flagged is fixed
# before the first byte is written. Deriving it in closed form rather than by replaying the writes
# also means the oracle shares no code path with the ingest that produced the table.
#
# Notation: commit c in 1..C writes every key k in 1..R with ordering value
#     lsn_c(k) = LSN_BASE(c) + (k - 1)
# and, for c >= 2, an equality delete over the rotating window W_c. Iceberg suppresses only rows at
# STRICTLY LOWER sequence numbers, so the delete at commit c leaves commit c's own row alive. If
# D_k is the last commit whose window covers k, the surviving versions of k are exactly commits
# D_k..C, i.e. C - D_k + 1 of them, and the discarded ones are commits 1..D_k-1.
#
# Because the (k-1) term is common to every version of a key, comparing a discarded ordering value
# against a surviving one cancels it: the comparison depends only on D_k, not on k. That is what
# makes the closed form exact rather than a sample.
# Payload determinism. Default 0 makes every run byte-reproducible; set MOR_PAYLOAD_SEED to vary
# payloads deliberately (e.g. to check a result is not an artifact of one particular file-size draw).
PAYLOAD_SEED = int(os.environ.get("MOR_PAYLOAD_SEED", "0"))
HIGH_LSN = 999_000_000     # ordering value of the injected discarded version (out-orders everything)
DUP_LSN_OFF = 1_000_000    # separates the two same-sequence copies so they are distinguishable rows


def _del_window(c, n_del, rpc):
    start = ((c - 2) * n_del) % max(1, rpc - n_del + 1) + 1
    return start, start + n_del


def _lsn_base(c, inverted):
    return (max(1, c - 2) if (inverted and c % 2 == 0) else c) * 10_000_000


def construction_oracle(cfg):
    # The closed form below rests on lsn_c(k) = LSN_BASE(c) + (k-1) holding for EVERY row. The
    # `interleave_frac` knob deliberately breaks that for a fraction of rows, so the derivation no
    # longer applies. Refuse rather than return a confidently wrong key set: the expected_* fields are
    # None so any consumer that uses them fails loudly instead of silently comparing against nonsense.
    if float(cfg.get("interleave_frac", 0.0)) > 0:
        return {"oracle_valid": False,
                "invalid_reason": "interleave_frac > 0 breaks lsn_c(k) = LSN_BASE(c) + (k-1), which the "
                                  "closed form assumes for every row; no expected key set is derivable",
                "interleave_frac": float(cfg["interleave_frac"]),
                "expected_live_rows": None, "expected_stale_wins": None,
                "expected_dup_risky": None, "risky_D_values": None}
    C = int(cfg["commits"])
    R = int(cfg["rows_per_commit"])
    n_del = max(1, int(R * float(cfg.get("delete_frac", 0.2))))
    inverted = cfg.get("ordering") == "inverted"
    n_dup = max(0, int(n_del * float(cfg.get("dup_frac", 0.0))))
    base = [None] + [_lsn_base(c, inverted) for c in range(1, C + 1)]

    # D_k for every key, by overwriting each window in commit order; 0 means never deleted.
    D = [0] * (R + 1)
    for c in range(2, C + 1):
        st, en = _del_window(c, n_del, R)
        D[st:en] = [c] * (en - st)

    # Per-D classification. suffix/prefix maxima of LSN_BASE decide the ordering comparison.
    suf = [0] * (C + 2)
    for c in range(C, 0, -1):
        suf[c] = max(suf[c + 1], base[c])
    pre = [None] * (C + 2)
    run = None
    for c in range(1, C + 2):
        pre[c] = run
        if c <= C:
            run = base[c] if run is None else max(run, base[c])

    cls = {}                                     # D -> ("stale" | "dup_risky" | "clean", n_surv)
    for d in range(0, C + 1):
        dd = max(d, 1)
        n_surv = C - dd + 1
        risky = pre[dd] is not None and pre[dd] > suf[dd]
        cls[d] = (("stale" if n_surv == 1 else "dup_risky") if risky else "clean", n_surv)

    dup_start = _del_window(C, n_del, R)[0] if C >= 2 else 1
    dup_keys = set(range(dup_start, dup_start + n_dup))

    stale, dup_risky, live_rows = [], [], 0
    for k in range(1, R + 1):
        kind, n_surv = cls[D[k]]
        if k in dup_keys:                        # injected second copy in the final commit
            n_surv += 1
            # its discarded commit-1 copy out-orders both survivors by construction
            kind = "dup_risky"
        live_rows += n_surv
        if kind == "stale":
            stale.append(k)
        elif kind == "dup_risky":
            dup_risky.append(k)
    return {"derivation": "closed form over generator parameters; no table read, no engine readback",
            "oracle_valid": True,
            "commits": C, "rows_per_commit": R, "n_del": n_del,
            "ordering": "inverted" if inverted else "contiguous", "n_injected_duplicates": n_dup,
            "expected_live_rows": live_rows,
            "expected_stale_wins": stale,
            "expected_dup_risky": dup_risky,
            "risky_D_values": [d for d in cls if cls[d][0] != "clean"]}


def peak_rss_mb():
    r = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return round(r / (1024 * 1024), 1) if sys.platform == "darwin" else round(r / 1024, 1)


def main():
    from pyspark.sql import SparkSession

    spark = (SparkSession.builder.appName(f"mor-harness-iceberg-{NAME}").master(os.environ.get("MOR_SPARK_MASTER", "local[2]"))
        .config("spark.jars.packages",
                "" if ICEBERG_JAR else f"org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:{ICEBERG_VERSION}")
        .config("spark.jars", ICEBERG_JAR or "")
        .config("spark.jars.ivy", IVY)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", WAREHOUSE)
        .config("spark.sql.catalog.local.cache-enabled", "false")
        .config("spark.sql.catalogImplementation", "in-memory")
        .config("spark.driver.host", "localhost").config("spark.ui.enabled", "false")
        # Default 1 is right for the KB-scale cells this harness was built for, and wrong at GB
        # scale: it forces an entire aggregation shuffle through a single task. Left as the
        # default so every earlier measurement remains reproducible, overridable for the ones
        # that are large enough to care.
        .config("spark.sql.shuffle.partitions",
                os.environ.get("MOR_SHUFFLE_PARTITIONS", "1"))
        .config("spark.driver.extraJavaOptions", ADD_OPENS)
        .config("spark.executor.extraJavaOptions", ADD_OPENS)
        .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    jvm = spark._jvm
    gw = spark.sparkContext._gateway

    Files = jvm.org.apache.iceberg.Files
    FileFormat = jvm.org.apache.iceberg.FileFormat
    GenRecord = jvm.org.apache.iceberg.data.GenericRecord
    GenAppFac = jvm.org.apache.iceberg.data.GenericAppenderFactory
    EncFiles = jvm.org.apache.iceberg.encryption.EncryptedFiles
    EMPTY_KEY = jvm.org.apache.iceberg.encryption.EncryptionKeyMetadata.EMPTY
    Long = jvm.java.lang.Long

    coltype = {c["name"]: c["type"] for c in COLUMNS}

    def box(name, v):
        """Match the Iceberg field type: LONG columns need a java.lang.Long, else the
        Parquet writer throws Integer-cannot-be-cast-to-Long at write time."""
        if v is None:
            return None
        if coltype[name] == "long":
            return Long.valueOf(int(v))
        if coltype[name] == "int":
            return int(v)
        return str(v)

    def load(path):
        return jvm.org.apache.iceberg.hadoop.HadoopTables(
            spark._jsc.hadoopConfiguration()).load(path)

    def enc(o):
        return EncFiles.encryptedOutput(o, EMPTY_KEY)

    def mk_record(schema, row, names):
        r = GenRecord.create(schema)
        for n in names:
            r.setField(n, box(n, row.get(n)))
        return r

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
            row = {kc: v for kc, v in zip(KEY_COLUMNS, kv)}
            w.write(mk_record(eq_schema, row, KEY_COLUMNS))
        w.close()
        return w.toDeleteFile()

    # ---- bulk ingest path (pyarrow write + metadata registration) ---------------------------
    ParquetUtil = jvm.org.apache.iceberg.parquet.ParquetUtil
    MetricsConfig = jvm.org.apache.iceberg.MetricsConfig
    DataFiles = jvm.org.apache.iceberg.DataFiles
    FileMetadata = jvm.org.apache.iceberg.FileMetadata
    MappingUtil = jvm.org.apache.iceberg.mapping.MappingUtil
    NameMappingParser = jvm.org.apache.iceberg.mapping.NameMappingParser

    _PA_TYPE = {"int": "int32", "long": "int64", "string": "string"}

    def pa_schema(t, names):
        """pyarrow schema carrying PARQUET:field_id so Iceberg resolves columns by field ID."""
        import pyarrow as pa
        fields = []
        for n in names:
            fid = t.schema().findField(n).fieldId()
            typ = getattr(pa, _PA_TYPE[coltype[n]])()
            fields.append(pa.field(n, typ, nullable=True,
                                   metadata={b"PARQUET:field_id": str(fid).encode()}))
        return pa.schema(fields)

    def pa_write(t, path, names, columns):
        import pyarrow as pa
        import pyarrow.parquet as pq
        pq.write_table(pa.table(columns, schema=pa_schema(t, names)), path)

    def metrics_of(t, path):
        """File metrics (incl. per-column lower/upper bounds) read back from the parquet footer.
        The NameMapping argument makes resolution work even if field IDs were not embedded; without
        correct bounds the audit's metadata gate silently degrades to auditing every group."""
        return ParquetUtil.fileMetrics(Files.localInput(path), MetricsConfig.forTable(t),
                                       MappingUtil.create(t.schema()))

    def bulk_write_data_file(t, path, rows):
        names = [c["name"] for c in COLUMNS]
        pa_write(t, path, names, {n: [r.get(n) for r in rows] for n in names})
        return (DataFiles.builder(t.spec())
                .withPath(path).withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(os.path.getsize(path))
                .withMetrics(metrics_of(t, path)).build())

    def bulk_write_eq_delete_file(t, path, key_rows):
        eq_ids = gw.new_array(jvm.int, len(KEY_COLUMNS))
        for i, kc in enumerate(KEY_COLUMNS):
            eq_ids[i] = t.schema().findField(kc).fieldId()
        cols = {kc: [kv[i] for kv in key_rows] for i, kc in enumerate(KEY_COLUMNS)}
        pa_write(t, path, list(KEY_COLUMNS), cols)
        return (FileMetadata.deleteFileBuilder(t.spec()).ofEqualityDeletes(eq_ids)
                .withPath(path).withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(os.path.getsize(path))
                .withMetrics(metrics_of(t, path)).build())

    write_data = bulk_write_data_file if BULK_INGEST else write_data_file
    write_eqdel = bulk_write_eq_delete_file if BULK_INGEST else write_eq_delete_file

    # 64-symbol alphabet; urandom bytes are mapped onto it at C speed via bytes.translate, so the
    # payload is genuinely high-entropy. This matters: an earlier version sliced overlapping windows out
    # of a small pool and parquet dictionary-compressed 24 MB of logical data down to 167 KB (143x),
    # which would have kept every table job-launch bound and silently invalidated the calibration.
    _ALPHA = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
    _XLAT = bytes.maketrans(bytes(range(256)), (_ALPHA * 4)[:256])

    def synth_columns(t, path, n_rows, payload_bytes, lsn_base, first_key=1, il=None, stride=1):
        """Build one commit's parquet column-wise (never per-row dicts) and register it. stdlib only:
        numpy is not a dependency of this venv.

        `il`, when given, is (frac, commit, n_commits, seed): that fraction of this file's rows take
        their ordering value from a DIFFERENT commit's window instead of this one's. The gate reads
        per-file [min, max] ordering bounds, so a single such row widens the file's interval and can
        create the inversion the gate looks for -- which is exactly the workload property the sweep is
        parameterising. Positions are drawn from a seeded RNG keyed on (seed, commit, first_key), so a
        cell is reproducible and two files of the same commit do not receive identical patterns."""
        from array import array
        import pyarrow as pa
        import pyarrow.parquet as pq
        # SEEDED, not os.urandom. The payload's bytes are irrelevant to every result, but its
        # COMPRESSED SIZE is not: it sets the data-file size, which sets bin-packing, which sets which
        # files land in which rewrite group -- and group composition is what clearance is measured
        # over. With os.urandom, an identical cell with identical seeds gave 64% then 56% clearance
        # (Entry 58). Seeding removes that.
        # Entropy is NOT lowered to get determinism: Mersenne Twister output translated through the
        # same alphabet compresses identically to os.urandom (zlib 0.7575 vs 0.7574, all 64 symbols
        # present), so the hazard the original comment warns about -- a nominally large table
        # dictionary-compressing away -- is unchanged. The seed is derived from the file's own
        # identity so different files still get different payloads, via crc32 rather than hash()
        # because str hashing is salted per process.
        _pseed = zlib.crc32(
            f"{PAYLOAD_SEED}|{os.path.basename(path)}|{first_key}|{n_rows}|{payload_bytes}".encode())
        buf = random.Random(_pseed).randbytes(n_rows * payload_bytes).translate(_XLAT)
        offs = array("i", range(0, (n_rows + 1) * payload_bytes, payload_bytes))
        vals = pa.StringArray.from_buffers(n_rows, pa.py_buffer(offs.tobytes()), pa.py_buffer(buf))
        ids = pa.array(array("i", range(first_key, first_key + n_rows * stride, stride)),
                       type=pa.int32())
        lsn_arr = array("i", range(lsn_base, lsn_base + n_rows * stride, stride))
        if il:
            frac, c_now, n_c, seed = il
            # Each row is interleaved INDEPENDENTLY with probability `frac`, sampled exactly by
            # geometric gaps (O(number of successes), no numpy). Rounding frac*n_rows to a whole
            # count instead would silently floor every rate below 0.5/n_rows to zero, putting the
            # apparent cliff wherever the rounding boundary happens to fall rather than where the
            # gate actually fails -- an artifact indistinguishable from the result being measured.
            if frac > 0 and n_c > 1:
                rng = random.Random((seed * 1_000_003) ^ (c_now * 7919) ^ first_key)
                idx = -1
                while True:
                    if frac >= 1.0:
                        idx += 1
                    else:
                        idx += 1 + int(math.log(1.0 - rng.random()) / math.log(1.0 - frac))
                    if idx >= n_rows:
                        break
                    c_other = rng.randrange(1, n_c + 1)
                    if c_other == c_now:
                        c_other = c_now % n_c + 1
                    lsn_arr[idx] = c_other * 10_000_000 + (first_key + idx) - 1
        lsns = pa.array(lsn_arr, type=pa.int32())
        names = [c["name"] for c in COLUMNS]
        cols = {n: (ids if n in KEY_COLUMNS else (lsns if n == PLAN["version_column"] else vals))
                for n in names}
        pq.write_table(pa.table(cols, schema=pa_schema(t, names)), path)
        return (DataFiles.builder(t.spec())
                .withPath(path).withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(os.path.getsize(path))
                .withMetrics(metrics_of(t, path)).build())

    def run_synth():
        """commits x rows_per_commit rows over a fixed key space; each commit after the first also
        writes an equality delete for a rotating `delete_frac` slice of the keys.

        delete_frac is deliberately < 1: deleting every key every commit makes the first data file load
        (n_commits-1) x rows_per_commit delete records, so compaction time becomes dominated by
        equality-delete loading rather than by scanning data -- which would inflate the baseline and
        flatter the audit's relative overhead."""
        cfg = SYNTH
        n_commits = int(cfg["commits"])
        rpc = int(cfg["rows_per_commit"])
        payload_bytes = int(cfg.get("payload_bytes", 400))
        del_frac = float(cfg.get("delete_frac", 0.2))
        n_del = max(1, int(rpc * del_frac))
        # ordering="contiguous" (default): commit c writes an lsn window strictly above commit c-1, the
        # shape real CDC produces and the one the metadata gate exploits. ordering="inverted": every
        # other commit reuses an earlier window, creating file-level bound inversions so the gate cannot
        # rule the group out and the capture path runs.
        inverted = cfg.get("ordering") == "inverted"
        # FLINK-38450 duplicate injection. `dup_frac` of the FINAL delete window's keys are written
        # TWICE inside one commit -- two data files in a single RowDelta, so both rows carry that
        # commit's sequence number and the same-seq equality delete suppresses neither. That is the
        # shape the defect produces, and it is NOT the shape the delete rotation already yields (those
        # keys have multiple survivors across distinct commits, at distinct sequence numbers).
        # To make the injection adversarial for the single-survivor guard rather than merely present,
        # the same keys also get a very high ordering value in commit 1, which IS discarded. The
        # discarded version then out-orders both survivors, so absent the guard the key would be
        # reported as a stale win. This is the only configuration in which the mechanism can emit a
        # false positive, so it is the one the guard has to be tested against.
        # Table size and commit depth are separate axes, and conflating them measures the wrong thing.
        # At one data file per commit, reaching 100 GB needs ~300 commits, and the first data file's
        # scan then has to build a delete set from ~300 equality-delete files -- so compaction time
        # becomes delete-set construction, not data scanning, and the audit's relative overhead is
        # flattered by a baseline inflated for an unrelated reason (the Entry 5 hazard, at scale).
        # `files_per_commit` splits a commit's rows across F files over disjoint key ranges, so bytes
        # grow with F while commit depth stays fixed. The ordering assignment is unchanged --
        # lsn_c(k) = LSN_BASE(c) + (k-1) regardless of which file k lands in -- so the closed-form
        # oracle is untouched by this knob.
        fpc = max(1, int(cfg.get("files_per_commit", 1)))
        # `interleave_frac`: the fraction of each commit's rows whose ordering value is drawn from
        # another commit's window rather than this commit's own contiguous one. 0.0 reproduces the
        # existing contiguous shape exactly (il is not even passed), 1.0 scatters every row across the
        # ordering domain. This is the axis the gate-selectivity sweep varies; it is NOT modelled by the
        # `inverted` flag, which relocates whole commits rather than individual rows.
        # `key_scatter="roundrobin"`: spread a commit's keys across its files round-robin rather than
        # in contiguous blocks. Default "block" preserves the existing behaviour exactly.
        scatter = cfg.get("key_scatter") == "roundrobin"
        il_frac = float(cfg.get("interleave_frac", 0.0))
        il_seed = int(cfg.get("interleave_seed", 1))
        il = (il_frac, None, n_commits, il_seed) if il_frac > 0 else None
        dup_frac = float(cfg.get("dup_frac", 0.0))
        dup_start = _del_window(n_commits, n_del, rpc)[0] if n_commits >= 2 else 1
        n_dup = max(0, int(n_del * dup_frac))
        for c in range(1, n_commits + 1):
            t = load(TABLE_DIR)
            lsn_base = _lsn_base(c, inverted)
            chunk = rpc // fpc
            dfs = []
            for j in range(fpc):
                if scatter:
                    # Hash-partitioned sink: file j holds keys j+1, j+1+F, j+1+2F, ... so its ordering
                    # interval spans the WHOLE commit window instead of a disjoint slice of it. Every
                    # value is unchanged -- lsn_c(k) = LSN_BASE(c) + (k-1) still holds for every row, so
                    # the construction oracle stays valid and this isolates the effect of FILE LAYOUT
                    # from the effect of the values themselves.
                    fk, stride = 1 + j, fpc
                    nrows = len(range(fk, rpc + 1, fpc))
                else:
                    fk, stride = 1 + j * chunk, 1
                    nrows = (rpc - (fpc - 1) * chunk) if j == fpc - 1 else chunk
                dfs.append(synth_columns(t, os.path.join(data_dir, f"synth{c}-data-{j}.parquet"),
                                         nrows, payload_bytes, lsn_base + fk - 1, first_key=fk,
                                         il=(il[0], c, il[2], il[3]) if il else None, stride=stride))
            if c == 1:
                app = t.newAppend()
                for d in dfs:
                    app.appendFile(d)
                if n_dup:                    # discarded high-ordering version of the duplicate keys
                    hp = os.path.join(data_dir, "synth1-duphigh.parquet")
                    app.appendFile(synth_columns(t, hp, n_dup, payload_bytes,
                                                 HIGH_LSN + dup_start - 1, first_key=dup_start))
                app.commit()
            else:
                start, _ = _del_window(c, n_del, rpc)
                dpath = os.path.join(data_dir, f"synth{c}-del.parquet")
                ddf = bulk_write_eq_delete_file(t, dpath,
                                                [(k,) for k in range(start, start + n_del)])
                rd = t.newRowDelta().addDeletes(ddf)
                for d in dfs:
                    rd.addRows(d)
                if n_dup and c == n_commits:  # second copy in the SAME commit => same sequence number
                    xp = os.path.join(data_dir, f"synth{c}-dupextra.parquet")
                    rd.addRows(synth_columns(t, xp, n_dup, payload_bytes,
                                             lsn_base + DUP_LSN_OFF + dup_start - 1,
                                             first_key=dup_start))
                rd.commit()

    # ---- create table ----
    ddl = ", ".join(f"{c['name']} {_SQL_TYPE[c['type']]}" for c in COLUMNS)
    spark.sql(f"DROP TABLE IF EXISTS local.db.{NAME}")
    spark.sql(f"CREATE TABLE local.db.{NAME} ({ddl}) USING iceberg "
              "TBLPROPERTIES('format-version'='2','write.delete.mode'='merge-on-read')")

    # ---- apply checkpoints, one commit == one sequence number ----
    t0 = time.time()
    data_dir = os.path.join(TABLE_DIR, "data")
    os.makedirs(data_dir, exist_ok=True)
    if BULK_INGEST:
        # Files written outside Iceberg's own writers resolve by name mapping if a reader ever needs it.
        t0_tbl = load(TABLE_DIR)
        t0_tbl.updateProperties().set(
            "schema.name-mapping.default",
            NameMappingParser.toJson(MappingUtil.create(t0_tbl.schema()))).commit()

    if SYNTH:
        run_synth()
    for ck in (PLAN["checkpoints"] if not SYNTH else []):
        t = load(TABLE_DIR)
        df = ddf = None
        if ck["data"]:
            df = write_data(t, os.path.join(data_dir, f"ck{ck['index']}-data.parquet"), ck["data"])
        if ck["deletes"]:
            ddf = write_eqdel(t, os.path.join(data_dir, f"ck{ck['index']}-del.parquet"), ck["deletes"])
        if df is not None and ddf is not None:
            t.newRowDelta().addRows(df).addDeletes(ddf).commit()
        elif df is not None:
            t.newAppend().appendFile(df).commit()
        elif ddf is not None:
            t.newRowDelta().addDeletes(ddf).commit()
    apply_time = time.time() - t0

    # ---- safe_compact / unsafe_compact: identical compaction pass (rewrite data files,
    # applying equality deletes). Same op for both arms; only the pre-compaction layout differs.
    # Cold-cache control. Compaction re-reads the table the ingest above just wrote, so on a machine
    # whose free page cache is comparable to the table it is partly served from RAM, and how much varies
    # with system state -- the confound behind the 2.1x baseline drift. macOS `purge` needs root, so this
    # evicts by streaming a junk file larger than the free cache. It runs BEFORE the compaction timer.
    # On Linux (as root) the page cache can be dropped exactly, via the kernel. That is strictly
    # better than the userspace substitute below -- which only evicts by pressure, and which the paper
    # has to caveat as approximate -- so prefer it whenever it is available.
    evict_s = 0.0
    if os.environ.get("MOR_DROP_CACHE_MODE") == "sysctl":
        te = time.time()
        os.sync()
        with open("/proc/sys/vm/drop_caches", "w") as _dc:
            _dc.write("3\n")
        evict_s = time.time() - te
        drop_cache_file = None
    else:
        drop_cache_file = os.environ.get("MOR_DROP_CACHE")
    if drop_cache_file and os.path.exists(drop_cache_file):
        te = time.time()
        with open(drop_cache_file, "rb", buffering=0) as _f:
            while _f.read(64 << 20):
                pass
        evict_s = time.time() - te

    compact_time = 0.0
    audit_verdict_path = None
    if PLAN.get("enforcement_mode") in ("safe_compact", "unsafe_compact"):
        tc = time.time()
        opt_pairs = []
        # Durability probe (default off): pass remove-dangling-deletes so 1.9.2+ strips the
        # orphaned equality deletes that default rewrite retains. Available >= 1.9.2 only.
        if os.environ.get("MOR_REWRITE_REMOVE_DANGLING") == "1":
            opt_pairs.append(("remove-dangling-deletes", "true"))
        # Stale-wins audit (forked runner; default off): capture the per-group verdict to a side file.
        audit_verdict_path = None
        if os.environ.get("MOR_AUDIT") == "1":
            audit_verdict_path = os.path.join(os.path.dirname(OUT), "audit_verdict.jsonl")
            if os.path.exists(audit_verdict_path):
                os.remove(audit_verdict_path)
            opt_pairs += [
                ("audit-stale-wins", "true"),
                ("audit-ordering-column", PLAN.get("version_column")),
                ("audit-key-columns", ",".join(KEY_COLUMNS)),
                ("audit-output-path", audit_verdict_path),
            ]
            # Opt-in cross-group merge (resolves keys straddling file groups); default off.
            if os.environ.get("MOR_AUDIT_CROSS_GROUP") == "1":
                opt_pairs.append(("audit-cross-group", "true"))
        # Arbitrary extra rewrite options as "k1=v1,k2=v2" (e.g. Phase 5: force multi-group via
        # max-file-group-size-bytes + min-input-files). Stock Iceberg planner options; no rebuild needed.
        for kv in os.environ.get("MOR_REWRITE_OPTS", "").split(","):
            if "=" in kv:
                k, v = kv.split("=", 1)
                opt_pairs.append((k.strip(), v.strip()))
        opts = ""
        if opt_pairs:
            mapargs = ",".join(f"'{k}','{v}'" for k, v in opt_pairs)
            opts = f", options => map({mapargs})"
        try:
            spark.sql(f"CALL local.system.rewrite_data_files(table => 'db.{NAME}'{opts})")
        except Exception:
            if opt_pairs:
                raise  # options require the named-arg form; do not silently drop them
            spark.sql(f"CALL local.system.rewrite_data_files('db.{NAME}')")
        compact_time = time.time() - tc

    # ---- readback: the merge-on-read current view ----
    t1 = time.time()
    spark.sql(f"REFRESH TABLE local.db.{NAME}")
    if SYNTH:
        # GB-scale: never collect the table into Python. Count only.
        n_live = spark.sql(f"SELECT count(*) c FROM local.db.{NAME}").collect()[0]["c"]
        materialized = []
    else:
        cols = ", ".join(c["name"] for c in COLUMNS)
        rows = spark.sql(f"SELECT {cols} FROM local.db.{NAME}").collect()
        materialized = [r.asDict() for r in rows]
        n_live = len(materialized)
    readback_time = time.time() - t1

    # ---- stats ----
    commit_count = spark.sql(f"SELECT count(*) c FROM local.db.{NAME}.snapshots").collect()[0]["c"]
    data_files = delete_files = bytes_data = bytes_delete = 0
    for r in spark.sql(
        f"SELECT content, count(*) n, sum(file_size_in_bytes) b "
        f"FROM local.db.{NAME}.files GROUP BY content"
    ).collect():
        if r["content"] == 0:
            data_files, bytes_data = r["n"], int(r["b"] or 0)
        elif r["content"] == 2:
            delete_files, bytes_delete = r["n"], int(r["b"] or 0)

    # ---- audited-rewrite verdict, read back from the current snapshot's summary ----
    audit_summary = {}
    try:
        srow = spark.sql(
            f"SELECT summary FROM local.db.{NAME}.snapshots ORDER BY committed_at DESC LIMIT 1"
        ).collect()
        if srow and srow[0]["summary"]:
            audit_summary = {k: v for k, v in srow[0]["summary"].items()
                             if k.startswith("mor.audit.")}
    except Exception:
        audit_summary = {}

    # A large verdict is spilled to a Puffin blob REGISTERED as a table statistics file (so routine
    # orphan cleanup cannot delete it). Read it back through the registered statistics entry, never by
    # guessing a path.
    if audit_summary.get("mor.audit.stale-wins-keys-spilled") == "true":
        try:
            t = load(TABLE_DIR)
            snap_id = t.currentSnapshot().snapshotId()
            blob_json = None
            it = t.statisticsFiles().iterator()
            while it.hasNext():
                sf = it.next()
                if sf.snapshotId() != snap_id:
                    continue
                reader = jvm.org.apache.iceberg.puffin.Puffin.read(
                    t.io().newInputFile(sf.path())).build()
                fm = reader.fileMetadata()
                pit = reader.readAll(fm.blobs()).iterator()
                while pit.hasNext():
                    pair = pit.next()
                    raw = jvm.org.apache.iceberg.util.ByteBuffers.toByteArray(pair.second())
                    blob_json = jvm.java.lang.String(raw, "UTF-8")
                reader.close()
            if blob_json is not None:
                audit_summary["mor.audit.stale-wins-keys"] = blob_json
                audit_summary["mor.audit.spill-source"] = "puffin-statistics-file"
        except Exception as e:
            audit_summary["mor.audit.spill-read-error"] = str(e)[:300]

    # ---- score the verdict against the construction oracle ----
    # Both expected sets are emitted, and exclusion is verified POSITIVELY: we intersect the captured
    # set with the duplicate set and require the intersection to be empty, rather than inferring the
    # guard worked from the absence of surprises in the stale-wins comparison.
    oracle = {}
    if SYNTH:
        orc = construction_oracle(SYNTH)
    # A configuration the closed form cannot describe gets NO scored verdict. The alternative -- score
    # against a derivation whose premise the generator has deliberately broken -- would manufacture
    # false positives and misses out of nothing. Callers that only need metadata-level outcomes (the
    # gate-selectivity sweep reads groups-gated, which is independent of any key set) are unaffected.
    if SYNTH and not orc.get("oracle_valid", True):
        oracle = dict(orc)
        oracle["scored"] = False
    elif SYNTH:
        exp_stale, exp_dup = set(orc["expected_stale_wins"]), set(orc["expected_dup_risky"])

        def parse_keys(raw):
            if not raw:
                return set()
            try:
                return {int(r[0]) if isinstance(r, list) else int(r) for r in json.loads(raw)}
            except Exception as e:
                oracle.setdefault("parse_error", str(e)[:200])
                return set()

        # Cross-group mode writes its MERGED verdict to a different property and leaves the per-group
        # list in place, so both are visible in one snapshot. Scoring only the per-group property in
        # cross-group mode reports zero recall for a mode whose entire purpose is to restore recall --
        # a false zero produced by the scorer, not by the mechanism. Score whichever the mode produced,
        # and keep the per-group number alongside so the recovery is attributable.
        per_group = parse_keys(audit_summary.get("mor.audit.stale-wins-keys"))
        merged = parse_keys(audit_summary.get("mor.audit.cross-group-keys"))
        cross_on = audit_summary.get("mor.audit.cross-group") == "true"
        got = merged if cross_on else per_group
        oracle.update({
            "scored_property": "cross-group-keys" if cross_on else "stale-wins-keys",
            "per_group_captured": len(per_group),
            "per_group_true_positives": len(per_group & exp_stale),
            "per_group_false_positives": len(per_group - exp_stale),
            "derivation": orc["derivation"],
            "expected_live_rows": orc["expected_live_rows"],
            "measured_live_rows": int(n_live),
            "live_rows_match": orc["expected_live_rows"] == int(n_live),
            "expected_stale_wins": len(exp_stale),
            "expected_dup_risky": len(exp_dup),
            "captured": len(got),
            "true_positives": len(got & exp_stale),
            "false_positives_other": len(got - exp_stale - exp_dup),
            "false_positives_from_duplicates": len(got & exp_dup),   # guard failure; must be 0
            "misses": len(exp_stale - got),
            "risky_D_values": orc["risky_D_values"],
            "stale_sample": sorted(exp_stale)[:5],
            "dup_sample": sorted(exp_dup)[:5],
            # the actual offending keys, so a false positive can be diagnosed rather than guessed at
            "false_positive_keys": sorted(got - exp_stale)[:20],
            "per_group_false_positive_keys": sorted(per_group - exp_stale)[:20],
        })

    result = {
        "materialized": materialized,
        "audit_summary": audit_summary,
        "oracle": oracle,
        "stats": {
            "apply_time_s": round(apply_time, 3),
            "compact_time_s": round(compact_time, 3),
            "readback_time_s": round(readback_time, 3),
            "commit_count": int(commit_count),
            "data_files": int(data_files),
            "delete_files": int(delete_files),
            "bytes_data": bytes_data,
            "bytes_delete": bytes_delete,
            "bytes_total": bytes_data + bytes_delete,
            "live_rows": int(n_live),
            "evict_s": round(evict_s, 2),
            "peak_rss_mb": peak_rss_mb(),
        },
        "table_dir": TABLE_DIR,
    }
    if audit_verdict_path and os.path.exists(audit_verdict_path):
        with open(audit_verdict_path) as vf:
            result["audit_verdict_lines"] = [json.loads(ln) for ln in vf if ln.strip()]
    with open(OUT, "w") as f:
        json.dump(result, f)
    spark.stop()


try:
    main()
except Exception:
    with open(OUT, "w") as f:
        json.dump({"error": "iceberg driver failed", "traceback": traceback.format_exc()}, f)
    sys.exit(1)
