#!/usr/bin/env python3
"""Phase 7 probe 3: does VACUUM actually DELETE the superseded files, observed rather than inferred?

WHY THIS EXISTS. Entry 36 characterised Delta's expiry from DEFAULT CONSTANTS read out of the shipped
classes, and said so: the deletion itself was never observed, because the default tombstone retention
is one week and `spark.databricks.delta.retentionDurationCheck.enabled` blocks a zero-hour VACUUM.
That guard is disableable. With it off, the deletion can be watched happening in a few seconds instead
of inferred from a constant or waited out for a week.

WHAT IS BEING TESTED. §4.5's claim about Delta: VACUUM deletes the superseded data files that time
travel depends on, so the evidence is destroyed on a clock independent of OPTIMIZE. The question is
whether the files a reader would need are the ones that actually disappear.

POSITIVE CONTROLS, all hard failures. This project has eight recorded cases of a measurement
declining to run while producing plausible output (RESULTS.md §11), and "the directory is empty" is
exactly the shape that fails silently -- an empty directory proves nothing if the files were never
written. So, in order:

  C1  the superseded files are ENUMERATED BY NAME from the log's `remove` actions before anything is
      deleted, and each one is asserted PRESENT on disk. A deletion cannot be believed unless the
      thing deleted is known to have existed.
  C2  the superseded files are asserted REACHABLE before the delete: time travel to an early version
      reads the expected rows. Files that exist but are already unreadable prove nothing either.
  C3  the guard is asserted REAL: `VACUUM RETAIN 0 HOURS` is attempted with the check left at its
      default and must FAIL. If a zero-hour vacuum were permitted by default, the run would be
      measuring a different configuration from the one Entry 36 describes.
  C4  VACUUM must report itself in `DESCRIBE HISTORY`, so a no-op cannot pass as a deletion.
  C5  the files named in C1 must be the ones GONE afterwards, checked by name, and the current
      version's own files must SURVIVE. A vacuum that deleted everything, or nothing, is not the
      claim.

Run:  JAVA_HOME=<jdk17> ../../../checker/.venv/bin/python probe_delta_vacuum.py <warehouse-dir>
Writes probe_delta_vacuum.json next to this script. Exits non-zero if any control fails.
"""
import json
import os
import shutil
import sys
from collections import Counter

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "probe_delta_vacuum.json")
WH = sys.argv[1] if len(sys.argv) > 1 else os.path.join("/tmp", "mor_delta_vacuum")
shutil.rmtree(WH, ignore_errors=True)
os.makedirs(WH, exist_ok=True)
os.environ.setdefault("PYSPARK_SUBMIT_ARGS", "--driver-memory 4g pyspark-shell")

ADD_OPENS = " ".join(
    f"--add-opens=java.base/{p}=ALL-UNNAMED"
    for p in ["java.lang", "java.lang.invoke", "java.lang.reflect", "java.io", "java.net", "java.nio",
              "java.util", "java.util.concurrent", "java.util.concurrent.atomic", "sun.nio.ch",
              "sun.nio.cs", "sun.security.action", "sun.util.calendar"]
) + " --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"

from pyspark.sql import SparkSession  # noqa: E402

spark = (SparkSession.builder.appName("delta-vacuum-probe").master("local[2]")
         .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
         .config("spark.jars.ivy", os.environ.get("MOR_IVY_DIR") or os.path.expanduser("~/.ivy2"))
         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
         .config("spark.driver.host", "localhost").config("spark.ui.enabled", "false")
         .config("spark.sql.shuffle.partitions", "2")
         .config("spark.driver.extraJavaOptions", ADD_OPENS)
         .config("spark.executor.extraJavaOptions", ADD_OPENS)
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
jvm = spark._jvm
TBL = os.path.join(WH, "t")
R = {"delta_package": "io.delta:delta-spark_2.12:3.2.0"}
FAIL = []


def check_(cond, msg):
    print(("  ok   " if cond else "  FAIL ") + msg)
    if not cond:
        FAIL.append(msg)
    return cond


def data_files():
    """Every .parquet outside _delta_log / _change_data, relative to the table root."""
    out = set()
    for dp, _, fns in os.walk(TBL):
        rel = os.path.relpath(dp, TBL)
        if rel.startswith("_delta_log") or "_change_data" in rel:
            continue
        for fn in fns:
            if fn.endswith(".parquet"):
                out.add(fn if rel == "." else os.path.join(rel, fn))
    return out


def log_actions():
    """add/remove paths per commit, read straight out of the JSON commits."""
    logdir = os.path.join(TBL, "_delta_log")
    adds, removes = set(), set()
    for fn in sorted(f for f in os.listdir(logdir) if f.endswith(".json")):
        for line in open(os.path.join(logdir, fn)):
            a = json.loads(line)
            if "add" in a:
                adds.add(a["add"]["path"])
            if "remove" in a:
                removes.add(a["remove"]["path"])
    return adds, removes


# --- the shipped constant, and the guard's own key, read from the classes not from docs ----------
CFG = jvm.org.apache.spark.sql.delta.DeltaConfigs
tomb = CFG.TOMBSTONE_RETENTION()
R["tombstone_retention_default"] = str(tomb.defaultValue())
R["tombstone_retention_key"] = str(tomb.key())
GUARD = "spark.databricks.delta.retentionDurationCheck.enabled"
try:
    SQLCONF = jvm.org.apache.spark.sql.delta.sources.DeltaSQLConf
    entry = SQLCONF.DELTA_VACUUM_RETENTION_CHECK_ENABLED()
    R["guard_key_in_shipped_classes"] = str(entry.key())
    R["guard_default_in_shipped_classes"] = str(entry.defaultValue())
except Exception as e:
    R["guard_key_in_shipped_classes"] = f"lookup failed: {str(e)[:120]}"
    R["guard_default_in_shipped_classes"] = "unknown"
try:                      # typed boolean conf: an untyped default string is rejected
    R["guard_default"] = spark.conf.get(GUARD)
except Exception:
    R["guard_default"] = f"unset in session; class default {R['guard_default_in_shipped_classes']}"
print(f"\n=== shipped defaults ===\n  {R['tombstone_retention_key']} = {R['tombstone_retention_default']}"
      f"\n  guard key from classes: {R['guard_key_in_shipped_classes']}"
      f"\n  guard session value:    {R['guard_default']}")
check_(R["guard_key_in_shipped_classes"] == GUARD,
       f"the guard key exists in the shipped delta-spark 3.2.0 classes and is {GUARD}")

# --- build: 6 MERGEs over the same 50 keys, so each commit supersedes the previous file ----------
print("\n=== build ===")
spark.sql(f"CREATE TABLE delta.`{TBL}` (id INT, lsn INT) USING delta")
for v in range(1, 7):
    spark.sql(f"CREATE OR REPLACE TEMP VIEW src AS SELECT explode(sequence(1,50)) AS id, {v} AS lsn")
    spark.sql(f"MERGE INTO delta.`{TBL}` t USING src s ON t.id = s.id "
              f"WHEN MATCHED THEN UPDATE SET t.lsn = s.lsn WHEN NOT MATCHED THEN INSERT *")

adds, removes = log_actions()
on_disk_before = data_files()
current = {r.path for r in spark.sql(f"SELECT input_file_name() AS path FROM delta.`{TBL}`").collect()}
current = {os.path.relpath(p.replace("file:", ""), TBL) for p in current}
superseded = sorted((adds & removes) & on_disk_before)   # written, then retired, still on disk
R["files_on_disk_before"] = len(on_disk_before)
R["log_add_actions"] = len(adds)
R["log_remove_actions"] = len(removes)
R["superseded_named_in_log"] = superseded
R["current_version_files"] = sorted(current)
print(f"  data files on disk: {len(on_disk_before)}   log adds: {len(adds)}   log removes: {len(removes)}")
print(f"  superseded (add+remove, still on disk): {len(superseded)}")
print(f"  current version files: {sorted(current)}")

# --- C1: the superseded files exist, by name, before anything is deleted -------------------------
print("\n=== controls before VACUUM ===")
check_(len(superseded) > 0, f"C1a: the log names superseded files still on disk ({len(superseded)})")
missing = [f for f in superseded if not os.path.exists(os.path.join(TBL, f))]
check_(not missing, f"C1b: every named superseded file is PRESENT on disk before VACUUM "
                    f"(absent: {missing or 'none'})")

# --- C2: they are reachable before the delete ----------------------------------------------------
try:
    tt = spark.read.format("delta").option("versionAsOf", 3).load(TBL) \
        .selectExpr("min(lsn) mn", "max(lsn) mx", "count(*) n").collect()[0]
    R["time_travel_v3_before"] = {"min_lsn": tt["mn"], "max_lsn": tt["mx"], "rows": tt["n"]}
    ok_tt = tt["n"] == 50
except Exception as e:
    R["time_travel_v3_before"] = f"FAILED: {str(e)[:160]}"; ok_tt = False
check_(ok_tt, f"C2: time travel to v3 READS before VACUUM -> {R['time_travel_v3_before']}")

# --- C3: the guard is real -- a zero-hour vacuum must be refused at the default ------------------
spark.conf.set(GUARD, "true")
try:
    spark.sql(f"VACUUM delta.`{TBL}` RETAIN 0 HOURS")
    R["guard_blocks_zero_hour"] = False
    guard_err = "<no exception: the zero-hour vacuum was PERMITTED>"
except Exception as e:
    R["guard_blocks_zero_hour"] = True
    guard_err = str(e).split("\n")[0][:150]
R["guard_error"] = guard_err
check_(R["guard_blocks_zero_hour"],
       f"C3: with the guard on, RETAIN 0 HOURS is refused -> {guard_err}")

# --- the measurement: guard off, zero retention ---------------------------------------------------
print("\n=== VACUUM with the guard disabled, RETAIN 0 HOURS ===")
spark.conf.set(GUARD, "false")
dry = spark.sql(f"VACUUM delta.`{TBL}` RETAIN 0 HOURS DRY RUN").collect()
R["dry_run_paths"] = len(dry)
print(f"  DRY RUN lists {len(dry)} path(s) as deletable")
spark.sql(f"VACUUM delta.`{TBL}` RETAIN 0 HOURS")

on_disk_after = data_files()
gone = sorted(on_disk_before - on_disk_after)
kept = sorted(on_disk_after)
R["files_on_disk_after"] = len(on_disk_after)
R["files_deleted"] = gone
R["files_kept"] = kept
print(f"  data files: {len(on_disk_before)} -> {len(on_disk_after)}   deleted {len(gone)}")

hist = [(r["version"], r["operation"]) for r in
        spark.sql(f"DESCRIBE HISTORY delta.`{TBL}`").select("version", "operation").collect()]
R["history"] = hist
check_(any(op.upper().startswith("VACUUM") for _, op in hist),
       f"C4: VACUUM appears in DESCRIBE HISTORY -> {[op for _, op in hist if 'VACUUM' in op.upper()]}")

# --- C5: the named superseded files are the ones gone; the current ones survive -------------------
print("\n=== controls after VACUUM ===")
still_there = [f for f in superseded if os.path.exists(os.path.join(TBL, f))]
check_(not still_there,
       f"C5a: every file named superseded BEFORE is now DELETED (survivors: {still_there or 'none'})")
lost_current = [f for f in current if not os.path.exists(os.path.join(TBL, f))]
check_(not lost_current,
       f"C5b: the current version's own files SURVIVE (lost: {lost_current or 'none'})")
check_(set(gone) == set(superseded),
       f"C5c: the deleted set is exactly the superseded set "
       f"(deleted-not-superseded: {sorted(set(gone)-set(superseded)) or 'none'}; "
       f"superseded-not-deleted: {sorted(set(superseded)-set(gone)) or 'none'})")

# --- consequence: is the evidence time travel depended on now gone? -------------------------------
# The PHYSICAL fact first: which file did v3's snapshot stand on, and is it still there?
logdir = os.path.join(TBL, "_delta_log")
snap3, removed_by_3 = set(), set()
for fn in sorted(f for f in os.listdir(logdir) if f.endswith(".json"))[:4]:   # 000..003
    for line in open(os.path.join(logdir, fn)):
        a = json.loads(line)
        if "add" in a:
            snap3.add(a["add"]["path"])
        if "remove" in a:
            removed_by_3.add(a["remove"]["path"])
v3_files = sorted(snap3 - removed_by_3)
v3_gone = [f for f in v3_files if not os.path.exists(os.path.join(TBL, f))]
R["v3_snapshot_files"] = v3_files
R["v3_snapshot_files_deleted"] = v3_gone
check_(v3_files and set(v3_gone) == set(v3_files),
       f"C6: every data file v3's snapshot stood on is now deleted from disk "
       f"({len(v3_gone)}/{len(v3_files)})")

# An in-session re-read is NOT evidence: the DeltaLog snapshot and the file data are cached in this
# JVM, so it can return rows for files that no longer exist. Read it in a FRESH PROCESS instead.
probe_src = os.path.join(WH, "_readback.py")
with open(probe_src, "w") as f:
    f.write(f"""import json, os, sys
from pyspark.sql import SparkSession
spark = (SparkSession.builder.appName("delta-vacuum-readback").master("local[2]")
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
    .config("spark.jars.ivy", {os.environ.get("MOR_IVY_DIR") or os.path.expanduser("~/.ivy2")!r})
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.driver.host", "localhost").config("spark.ui.enabled", "false")
    .config("spark.driver.extraJavaOptions", {ADD_OPENS!r})
    .config("spark.executor.extraJavaOptions", {ADD_OPENS!r}).getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
out = {{}}
# count(*) and min/max are answerable from the log's per-file statistics WITHOUT opening a data
# file, so they report success against files that no longer exist. sum() and fetching rows are not
# servable from those stats: they force a real read. Both are recorded so the difference is visible.
def probe(df, tag):
    r = {{}}
    try:
        r["count_star"] = df.selectExpr("count(*) n").collect()[0]["n"]
        r["count_star_note"] = "may be served from log statistics, not from data"
    except Exception as e:
        r["count_star"] = "FAILED: " + type(e).__name__
    try:
        r["sum_lsn"] = df.selectExpr("sum(lsn) s").collect()[0]["s"]
        r["rows_fetched"] = [x["lsn"] for x in df.select("lsn").limit(3).collect()]
        r["data_readable"] = True
    except Exception as e:
        r["data_readable"] = False
        msg = str(e)
        root = next((ln.strip() for ln in msg.splitlines()
                     if "FileNotFound" in ln or "does not exist" in ln
                     or "FileReadException" in ln or "No such file" in ln), "")
        r["data_error"] = (root or msg.splitlines()[0])[:260]
        r["data_error_type"] = type(e).__name__
    return r
try:
    out["v3"] = probe(spark.read.format("delta").option("versionAsOf", 3).load({TBL!r}), "v3")
except Exception as e:
    out["v3"] = {{"data_readable": False, "error": type(e).__name__ + ": " + str(e).split("\\n")[0][:200]}}
try:
    out["current"] = probe(spark.read.format("delta").load({TBL!r}), "current")
except Exception as e:
    out["current"] = {{"data_readable": False, "error": type(e).__name__ + ": " + str(e).split("\\n")[0][:200]}}
print("READBACK_JSON " + json.dumps(out))
spark.stop()
""")
import subprocess
rb = subprocess.run([sys.executable, probe_src], capture_output=True, text=True, timeout=900)
line = [l for l in rb.stdout.splitlines() if l.startswith("READBACK_JSON ")]
R["fresh_process_readback"] = json.loads(line[0][len("READBACK_JSON "):]) if line else {
    "error": "readback subprocess produced no result", "stderr_tail": rb.stderr[-400:]}
print(f"  fresh-process readback: {R['fresh_process_readback']}")

# In-session read, recorded only to show it is misleading, never as evidence.
try:
    tt2 = spark.read.format("delta").option("versionAsOf", 3).load(TBL) \
        .selectExpr("count(*) n").collect()[0]
    R["time_travel_v3_after_in_session"] = {"rows": tt2["n"], "readable": True,
                                            "note": "served from this JVM's caches; not evidence"}
except Exception as e:
    R["time_travel_v3_after_in_session"] = {"readable": False,
                                            "error": type(e).__name__ + ": " + str(e).split("\n")[0][:160]}
print(f"  time travel to v3 AFTER vacuum, same session: {R['time_travel_v3_after_in_session']}")
R["current_rows_after"] = spark.sql(f"SELECT count(*) n FROM delta.`{TBL}`").collect()[0]["n"]
print(f"  current version still reads: {R['current_rows_after']} rows")

R["failures"] = FAIL
print()
if FAIL:
    print("CONTROLS FAILED -- the run did not measure the intended shape:")
    for f in FAIL:
        print("   -", f)
else:
    print(f"  => {len(gone)} superseded file(s) named in the log before the run were DELETED by")
    print(f"     VACUUM RETAIN 0 HOURS with the guard disabled; the current version's "
          f"{len(kept)} file(s) survived")
    fr = R["fresh_process_readback"].get("v3", {})
    print(f"     and still read {R['current_rows_after']} rows. Time travel to v3 in a FRESH process: "
          f"count(*)={fr.get('count_star')} (may be metadata), real data read="
          f"{'YES' if fr.get('data_readable') else 'NO -- ' + str(fr.get('data_error'))[:70]}.")
with open(OUT, "w") as f:
    json.dump(R, f, indent=1)
print(f"\n  -> {OUT}")
spark.stop()
shutil.rmtree(WH, ignore_errors=True)
sys.exit(1 if FAIL else 0)
