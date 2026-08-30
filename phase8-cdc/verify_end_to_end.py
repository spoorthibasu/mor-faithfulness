#!/usr/bin/env python3
"""The three-part success criterion, checked end to end against the real-CDC table.

  (a) the checker flags at least one key STALE_WINS on the table the pipeline produced
  (b) the independently captured LSN record confirms that key's surviving row is NOT the
      logically-latest version
  (c) rewrite_data_files then runs, the checker reports the key FAITHFUL, and the served row is
      unchanged

(c) is the point: it is the paper's central claim -- compaction laundering a real violation --
demonstrated outside the synthetic generator for the first time.

WHAT IS AND IS NOT BEING CLAIMED. One induced failure in one pipeline. Not a rate, not a probability,
not a performance number, and not a comparison against anything in the cost study. The reorder was
induced deliberately (see build_write_plan.py); nothing here says how often such a reorder happens in
the field, and no sentence in the write-up may imply it.

INDEPENDENCE OF THE ORACLE. The expected answer comes from `oracle/lsn_oracle.json`, captured from
Debezium's `source.lsn` before the Iceberg table existed. The checker is never used to validate itself.
The SERVED ROW is read with Spark, a different engine from the checker's metadata reader, so "unchanged
across compaction" is not the checker's opinion of itself either.

POSITIVE CONTROLS. Six measurements in this project have reported success while doing nothing, most
recently an absent experimental arm read as an unstable one. So:
  * the target key must actually be STALE_WINS with mult_phys == 1 (a DUPLICATE would be a different
    failure class and would not be laundered at all)
  * compaction must actually have rewritten files -- a rewrite that selected nothing is the fastest
    possible way to "fix" a violation and means nothing
  * the verdict must CHANGE between the two observations; if it was already FAITHFUL there was
    nothing to launder
  * the served row must be read successfully both times; a failed read must not pass as "unchanged"
A run in which no key is flagged is a NULL RESULT and is reported as one.
"""
import json
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
WH = os.path.join(HERE, "results", "phase8_cdc_wh")
TABLE_DIR = os.path.join(WH, "realworld", "phase8_cdc")
ORACLE = os.path.join(HERE, "oracle", "lsn_oracle.json")
MOR_CHECK = os.path.join(REPO, "checker", ".venv", "bin", "mor-check")
PY = os.path.join(REPO, "checker", ".venv", "bin", "python")
JAR = os.path.expanduser("~/IdeaProjects/iceberg-mor-fork/spark/v3.5/spark-runtime/build/libs/"
                         "iceberg-spark-runtime-3.5_2.12-1.11.0-SNAPSHOT.jar")

def regenerate():
    """Rebuild the table from the write plan. Compaction mutates the table in place, so a second run
    against the leftover state would be checking an already-laundered table and would report a clean
    pass for the wrong reason."""
    m = ("flink-cdc-connect/flink-cdc-pipeline-connectors/"
         "flink-cdc-pipeline-connector-iceberg")
    p = subprocess.run(
        ["mvn", "-o", "-q", "-pl", m, "test", "-Dtest=MorPhase8CdcTest",
         f"-Dmor.out.dir={os.path.join(HERE, 'results')}",
         f"-Dmor.plan={os.path.join(HERE, 'oracle', 'write_plan.tsv')}",
         "-Dmor.table=phase8_cdc", "-Dcheckstyle.skip=true", "-Dspotless.check.skip=true",
         "-Drat.skip=true", "-Denforcer.skip=true", "-DfailIfNoTests=false"],
        cwd=os.path.expanduser("~/IdeaProjects/flink-cdc"), capture_output=True, text=True,
        env={**os.environ,
             "JAVA_HOME": "/Library/Java/JavaVirtualMachines/temurin-17.jdk/Contents/Home"})
    if p.returncode != 0:
        raise RuntimeError(f"table regeneration failed: {p.stdout[-600:]} {p.stderr[-400:]}")


fail, out = [], {}
regenerate()
oracle = json.load(open(ORACLE))
TARGET = oracle["target_key"]
latest = oracle["logically_latest"][str(TARGET)]


def check(tag):
    """Run the read-only checker and return its JSON report."""
    p = subprocess.run([MOR_CHECK, TABLE_DIR, "--version-column", "lsn", "--format", "json"],
                       capture_output=True, text=True)
    try:
        rep = json.loads(p.stdout)
    except json.JSONDecodeError:
        raise RuntimeError(f"{tag}: checker produced no parseable JSON: {p.stdout[-600:]} "
                           f"{p.stderr[-600:]}")
    return rep


def finding_for(rep, key):
    for f in rep.get("findings", []) or []:
        k = f.get("key") or {}
        if int(k.get("id", -1)) == key:
            return f
    return None


SPARK = f"""
import json, sys
from pyspark.sql import SparkSession
s = (SparkSession.builder.appName("phase8")
     .master("local[2]")
     .config("spark.jars", "{JAR}")
     .config("spark.sql.extensions",
             "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
     .config("spark.sql.catalog.h", "org.apache.iceberg.spark.SparkCatalog")
     .config("spark.sql.catalog.h.type", "hadoop")
     .config("spark.sql.catalog.h.warehouse", "{WH}")
     .config("spark.sql.shuffle.partitions", "1")
     .getOrCreate())
s.sparkContext.setLogLevel("ERROR")
mode = sys.argv[1]
res = {{}}
if mode in ("serve", "both"):
    r = s.sql("SELECT id, balance, note, lsn FROM h.realworld.phase8_cdc WHERE id = {TARGET}").collect()
    res["served"] = [dict(id=x[0], balance=x[1], note=x[2], lsn=x[3]) for x in r]
    res["total_rows"] = s.sql("SELECT count(*) c FROM h.realworld.phase8_cdc").collect()[0][0]
if mode == "compact":
    d = s.sql("CALL h.system.rewrite_data_files(table => 'realworld.phase8_cdc', "
              "options => map('min-input-files','2'))").collect()[0]
    res["rewritten_data_files"] = int(d[0])
    res["added_data_files"] = int(d[1])
print("PHASE8_JSON " + json.dumps(res))
"""


def spark(mode):
    p = subprocess.run([PY, "-c", SPARK, mode], capture_output=True, text=True,
                       env={**os.environ,
                            "JAVA_HOME": "/Library/Java/JavaVirtualMachines/temurin-17.jdk/Contents/Home"})
    for line in p.stdout.splitlines():
        if line.startswith("PHASE8_JSON "):
            return json.loads(line[len("PHASE8_JSON "):])
    raise RuntimeError(f"spark[{mode}] produced no result: {p.stdout[-500:]} {p.stderr[-800:]}")


# ---------------- (a) the checker flags a stale win ----------------
pre = check("pre")
out["pre_verdict"] = pre.get("verdict")
out["pre_counts"] = pre.get("counts")
f_pre = finding_for(pre, TARGET)
if f_pre is None:
    fail.append(f"NULL RESULT: no finding for key {TARGET} before compaction. The pipeline did not "
                f"produce a detectable violation on the target key; this is reported as a null "
                f"result, not adjusted away")
else:
    sa = f_pre.get("sequence_arithmetic") or {}
    out["pre_finding"] = {
        "type": f_pre.get("type"), "mult_phys": f_pre.get("mult_phys"),
        "surviving": [(r["seq"], r["version"]) for r in sa.get("surviving_records", [])],
        "suppressed": [(r["seq"], r["version"]) for r in sa.get("suppressed_records", [])]}
    if f_pre.get("type") != "STALE_WINS":
        fail.append(f"key {TARGET} is {f_pre.get('type')}, not STALE_WINS; a different "
                    f"failure class is being demonstrated than the one claimed")
    if f_pre.get("mult_phys") != 1:
        fail.append(f"mult_phys={f_pre.get('mult_phys')} for key {TARGET}; STALE_WINS requires "
                    f"exactly one surviving row")

# ---------------- served row before ----------------
serve_pre = spark("serve")
out["served_before"] = serve_pre["served"]
out["total_rows"] = serve_pre["total_rows"]
if len(serve_pre["served"]) != 1:
    fail.append(f"the table serves {len(serve_pre['served'])} rows for key {TARGET}; the stale-win "
                f"claim is about a single surviving row")

# ---------------- (b) the oracle confirms the survivor is not logically latest ----------------
if serve_pre["served"]:
    surv = serve_pre["served"][0]
    out["oracle_latest"] = latest
    out["survivor"] = surv
    if surv["lsn"] == latest["lsn"]:
        fail.append(f"the surviving row IS the logically-latest version (lsn {surv['lsn']}); there is "
                    f"no stale win, whatever the checker said")
    elif surv["lsn"] > latest["lsn"]:
        fail.append(f"survivor lsn {surv['lsn']} exceeds the oracle's latest {latest['lsn']}; the "
                    f"oracle is not describing this table")
    else:
        out["oracle_confirms_stale"] = True

# ---------------- (c) compaction, then the verdict and the served row ----------------
comp = spark("compact")
out["compaction"] = comp
if comp.get("rewritten_data_files", 0) < 1:
    fail.append(f"COMPACTION REWROTE NOTHING (rewritten_data_files="
                f"{comp.get('rewritten_data_files')}). A rewrite that selected no files cannot have "
                f"laundered anything, and a FAITHFUL verdict after it would be meaningless")

post = check("post")
out["post_verdict"] = post.get("verdict")
out["post_counts"] = post.get("counts")
f_post = finding_for(post, TARGET)
out["post_finding"] = None if f_post is None else {
    "type": f_post.get("type"), "mult_phys": f_post.get("mult_phys")}

serve_post = spark("serve")
out["served_after"] = serve_post["served"]
out["total_rows_after"] = serve_post["total_rows"]

still_flagged = f_post is not None and f_post.get("type") == "STALE_WINS"
if still_flagged:
    fail.append(f"key {TARGET} is STILL STALE_WINS after compaction; the laundering claim is NOT "
                f"demonstrated on this run")
if f_pre is not None and not still_flagged:
    out["verdict_changed"] = True

# the served row must be unchanged: same values, still the stale one
if serve_pre["served"] and serve_post["served"]:
    a, b = serve_pre["served"][0], serve_post["served"][0]
    out["served_unchanged"] = (a == b)
    if a != b:
        fail.append(f"the served row CHANGED across compaction: {a} -> {b}. The paper's claim is that "
                    f"compaction changes what is checkable, not what is served")
elif not serve_post["served"]:
    fail.append("could not read the served row after compaction; a failed read must not be reported "
                "as 'unchanged'")

# ---------------- report ----------------
print("=" * 96)
print(f"  (a) checker, before compaction : {out.get('pre_verdict')}  counts={out.get('pre_counts')}")
if out.get("pre_finding"):
    print(f"      key {TARGET}: {out['pre_finding']}")
print(f"  (b) oracle (Postgres LSN)      : logically latest lsn={latest['lsn']} "
      f"balance={latest['balance']} note={latest['note']}")
if out.get("survivor"):
    s = out["survivor"]
    print(f"      surviving row             : lsn={s['lsn']} balance={s['balance']} note={s['note']}")
    print(f"      oracle confirms stale     : {out.get('oracle_confirms_stale', False)}")
print(f"  (c) compaction                 : rewrote {out['compaction'].get('rewritten_data_files')} "
      f"data file(s), added {out['compaction'].get('added_data_files')}")
print(f"      checker, after compaction : {out.get('post_verdict')}  counts={out.get('post_counts')}")
print(f"      key {TARGET} after         : {out.get('post_finding')}")
print(f"      served row unchanged      : {out.get('served_unchanged')}")
print(f"      table rows {out.get('total_rows')} -> {out.get('total_rows_after')}")

dst = os.path.join(HERE, "results", "phase8_end_to_end.json")
json.dump({"result": out, "failures": fail,
           "scope": "one induced failure in one pipeline; not a rate, not a performance claim"},
          open(dst, "w"), indent=1)
print(f"\n  evidence -> {dst}")
print("\nPASS" if not fail else "\nFAIL:\n  " + "\n  ".join(fail))
sys.exit(1 if fail else 0)
