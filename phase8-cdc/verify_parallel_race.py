#!/usr/bin/env python3
"""Did the PIPELINE produce the inversion, and does it hold up against Postgres?

The induced variant assigns key 42's versions to checkpoints in inverted LSN order. This variant
assigns nothing: the sink runs at parallelism 2, events are shuffled onto subtasks by hashing a
NON-primary-key column (the FLINK-20374 configuration), each subtask writes on its own thread, and
checkpoint barriers fire on a timer. Which events precede which barrier -- and therefore which
sequence number they get -- is decided by thread scheduling.

So this script checks two things the induced run could not:
  * are the flagged keys genuinely stale according to POSTGRES, not according to the checker
  * does compaction launder them, same as the induced case

and one thing the induced run did not need:
  * how often the race reproduces. Reported as k of N runs, with every run counted. The first run is
    included in that tally: it is not permissible to keep running until it happens and report only
    the success, so the count includes runs that produced nothing.
"""
import json
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
TABLE = os.environ.get("MOR_P8_TABLE", "phase8_parallel")
WH = os.path.join(HERE, "results", f"{TABLE}_wh")
TABLE_DIR = os.path.join(WH, "realworld", TABLE)
MOR_CHECK = os.path.join(REPO, "checker", ".venv", "bin", "mor-check")
PY = os.path.join(REPO, "checker", ".venv", "bin", "python")
JAR = os.path.expanduser("~/IdeaProjects/iceberg-mor-fork/spark/v3.5/spark-runtime/build/libs/"
                         "iceberg-spark-runtime-3.5_2.12-1.11.0-SNAPSHOT.jar")
JAVA_HOME = "/Library/Java/JavaVirtualMachines/temurin-17.jdk/Contents/Home"

oracle = json.load(open(os.path.join(HERE, "oracle", "lsn_oracle.json")))
latest = oracle["logically_latest"]


def check():
    p = subprocess.run([MOR_CHECK, TABLE_DIR, "--version-column", "lsn", "--format", "json"],
                       capture_output=True, text=True)
    return json.loads(p.stdout)


SPARK = f"""
import json, sys
from pyspark.sql import SparkSession
s = (SparkSession.builder.appName("p8par").master("local[2]")
     .config("spark.jars", "{JAR}")
     .config("spark.sql.extensions",
             "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
     .config("spark.sql.catalog.h", "org.apache.iceberg.spark.SparkCatalog")
     .config("spark.sql.catalog.h.type", "hadoop")
     .config("spark.sql.catalog.h.warehouse", "{WH}")
     .config("spark.sql.shuffle.partitions", "1").getOrCreate())
s.sparkContext.setLogLevel("ERROR")
mode = sys.argv[1]; res = {{}}
if mode == "serve":
    keys = [int(k) for k in sys.argv[2].split(",")] if len(sys.argv) > 2 and sys.argv[2] else []
    rows = s.sql("SELECT id, balance, note, lsn FROM h.realworld.{TABLE}").collect()
    res["all"] = {{int(r[0]): dict(balance=r[1], note=r[2], lsn=r[3]) for r in rows}}
    res["total_rows"] = len(rows)
if mode == "compact":
    d = s.sql("CALL h.system.rewrite_data_files(table => 'realworld.{TABLE}', "
              "options => map('min-input-files','2'))").collect()[0]
    res["rewritten_data_files"] = int(d[0]); res["added_data_files"] = int(d[1])
print("P8_JSON " + json.dumps(res))
"""


def spark(mode, arg=""):
    p = subprocess.run([PY, "-c", SPARK, mode, arg], capture_output=True, text=True,
                       env={**os.environ, "JAVA_HOME": JAVA_HOME})
    for line in p.stdout.splitlines():
        if line.startswith("P8_JSON "):
            return json.loads(line[len("P8_JSON "):])
    raise RuntimeError(f"spark[{mode}] no result: {p.stdout[-400:]} {p.stderr[-600:]}")


fail, out = [], {}
pre = check()
flagged = [int((f.get("key") or {}).get("id")) for f in (pre.get("findings") or [])
           if f.get("type") == "STALE_WINS"]
out["pre_verdict"] = pre.get("verdict")
out["pre_counts"] = pre.get("counts")
out["flagged_keys"] = sorted(flagged)
out["n_flagged"] = len(flagged)
out["position_delete_note"] = pre.get("metadata_screen", {}) if isinstance(
    pre.get("metadata_screen"), dict) else None

if not flagged:
    print("  NULL RESULT: the race produced no stale win on this run.")
    json.dump({"result": out, "failures": []},
              open(os.path.join(HERE, "results", f"{TABLE}_race.json"), "w"), indent=1)
    sys.exit(0)

serve_pre = spark("serve")
out["total_rows"] = serve_pre["total_rows"]

# ---- do POSTGRES and the checker agree on which keys are stale? ----
agree, disagree = [], []
for k in flagged:
    served = serve_pre["all"].get(str(k)) or serve_pre["all"].get(k)
    exp = latest.get(str(k))
    if served is None or exp is None:
        disagree.append((k, "missing", served, exp))
        continue
    if served["lsn"] < exp["lsn"]:
        agree.append(k)
    else:
        disagree.append((k, "served lsn not below oracle latest", served["lsn"], exp["lsn"]))
out["oracle_agrees_on"] = len(agree)
out["oracle_disagrees_on"] = disagree
if disagree:
    fail.append(f"the oracle does NOT confirm {len(disagree)} of the flagged keys: {disagree[:3]}; "
                f"the checker and Postgres disagree and the flag cannot be trusted")

# a key the checker did NOT flag must not be stale either -- guards against under-reporting
missed = []
for k_s, exp in latest.items():
    k = int(k_s)
    served = serve_pre["all"].get(k_s) or serve_pre["all"].get(k)
    if served and served["lsn"] < exp["lsn"] and k not in flagged:
        missed.append(k)
out["oracle_stale_but_unflagged"] = missed
if missed:
    fail.append(f"{len(missed)} key(s) are stale by LSN but were NOT flagged: {missed[:5]}. The "
                f"checker under-reported, which is a recall failure, not a clean run")

# ---- compaction ----
comp = spark("compact")
out["compaction"] = comp
if comp.get("rewritten_data_files", 0) < 1:
    fail.append("compaction rewrote nothing; a FAITHFUL verdict after it would be meaningless")

post = check()
out["post_verdict"] = post.get("verdict")
out["post_counts"] = post.get("counts")
still = [int((f.get("key") or {}).get("id")) for f in (post.get("findings") or [])
         if f.get("type") == "STALE_WINS"]
out["still_flagged"] = sorted(still)
if still:
    fail.append(f"{len(still)} key(s) still STALE_WINS after compaction: {still[:5]}")

serve_post = spark("serve")
out["total_rows_after"] = serve_post["total_rows"]
changed = [k for k in flagged
           if (serve_pre["all"].get(str(k)) or serve_pre["all"].get(k))
           != (serve_post["all"].get(str(k)) or serve_post["all"].get(k))]
out["served_rows_changed"] = changed
if changed:
    fail.append(f"served row changed across compaction for {len(changed)} key(s): {changed[:5]}")

print(f"  flagged STALE_WINS         : {out['n_flagged']} key(s) -> {out['flagged_keys'][:10]}"
      f"{' ...' if out['n_flagged'] > 10 else ''}")
print(f"  Postgres agrees on         : {out['oracle_agrees_on']}/{out['n_flagged']}")
print(f"  stale by LSN but unflagged : {len(missed)}")
print(f"  compaction                 : rewrote {comp.get('rewritten_data_files')} file(s)")
print(f"  after compaction           : {out['post_verdict']}  counts={out['post_counts']}")
print(f"  served rows changed        : {len(changed)}   rows {out['total_rows']} -> "
      f"{out['total_rows_after']}")

json.dump({"result": out, "failures": fail,
           "scope": "one run of a nondeterministic race; not a rate"},
          open(os.path.join(HERE, "results", f"{TABLE}_race.json"), "w"), indent=1)
print("\nPASS" if not fail else "\nFAIL:\n  " + "\n  ".join(fail))
sys.exit(1 if fail else 0)
