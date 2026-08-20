#!/usr/bin/env python3
"""L2 + L3 -- multi-group behaviour and the cross-group mode's scaling limit.

L2. Per-group detection is one-sided but INCOMPLETE: a key whose discarded version lands in a different
file group than its survivor cannot be resolved within either group, so it is missed. Until now the
miss rate was only measured at toy scale with a pathological 20 KB group size, which tells you the
failure exists but not what it costs an operator. Here the group size is one a person would actually
set, and the construction oracle supplies the true violation set, so the miss rate is measured rather
than inferred. Both arms must still show ZERO false positives -- straddling costs recall, and the claim
is that it never costs soundness.

L3. Cross-group mode fixes the misses by accumulating a per-key candidate map ON THE DRIVER, so its
memory is O(distinct keys) and independent of table bytes. Pre-registered prediction 7 said this OOMs
around 100M keys; with an 8 GB heap it should fail well below that. Finding where is the result, so a
failure here is data, not an error -- the sweep catches it and keeps going.
"""
import json
import os
import shutil
import sys
import tempfile
import time

_REPO = os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__)))))
sys.path.insert(0, os.path.join(_REPO, "cost-study/src"))
from mor_harness.adapters.base import run_driver, serialize_plan  # noqa: E402
from mor_harness.model import WritePlan                            # noqa: E402

WH = os.path.join(tempfile.gettempdir(), "mor_groups")
JAR = os.environ.get("MOR_ICEBERG_JAR", os.path.expanduser(
    "~/IdeaProjects/iceberg-mor-fork/spark/v3.5/spark-runtime/build/libs/"
    "iceberg-spark-runtime-3.5_2.12-1.11.0-SNAPSHOT.jar"))
COLS = [{"name": "id", "type": "int"}, {"name": "val", "type": "string"},
        {"name": "lsn", "type": "int"}]


def run(name, synth, opts, tag):
    tdir = os.path.join(WH, "db", name)
    shutil.rmtree(tdir, ignore_errors=True)
    plan = WritePlan(checkpoints=[], key_columns=["id"], payload_columns=["val"],
                     version_column="lsn", enforcement_mode="unsafe_compact")
    pj = serialize_plan(plan, name, tdir, WH, "lsn", COLS)
    pj["synth"] = synth
    os.environ["MOR_ICEBERG_JAR"] = JAR
    os.environ["MOR_BULK_INGEST"] = "1"
    os.environ["MOR_AUDIT"] = "1"
    os.environ["MOR_AUDIT_CROSS_GROUP"] = "1" if "cross" in tag else "0"
    os.environ["MOR_REWRITE_OPTS"] = opts
    os.environ.pop("MOR_DROP_CACHE", None)
    t0 = time.time()
    try:
        res = run_driver("iceberg_driver.py", pj, os.path.join(WH, "_io", name))
        err = res.get("error")
    except Exception as e:                       # a scaling failure is the measurement, not a crash
        res, err = {}, f"{type(e).__name__}: {str(e)[:400]}"
    wall = time.time() - t0
    shutil.rmtree(tdir, ignore_errors=True)
    return res, err, wall


# ------------------------------------------------------------------ L2
# ~6 GB across 16 commits x 4 files. max-file-group-size-bytes=1 GB is a size an operator would set;
# the earlier 20 KB was chosen to force straddling and is not representative of anything.
L2_SYNTH = {"commits": 16, "rows_per_commit": 900_000, "payload_bytes": 400, "delete_frac": 0.2,
            "ordering": "inverted", "dup_frac": 0.05, "files_per_commit": 4}
GROUP_OPT = "max-file-group-size-bytes=1073741824"
L2_ARMS = [("base_1group", ""), ("base_6groups", GROUP_OPT), ("cross_6groups", GROUP_OPT)]

out = {"L2": {}, "L3": {}}
failures = []
print("=" * 84 + "\nL2 -- straddling at a realistic group size\n" + "=" * 84, flush=True)
for tag, opts in L2_ARMS:
    res, err, wall = run(f"l2_{tag}", L2_SYNTH, opts, tag)
    o = res.get("oracle") or {}
    summ = res.get("audit_summary") or {}
    st = res.get("stats") or {}
    out["L2"][tag] = {"oracle": o, "err": err, "wall_s": round(wall, 1),
                      "groups": summ.get("mor.audit.groups-audited"),
                      "gated": summ.get("mor.audit.groups-gated"),
                      "compact_s": st.get("compact_time_s")}
    print(f"\n--- {tag} (wall {wall:.0f}s, compact {st.get('compact_time_s')}s) ---", flush=True)
    if err or not o:
        failures.append(f"L2/{tag}: {err or 'no oracle block'}")
        print(f"  FAILED: {err or 'no oracle block'}", flush=True)
        continue
    rec = o["true_positives"] / o["expected_stale_wins"] if o["expected_stale_wins"] else float("nan")
    print(f"  groups audited={out['L2'][tag]['groups']}  expected stale={o['expected_stale_wins']:,}")
    print(f"  captured={o['captured']:,}  TP={o['true_positives']:,}  misses={o['misses']:,}  "
          f"recall={rec:.1%}")
    print(f"  false positives: dup={o['false_positives_from_duplicates']} "
          f"other={o['false_positives_other']}  live_rows_match={o['live_rows_match']}")
    if o["false_positives_from_duplicates"] or o["false_positives_other"]:
        failures.append(f"L2/{tag}: SOUNDNESS BROKEN -- straddling produced false positives")
    if not o["live_rows_match"]:
        failures.append(f"L2/{tag}: live rows {o['measured_live_rows']} != {o['expected_live_rows']}")

# ------------------------------------------------------------------ L3
# Distinct keys is the axis; bytes are held down with a small payload so the driver-side map, not disk,
# is what runs out. files_per_commit keeps each file under the selection floor.
print("\n" + "=" * 84 + "\nL3 -- cross-group candidate map vs distinct keys\n" + "=" * 84, flush=True)
for keys, fpc in [(1_000_000, 1), (5_000_000, 2), (20_000_000, 6), (50_000_000, 14)]:
    synth = {"commits": 3, "rows_per_commit": keys, "payload_bytes": 24, "delete_frac": 0.2,
             "ordering": "inverted", "dup_frac": 0.0, "files_per_commit": fpc}
    res, err, wall = run(f"l3_{keys//1000}k", synth, "", "cross")
    o = res.get("oracle") or {}
    st = res.get("stats") or {}
    ok = bool(o) and not err
    out["L3"][str(keys)] = {"ok": ok, "err": err, "wall_s": round(wall, 1),
                            "peak_rss_mb": st.get("peak_rss_mb"),
                            "compact_s": st.get("compact_time_s"),
                            "captured": o.get("captured"), "misses": o.get("misses"),
                            "fp": (o.get("false_positives_from_duplicates", 0)
                                   + o.get("false_positives_other", 0)) if o else None}
    print(f"\n--- {keys:,} distinct keys (wall {wall:.0f}s) ---", flush=True)
    if not ok:
        print(f"  FAILED at {keys:,} keys -- this is the measurement: {err or 'no oracle block'}"[:500],
              flush=True)
        break                                     # the first failure is the limit; no point going higher
    print(f"  captured={o['captured']:,} misses={o['misses']:,} "
          f"fp={out['L3'][str(keys)]['fp']}  peak RSS={st.get('peak_rss_mb')} MB  "
          f"compact={st.get('compact_time_s')}s")
    if o["false_positives_from_duplicates"] or o["false_positives_other"]:
        failures.append(f"L3/{keys}: false positives in cross-group mode")

print("\n" + "=" * 84)
print("PASS" if not failures else "FAIL:\n  " + "\n  ".join(failures))
dst = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bench_scale_groups.json")
json.dump({"L2": out["L2"], "L3": out["L3"], "failures": failures}, open(dst, "w"), indent=1)
print(f"evidence -> {dst}")
