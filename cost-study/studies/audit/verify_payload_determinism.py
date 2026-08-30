#!/usr/bin/env python3
"""Did seeding the payload actually remove the run-to-run variation? Checked, not assumed.

Entry 58 measured an identical sweep cell -- same interleave fraction, same five seeds, same gate --
returning 64% clearance and then 56%. The cause was `os.urandom` payloads: compressed data-file sizes
drift slightly between runs, bin-packing shifts, group composition changes, and clearance is a rate
measured over groups.

Two things must now hold, and BOTH are checked because either alone can mislead:

  * identical DATA FILE SIZES, byte for byte, across two runs of the same configuration. This is the
    direct check on the cause. Equal clearance with differing file sizes would mean the variation is
    still there and simply did not happen to change the answer this time.
  * identical CLEARANCE. This is the check on the effect.

The entropy guard from the original generator is re-checked here too: seeding must not have bought
determinism by lowering entropy, since a payload that compresses collapses the table size and
silently invalidates every configuration built on bytes-on-disk.
"""
import json
import os
import shutil
import sys
import tempfile

_REPO = os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__)))))
sys.path.insert(0, os.path.join(_REPO, "cost-study/src"))
from mor_harness.adapters.base import run_driver, serialize_plan  # noqa: E402
from mor_harness.model import WritePlan                            # noqa: E402

WH = os.path.join(tempfile.gettempdir(), "mor_paydet")
JAR = os.environ.get("MOR_ICEBERG_JAR", os.path.expanduser(
    "~/IdeaProjects/iceberg-mor-fork/spark/v3.5/spark-runtime/build/libs/"
    "iceberg-spark-runtime-3.5_2.12-1.11.0-SNAPSHOT.jar"))
COLS = [{"name": "id", "type": "int"}, {"name": "val", "type": "string"},
        {"name": "lsn", "type": "int"}]
# The exact cell that showed 64% then 56%.
COMMITS, RPC, PAYLOAD, FRAC = 40, 1_500, 200, 1e-4
SEEDS = [1, 2, 3, 4, 5]
OPTS = "max-file-group-size-bytes=1500000,min-input-files=2,audit-cache-scan=false"


def one(seed, tag):
    name = f"pd_{tag}_{seed}"
    tdir = os.path.join(WH, "db", name)
    shutil.rmtree(tdir, ignore_errors=True)
    plan = WritePlan(checkpoints=[], key_columns=["id"], payload_columns=["val"],
                     version_column="lsn", enforcement_mode="unsafe_compact")
    pj = serialize_plan(plan, name, tdir, WH, "lsn", COLS)
    pj["synth"] = {"commits": COMMITS, "rows_per_commit": RPC, "payload_bytes": PAYLOAD,
                   "delete_frac": 0.2, "ordering": "contiguous",
                   "interleave_frac": FRAC, "interleave_seed": seed}
    os.environ.update({"MOR_ICEBERG_JAR": JAR, "MOR_BULK_INGEST": "1", "MOR_AUDIT": "1",
                       "MOR_AUDIT_CROSS_GROUP": "0", "MOR_REWRITE_OPTS": OPTS})
    res = run_driver("iceberg_driver.py", pj, os.path.join(WH, "_io", name))
    # file sizes BEFORE the table is removed; only the synthetic input files, not rewrite output
    ddir = os.path.join(tdir, "data")
    sizes = {}
    if os.path.isdir(ddir):
        for f in sorted(os.listdir(ddir)):
            if f.startswith("synth") and f.endswith(".parquet"):
                sizes[f] = os.path.getsize(os.path.join(ddir, f))
    shutil.rmtree(tdir, ignore_errors=True)
    if res.get("error"):
        raise RuntimeError(f"{name}: ...{res['error'][-1200:]}")
    s = res.get("audit_summary") or {}
    total = int(s.get("mor.audit.groups-total", 0))
    gated = int(s.get("mor.audit.groups-gated", 0))
    audited = int(s.get("mor.audit.groups-audited", 0))
    if total == 0:
        raise RuntimeError(f"{name}: no group formed; nothing gated is not clearance")
    if gated + audited != total:
        raise RuntimeError(f"{name}: counters do not account for every group")
    if total < 3:
        raise RuntimeError(f"{name}: only {total} groups; a rate is not resolvable")
    if not sizes:
        raise RuntimeError(f"{name}: no synthetic data files found; the size check would be vacuous")
    return {"groups": total, "gated": gated, "audited": audited, "sizes": sizes,
            "bytes_total": sum(sizes.values()), "rows": res["stats"]["live_rows"]}


print(f"payload determinism: frac={FRAC:g}, seeds={SEEDS}, {COMMITS} commits x {RPC:,} rows")
runs, fail = {}, []
for tag in ("A", "B"):
    per = [one(sd, tag) for sd in SEEDS]
    tot = sum(r["groups"] for r in per)
    gat = sum(r["gated"] for r in per)
    runs[tag] = {"per_seed": per, "groups": tot, "gated": gat, "clearance": gat / tot}
    print(f"  run {tag}: {tot} groups, {gat} gated, clearance {gat/tot:.1%}, "
          f"{sum(r['bytes_total'] for r in per):,} payload bytes on disk", flush=True)

A, B = runs["A"], runs["B"]
print("\n" + "=" * 88)

# --- the direct check: identical file sizes ---
size_mismatch = []
for i, (a, b) in enumerate(zip(A["per_seed"], B["per_seed"])):
    if a["sizes"] != b["sizes"]:
        diff = {k: (a["sizes"].get(k), b["sizes"].get(k))
                for k in set(a["sizes"]) | set(b["sizes"])
                if a["sizes"].get(k) != b["sizes"].get(k)}
        size_mismatch.append((SEEDS[i], dict(list(diff.items())[:4]), len(diff)))
if size_mismatch:
    fail.append(f"FILE SIZES STILL VARY across runs for {len(size_mismatch)} seed(s): {size_mismatch}. "
                f"Payload seeding did not remove the nondeterminism, or something else is varying.")
    print(f"  FILE SIZES DIFFER for seeds {[m[0] for m in size_mismatch]}")
    for sd, d, n in size_mismatch:
        print(f"    seed {sd}: {n} file(s) differ, e.g. {d}")
else:
    nfiles = sum(len(r["sizes"]) for r in A["per_seed"])
    print(f"  FILE SIZES IDENTICAL: all {nfiles} data files byte-for-byte the same size across both runs")

# --- the effect ---
if A["clearance"] != B["clearance"]:
    fail.append(f"CLEARANCE STILL VARIES: {A['clearance']:.1%} vs {B['clearance']:.1%} "
                f"({A['gated']} vs {B['gated']} gated of {A['groups']})")
    print(f"  CLEARANCE DIFFERS: {A['clearance']:.1%} vs {B['clearance']:.1%}")
else:
    print(f"  CLEARANCE IDENTICAL: {A['clearance']:.1%} in both runs "
          f"({A['gated']}/{A['groups']} groups gated)")

# --- entropy must not have been traded away for determinism ---
bytes_per_row = A["per_seed"][0]["bytes_total"] / (COMMITS * RPC)
expect = PAYLOAD * 0.975
if not (expect * 0.55 <= bytes_per_row <= expect * 1.45):
    fail.append(f"ENTROPY GUARD TRIPPED: {bytes_per_row:.0f} B/row on disk against {expect:.0f} "
                f"expected. The seeded payload is compressing, so determinism was bought by lowering "
                f"entropy and every bytes-on-disk configuration is invalid")
    print(f"  ENTROPY GUARD TRIPPED: {bytes_per_row:.0f} B/row vs {expect:.0f} expected")
else:
    print(f"  ENTROPY HELD: {bytes_per_row:.0f} B/row on disk against {expect:.0f} expected "
          f"(payload is not compressing)")

dst = os.path.join(os.path.dirname(__file__), "verify_payload_determinism.json")
json.dump({"config": {"commits": COMMITS, "rows_per_commit": RPC, "payload_bytes": PAYLOAD,
                      "interleave_frac": FRAC, "seeds": SEEDS},
           "runs": {k: {kk: vv for kk, vv in v.items() if kk != "per_seed"} for k, v in runs.items()},
           "per_seed_sizes_equal": not size_mismatch,
           "clearance_A": A["clearance"], "clearance_B": B["clearance"],
           "failures": fail}, open(dst, "w"), indent=1)
print(f"\nevidence -> {dst}")
print("\nPASS" if not fail else "\nFAIL:\n  " + "\n  ".join(fail))
sys.exit(1 if fail else 0)
