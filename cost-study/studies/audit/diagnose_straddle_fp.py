#!/usr/bin/env python3
"""Chase the false positive L2 produced, and decide whether it is a bug or a real limitation.

L2's `base_6groups` arm reported ONE key that the construction oracle says is not a violation. A false
positive is the one outcome the paper's central claim does not tolerate, so it gets chased rather than
noted -- and it is small enough (1 in 171,000) to be tempting to dismiss as noise, which is exactly why
it is not being dismissed.

The hypothesis under test: the single-survivor guard is evaluated PER GROUP, so it is sound only while
all of a key's survivors are co-resident in that group. Under straddling a key with several global
survivors can present as single-survivor locally; if a locally visible discarded version out-orders the
local survivor, the key is reported. If that is what is happening, per-group mode is NOT one-sided under
straddling -- straddling would cost soundness, not only recall, which is a stronger and worse limitation
than the paper currently states.

The alternative -- an off-by-one at a key-range or window boundary -- predicts the offending key sits at
a partition edge and does not move when the group size changes. The two hypotheses are separated by
sweeping the group size and looking at where the offending keys land.
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

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
WH = os.path.join(tempfile.gettempdir(), "mor_fpdiag")
JAR = os.environ.get("MOR_ICEBERG_JAR", os.path.expanduser(
    "~/IdeaProjects/iceberg-mor-fork/spark/v3.5/spark-runtime/build/libs/"
    "iceberg-spark-runtime-3.5_2.12-1.11.0-SNAPSHOT.jar"))
COLS = [{"name": "id", "type": "int"}, {"name": "val", "type": "string"},
        {"name": "lsn", "type": "int"}]
SYNTH = {"commits": 16, "rows_per_commit": 900_000, "payload_bytes": 400, "delete_frac": 0.2,
         "ordering": "inverted", "dup_frac": 0.05, "files_per_commit": 4}

# Import the oracle's own helpers so the diagnosis uses the same closed form the scoring used.
_drv = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                    "../../src/mor_harness/adapters/drivers/iceberg_driver.py")
_src = open(_drv).read()
_ns = {}
exec(compile(_src.split('_SQL_TYPE = {"int": "INT", "long": "BIGINT", "string": "STRING"}')[1]
             .split("def peak_rss_mb")[0], "<oracle>", "exec"), _ns)
_del_window, _lsn_base = _ns["_del_window"], _ns["_lsn_base"]


def classify(k):
    """Everything the closed form knows about one key: its whole version history."""
    C, R = SYNTH["commits"], SYNTH["rows_per_commit"]
    n_del = max(1, int(R * SYNTH["delete_frac"]))
    inv = SYNTH["ordering"] == "inverted"
    dk = 0
    for c in range(2, C + 1):
        st, en = _del_window(c, n_del, R)
        if st <= k < en:
            dk = c
    d0 = max(dk, 1)
    surv = [(c, _lsn_base(c, inv) + k - 1) for c in range(d0, C + 1)]
    disc = [(c, _lsn_base(c, inv) + k - 1) for c in range(1, d0)]
    dup_start = _del_window(C, n_del, R)[0]
    is_dup = dup_start <= k < dup_start + max(0, int(n_del * SYNTH["dup_frac"]))
    if is_dup:
        disc.append((1, 999_000_000 + k - 1))
        surv.append((C, _lsn_base(C, inv) + 1_000_000 + k - 1))
    return {"key": k, "D_k": dk, "n_survivors": len(surv),
            "max_discarded": max([o for _, o in disc], default=None),
            "max_survivor": max(o for _, o in surv),
            "injected_duplicate": is_dup,
            "globally_a_violation": len(surv) == 1 and disc
            and max(o for _, o in disc) > max(o for _, o in surv),
            "survivor_commits": [c for c, _ in surv][:8],
            "discarded_commits": [c for c, _ in disc][:8]}


def run(tag, group_bytes):
    name = f"fp_{tag}"
    tdir = os.path.join(WH, "db", name)
    shutil.rmtree(tdir, ignore_errors=True)
    plan = WritePlan(checkpoints=[], key_columns=["id"], payload_columns=["val"],
                     version_column="lsn", enforcement_mode="unsafe_compact")
    pj = serialize_plan(plan, name, tdir, WH, "lsn", COLS)
    pj["synth"] = SYNTH
    os.environ["MOR_ICEBERG_JAR"] = JAR
    os.environ["MOR_BULK_INGEST"] = "1"
    os.environ["MOR_AUDIT"] = "1"
    os.environ["MOR_AUDIT_CROSS_GROUP"] = "0"
    os.environ["MOR_REWRITE_OPTS"] = f"max-file-group-size-bytes={group_bytes}"
    os.environ.pop("MOR_DROP_CACHE", None)
    res = run_driver("iceberg_driver.py", pj, os.path.join(WH, "_io", name))
    shutil.rmtree(tdir, ignore_errors=True)
    return res.get("oracle") or {}, res.get("audit_summary") or {}


out = {}
for tag, gb in [("2GB", 2 * 1024**3), ("1GB", 1024**3), ("512MB", 512 * 1024**2)]:
    o, summ = run(tag, gb)
    fps = o.get("per_group_false_positive_keys", [])
    out[tag] = {"oracle": o, "groups_audited": summ.get("mor.audit.groups-audited"),
                "groups_gated": summ.get("mor.audit.groups-gated"),
                "groups_total": summ.get("mor.audit.groups-total"),
                "fp_keys": fps, "fp_analysis": [classify(k) for k in fps[:10]]}
    print(f"\n=== group size {tag}: {summ.get('mor.audit.groups-audited')} audited / "
          f"{summ.get('mor.audit.groups-total')} total ===", flush=True)
    print(f"  captured={o.get('captured')}  TP={o.get('true_positives')}  "
          f"misses={o.get('misses')}  FP={len(fps)}")
    for a in out[tag]["fp_analysis"]:
        print(f"    key {a['key']}: D_k={a['D_k']} survivors={a['n_survivors']} "
              f"(commits {a['survivor_commits']}) maxDisc={a['max_discarded']} "
              f"maxSurv={a['max_survivor']} globally_violation={a['globally_a_violation']} "
              f"injected_dup={a['injected_duplicate']}", flush=True)

print("\n" + "=" * 84)
multi = [a for v in out.values() for a in v["fp_analysis"] if a["n_survivors"] > 1]
if multi:
    print(f"DIAGNOSIS: {len(multi)} of the reported false positives have MULTIPLE global survivors.")
    print("  Per-group detection saw only one of them, so the single-survivor guard passed locally.")
    print("  => straddling costs SOUNDNESS in base mode, not only recall. The paper's claim that the")
    print("     error profile is one-sided at every group size is WRONG as stated and must be fixed.")
elif any(v["fp_keys"] for v in out.values()):
    print("DIAGNOSIS: false positives are NOT explained by multi-survivor straddling. Investigate as a")
    print("  possible boundary bug -- check whether the keys sit at key-range or delete-window edges.")
else:
    print("No false positives reproduced at any group size. The single L2 occurrence needs re-running")
    print("before anything is concluded from it -- do NOT record it as resolved.")

dst = os.path.join(os.path.dirname(os.path.abspath(__file__)), "diagnose_straddle_fp.json")
json.dump(out, open(dst, "w"), indent=1, default=str)
print(f"evidence -> {dst}")
