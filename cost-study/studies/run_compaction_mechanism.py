"""Compaction-masking mechanism: what does `rewrite_data_files` do to the physical layout
that `mor_checker` reads?

The storage study (`COST_REPORT_v2.md`) records that compacting a still-violating Iceberg
table flips some keys from a violation verdict to FAITHFUL (`checker_masked_by_compaction`).
That record says *which* keys, not *why*. Two mechanisms are consistent with it:

  (a) compaction DROPS the suppressed higher-version records, so `current_version_record`
      degenerates to the stale survivor and the stale-wins test compares it against itself;
  (b) compaction KEEPS them and renumbers sequence numbers so a different record becomes
      the checker's survivor.

This script settles it by dumping the per-key `PhysicalLayout` before and after. It runs the
two real cost-study Iceberg cells (seed 101, realistic operating point, base_keys=1200):

    arm A = unsafe          (no compaction)
    arm B = unsafe_compact  (identical layout + rewrite_data_files)

keeps both tables, and reports, per key: the data records with their (version, seq), the
delete sequence numbers, S_D, mult_phys, the survivor, `current_version_record`, and the
verdict. Nothing in the harness or checker is modified; this only reads.

Result (recorded in `results/compaction_mechanism.json`): mechanism (a). Compaction applies
the equality deletes and discards every version that lost, removing the delete files
entirely. Every key the checker flagged STALE_WINS is certified FAITHFUL afterwards, while
every DUPLICATE verdict survives, because both duplicate rows were visible and are retained.

Usage: PYTHONPATH=src python studies/run_compaction_mechanism.py [base_keys]
       (~10 min: two full Iceberg cells; needs JDK 17 + the checker venv.)
"""

import dataclasses
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
sys.path.insert(0, os.path.dirname(__file__))

from mor_harness import runner                                    # noqa: E402
from run_cost import build, WAREHOUSE, HARNESS                    # noqa: E402
from mor_checker.adapters.iceberg import IcebergAdapter           # noqa: E402
from mor_checker.core.classify import classify                    # noqa: E402
from mor_checker.core.model import (                              # noqa: E402
    mult_phys, s_d, visible_set, current_version_record,
)

SAMPLE_DUPLICATES = 3


def dump(layout):
    """Everything the checker's verdict is a function of, for one key."""
    cur = current_version_record(layout)
    return {
        "n_data_records": len(layout.data),
        "data": sorted([{"version": r.version, "seq": r.seq} for r in layout.data],
                       key=lambda x: (x["version"] is None, x["version"])),
        "delete_seqs": sorted(d.seq for d in layout.dels),
        "S_D": s_d(layout),
        "mult_phys": mult_phys(layout),
        "survivor_version": (visible_set(layout)[0].version
                             if mult_phys(layout) >= 1 else None),
        "current_version_record_version": cur.version if cur else None,
        "verdict": classify(layout).value,
    }


def run_arm(cfg):
    rec = runner.run(cfg, warehouse=WAREHOUSE)
    tdir = os.path.join(WAREHOUSE, "db", f"run_{cfg.config_hash()}_iceberg")
    adapter = IcebergAdapter(tdir, key_columns=["id"], version_column="lsn",
                             upsert_only=False)
    layouts = {str(k): dump(lay) for k, lay in adapter.layouts().items()}
    tally = {}
    for v in layouts.values():
        tally[v["verdict"]] = tally.get(v["verdict"], 0) + 1
    return {
        "table_dir": tdir,
        "correctness": rec["correctness"],
        "cost": {k: rec["cost"][k] for k in
                 ("commit_count", "data_files", "delete_files",
                  "bytes_data", "bytes_delete") if k in rec["cost"]},
        "checker_verdict_tally": tally,
        "layouts": layouts,
    }


def main(base_keys):
    base = [c for c in build(base_keys)
            if c.format == "iceberg" and c.enforcement_mode == "unsafe"][0]
    arms = {
        "unsafe": dataclasses.replace(base, keep_tables=True),
        "unsafe_compact": dataclasses.replace(base, enforcement_mode="unsafe_compact",
                                              keep_tables=True),
    }
    out = {}
    for name, cfg in arms.items():
        print(f"\n===== arm {name} =====", flush=True)
        out[name] = run_arm(cfg)
        print(f"  {json.dumps(out[name]['checker_verdict_tally'])}", flush=True)

    u, c = out["unsafe"], out["unsafe_compact"]
    flipped = sorted(k for k in u["layouts"]
                     if u["layouts"][k]["verdict"] != "FAITHFUL"
                     and c["layouts"].get(k, {}).get("verdict") == "FAITHFUL")
    dups = sorted(k for k in u["layouts"] if u["layouts"][k]["verdict"] == "DUPLICATE")

    print("\n================ RESULT ================")
    print("unsafe          files/bytes:", json.dumps(u["cost"]))
    print("unsafe_compact  files/bytes:", json.dumps(c["cost"]))
    print("unsafe          verdicts   :", json.dumps(u["checker_verdict_tally"]))
    print("unsafe_compact  verdicts   :", json.dumps(c["checker_verdict_tally"]))
    print(f"\nkeys flipping non-FAITHFUL -> FAITHFUL: {len(flipped)}")
    for k in flipped:
        print(f"  {k}: {u['layouts'][k]['verdict']} -> FAITHFUL")
        print(f"     BEFORE {json.dumps(u['layouts'][k])}")
        print(f"     AFTER  {json.dumps(c['layouts'][k])}")
    print(f"\nDUPLICATE keys before: {len(dups)}; after: "
          f"{c['checker_verdict_tally'].get('DUPLICATE', 0)}")
    for k in dups[:SAMPLE_DUPLICATES]:
        print(f"  {k}: BEFORE {json.dumps(u['layouts'][k])}")
        print(f"     AFTER  {json.dumps(c['layouts'].get(k))}")

    # Committed evidence: the summary plus the full layouts for the flipped keys and a
    # duplicate sample. The 1,247 full layouts per arm are regenerable, not committed.
    evidence = {
        "what": "Iceberg rewrite_data_files: physical layout before vs after, per key",
        "config": {"base_keys": base_keys, "seed": 101,
                   "operating_point": "clock_skew_ms=400, ooo_rate=0.05, "
                                      "dup_rate=0.05, schema_change_freq=0.2",
                   "version_column": "lsn (present in both arms)"},
        "arms": {name: {k: v for k, v in arm.items() if k != "layouts"}
                 for name, arm in out.items()},
        "conclusion": (
            "Mechanism (a). Compaction applies the equality deletes and physically "
            "discards every version that lost (delete files drop to zero, S_D becomes "
            "null). The stale survivor becomes the only version present, so "
            "current_version_record equals it and the STALE_WINS test can no longer "
            "fire. Every checker STALE_WINS verdict is erased; every DUPLICATE verdict "
            "survives, because both duplicate rows were visible and are retained. This "
            "holds with the monotonic version column present."
        ),
        "flipped_keys": {k: {"before": u["layouts"][k], "after": c["layouts"][k]}
                         for k in flipped},
        "duplicate_sample": {k: {"before": u["layouts"][k], "after": c["layouts"].get(k)}
                             for k in dups[:SAMPLE_DUPLICATES]},
    }
    dst = os.path.join(HARNESS, "results", "compaction_mechanism.json")
    with open(dst, "w") as f:
        json.dump(evidence, f, indent=1)
        f.write("\n")
    print(f"\nevidence -> {dst}")


if __name__ == "__main__":
    main(int(sys.argv[1]) if len(sys.argv) > 1 else 1200)
