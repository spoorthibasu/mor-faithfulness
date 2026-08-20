"""Auditable compaction-corollary check for the storage study.

For Iceberg at each scale, runs the unsafe arm and the unsafe_compact arm (same seed 101
workload) and compares, per key, the materialized current-view content. The compaction
corollary says a physical rewrite preserves visible content, so a still-violating table stays
violating. If keys_changed == 0, unsafe_compact is byte-for-byte the same materialized content
as unsafe (the violation is preserved), which is why the checker_masked_by_compaction flag on
those runs is a checker-model artifact, not a content change. Emits results/cost_storage_corollary.json.

Usage: python studies/check_compaction_corollary.py
"""

import dataclasses
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
sys.path.insert(0, os.path.dirname(__file__))

from mor_harness import check, imperfections, tpcds
from mor_harness.adapters import make_adapter
from mor_harness.batching import build_write_plan
from mor_harness.rng import SeededRng
from mor_harness.stream import synthesize
from run_cost import build, HARNESS

WH = os.path.join(HARNESS, "results", "_corollary_wh")


def materialized_by_key(cfg):
    seeded = SeededRng(cfg.seed)
    base = tpcds.base_customer(cfg, os.path.join(WH, "_io"))
    stream = synthesize(base, cfg, seeded)
    imperfections.apply(stream, cfg, seeded)
    plan = build_write_plan(stream, cfg, seeded)
    adapter = make_adapter(cfg.format)
    name = f"cor_{cfg.enforcement_mode}"
    res = adapter.apply(plan, name, os.path.join(WH, "db", name), WH,
                        cfg.precombine_field(), os.path.join(WH, "_io", name))
    kcols, vcol = stream.key_columns, stream.version_column
    oracle = check.oracle_verdicts(res.materialized, stream.truth, kcols, vcol)
    mat = {}
    for row in res.materialized:
        mat.setdefault(tuple(row[c] for c in kcols), []).append(row.get(vcol))
    return {k: sorted(v) for k, v in mat.items()}, oracle


def main():
    out = {"margin_note": "keys_changed==0 => materialized content preserved (corollary holds)",
           "scales": {}}
    for base_keys, sf in [(1200, "1"), (4000, "10")]:
        ice = next(c for c in build(base_keys)
                   if c.format == "iceberg" and c.enforcement_mode == "unsafe")
        ice = dataclasses.replace(ice, keep_tables=False)
        uc = dataclasses.replace(ice, enforcement_mode="unsafe_compact")
        mat_u, or_u = materialized_by_key(ice)
        mat_c, or_c = materialized_by_key(uc)
        allk = set(mat_u) | set(mat_c)
        changed = [list(k) for k in allk if mat_u.get(k, []) != mat_c.get(k, [])]
        # keys the oracle still calls a violation in the compacted arm
        viol_c = sum(1 for v in or_c.values() if v in check.VIOLATIONS)
        out["scales"][f"sf{sf}"] = {
            "base_keys": base_keys, "n_keys": len(allk),
            "keys_changed_unsafe_vs_unsafe_compact": len(changed),
            "sample_changed": changed[:10],
            "oracle_violations_unsafe": sum(1 for v in or_u.values() if v in check.VIOLATIONS),
            "oracle_violations_unsafe_compact": viol_c,
            "corollary_holds": len(changed) == 0,
        }
        print(f"SF{sf}: keys_changed={len(changed)}  oracle_viol unsafe="
              f"{out['scales'][f'sf{sf}']['oracle_violations_unsafe']} "
              f"unsafe_compact={viol_c}  corollary_holds={len(changed) == 0}", flush=True)

    path = os.path.join(HARNESS, "results", "cost_storage_corollary.json")
    with open(path, "w") as f:
        json.dump(out, f, indent=2)
    print("wrote", path)


if __name__ == "__main__":
    main()
