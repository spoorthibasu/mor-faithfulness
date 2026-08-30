"""Cost study v2, storage counterpart: apples-to-apples recovery.

Adds a fourth enforcement arm, unsafe_compact = the UNSAFE layout followed by the SAME
compaction pass safe_compact uses (byte-identical op; only the pre-compaction layout differs).
This lets the "recoverable" claim be priced like-to-like: safe_compact vs unsafe_compact, not
safe_compact vs uncompacted-unsafe.

The four cells per (format, scale) are derived from run_cost.build() via dataclasses.replace, so
they are byte-identical to the v1/v2 study (same realistic operating point, seed 101,
commit_coarsening=6). Storage at rest is deterministic for fixed input, so this runs N=2 per cell
purely as a byte-stability check (no warmup: warmup was a throughput concern).

Scope: storage-comparison only. The four gate predicates (batching + 3 drivers) were widened to
recognize unsafe_compact; nothing else changed. Requirement-A/B stays live per run.

Usage: python studies/run_cost_storage.py <base_keys> <sf_label> [N_runs]
       python studies/run_cost_storage.py 1200 1
       python studies/run_cost_storage.py 4000 10
"""

import dataclasses
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
sys.path.insert(0, os.path.dirname(__file__))

from mor_harness import runner
from mor_harness.check import CheckerOracleDisagreement
from run_cost import build, WAREHOUSE, HARNESS  # identical base params to v1/v2

N_RUNS_DEFAULT = 2


def build_storage(base_keys):
    """The v1/v2 3 arms + a 4th unsafe_compact per format, derived by replace so the base
    params cannot drift from run_cost.build()."""
    base = build(base_keys)  # 3 formats x {unsafe, safe, safe_compact}
    extra = [dataclasses.replace(c, enforcement_mode="unsafe_compact")
             for c in base if c.enforcement_mode == "unsafe"]
    return base + extra


def _existing(jsonl_path, scale):
    counts = {}
    if os.path.exists(jsonl_path):
        with open(jsonl_path) as f:
            for line in f:
                try:
                    r = json.loads(line)
                except Exception:
                    continue
                if str(r.get("scale_label")) != str(scale):
                    continue
                if r.get("status") == "ok":
                    h = (r.get("config") or {}).get("config_hash")
                    counts[h] = counts.get(h, 0) + 1
    return counts


def main(base_keys, sf, n_runs):
    out = os.path.join(HARNESS, "results", f"cost_storage_sf{sf}")
    jsonl_path = out + ".jsonl"
    os.makedirs(os.path.dirname(jsonl_path), exist_ok=True)
    cfgs = build_storage(base_keys)
    done = _existing(jsonl_path, sf)

    print(f"cost-storage SF{sf}: {len(cfgs)} cells x {n_runs} runs (4 arms x 3 formats), "
          f"base_keys={base_keys}", flush=True)

    for cfg in cfgs:
        h = cfg.config_hash()
        for i in range(done.get(h, 0) + 1, n_runs + 1):
            try:
                rec = runner.run(cfg, warehouse=WAREHOUSE)
            except CheckerOracleDisagreement as e:
                rec = {"config": cfg.to_dict(), "status": "failed",
                       "error": f"checker/oracle disagreement: {e}"}
            except Exception as e:  # noqa: BLE001
                rec = {"config": cfg.to_dict(), "status": "failed", "error": repr(e)}
            rec["rep"], rec["scale_label"] = i, sf
            with open(jsonl_path, "a") as f:
                f.write(json.dumps(rec) + "\n")
            co = rec.get("cost") or {}
            k = rec.get("correctness") or {}
            print(f"  SF{sf} {cfg.format:8} {cfg.enforcement_mode:14} run {i}/{n_runs} "
                  f"bytes={co.get('bytes_total')} (d={co.get('bytes_data')}/del={co.get('bytes_delete')}) "
                  f"viol={k.get('violation_rate')} status={rec['status']}", flush=True)

    print(f"DONE: {jsonl_path}", flush=True)


if __name__ == "__main__":
    base_keys = int(sys.argv[1]) if len(sys.argv) > 1 else 1200
    sf = sys.argv[2] if len(sys.argv) > 2 else "1"
    n = int(sys.argv[3]) if len(sys.argv) > 3 else N_RUNS_DEFAULT
    main(base_keys, sf, n)
