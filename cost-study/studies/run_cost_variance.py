"""Cost study v2: throughput-variance hardening (statistical rigor, not a new study).

Same 9 cost cells as run_cost.py (identical build(), identical seed-101 workload), but each
cell is run N_MEASURED times plus 1 LEADING WARMUP, each in a FRESH subprocess / fresh JVM.
The fresh JVM is automatic: runner.run -> adapters.base.run_driver -> subprocess.run spawns
a new process that builds a new SparkSession and tears it down on exit, so one repeat == one
runner.run == one cold JVM. We deliberately do NOT use sweep.run_sweep: its config_hash dedup
would collapse identical repeats into one. Instead we loop runner.run directly and resume by
counting the ok measured rows already present per (config_hash, scale).

Scope: throughput-variance ONLY. Storage / violation-rate / adapters / operating point /
checker are untouched. Requirement-A/B stays live: CheckerOracleDisagreement is caught per
repeat (status=failed) so one bad repeat cannot abort the multi-hour run, and the mismatch
tally is preserved for the analyzer.

Writes ONE JSONL line per repeat (the per-repeat source of truth, resumable). The auditable
raw per-repeat CSV and all statistics are produced downstream by analyze_cost_variance.py.

Usage: python studies/run_cost_variance.py <base_keys> <sf_label> [N_measured]
       python studies/run_cost_variance.py 1200 1
       python studies/run_cost_variance.py 4000 10
"""

import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
sys.path.insert(0, os.path.dirname(__file__))

from mor_harness import runner
from mor_harness.check import CheckerOracleDisagreement
from run_cost import build, WAREHOUSE, HARNESS  # reuse the EXACT v1 cost cells

N_MEASURED_DEFAULT = 10


def _existing(jsonl_path, scale):
    """Per config_hash for this scale: count of ok measured (non-warmup) rows, and whether
    a warmup row already exists. Drives resume so a killed run continues, not restarts."""
    counts, warm = {}, {}
    if os.path.exists(jsonl_path):
        with open(jsonl_path) as f:
            for line in f:
                try:
                    r = json.loads(line)
                except Exception:
                    continue
                if str(r.get("scale_label")) != str(scale):
                    continue
                h = (r.get("config") or {}).get("config_hash")
                if r.get("warmup"):
                    warm[h] = True
                elif r.get("status") == "ok":
                    counts[h] = counts.get(h, 0) + 1
    return counts, warm


def main(base_keys, sf, n_measured):
    out = os.path.join(HARNESS, "results", f"cost_variance_sf{sf}")
    jsonl_path = out + ".jsonl"
    os.makedirs(os.path.dirname(jsonl_path), exist_ok=True)
    cfgs = build(base_keys)
    done, warm = _existing(jsonl_path, sf)

    print(f"cost-variance SF{sf}: {len(cfgs)} cells x (1 warmup + {n_measured} measured), "
          f"base_keys={base_keys}", flush=True)

    for cfg in cfgs:
        h = cfg.config_hash()
        have = done.get(h, 0)
        # repeat index 0 = warmup (excluded from stats); 1..n_measured = measured sample.
        plan = ([0] if not warm.get(h) else []) + list(range(have + 1, n_measured + 1))
        for i in plan:
            is_warm = (i == 0)
            try:
                rec = runner.run(cfg, warehouse=WAREHOUSE)
            except CheckerOracleDisagreement as e:
                rec = {"config": cfg.to_dict(), "status": "failed",
                       "error": f"checker/oracle disagreement: {e}"}
            except Exception as e:  # noqa: BLE001
                rec = {"config": cfg.to_dict(), "status": "failed", "error": repr(e)}
            rec["repeat"], rec["warmup"], rec["scale_label"] = i, is_warm, sf
            with open(jsonl_path, "a") as f:
                f.write(json.dumps(rec) + "\n")
            evs = (rec.get("cost") or {}).get("events_per_s")
            tag = "warmup" if is_warm else f"rep {i}/{n_measured}"
            print(f"  SF{sf} {cfg.format:8} {cfg.enforcement_mode:13} {tag:12} "
                  f"ev/s={evs} status={rec['status']}", flush=True)

    print(f"DONE: {jsonl_path}", flush=True)


if __name__ == "__main__":
    base_keys = int(sys.argv[1]) if len(sys.argv) > 1 else 1200
    sf = sys.argv[2] if len(sys.argv) > 2 else "1"
    n = int(sys.argv[3]) if len(sys.argv) > 3 else N_MEASURED_DEFAULT
    main(base_keys, sf, n)
