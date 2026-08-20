"""Analyze an enforcement-cost sweep: per format & enforcement_mode, throughput / latency
/ storage, absolute AND as overhead vs the unsafe default. Prints the headline per format.

Usage: python studies/analyze_cost.py results/cost_sf1.jsonl
"""
import json
import os
import sys

MECHANISM = {
    "iceberg": "per-snapshot ascending-seq (fine commits)  vs  coarse-commit default",
    "hudi": "LSN precombine  vs  ts_ms precombine",
    "delta": "LSN-ordered apply  vs  out-of-order commit order",
}
MODES = ["unsafe", "safe", "safe_compact"]


def main(path):
    rows = [json.loads(l) for l in open(path)]
    by = {}
    for r in rows:
        if r["status"] != "ok":
            print("FAILED:", r["config"]["format"], r["config"]["enforcement_mode"], r.get("error", "")[:100])
            continue
        by[(r["config"]["format"], r["config"]["enforcement_mode"])] = r

    def cell(fmt, mode):
        r = by.get((fmt, mode))
        if not r:
            return None
        return {**r["cost"], "viol": r["correctness"]["violation_rate"]}

    print("=" * 108)
    print(f"ENFORCEMENT-COST STUDY  ({os.path.basename(path)})   knobs = realistic operating point")
    print("  storage-engine enforcement cost (Flink runtime excluded; py4j direct writer)")
    print("=" * 108)

    for fmt in ("iceberg", "hudi", "delta"):
        print(f"\n### {fmt.upper()}   priced fix: {MECHANISM[fmt]}")
        print(f"  {'mode':13} {'viol':>6} {'ev/s':>8} {'apply_s':>8} {'read_s':>7} {'cmpct_s':>8} "
              f"{'commits':>7} {'files(d/del)':>12} {'bytes':>9} {'rss_MB':>7}")
        u = cell(fmt, "unsafe")
        for mode in MODES:
            c = cell(fmt, mode)
            if not c:
                print(f"  {mode:13} (missing)")
                continue
            files = f"{c['data_files']}/{c['delete_files']}"
            print(f"  {mode:13} {c['viol']:6.3f} {str(c['events_per_s']):>8} {c['apply_time_s']:8.1f} "
                  f"{c['readback_time_s']:7.1f} {c.get('compact_time_s',0):8.1f} {c['commit_count']:7} "
                  f"{files:>12} {c['bytes_total']:9} {c.get('peak_rss_mb',0):7.0f}")
        # headline vs unsafe. Signed change of SAFE relative to UNSAFE:
        #   dthr > 0  => safe is FASTER (throughput gain, negative cost)
        #   dsto > 0  => safe writes MORE bytes (storage cost)
        s = cell(fmt, "safe")
        if u and s and u["events_per_s"] and u["bytes_total"]:
            dthr = (s["events_per_s"] - u["events_per_s"]) / u["events_per_s"] * 100
            dsto = (s["bytes_total"] - u["bytes_total"]) / u["bytes_total"] * 100
            dcmt = (s["commit_count"] - u["commit_count"]) / max(u["commit_count"], 1) * 100
            tw = "faster" if dthr >= 0 else "SLOWER"
            print(f"  --> SAFE vs UNSAFE: throughput {dthr:+.0f}% ({tw}, {u['events_per_s']}->{s['events_per_s']} ev/s), "
                  f"storage {dsto:+.0f}% ({u['bytes_total']}->{s['bytes_total']} B), commits {dcmt:+.0f}%")
            thr_cost = max(0.0, -dthr)
            print(f"      HEADLINE: enforcing faithfulness costs {thr_cost:.0f}% throughput and "
                  f"{dsto:+.0f}% storage vs unsafe  (viol {u['viol']:.3f} -> {s['viol']:.3f})")
        sc = cell(fmt, "safe_compact")
        if u and sc and sc["bytes_total"] and u["bytes_total"]:
            dsto = (sc["bytes_total"] - u["bytes_total"]) / u["bytes_total"] * 100
            print(f"  --> SAFE_COMPACT vs UNSAFE: storage {dsto:+.0f}% "
                  f"({u['bytes_total']}->{sc['bytes_total']} B), readback {sc['readback_time_s']:.1f}s "
                  f"(vs {u['readback_time_s']:.1f}s), +{sc.get('compact_time_s',0):.1f}s compaction")

    n_mism = sum(1 for r in rows if r.get("correctness", {}).get("checker_oracle_mismatch"))
    print(f"\nruns: {len(rows)}  checker_oracle_mismatch: {n_mism}")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "results/cost_sf1.jsonl")
