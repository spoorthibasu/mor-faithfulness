"""Analyze the sensitivity sweep JSONL: OFAT trend per knob per format + breakdown."""
import json
import os
import sys

RES = os.path.join(os.path.dirname(__file__), "results", "sensitivity.jsonl")
KNOBS = ["clock_skew_ms", "ooo_rate", "dup_rate", "schema_change_freq"]


def load():
    return [json.loads(l) for l in open(RES)]


def knobs_of(c):
    return {k: c[k] for k in KNOBS}


def is_pure(c, knob):
    return all(c[k] == 0 for k in KNOBS if k != knob)


def fmt_row(r):
    c = r["config"]
    if r["status"] != "ok":
        return f"  {'FAILED':>10}  {r.get('error','')[:70]}"
    cr = r["correctness"]
    return (f"  viol={cr['violation_rate']:.3f}  dup={cr['n_duplicate']:>4} stale={cr['n_stale_wins']:>4} "
            f"miss={cr['n_missing_current']:>3} ghost={cr['n_ghost']:>3} blind={cr.get('n_delete_tail_blind',0):>3} "
            f"(match={cr['n_match']}/{cr['n_keys']})")


def main():
    rows = load()
    by = {}
    for r in rows:
        c = r["config"]
        by[(c["format"], tuple(c[k] for k in KNOBS))] = r
    formats = ["iceberg", "hudi", "delta"]

    print("=" * 100)
    print("SENSITIVITY STUDY — OFAT trends (enforcement=unsafe, SF1, base_keys=1200, seed=101)")
    print("=" * 100)

    labels = {"clock_skew_ms": "CLOCK SKEW (ms)", "ooo_rate": "OUT-OF-ORDER rate",
              "dup_rate": "DUPLICATE rate", "schema_change_freq": "SCHEMA-CHANGE freq"}
    for knob in KNOBS:
        print(f"\n### {labels[knob]}  (other knobs = 0)")
        for fmt in formats:
            pts = sorted({r["config"][knob] for r in rows
                          if r["config"]["format"] == fmt and is_pure(r["config"], knob)})
            if not pts:
                continue
            print(f" {fmt}:")
            for v in pts:
                match = [r for r in rows if r["config"]["format"] == fmt
                         and is_pure(r["config"], knob) and r["config"][knob] == v]
                if match:
                    print(f"   {knob}={v:<7}{fmt_row(match[0])}")

    print("\n" + "=" * 100)
    print("COMBINED POINTS (multiple knobs nonzero)")
    print("=" * 100)
    for r in rows:
        c = r["config"]
        nz = {k: c[k] for k in KNOBS if c[k]}
        if len(nz) >= 2:
            tag = " ".join(f"{k.split('_')[0]}={v}" for k, v in nz.items())
            print(f" {c['format']:8} {tag:45}{fmt_row(r)}")

    n_ok = sum(1 for r in rows if r["status"] == "ok")
    n_fail = len(rows) - n_ok
    n_mism = sum(1 for r in rows if r.get("correctness", {}).get("checker_oracle_mismatch"))
    print("\n" + "=" * 100)
    print(f"runs: {len(rows)}  ok: {n_ok}  failed: {n_fail}  checker_oracle_mismatch: {n_mism}")
    if n_fail:
        print("FAILED cells:")
        for r in rows:
            if r["status"] != "ok":
                c = r["config"]
                print(f"  {c['format']} {knobs_of(c)} :: {r.get('error','')[:120]}")


if __name__ == "__main__":
    main()
