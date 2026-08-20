"""READ-ONLY extraction for the clock-skew derivation.

Drives the real mor_harness generator (synthesize + imperfections.apply) on a fresh
SeededRng(101) with the exact sec.6 sensitivity config. Writes data files only; does
NOT modify any repo code. All numbers come from the shipped generator.
"""
import csv
import json
import os
import statistics as st
import sys

# The harness lives in the sibling cost-study/ package; override with MOR_HARNESS_SRC.
HARNESS_SRC = os.environ.get(
    "MOR_HARNESS_SRC",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "cost-study", "src"),
)
sys.path.insert(0, HARNESS_SRC)

from mor_harness import tpcds, imperfections            # noqa: E402
from mor_harness.stream import synthesize               # noqa: E402
from mor_harness.config import RunConfig                # noqa: E402
from mor_harness.rng import SeededRng                   # noqa: E402
from mor_harness.model import Op                        # noqa: E402

OUT = os.path.dirname(os.path.abspath(__file__))

# ---- exact sec.6 sensitivity BASE (sensitivity/run_sensitivity.py:33-35) ----
def make_cfg(skew):
    return RunConfig(
        scale_factor=1, base_keys=1200, keys_sampled=1.0, versions_per_key_mean=4,
        op_mix=(0.8, 0.15, 0.05), key_columns=("id",), payload_columns=("val",),
        enforcement_mode="unsafe", ts_step_ms=1, seed=101, clock_skew_ms=skew,
    )

CANON = make_cfg(400.0)   # canonical run for structure + real sigma=400 deltas
COMPETE = {Op.READ, Op.CREATE, Op.UPDATE}


def build_run(cfg):
    """Fresh reproduction of runner stages 1-3 (synthesize + clock skew). Returns the
    stream plus the pre-skew original ts_ms per lsn (ts_step_ms=1 => base+lsn)."""
    seeded = SeededRng(cfg.seed)
    base = tpcds.base_customer(cfg)                       # deterministic ids 1..1200
    stream = synthesize(base, cfg, seeded)               # consumes seeded["stream"] only
    orig_ts = {e.lsn: e.ts_ms for e in stream.events}    # snapshot BEFORE skew
    imperfections.apply(stream, cfg, seeded)             # consumes seeded["skew"]
    return stream, orig_ts


# ---- run the three sigma points (identical stream; only ts_ms perturbation differs) ----
stream, orig_ts = build_run(CANON)
real_delta = {400.0: {e.lsn: e.ts_ms - orig_ts[e.lsn] for e in stream.events}}
for sig in (1500.0, 6000.0):
    s2, o2 = build_run(make_cfg(sig))
    real_delta[sig] = {e.lsn: e.ts_ms - o2[e.lsn] for e in s2.events}

# ---- pure standard-normal draws: a parallel skew child reproduces z_v exactly ----
# _apply_clock_skew calls rng.gauss(0,sigma) once per event in stream.events order; sigma
# only scales the result, so gauss(0,1) on an identical fresh child gives the same z_v.
zc = SeededRng(CANON.seed)["skew"]
z_by_lsn = {e.lsn: zc.gauss(0.0, 1.0) for e in stream.events}


def clamp4(z):
    return max(-4.0, min(4.0, z))


# ---- per-key structure: competitive versions, terminal type, eligibility, gaps ----
by_key = {}
for e in stream.events:
    by_key.setdefault(e.key, []).append(e)

rows = []
for key, evs in by_key.items():
    kid = key[0]
    ops = [e.op for e in evs]
    max_ev = max(evs, key=lambda e: e.lsn)               # true-latest event (max lsn)
    origin = "base" if any(o == Op.READ for o in ops) else "insert"
    if max_ev.op == Op.DELETE:
        terminal = "delete-tail"
    elif max_ev.op == Op.CREATE and any(o == Op.DELETE for o in ops):
        terminal = "reinsert-tail"
    else:
        terminal = "update-tail"
    eligible = terminal != "delete-tail"                 # delete-tail => predicted ABSENT

    comp = sorted(e.lsn for e in evs if e.op in COMPETE)  # candidate versions (READ/CREATE/UPDATE)
    if len(comp) >= 2:
        min_pos_gap = comp[-1] - comp[-2]                # closest competitor to the max
    else:
        min_pos_gap = None
    rows.append({
        "key_id": kid, "origin": origin, "terminal_type": terminal,
        "eligible": int(eligible), "n_competitive": len(comp),
        "competitive_lsns": ";".join(str(x) for x in comp),
        "current_lsn": comp[-1], "min_positive_gap": min_pos_gap,
    })

rows.sort(key=lambda r: r["key_id"])

# ---- write per-key CSV ----
perkey_path = os.path.join(OUT, "seed101_perkey.csv")
with open(perkey_path, "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
    w.writeheader()
    w.writerows(rows)

# ---- skew-draw sample: first 50 eligible keys, per competitive version ----
elig_keys = [r["key_id"] for r in rows if r["eligible"]]
sample_ids = set(elig_keys[:50])
sample_path = os.path.join(OUT, "seed101_skew_sample.csv")
with open(sample_path, "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["key_id", "lsn", "op", "z", "w_clamped", "sigma400_w", "sigma1500_w",
                "sigma6000_w", "real_delta400", "real_delta1500", "real_delta6000",
                "original_ts", "perturbed_ts400"])
    for e in sorted((e for e in stream.events if e.key[0] in sample_ids and e.op in COMPETE),
                    key=lambda e: (e.key[0], e.lsn)):
        z = z_by_lsn[e.lsn]
        w_ = clamp4(z)
        w.writerow([e.key[0], e.lsn, e.op.value, round(z, 6), round(w_, 6),
                    round(400.0 * w_, 3), round(1500.0 * w_, 3), round(6000.0 * w_, 3),
                    real_delta[400.0][e.lsn], real_delta[1500.0][e.lsn],
                    real_delta[6000.0][e.lsn], orig_ts[e.lsn], orig_ts[e.lsn] + real_delta[400.0][e.lsn]])

# ---- verification: does the real harness delta == sigma * w (scaling property)? ----
scaling_dev = {}
for sig in (400.0, 1500.0, 6000.0):
    devs = [abs(real_delta[sig][e.lsn] - sig * clamp4(z_by_lsn[e.lsn])) for e in stream.events]
    scaling_dev[sig] = {"max_abs": round(max(devs), 4), "mean_abs": round(st.mean(devs), 6)}

# ---- gap distribution over ELIGIBLE keys ----
gaps = sorted(r["min_positive_gap"] for r in rows if r["eligible"] and r["min_positive_gap"] is not None)


def q(p):
    if not gaps:
        return None
    i = min(len(gaps) - 1, max(0, int(round(p * (len(gaps) - 1)))))
    return gaps[i]


# histogram buckets
edges = [0, 50, 100, 200, 400, 800, 1600, 3200, 6400, 10**9]
labels = ["0-49", "50-99", "100-199", "200-399", "400-799", "800-1599",
          "1600-3199", "3200-6399", ">=6400"]
hist = {lab: 0 for lab in labels}
for g in gaps:
    for i in range(len(edges) - 1):
        if edges[i] <= g < edges[i + 1]:
            hist[labels[i]] += 1
            break

term_counts = {}
orig_counts = {}
for r in rows:
    term_counts[r["terminal_type"]] = term_counts.get(r["terminal_type"], 0) + 1
    orig_counts[r["origin"]] = orig_counts.get(r["origin"], 0) + 1

summary = {
    "config": {"seed": 101, "base_keys": 1200, "versions_per_key_mean": 4,
               "op_mix": [0.8, 0.15, 0.05], "ts_step_ms": 1, "insert_rate": 0.05,
               "base_ts_ms": CANON.base_ts_ms},
    "n_keys_total": len(rows),
    "origin_counts": orig_counts,
    "terminal_counts": term_counts,
    "eligible_count": sum(r["eligible"] for r in rows),
    "eligible_fraction": round(sum(r["eligible"] for r in rows) / len(rows), 4),
    "n_competitive": {
        "min": min(r["n_competitive"] for r in rows),
        "mean": round(st.mean(r["n_competitive"] for r in rows), 3),
        "max": max(r["n_competitive"] for r in rows),
    },
    "min_positive_gap_over_eligible": {
        "n": len(gaps), "min": gaps[0], "max": gaps[-1],
        "mean": round(st.mean(gaps), 2), "median": st.median(gaps),
        "p05": q(0.05), "p10": q(0.10), "p25": q(0.25), "p50": q(0.50),
        "p75": q(0.75), "p90": q(0.90), "p95": q(0.95),
        "histogram": hist,
    },
    "scaling_property_check": {
        "note": "real harness ts-delta vs sigma*clamp(z,+-4); deviation is pure int() rounding",
        "max_abs_deviation_ms": scaling_dev,
    },
    "files": {"per_key_csv": perkey_path, "skew_sample_csv": sample_path},
}
summary_path = os.path.join(OUT, "seed101_summary.json")
with open(summary_path, "w") as f:
    json.dump(summary, f, indent=2)

print(json.dumps(summary, indent=2))
print("\nWROTE:", perkey_path)
print("WROTE:", sample_path)
print("WROTE:", summary_path)
