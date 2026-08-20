"""Analyze cost-variance sweeps (throughput statistical rigor).

Per (format, scale) cell, over the N measured (non-warmup, ok) repeats of each enforcement
mode, compute: N, mean ev/s, sd, 95% CI (1.96 headline per spec + Student-t robustness), the
safe-vs-unsafe CI-OVERLAP verdict, and a TOST equivalence test (two one-sided t-tests) against
a declared +/-10%-of-unsafe-mean margin at alpha=0.05. Also writes the auditable raw per-repeat
CSV and tallies checker_oracle mismatches across every run. Stdlib only (no numpy / scipy):
the Student-t CDF is the regularized incomplete beta (Numerical Recipes betai).

Usage:
  python studies/analyze_cost_variance.py results/cost_variance_sf1.jsonl results/cost_variance_sf10.jsonl
"""

import csv
import json
import math
import os
import statistics
import sys

MARGIN = 0.10   # +/-10% of the unsafe mean: declared negligible-difference threshold (see report)
ALPHA = 0.05
Z95 = 1.96
# two-sided t_0.975 by sample size N (df = N-1), for the robustness CI column.
T975 = {2: 12.706, 3: 4.303, 4: 3.182, 5: 2.776, 6: 2.571, 7: 2.447, 8: 2.365,
        9: 2.306, 10: 2.262, 11: 2.228, 12: 2.201, 15: 2.145, 20: 2.093}
MECHANISM = {
    "iceberg": "per-snapshot ascending-seq (fine commits) vs coarse-commit default",
    "hudi": "LSN precombine vs ts_ms precombine",
    "delta": "LSN-ordered apply vs out-of-order commit order",
}
MODES = ["unsafe", "safe", "safe_compact"]
FORMATS = ["iceberg", "hudi", "delta"]
RAW_CSV = "results/cost_variance_raw.csv"
RAW_FIELDS = ["scale", "format", "enforcement_mode", "config_hash", "repeat", "warmup",
              "events", "apply_time_s", "readback_time_s", "events_per_s",
              "checker_oracle_mismatch", "status"]


# ---------------------------------------------------------------------------
# Student-t CDF via the regularized incomplete beta function (Numerical Recipes).
# ---------------------------------------------------------------------------
def _betacf(a, b, x):
    MAXIT, EPS, FPMIN = 300, 3.0e-14, 1.0e-300
    qab, qap, qam = a + b, a + 1.0, a - 1.0
    c = 1.0
    d = 1.0 - qab * x / qap
    if abs(d) < FPMIN:
        d = FPMIN
    d = 1.0 / d
    h = d
    for m in range(1, MAXIT + 1):
        m2 = 2 * m
        aa = m * (b - m) * x / ((qam + m2) * (a + m2))
        d = 1.0 + aa * d
        if abs(d) < FPMIN:
            d = FPMIN
        c = 1.0 + aa / c
        if abs(c) < FPMIN:
            c = FPMIN
        d = 1.0 / d
        h *= d * c
        aa = -(a + m) * (qab + m) * x / ((a + m2) * (qap + m2))
        d = 1.0 + aa * d
        if abs(d) < FPMIN:
            d = FPMIN
        c = 1.0 + aa / c
        if abs(c) < FPMIN:
            c = FPMIN
        d = 1.0 / d
        de = d * c
        h *= de
        if abs(de - 1.0) < EPS:
            break
    return h


def _betai(a, b, x):
    if x <= 0.0:
        return 0.0
    if x >= 1.0:
        return 1.0
    lbeta = math.lgamma(a + b) - math.lgamma(a) - math.lgamma(b)
    bt = math.exp(lbeta + a * math.log(x) + b * math.log(1.0 - x))
    if x < (a + 1.0) / (a + b + 2.0):
        return bt * _betacf(a, b, x) / a
    return 1.0 - bt * _betacf(b, a, 1.0 - x) / b


def t_sf(t, df):
    """P(T > t) for Student-t with df degrees of freedom."""
    if df <= 0:
        return float("nan")
    x = df / (df + t * t)
    tail = 0.5 * _betai(df / 2.0, 0.5, x)   # = 0.5 * P(|T| >= |t|)
    return tail if t > 0 else 1.0 - tail


def t_cdf(t, df):
    return 1.0 - t_sf(t, df)


# ---------------------------------------------------------------------------
def ci(vals, mult):
    n = len(vals)
    m = statistics.mean(vals)
    sd = statistics.stdev(vals) if n > 1 else 0.0
    half = mult * sd / math.sqrt(n) if n > 0 else 0.0
    return m, sd, m - half, m + half


def overlaps(lo1, hi1, lo2, hi2):
    return not (hi1 < lo2 or hi2 < lo1)


def tost(safe, unsafe, margin_frac=MARGIN, alpha=ALPHA):
    """Two one-sided tests for equivalence of safe vs unsafe mean ev/s within +/- margin.
    Margin is margin_frac * mean(unsafe). Welch (unequal-variance) t. Equivalent iff BOTH
    one-sided nulls are rejected (p1<alpha AND p2<alpha)."""
    ns, nu = len(safe), len(unsafe)
    ms, mu = statistics.mean(safe), statistics.mean(unsafe)
    vs = statistics.variance(safe) if ns > 1 else 0.0
    vu = statistics.variance(unsafe) if nu > 1 else 0.0
    delta = margin_frac * mu
    diff = ms - mu
    se = math.sqrt(vs / ns + vu / nu)
    if se == 0.0:
        equiv = abs(diff) < delta
        return dict(diff=diff, delta=delta, se=0.0, df=float("inf"),
                    p1=(0.0 if diff > -delta else 1.0), p2=(0.0 if diff < delta else 1.0),
                    p_tost=(0.0 if equiv else 1.0), equivalent=equiv)
    df = (vs / ns + vu / nu) ** 2 / ((vs / ns) ** 2 / (ns - 1) + (vu / nu) ** 2 / (nu - 1))
    t1 = (diff + delta) / se     # H01: diff <= -delta ; reject if t1 large +ve
    p1 = t_sf(t1, df)
    t2 = (diff - delta) / se     # H02: diff >= +delta ; reject if t2 large -ve
    p2 = t_cdf(t2, df)
    return dict(diff=diff, delta=delta, se=se, df=df, t1=t1, t2=t2,
                p1=p1, p2=p2, p_tost=max(p1, p2), equivalent=(p1 < alpha and p2 < alpha))


# ---------------------------------------------------------------------------
def load(paths):
    rows = []
    for p in paths:
        if not os.path.exists(p):
            continue
        with open(p) as f:
            for line in f:
                try:
                    rows.append(json.loads(line))
                except Exception:
                    pass
    return rows


def raw_row(r):
    c = r.get("config") or {}
    co = r.get("cost") or {}
    k = r.get("correctness") or {}
    return {
        "scale": r.get("scale_label"), "format": c.get("format"),
        "enforcement_mode": c.get("enforcement_mode"), "config_hash": c.get("config_hash"),
        "repeat": r.get("repeat"), "warmup": r.get("warmup"),
        "events": co.get("events"), "apply_time_s": co.get("apply_time_s"),
        "readback_time_s": co.get("readback_time_s"), "events_per_s": co.get("events_per_s"),
        "checker_oracle_mismatch": k.get("checker_oracle_mismatch"), "status": r.get("status"),
    }


def main(paths):
    rows = load(paths)

    # write the auditable raw per-repeat CSV (every run, incl warmup + failed).
    os.makedirs(os.path.dirname(RAW_CSV) or ".", exist_ok=True)
    with open(RAW_CSV, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=RAW_FIELDS)
        w.writeheader()
        for r in rows:
            w.writerow(raw_row(r))

    # measured ev/s samples per (scale, format, mode).
    samples = {}
    for r in rows:
        if r.get("warmup") or r.get("status") != "ok":
            continue
        c = r.get("config") or {}
        evs = (r.get("cost") or {}).get("events_per_s")
        if evs is None:
            continue
        key = (str(r.get("scale_label")), c.get("format"), c.get("enforcement_mode"))
        samples.setdefault(key, []).append(float(evs))

    scales = sorted({str(r.get("scale_label")) for r in rows if r.get("scale_label") is not None},
                    key=lambda s: (len(s), s))

    print("=" * 112)
    print(f"COST-VARIANCE (v2)  throughput repeated-measures   files={', '.join(os.path.basename(p) for p in paths)}")
    print(f"  N measured repeats/cell, fresh JVM each; warmup discarded. Equivalence margin "
          f"= +/-{int(MARGIN*100)}% of unsafe mean, alpha={ALPHA}")
    print("=" * 112)

    for sf in scales:
        print(f"\n############################  SF{sf}  ############################")
        for fmt in FORMATS:
            print(f"\n### {fmt.upper()}   priced fix: {MECHANISM[fmt]}")
            print(f"  {'mode':13} {'N':>2} {'mean ev/s':>10} {'sd':>7} "
                  f"{'95% CI (1.96)':>22} {'t-CI (N-1)':>22}")
            cells = {}
            for mode in MODES:
                vals = samples.get((sf, fmt, mode), [])
                if not vals:
                    print(f"  {mode:13} (no data)")
                    continue
                n = len(vals)
                tm = T975.get(n, Z95)
                m, sd, z_lo, z_hi = ci(vals, Z95)
                _, _, t_lo, t_hi = ci(vals, tm)
                cells[mode] = dict(n=n, vals=vals, mean=m, sd=sd,
                                   z=(z_lo, z_hi), t=(t_lo, t_hi))
                print(f"  {mode:13} {n:>2} {m:>10.1f} {sd:>7.1f} "
                      f"[{z_lo:>8.1f},{z_hi:>8.1f}] [{t_lo:>8.1f},{t_hi:>8.1f}]")

            u, s = cells.get("unsafe"), cells.get("safe")
            if not (u and s):
                continue
            # (2) CI-overlap verdict (1.96 headline + t robustness flag)
            ov_z = overlaps(s["z"][0], s["z"][1], u["z"][0], u["z"][1])
            ov_t = overlaps(s["t"][0], s["t"][1], u["t"][0], u["t"][1])
            gap = (s["mean"] - u["mean"]) / u["mean"] * 100.0
            if ov_z:
                ov_word = "OVERLAP -> no measurable throughput cost"
            elif s["mean"] < u["mean"]:
                ov_word = f"NO overlap -> measurable COST: safe {abs(gap):.0f}% slower"
            else:
                ov_word = f"NO overlap -> safe {gap:.0f}% FASTER (not a cost)"
            flag = "" if ov_z == ov_t else "  [!! 1.96 and t-CI give DIFFERENT overlap verdicts]"
            print(f"  --> safe-vs-unsafe overlap (1.96 CI): {ov_word}{flag}")

            # (3) TOST equivalence within +/-10% of unsafe mean
            tt = tost(s["vals"], u["vals"])
            verdict = ("EQUIVALENT within +/-10%" if tt["equivalent"]
                       else "NOT established at +/-10% (N too small / true diff near margin)")
            print(f"  --> TOST(+/-{int(MARGIN*100)}%, a={ALPHA}): {verdict}  "
                  f"[margin=+/-{tt['delta']:.1f} ev/s, diff={tt['diff']:+.1f}, "
                  f"p1={tt['p1']:.3f}, p2={tt['p2']:.3f}, p_TOST={tt['p_tost']:.3f}]")

    # mismatch tally + run accounting (requirement-A/B backbone).
    total = len(rows)
    ok = sum(1 for r in rows if r.get("status") == "ok")
    failed = total - ok
    mism = sum(1 for r in rows if (r.get("correctness") or {}).get("checker_oracle_mismatch"))
    warm = sum(1 for r in rows if r.get("warmup"))
    print(f"\nruns: {total} total ({warm} warmup + {total - warm} measured/attempted), "
          f"{ok} ok, {failed} failed;  checker_oracle_mismatch: {mism}")
    print(f"raw per-repeat CSV written: {RAW_CSV}")


if __name__ == "__main__":
    args = sys.argv[1:] or ["results/cost_variance_sf1.jsonl", "results/cost_variance_sf10.jsonl"]
    main(args)
