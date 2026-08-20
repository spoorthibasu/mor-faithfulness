"""READ-ONLY verification export for the ooo/dup derivation (seed=101, §6 config).

Emits, in this directory:
  ooo_perkey.csv          per key x {ooo_rate 0.25, 0.50}: competitive seqs, which adjacent
                          transpositions fired, each firing classified as descent (flipped the
                          argmax -> visible) or invisible-ascent (permuted losers only), violated.
  dup_perkey.csv          per key x {dup_rate 0.05,0.15,0.30}: duplicate injected? caused a
                          violation? (only a dup on the current/max-seq version is visible).
  composition_check.json  combined operating point skew=1500 + ooo=0.10: measured per-format
                          violation rate vs the product-law prediction 1-Prod(1-P_mech), by
                          mechanism component.

Imports mor_harness and replays batching._perturb exactly; mutates nothing. The ooo/dup
per-key verdicts are validated to equal the engine-measured aggregates (asserted below).

Definitions used in ooo_perkey.csv (documented so the paper's "1/2 factor" can be checked):
  * A key's m competitive change events (c/u/d), in lsn order, get clean seqs 1..m
    (version index j -> seq j+1); the current (max-lsn) version j=m-1 gets the top seq m.
  * ooo fires an INDEPENDENT Bernoulli(ooo_rate) adjacent transposition at each pair
    (j-1,j), left-to-right, in place (batching.py:63-65).
  * "argmax" = the version holding the max seq m == the materialized current row
    (Iceberg equality-delete max-seq-survivor == Delta last-commit-wins; identical).
  * Per firing, flipped_argmax = did THIS swap move value m to the other version.
      - descent  : flipped_argmax=True  (moved the higher-lsn/top version below -> violation)
      - invisible: flipped_argmax=False (ascent/descent among the losers, no effect on current)
    The "fraction of fired transpositions that flipped the argmax" = (sum descents)/(sum fired).
"""
import csv
import json
import os
import sys
from collections import Counter

# The harness src is the sibling cost-study/ package; override with MOR_HARNESS_SRC.
HARNESS_SRC = os.environ.get(
    "MOR_HARNESS_SRC",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "..", "cost-study", "src"),
)
sys.path.insert(0, HARNESS_SRC)

from mor_harness import tpcds, batching
from mor_harness.config import RunConfig
from mor_harness.rng import SeededRng
from mor_harness.stream import synthesize
from mor_harness.model import Op

OUT = os.path.dirname(os.path.abspath(__file__))

BASE = dict(scale_factor=1, base_keys=1200, keys_sampled=1.0, versions_per_key_mean=4,
            op_mix=(0.8, 0.15, 0.05), key_columns=("id",), payload_columns=("val",),
            enforcement_mode="unsafe", ts_step_ms=1, seed=101)

def base_cfg(**kw):
    return RunConfig(**{**BASE, "format": "iceberg", **kw})

# ---- build the stream ONCE (independent of ooo/dup values) ----
cfg0 = base_cfg()
seeded0 = SeededRng(cfg0.seed)
base_rows = tpcds.base_customer(cfg0, os.path.join(OUT, "_io"))
stream = synthesize(base_rows, cfg0, seeded0)
reads, by_key = batching._changes_and_reads(stream)     # exact _perturb iteration order
truth = stream.truth
KEYS = list(by_key.keys())
N_KEYS = len(KEYS)
N_ELIG_OOO = sum(1 for k in KEYS if len(by_key[k]) >= 2)          # reorderable keys (m>=2)
N_ELIG_DUP = sum(1 for k in KEYS
                 if by_key[k][-1].op in (Op.CREATE, Op.UPDATE))    # current version is c/u


# =============================================================== OOO export
def replay_ooo(rate):
    """Replay the ooo loop; track the argmax (holder of seq m) through each swap so every
    firing is labelled descent (flipped argmax) or invisible."""
    rng = SeededRng(101)["ooo"]                          # pristine child, as each run sees it
    rows = []
    for k in KEYS:
        evs = by_key[k]
        m = len(evs)
        ck = [j + 1 for j in range(m)]
        argmax_pos = m - 1 if m else -1                  # position holding value m
        firings = []                                     # (pair_j, uniform, flipped_argmax)
        for j in range(1, m):
            u = rng.random()
            if u < rate:
                ck[j - 1], ck[j] = ck[j], ck[j - 1]
                flipped = argmax_pos in (j - 1, j)
                if flipped:
                    argmax_pos = (j - 1) if argmax_pos == j else j
                firings.append((j, u, flipped))
        w = max(range(m), key=lambda i: ck[i])           # winner = argmax seq
        wop = evs[w].op
        absent = truth.get(k) is None
        if wop == Op.DELETE:
            verdict = "MATCH" if absent else "MISSING_CURRENT"
        else:
            verdict = "GHOST" if absent else (
                "MATCH" if evs[w].lsn == evs[-1].lsn else "STALE_WINS")
        violated = verdict != "MATCH"
        n_descent = sum(1 for _, _, f in firings if f)     # 0 or 1 (only last pair can flip)
        n_invis = sum(1 for _, _, f in firings if not f)
        rows.append(dict(
            ooo_rate=rate, key_id=k[0], m=m,
            tail_op=evs[-1].op.value,
            competitive_lsns=";".join(str(e.lsn) for e in evs),
            competitive_ops="".join(e.op.value for e in evs),
            clean_seqs=";".join(str(j + 1) for j in range(m)),
            final_seqs=";".join(map(str, ck)),
            n_pairs=m - 1,
            n_fired=len(firings),
            fired_pairs=";".join(str(j) for j, _, _ in firings) or "-",
            # per-firing classification: "<pair_j>:<descent|invisible>"
            fired_detail=";".join(f"{j}:{'descent' if f else 'invisible'}"
                                  for j, _, f in firings) or "-",
            n_descent=n_descent, n_invisible=n_invis,
            last_pair_fired=(len(firings) > 0 and firings[-1][0] == m - 1),
            winner_lsn=evs[w].lsn, winner_op=wop.value,
            current_lsn=evs[-1].lsn,
            violated=violated, verdict=verdict,
        ))
    return rows

OOO_EXPECT = {0.25: dict(stale=206, miss=16, ghost=50, viol=272),
              0.50: dict(stale=405, miss=40, ghost=88, viol=533)}

ooo_rows_all = []
ooo_summary = {}
for rate in (0.25, 0.50):
    rows = replay_ooo(rate)
    c = Counter(r["verdict"] for r in rows)
    viol = sum(c[v] for v in ("STALE_WINS", "MISSING_CURRENT", "GHOST"))
    exp = OOO_EXPECT[rate]
    assert (c["STALE_WINS"], c["MISSING_CURRENT"], c["GHOST"], viol) == \
        (exp["stale"], exp["miss"], exp["ghost"], exp["viol"]), (rate, c)
    total_fired = sum(r["n_fired"] for r in rows)
    total_descent = sum(r["n_descent"] for r in rows)          # == viol (flips == violations)
    total_invis = sum(r["n_invisible"] for r in rows)
    ooo_summary[rate] = dict(
        total_keys=N_KEYS, eligible_keys=N_ELIG_OOO,
        measured_violation_rate=round(viol / N_KEYS, 6), n_violations=viol,
        total_transpositions_fired=total_fired,
        fired_that_flipped_argmax=total_descent,
        fired_invisible=total_invis,
        fraction_fired_flipping_argmax=round(total_descent / total_fired, 6) if total_fired else None,
    )
    ooo_rows_all.extend(rows)

with open(os.path.join(OUT, "ooo_perkey.csv"), "w", newline="") as f:
    wr = csv.DictWriter(f, fieldnames=list(ooo_rows_all[0].keys()))
    wr.writeheader(); wr.writerows(ooo_rows_all)


# =============================================================== DUP export
def replay_dup(rate):
    rng = SeededRng(101)["dup"]
    rows = []
    for k in KEYS:
        evs = by_key[k]
        m = len(evs)
        flags = [False] * m
        for j in range(m):
            if evs[j].op in (Op.CREATE, Op.UPDATE):
                if rng.random() < rate:                  # drawn ONLY for c/u versions
                    flags[j] = True
        cur = m - 1
        cur_is_cu = evs[cur].op in (Op.CREATE, Op.UPDATE)
        injected_any = any(flags)
        injected_on_current = cur_is_cu and flags[cur]   # only the max-seq copy is visible
        rows.append(dict(
            dup_rate=rate, key_id=k[0], m=m,
            tail_op=evs[-1].op.value,
            n_cu_versions=sum(1 for e in evs if e.op in (Op.CREATE, Op.UPDATE)),
            current_op=evs[cur].op.value,
            eligible=cur_is_cu,
            dup_injected_any=injected_any,
            n_versions_duplicated=sum(flags),
            dup_on_current=injected_on_current,
            caused_violation=injected_on_current,
            verdict="DUPLICATE" if injected_on_current else "MATCH_OR_OTHER",
        ))
    return rows

DUP_EXPECT = {0.05: 62, 0.15: 179, 0.30: 344}
dup_rows_all = []
dup_summary = {}
for rate in (0.05, 0.15, 0.30):
    rows = replay_dup(rate)
    n_viol = sum(1 for r in rows if r["caused_violation"])
    n_inj = sum(1 for r in rows if r["dup_injected_any"])
    assert n_viol == DUP_EXPECT[rate], (rate, n_viol)
    dup_summary[rate] = dict(
        total_keys=N_KEYS, eligible_keys=N_ELIG_DUP,
        eligible_fraction=round(N_ELIG_DUP / N_KEYS, 6),
        measured_violation_rate=round(n_viol / N_KEYS, 6), n_violations=n_viol,
        keys_with_any_dup_injected=n_inj,
        predicted_0p85_times_p=round((N_ELIG_DUP / N_KEYS) * rate, 6),
    )
    dup_rows_all.extend(rows)

with open(os.path.join(OUT, "dup_perkey.csv"), "w", newline="") as f:
    wr = csv.DictWriter(f, fieldnames=list(dup_rows_all[0].keys()))
    wr.writeheader(); wr.writerows(dup_rows_all)


# =============================================================== composition_check.json
# Combined operating point skew=1500 + ooo=0.10. Per format the two mechanisms are
# orthogonal: iceberg/delta order by lsn (skew-inert) and are hit by ooo; hudi precombines
# on ts_ms (skew-sensitive) and ignores checkpoint order (ooo-inert). Product law:
#   P_total = 1 - (1 - P_skew)(1 - P_ooo)
# Components are the standalone single-mechanism MEASURED rates from the pure sweeps
# (delta@ooo=0.10 was not run standalone; it is reconstructed here and equals iceberg's).
MEAS_COMBINED_N = {"iceberg": 113, "hudi": 391, "delta": 113}   # from results/sensitivity.jsonl
MEAS_COMBINED = {f: round(n / N_KEYS, 6) for f, n in MEAS_COMBINED_N.items()}

# reconstruct P_ooo(0.10) directly (validate iceberg==measured 113)
_ooo010 = replay_ooo(0.10)
_p_ooo010 = sum(1 for r in _ooo010 if r["violated"]) / N_KEYS
assert sum(1 for r in _ooo010 if r["violated"]) == 113, "reconstructed ooo=0.10 != 113"

P_skew1500 = {"iceberg": 0.0, "hudi": round(391 / N_KEYS, 6), "delta": 0.0}   # hudi measured 391/1260; ice/delta lsn-inert (skew=6000 measured 0.000)
P_ooo010 = {"iceberg": round(_p_ooo010, 6), "hudi": 0.0,        # hudi ooo-inert (pure hudi ooo=0.5 measured 0.000)
            "delta": round(_p_ooo010, 6)}

comp = {
    "operating_point": {"clock_skew_ms": 1500, "ooo_rate": 0.10, "dup_rate": 0.0,
                        "schema_change_freq": 0.0},
    "note": "Combined skew+ooo point not used to fit anything; components are standalone "
            "single-mechanism rates. Product law assumes independent mechanisms.",
    "total_keys": N_KEYS,
    "formats": {},
}
for fmt in ("iceberg", "hudi", "delta"):
    ps, po = P_skew1500[fmt], P_ooo010[fmt]
    predicted = 1 - (1 - ps) * (1 - po)
    measured = MEAS_COMBINED[fmt]
    comp["formats"][fmt] = {
        "measured_violation_rate": measured,
        "measured_n_violations": MEAS_COMBINED_N[fmt],
        "components": {
            "clock_skew_1500": {
                "standalone_P_viol": ps,
                "source": ("pure skew=1500 measured (391/1260)" if fmt == "hudi"
                           else "mechanism-inert: orders by lsn; pure skew=6000 measured 0.000"),
            },
            "ooo_0.10": {
                "standalone_P_viol": po,
                "source": ("pure ooo=0.10 measured (113/1260)" if fmt == "iceberg"
                           else "reconstructed = iceberg (Delta identical on ooo)" if fmt == "delta"
                           else "mechanism-inert: precombine ignores checkpoint order; pure hudi ooo=0.5 measured 0.000"),
            },
        },
        "predicted_product_law": round(predicted, 6),
        "abs_error_measured_minus_predicted": round(measured - predicted, 6),
    }

with open(os.path.join(OUT, "composition_check.json"), "w") as f:
    json.dump(comp, f, indent=2)


# =============================================================== summary
print("=" * 82)
print(f"total keys = {N_KEYS}   ooo-eligible (m>=2) = {N_ELIG_OOO} "
      f"({N_ELIG_OOO/N_KEYS:.4f})   dup-eligible (current is c/u) = {N_ELIG_DUP} "
      f"({N_ELIG_DUP/N_KEYS:.4f})")
print("=" * 82)
print("\nOOO (one-line summary per operating point):")
for rate, s in ooo_summary.items():
    print(f"  ooo={rate}: keys={s['total_keys']} eligible={s['eligible_keys']} "
          f"measured_viol_rate={s['measured_violation_rate']:.4f} ({s['n_violations']}) | "
          f"fired={s['total_transpositions_fired']} flipped_argmax={s['fired_that_flipped_argmax']} "
          f"invisible={s['fired_invisible']} -> fraction_flipping="
          f"{s['fraction_fired_flipping_argmax']:.4f}")
print("\nDUP (one-line summary per operating point):")
for rate, s in dup_summary.items():
    print(f"  dup={rate}: keys={s['total_keys']} eligible={s['eligible_keys']} "
          f"({s['eligible_fraction']:.4f}) measured_viol_rate={s['measured_violation_rate']:.4f} "
          f"({s['n_violations']}) | 0.85*p predicted={s['predicted_0p85_times_p']:.4f} | "
          f"any_dup_injected={s['keys_with_any_dup_injected']}")
print("\nCOMPOSITION (skew=1500 + ooo=0.10):")
for fmt, d in comp["formats"].items():
    print(f"  {fmt:8} measured={d['measured_violation_rate']:.4f}  "
          f"predicted(product-law)={d['predicted_product_law']:.4f}  "
          f"abs_err={d['abs_error_measured_minus_predicted']:+.4f}  "
          f"[P_skew={d['components']['clock_skew_1500']['standalone_P_viol']}, "
          f"P_ooo={d['components']['ooo_0.10']['standalone_P_viol']}]")
print("\nwrote: ooo_perkey.csv  dup_perkey.csv  composition_check.json")
