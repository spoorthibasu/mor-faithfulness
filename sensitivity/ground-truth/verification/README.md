# Verification export (read-only)

Realized per-key ground truth behind the ooo/dup derivation, for independent checking
before the paper. Regenerate with `python3 export_verification.py` (imports the harness
from `cost-study/src`, mutates nothing; asserts every reconstructed aggregate
equals the engine-measured value in `sensitivity/results/sensitivity.jsonl`).

Stream: seed 101, §6 BASE config (SF1, base_keys=1200, keys_sampled=1.0,
versions_per_key_mean=4, op_mix=(0.8,0.15,0.05), enforcement=unsafe).
**Total keys = 1260** (1200 base + 60 inserted). **ooo-eligible (m≥2) = 1047** (0.8310).
**dup-eligible (current version is c/u) = 1075** (0.8532).

## ooo_perkey.csv  (one row per key × ooo_rate ∈ {0.25, 0.50})

Sequence model: the key's `m` competitive change events, in lsn order, get clean seqs
`1..m` (version index `j` → seq `j+1`); the current (max-lsn) version `j=m-1` gets the
top seq `m`. `ooo` fires an independent Bernoulli(ooo_rate) adjacent transposition at each
pair `(j-1,j)`, left-to-right, in place. The materialized current row (identical for
Iceberg's max-seq equality-delete survivor and Delta's last-commit-wins) is the version
holding the max seq — the **argmax**.

| column | meaning |
|---|---|
| `competitive_lsns`, `competitive_ops` | the m competitive versions (lsn order) |
| `clean_seqs`, `final_seqs` | seq assignment before / after the ooo pass |
| `fired_pairs`, `n_fired` | which adjacent pairs `(j-1,j)` actually transposed |
| `fired_detail` | per firing, `"<pair_j>:<descent\|invisible>"` |
| `n_descent`, `n_invisible` | descents = firings that flipped the argmax; invisible = firings that only permuted the losers |
| `last_pair_fired` | did the top pair `(m-2,m-1)` fire |
| `violated`, `verdict` | STALE_WINS / MISSING_CURRENT / GHOST / MATCH |

**Classification.** Per firing, `descent` ⟺ the swap moved value `m` (the argmax) to the
other version ⟺ it flipped which version is current. Everything else is an `invisible`
ascent among the losers. Because the top seq `m` sits on the current version until the
last pair (leftward displacement is ≤1), **only a last-pair firing ever flips the argmax**,
so per key `n_descent ∈ {0,1}` and equals `last_pair_fired` and equals `violated`.

### The ½-factor check (realized)

> "of all transpositions that fired, what fraction actually flipped the argmax"

| ooo_rate | fired | flipped argmax (=violations) | invisible | **fraction flipping** |
|---|---|---|---|---|
| 0.25 | 1201 | 272 | 929 | **0.2265** |
| 0.50 | 2440 | 533 | 1907 | **0.2184** |

The realized fraction is **≈ 0.22, not ½**. Structurally it is
`N_eligible / Σ(m−1) = 1047 / ≈4800 ≈ 0.218`, rate-independent in expectation (the small
0.25↔0.50 gap is finite-sample). A fired transposition flips the argmax only when it is the
top pair; the other `m−2` pairs shuffle non-winning versions and are invisible. If the
derivation carries a ½ coefficient for the ooo mechanism, this is the number to reconcile
it against. Per-key violation probability itself is `p` for eligible keys (each eligible
key's single top-pair Bernoulli), giving aggregate `≈ 0.831·p`.

## dup_perkey.csv  (one row per key × dup_rate ∈ {0.05, 0.15, 0.30})

A duplicate is an equal-seq re-write of a c/u version in its own checkpoint. Under the
clean assignment only the max-seq (current) version survives, so a duplicate is visible
only when it lands on the current version.

| column | meaning |
|---|---|
| `eligible` | current (max-lsn) version is c/u (dup-visible) |
| `dup_injected_any`, `n_versions_duplicated` | any c/u version got a dup copy |
| `dup_on_current` / `caused_violation` | the visible case → DUPLICATE |

**0.85 × p scaling (realized).** eligible_fraction = 1075/1260 = **0.8532**.

| dup_rate | DUPLICATE (measured=recon) | rate | 0.8532·p |
|---|---|---|---|
| 0.05 | 62 | 0.0492 | 0.0427 |
| 0.15 | 179 | 0.1421 | 0.1280 |
| 0.30 | 344 | 0.2730 | 0.2560 |

Realized runs slightly above `0.8532·p` (seed-101 binomial draw about mean `1075·p` =
53.8 / 161.2 / 322.5). `dup_injected_any` (270 / 582 / 857) counts keys with a dup on *any*
version, including invisible ones on non-current versions — the gap to the violation count
is exactly the invisible-duplicate mass.

## composition_check.json  (skew=1500 + ooo=0.10, not used to fit anything)

Product law `P = 1 − (1−P_skew)(1−P_ooo)`, components = standalone single-mechanism rates.
The mechanisms are orthogonal per format (Iceberg/Delta order by lsn → skew-inert;
Hudi precombines on ts_ms → ooo-inert), so the combined measured rate equals the product
prediction **exactly** (abs_err = 0.0 for all three formats): iceberg/delta 0.089683,
hudi 0.310317.

This is **independence where isolable**, not a general-independence claim: the product holds
exactly only because one factor is always 1 (each format is inert to one of the two knobs at
this point), so what is confirmed is that adding a mechanism a format cannot see does not
change its rate. It is **not** evidence that two *co-active* mechanisms on the same format
compose multiplicatively; this point does not test that, and the paper does not claim it.
