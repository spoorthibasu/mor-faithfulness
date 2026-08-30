# MOR harness: imperfection generator ground truth + seed-101 clock-skew data

Reference for deriving the §6 sensitivity violation rates against the *real* generator.
Inspection + read-only extraction only; no `mor_harness` code was modified. All numbers
are produced by the shipped generator on `SeededRng(101)`.

Companion data files (same directory):
- `seed101_perkey.csv` — per-key competitive-version structure + gaps (1260 rows)
- `seed101_skew_sample.csv` — realized standard-normal draws + real perturbations (50 keys)
- `seed101_summary.json` — machine-readable summary
- `extract_skew.py` / `validate_rates.py` — the read-only extractors (regenerate everything)

---

## 0. Authoritative source files

Under the harness sources (`cost-study/src/mor_harness/`):
- `stream.py` — synthesizes per-key histories, assigns global `lsn`, derives `ts_ms`, computes ground truth.
- `imperfections.py` — **clock skew only** (perturbs `ts_ms`).
- `batching.py` — **out-of-order, duplicate, schema-change** (perturb the physical checkpoint/sequence assignment, not payloads).
- `check.py` — the oracle that defines the measured violation; `hudi_predictions` = precombine argmax.
- `rng.py` — one independent `random.Random` child per knob (`stream, skew, ooo, dup, schema`).

## 1. The exact §6 sensitivity configuration

From `sensitivity/run_sensitivity.py:33-35`, OFAT from an all-zero baseline, `enforcement_mode="unsafe"`:

```
base_keys=1200, keys_sampled=1.0, versions_per_key_mean=4,
op_mix=(0.8, 0.15, 0.05), insert_rate=0.05 (default),
ts_step_ms=1, base_ts_ms=1_700_000_000_000, seed=101   ->  1260 keys/run
SKEW=[0,400,1500,6000]  OOO=[0,0.05,0.10,0.25,0.50]  DUP=[0,0.05,0.15,0.30]
```

`ts_step_ms=1` means `ts_ms = base_ts_ms + lsn`, so **one unit of `ts_ms` = one unit of `lsn`**;
clock-skew sigma (ms) is directly comparable to lsn gaps with no conversion.

## 2. Shared substrate (common to all three imperfections)

### 2.1 Per-key logical version structure (`stream.py:37-118`)
- Each of the 1200 base keys is seeded with one `READ`; with prob `keys_sampled` (=1.0, so all)
  it receives `nver` `UPDATE`s then a terminal op.
- `insert_rate` adds `floor(1200*0.05)=60` brand-new keys, seeded with a `CREATE` (no READ), then updates + terminal.
- Update count (`_n_updates`, `stream.py:31-34`): `nver = 1 + floor(Exp(mean=4))`, capped at 50.
  `floor(Exp)` is exactly **Geometric**: `nver = 1 + G`, `G ~ Geom(p)`, `p = 1 - e^{-1/4} ≈ 0.2212`,
  `E[nver] ≈ 4.52`. **Geometric, not Poisson, not fixed.** The knob is the mean of the *update* count.
- Terminal op (`stream.py:70-75`), `r ~ U[0,1)`: `r<0.15` delete-tail; `0.15≤r<0.20` reinsert-tail
  (DELETE then CREATE); else update-tail. Realized split 0.80 / 0.15 / 0.05.

### 2.2 Ordering values and spacing (`stream.py:82-118`)
- `lsn` is a **global** monotone counter: base READs get `lsn 1..1200`; all non-READ events across
  **all keys** are merged into `lsn 1201..(1200+N_c)` by a **length-weighted random merge** that
  preserves per-key order. Picking a queue with probability proportional to remaining length is
  exactly a **uniformly random interleaving** subject to per-key order.
- `ts_ms = base_ts_ms + lsn * ts_step_ms` (slope 1 here). True ordering value is strictly monotone in `lsn`.
- **There is no fixed inter-version spacing.** `ts_step_ms` is the spacing between *globally consecutive*
  lsns (the minimum gap), not the per-key gap. Within a key, adjacent-version gap
  `Δ_i = L_{i+1} - L_i` is a **random gap** (approximately memoryless/exponential from the uniform
  merge), with `mean ≈ N_c / m` where `m` = the key's competitive version count. Small gaps are common.
  This ratio (noise / gap) is what drives the violation probability.

### 2.3 What "a violation" means (`check.py:92-128`)
Measured `violation_rate = n_violations / n_keys`, a **per-key final-state verdict** on the
materialized MOR current view vs ground truth: `MISSING_CURRENT` (0 rows, should be present),
`DUPLICATE` (>=2 rows), `STALE_WINS` (1 row, wrong version), `GHOST` (rows present, should be absent).
Denominator is **all 1260 keys**, including keys immune to a given knob. This is the
**current-fails-to-be-unique-maximum (final-state)** criterion, NOT a per-update/prefix inversion count.

### 2.4 RNG independence + pairing fact (`rng.py`)
Each knob draws from its own child seeded up front, so the **stream structure is identical across a
knob sweep at fixed seed**; only the perturbation draws change. For skew, `gauss(0,σ)=σ·z` with the
**same** standard-normal sequence for a fixed child and skew-independent event count, so
**σ=400/1500/6000 reuse one realized (gap, z) sample scaled by σ** (verified below).

## 3. The three imperfections (mechanisms)

### A. Clock skew (`imperfections.py:20-33`) — primary format: Hudi (`precombine=ts_ms`)
Per event, in delivery/lsn order (per-event, iid):
```
delta = clamp(gauss(0, sigma), -4*sigma, +4*sigma);  e.ts_ms = int(e.ts_ms + delta)
```
Additive **Gaussian** noise on `ts_ms`, **sd = `clock_skew_ms`** (a std dev, not a max/uniform),
clamped **±4σ** (clamp mass ≈ 6.3e-5, negligible). `lsn`/truth untouched.
Perturbed ordering value `\tilde t_v = floor(base + L_v + σ·w_v)`, `w_v = clamp(z_v, ±4)`, `z_v ~ N(0,1)` iid.
**Violation event** (`hudi_predictions` + oracle): `STALE_WINS` iff `argmax_v \tilde t_v != argmax_v L_v`,
tie-break `(pc, lsn)` toward faithful. Delete-tail keys predicted `ABSENT`, immune, still in denominator.

### B. Out-of-order (`batching.py:55-65`) — primary: Iceberg (seq inversion), also Delta
Acts on the **checkpoint/sequence assignment**, not `ts_ms` or delivery stream. Clean assign is
`ck=[1..m]`; then for each **adjacent** version pair `(j-1, j)`, iid with prob `ooo_rate`, **swap their
sequence numbers**. It is a **reordering (adjacent transposition), not a discrete skew**; sequential
in-place so consecutive triggers cascade (displacement > 1). `ooo_window` is **dead** (defined in config,
referenced nowhere). Violation: stale version left at highest surviving seq -> STALE_WINS / MISSING /
(Iceberg only) GHOST. Pure ooo cannot make DUPLICATE. Hudi immune (keeps clean assign).

### C. Duplicate/retry (`batching.py:71-93`) — primary: Iceberg (equal-seq dup)
Per CREATE/UPDATE version, iid with prob `dup_rate`, the data row is written **twice into the same
checkpoint = same sequence number** (`data_by_ck[c].append(e.after)` twice). A duplicate is an
**exact repeat at the same ordering value (equal seq), NOT a later re-delivery**. Because every
checkpoint that writes a key also equality-deletes it, only the highest-seq checkpoint's data survives,
so a duplicate becomes a `DUPLICATE` violation iff the duplicated version is the surviving current one.
Hudi/Delta immune.

## 4. Design-vs-code discrepancies (trust the code)

`DESIGN.md §3.4` misdescribes the implementation in four ways that would corrupt a memo-based derivation:
1. **Clamp**: memo says `±clock_skew_ms` (±σ); code clamps **±4σ** (`imperfections.py:27`). Treat as effectively unclamped Gaussian.
2. **Out-of-order shape**: memo says "displace within bounded window `W`"; code does **adjacent seq transposition**, and `ooo_window` is **unused**.
3. **Out-of-order object**: memo says "arrival order"; code perturbs the **commit/sequence assignment** in `batching.py`.
4. **Duplicate**: memo says "re-emit later in the delivery stream"; code writes a **second copy at the same seq** (`batching.py:93`).

## 5. Seed-101 realized data (the derivation set)

Stream length **7407 events**; base READs `lsn 1..1200`, change block `lsn 1201..7407`, so **N_c = 6207**.

### 5.1 Eligibility / structure
| dimension | value |
|---|---|
| keys | 1260 (base 1200, insert 60) |
| terminal_type | update-tail 1006 (0.798), reinsert-tail 69 (0.055), delete-tail 185 (0.147) |
| **eligible (not delete-tail)** | **1075 / 1260 = 0.8532** |
| competitive versions/key | min 2, mean 5.68, max 50 (cap binds for a few keys) |

Delete-tail keys (185) are the immune set (truth=None -> predicted ABSENT -> never STALE_WINS), but
remain in the rate denominator (1260). The READ version of each base key sits at `lsn ≤ 1200`, far
below its update pack, so it is a candidate but effectively never the closest competitor.

### 5.2 Closest-competitor gap distribution (`min_positive_gap = L_max - L_secondhighest`, over 1075 eligible)
| min | p05 | p10 | p25 | median | mean | p75 | p90 | p95 | max |
|---|---|---|---|---|---|---|---|---|---|
| 6 | 69 | 138 | 433 | 1166 | 1722 | 2517 | 4349 | 5405 | 7127 |

Histogram (lsn units = ms):
```
   0-49 :  38     400-799  : 171    3200-6399 : 179
  50-99 :  43     800-1599 : 232    >=6400    :  17
 100-199:  63    1600-3199 : 229
 200-399: 103
```
Heavy-tailed, small-gap-rich (memoryless from the uniform merge), NOT a fixed Δ. ~13% of eligible keys
have a closest gap under 200. The full per-competitor gaps `δ_v` are recoverable from the sorted
`competitive_lsns` column in `seed101_perkey.csv`.

Coarse shape anchor (fraction with gap < √2·σ, the overtake band):
| σ | frac gap < √2σ | observed §6 rate |
|---|---|---|
| 400 | 0.308 | 0.106 |
| 1500 | 0.702 | 0.310 |
| 6000 | 1.000 | 0.536 |
(Band over-counts the rate; the exact rate integrates `1 - Φ(δ_v/(σ√2))` over competitors, closest term dominant.)

### 5.3 Scaling property (verified): one standard-normal sample scaled by σ
`real_deltaσ` (the actual integer perturbation the harness applied) vs `σ·w` over all 7407 events:
| σ | max \|real_delta − σ·w\| | mean \|·\| |
|---|---|---|
| 400 | 0.9998 | 0.49 |
| 1500 | 0.9997 | 0.50 |
| 6000 | 0.9996 | 0.51 |
Deviation < 1 ms is pure `int()` truncation. `w_v` is identical across σ; only σ scales. Overtake by
hand: version `v` beats current `v*` iff `w_v − w_{v*} > δ_v / σ`. Sample (key 1): READ `w=−1.208` gives
real deltas −484 / −1812 / −7247 (ratios ×3.75, ×15).

## 6. Validation (rate reproduction)

Replaying the realized perturbed timestamps through Hudi's precombine-argmax reproduces §6 exactly, and
an independent argmax equals the harness `hudi_predictions` to the last digit (`validate_rates.py`):

| σ | independent argmax | harness `hudi_predictions` | §6 report |
|---|---|---|---|
| 0 | 0.0000 | 0.0000 | 0.000 |
| 400 | 0.1063 | 0.1063 | 0.106 |
| 1500 | 0.3103 | 0.3103 | 0.310 |
| 6000 | 0.5357 | 0.5357 | 0.536 |

So `seed101_perkey.csv` + `seed101_skew_sample.csv` are provably the data behind those three numbers.

## 7. `seed101_perkey.csv` schema
`key_id, origin{base|insert}, terminal_type{update-tail|reinsert-tail|delete-tail}, eligible{0|1},
n_competitive, competitive_lsns (";"-sep, sorted asc), current_lsn, min_positive_gap`

The rate is `Σ_eligible 1[argmax_v(L_v + σ·w_v) ≠ argmax_v L_v] / 1260`, with `w_v` iid clamped-standard-
normal (same draws for every σ) and `L_v` the `competitive_lsns`. The gaps are deterministic realized
structure; only σ scales the noise.
