# Out-of-order & duplicate injection in `mor_harness` — exact stochastic model

Ground truth for the §6 sensitivity study (seed 101), read from the harness code in
`cost-study/src` and **validated against the stored engine-measured
aggregates** (`sensitivity/results/sensitivity.jsonl`). No harness code was modified. The
reconstruction matches the engine exactly on all 5 config points (2 ooo + 3 dup),
so it is the ground truth, not a proxy.

Reproduce with:

```
python3 reproduce_ooo_dup.py      # rewrites data/*.csv and prints VALIDATED lines
```

## 0. Where the knobs live

Contrary to the `imperfections.py` docstring, ooo/dup do **not** act on the event
stream. They act on the **checkpoint (= sequence-number) assignment** in
`batching._perturb` (`src/mor_harness/batching.py:55–76`). A checkpoint index *is* the
Iceberg snapshot sequence number (`iceberg_driver.py:4–5`: "data + equality delete
share the snapshot's sequence number"). The clean base assignment (`_assign_clean`,
lines 50–52) gives key *k*'s j-th change (0-indexed) sequence number `j+1`; version
index `m-1` (the current, max-lsn version) gets the top seq `m`.

**Dead parameter:** `config.ooo_window` (config.py:38, "max displacement distance") is
**referenced nowhere** in `src/`, `studies/`, or `tests/` — its only occurrence is its
own definition. The mechanism is governed **solely by `ooo_rate`**. The
"adjacent transposition, not windowed displacement" conclusion is confirmed at the code
level: there is no window.

---

## 1. The exact reordering mechanism

```python
# batching._perturb, lines 63–65
for j in range(1, m):
    if rng_ooo.random() < config.ooo_rate:
        ck[j - 1], ck[j] = ck[j], ck[j - 1]
```

Per key with `m` competitive change events (indices `0..m-1`, lsns `L_0<…<L_{m-1}`),
starting from clean assignment `σ0(j)=j+1`:

- **Adjacent transposition of sequence numbers.** For each adjacent version-pair
  `(j-1, j)`, `j = 1..m-1`, an **independent** draw `u_j = rng_ooo.random()` swaps the
  two versions' *sequence numbers* iff `u_j < ooo_rate`. Swap distance = exactly 1 in
  version-index space; never a multi-slot displacement.
- **Not "a fraction of pairs."** Each pair is selected **independently** with
  probability `p = ooo_rate` (Bernoulli per pair). `m-1` uniforms are drawn per key, one
  per pair, **always** (the `random()` call is unconditional). The uniform sequence is
  identical across `ooo_rate` values (same pristine `"ooo"` child of `SeededRng(101)`),
  so **fired sets are nested in `p`**: a pair firing at 0.25 also fires at 0.50.
- **In-place, left-to-right.** The swap mutates `ck` before the next pair is examined —
  this is what admits cascades (§2).

**Mathematical form.** Independent bits `s_j = 1[u_j < p] ~ Bernoulli(p)`, `j=1..m-1`,
applied as a single left-to-right bubble pass of conditional adjacent swaps. Result: a
permutation `π` of `{1,…,m}` over the `m` versions.

---

## 2. Per-key effect on the sequence

For a key with versions at `L_1<…<L_m`:

- **Which pairs can transpose:** only the `m-1` *adjacent* pairs `(j-1,j)`. Non-adjacent
  versions never swap directly.
- **Independence:** the decisions `s_1,…,s_{m-1}` are mutually independent Bernoulli(p).
  (The *effects* share array slots, so displacements are coupled even though the
  *decisions* are not.)
- **Cascade — asymmetric, verified empirically over all 1260 keys:**
  - **Rightward: multi-position (cascade).** A run of consecutive fired pairs
    left-cyclically rotates that block; the value at the run's left edge bubbles right
    across the whole run. Measured max rightward move = **+6** (ooo=0.25) / **+7**
    (ooo=0.50).
  - **Leftward: at most one position.** A version can move left by ≤1 (only its own
    pair, processed before any pair further left could touch it again). Measured max
    leftward move = **−1** in both sweeps — a hard bound.

Example (`data/ooo_perkey_050.csv`, key 358, `ops=uuuuuuuud`): `clean=[1..9]`, fired
pairs `{3,4,6}` → `final=[1,2,4,5,3,7,6,8,9]` — seq value 3 bubbled right two slots
(index 2→4); nothing moved left more than one.

---

## 3. The violation event — Iceberg **and** Delta

Both engines reduce to the **same rule** (verified: identical `stale/miss/ghost` counts
for the two formats):

> **The version holding the maximum sequence number determines the current
> materialized row.**

- **Iceberg** (`iceberg_driver.py:4–11`): every c/u/d writes an equality delete at its
  own seq; the max-seq delete (= `m`) suppresses all data at seq `< m`; only data at seq
  `m` survives.
- **Delta** (`delta_driver.py:82–93`): checkpoints applied in ascending-seq order as
  MERGE-upsert (+ DELETE for real `d`) → last commit (highest seq) wins. Source rows
  deduped per key, so **Delta structurally cannot show DUPLICATE** — it is the control
  for the dup knob.

**Reduction to a single Bernoulli.** The top sequence value `m` starts on the current
version (index `m-1`) and can be displaced **only by the last pair** `s_{m-1}`
(leftward-≤1 bound). Therefore:

> **A key violates ⟺ `s_{m-1} = 1`** (the last adjacent pair, between the two
> highest-lsn versions, fires). For `m=1` keys, no pair exists → never violates.

Verified exactly: `#violations == #{keys with last_pair_fired}` = **272** (ooo=0.25) and
**533** (ooo=0.50), with **0** keys where the top seq moved without the last pair firing.
Bits `s_1..s_{m-2}` shuffle the losers among themselves and are irrelevant to the verdict.

**It is a mix of three verdict types; the type is fully determined by the key's tail op**
(mapping verified with zero counterexamples):

| tail op | key class | violation verdict | mechanism |
|---|---|---|---|
| `u` | update-tail | **STALE_WINS** | stale update takes seq `m`, wins |
| `c` (preceded by `d`) | reinsert-tail | **MISSING_CURRENT** | last pair swaps `d↔c`; the delete takes seq `m` → 0 rows |
| `d` | delete-tail | **GHOST** | delete loses seq `m`; a data version resurrects |

**What the oracle counts for the measured rates:** `STALE_WINS + MISSING_CURRENT +
GHOST` (no DUPLICATE from ooo). Exact split, both formats:

| ooo | STALE_WINS | MISSING_CURRENT | GHOST | total | rate |
|---|---|---|---|---|---|
| 0.25 | 206 | 16 | 50 | 272 | 0.2159 |
| 0.50 | 405 | 40 | 88 | 533 | 0.4230 |

> **Reporting caveat (ground truth):** the Delta records store `n_ghost=0`, but that is
> an artifact — `runner.py:83,86` merges an empty `AgreementReport` (whose `n_ghost=0`)
> *over* the oracle tally, while `n_violations`/`violation_rate` come from
> `check.tally(oracle)` and still include the 50/88 ghosts. The oracle counts ghosts
> identically for Delta and Iceberg; only the printed `n_ghost` field is suppressed for
> Delta.

**Eligible-key population** (denominators for the derivation), from the seed-101 stream:

- Total keys: **1260** (1200 base @ `keys_sampled=1.0` + 60 inserted @ `insert_rate=0.05`).
- ooo-eligible (`m ≥ 2`): **1047** (fraction 0.8310). Non-eligible: 213 single-update
  keys (`m=1`, all `u`).
- By tail-op among the 1047: **u=793** (STALE pool), **d=185** (GHOST pool), **c=69**
  (MISS pool).
- Per-key violation law: each eligible key violates iff its own `s_{m-1}` fires ⇒
  `E[violations] = p · 1047`; per bucket `793p / 69p / 185p`. Realized counts
  (272; 206/16/50 and 533; 405/40/88) are the exact seed-101 draws, within binomial
  noise of these means.

---

## 4. Realized per-key data (out-of-order)

`data/ooo_perkey_025.csv`, `data/ooo_perkey_050.csv` — 1260 rows each. Columns:

`key_id, m, competitive_lsns, competitive_ops, clean_seqs, final_seqs, n_pairs,
fired_pairs, last_pair_uniform, last_pair_fired, tail_op, current_lsn, winner_vidx,
winner_seq, winner_lsn, winner_op, present, violated, verdict`

Representative rows (ooo=0.50):

```
key=  49 m=7  ops=uuuuuuu  final_seqs=[1;3;2;4;5;7;6] fired=[2;6] last_pair_fired=True  => STALE_WINS
key=1161 m=5  ops=uuuud    final_seqs=[2;3;4;5;1]     fired=[1;2;3;4] last_pair=True     => GHOST
key= 527 m=50 ops=…udc     last pair fires -> delete takes seq 50                        => MISSING_CURRENT
key=1253 m=4  ops=cuuu     final_seqs=[2;3;1;4]       fired=[1;2] last_pair_fired=False  => MATCH (cascade, current keeps top)
```

---

## 5. Duplicate case

**Equal-seq copy** (`batching.py:70–75, 92–93`): a version with `op ∈ {c,u}` selected
with independent prob `dup_rate` is appended **twice to its own checkpoint** → two rows
at the **same sequence number**; the equality delete at that seq cannot suppress same-seq
data (`iceberg_driver.py:11`). `rng_dup.random()` is drawn **only for c/u versions**
(short-circuit AND).

Under the clean assignment (ooo=0), only the max-seq (= current, index `m-1`) version
survives, so:

> **A key shows DUPLICATE ⟺ its current version is c/u AND its dup bit fired.**
> ⇒ `E[dups] = dup_rate · N_elig`, `N_elig` = keys whose current (max-lsn) version is c/u
> = non-delete-tail keys.

Realized (validated exactly against stored):

| dup_rate | DUPLICATE keys (recon = engine) | rate | eligible `N_elig` |
|---|---|---|---|
| 0.05 | 62 | 0.0492 | 1075 |
| 0.15 | 179 | 0.1421 | 1075 |
| 0.30 | 344 | 0.2730 | 1075 |

**`eligible_fraction = 1075/1260 = 0.8532`** (= 1006 update-tail + 69 reinsert-tail; the
185 delete-tail keys are ineligible). So `f(dup) ≈ dup_rate × 0.8532`, near-linear, with
the realized counts being the exact seed-101 binomial draws about `1075·dup_rate`
(53.8 / 161.2 / 322.5).

`data/dup_perkey_{005,015,030}.csv` — 1260 rows each. Columns:

`key_id, m, competitive_ops, tail_op, n_cu_versions, current_op, current_is_cu,
current_dup_uniform, current_dup_fired, n_flags_fired, duplicate, verdict`

---

## Model summary (for the derivation)

- **ooo** = independent per-pair Bernoulli(`p`) adjacent transpositions of the seq
  assignment (in-place left→right bubble pass; rightward cascade, leftward ≤1). The
  current-row verdict for both Iceberg and Delta depends **only** on the last bit
  `s_{m-1}`, so per-key `P(violate)=p` for `m≥2` and 0 otherwise, with the verdict type
  pinned by tail op (u→STALE, reinsert-c→MISSING, d→GHOST).
- **dup** = independent per-version Bernoulli(`dup_rate`) equal-seq copy, visible only on
  the current c/u version, so per-key `P(duplicate)=dup_rate·1[current is c/u]`.

## Files

```
GROUND_TRUTH.md            this document
reproduce_ooo_dup.py       read-only reproduction (imports mor_harness, mutates nothing)
data/ooo_perkey_025.csv    1260 rows, validated (272 viol = 206/16/50)
data/ooo_perkey_050.csv    1260 rows, validated (533 viol = 405/40/88)
data/dup_perkey_005.csv    1260 rows, validated (62 dup)
data/dup_perkey_015.csv    1260 rows, validated (179 dup)
data/dup_perkey_030.csv    1260 rows, validated (344 dup)
```

Requires the harness `src` (`cost-study/src`) on the path (the script adds the sibling
`../../cost-study/src` automatically) and Python 3 (stdlib only; no numpy/Spark —
`base_keys=1200` uses the synthetic base).
